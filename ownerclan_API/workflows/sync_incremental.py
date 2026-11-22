from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from product_data_collector.persistence.postgres_storage import require_product_save, save_products_with_raw_samples_if_enabled

from product_data_collector.common.collection_state import collection_outcome, next_page_cursor, validate_limits

from ..api.client import OwnerclanGraphQLError, OwnerclanHttpError
from ..config import OwnerclanConfig, find_project_root, load_config
from ..api.factory import make_client
from ..services.logging_config import configure_logging
from ..services.normalization import extract_connection_items, normalize_item
from ..api.queries import all_items_query, item_histories_query
from ..persistence.storage import (
    load_state,
    save_state,
)
from ..services.time_utils import now_iso, to_unix_millis


LOGGER = logging.getLogger("ownerclan_API.workflows.sync_incremental")


def sync_incremental(project_root, config, **kwargs):
    """Daily default: source histories only; detail completion is independent."""
    from .history_sync import collect_history
    return collect_history(project_root, config, **kwargs)


def sync_all_items_legacy(
    project_root: Path,
    config: OwnerclanConfig,
    *,
    page_limit: int | None = None,
    item_limit: int | None = None,
    dry_run: bool = False,
    client: Any | None = None,
) -> dict[str, int]:
    validate_limits(page_limit=page_limit, item_limit=item_limit)
    client = client or make_client(project_root, config)
    collected_at = now_iso(config.timezone)
    state_path = config.output.state_dir / "legacy-all-items-state.json"
    state = load_state(state_path)
    pending = state.get("pendingIncremental") or {}
    date_to_iso = str(pending.get("dateTo") or collected_at)
    date_from_iso = str(pending.get("dateFrom") or _incremental_from(state.get("lastSuccessfulItemSyncAt"), config))
    collected_at = str(pending.get("runCollectedAt") or collected_at)
    date_from = to_unix_millis(date_from_iso)
    date_to = to_unix_millis(date_to_iso)
    after = pending.get("after")
    completed = bool(pending.get("itemsCompleted"))
    products: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    rate_limit_failures = 0
    seen_cursors: set[str] = set()
    pages = 0
    pause_reason = None

    def checkpoint_page(cursor, items_completed=False):
        state["pendingIncremental"] = {
            "runCollectedAt": collected_at, "dateFrom": date_from_iso,
            "dateTo": date_to_iso, "after": cursor, "itemsCompleted": items_completed,
        }
        save_state(state_path, state)

    if not dry_run:
        checkpoint_page(after, completed)
    try:
        while not completed:
            try:
                data = client.graphql(all_items_query(
                    first=config.incremental.page_size, sort_by="dateAsc",
                    after=after, date_from=date_from, date_to=date_to,
                ))
            except OwnerclanGraphQLError as exc:
                if not exc.looks_like_unknown_field() or "allitems" in str(exc).lower():
                    raise
                data = client.graphql(all_items_query(
                    first=config.incremental.page_size, sort_by="dateAsc",
                    after=after, date_from=date_from, date_to=date_to, minimal=True,
                ))
            items, page_info = extract_connection_items(data, "allItems")
            next_cursor = next_page_cursor(page_info, after, seen_cursors)
            pages += 1
            selected = items
            if item_limit is not None:
                selected = items[:max(item_limit - len(products), 0)]
            if len(selected) < len(items):
                if not selected or not selected[-1].get("_cursor"):
                    raise ValueError("Cannot resume a partially consumed page without an edge cursor")
                next_cursor = str(selected[-1]["_cursor"])
                if next_cursor == after or next_cursor in seen_cursors:
                    raise ValueError("Repeated partial-page cursor")
            if any(not item.get("key") for item in selected):
                raise ValueError("Ownerclan item missing key")
            page_products = [normalize_item(item, collected_at) for item in selected]
            if not dry_run:
                result = save_products_with_raw_samples_if_enabled(
                    project_root=project_root, platform="ownerclan",
                    collected_at=collected_at, products=page_products,
                    raw_sample_limit=config.output.raw_sample_limit, logger=LOGGER,
                )
                require_product_save(result, page_products)
                checkpoint_page(next_cursor, next_cursor is None)
            products.extend(page_products)
            completed = next_cursor is None
            if completed:
                break
            after = next_cursor
            seen_cursors.add(next_cursor)
            if item_limit is not None and len(products) >= item_limit:
                pause_reason = "item_limit_reached"
                break
            if page_limit is not None and pages >= page_limit:
                pause_reason = "page_limit_reached"
                break
    except Exception as exc:
        if _is_rate_limit_exception(exc):
            rate_limit_failures += 1
        failures.append({"error": str(exc), "stage": "allItems", "dateFrom": date_from, "dateTo": date_to})
        LOGGER.error("ownerclan incremental sync failed error=%s", exc)

    histories = []
    if completed and not failures and config.incremental.include_item_histories:
        try:
            histories = fetch_item_histories(client, config, date_from, date_to)
        except Exception as exc:
            failures.append({"error": str(exc), "stage": "itemHistories", "dateFrom": date_from, "dateTo": date_to})
            LOGGER.error("ownerclan itemHistories sync failed error=%s", exc)

    state_updated = False
    if completed and not failures and not dry_run:
        state["lastSuccessfulItemSyncAt"] = date_to_iso
        state.pop("pendingIncremental", None)
        save_state(state_path, state)
        state_updated = True

    return {
        **collection_outcome(failed=bool(failures), pause_reason=pause_reason),
        "pageCount": pages,
        "collectedProductCount": len(products),
        "successCount": len(products),
        "historyCount": len(histories),
        "failureCount": len(failures),
        "rateLimitFailureCount": rate_limit_failures,
        "stateUpdated": int(state_updated),
    }


def _is_rate_limit_exception(exc: Exception) -> bool:
    if isinstance(exc, OwnerclanHttpError) and exc.status_code == 429:
        return True
    if isinstance(exc, OwnerclanGraphQLError) and exc.is_retryable_rate_limit():
        return True
    text = str(exc).lower()
    return any(term in text for term in ("http 429", "too many requests", "rate limit", "quota"))


def fetch_item_histories(client: Any, config: OwnerclanConfig, date_from: int, date_to: int) -> list[dict[str, Any]]:
    histories: list[dict[str, Any]] = []
    after: str | None = None
    seen_cursors: set[str] = set()
    while True:
        data = client.graphql(item_histories_query(first=config.incremental.page_size, after=after, date_from=date_from, date_to=date_to))
        items, page_info = extract_connection_items(data, "itemHistories")
        histories.extend(items)
        next_cursor = next_page_cursor(page_info, after, seen_cursors)
        if next_cursor is None:
            break
        seen_cursors.add(str(next_cursor))
        after = str(next_cursor)
    return histories


def _incremental_from(last_success: Any, config: OwnerclanConfig) -> str:
    tz = ZoneInfo(config.timezone)
    if isinstance(last_success, str) and last_success:
        base = datetime.fromisoformat(last_success)
    else:
        base = datetime.now(tz) - timedelta(minutes=config.incremental.overlap_minutes)
    return (base - timedelta(minutes=config.incremental.overlap_minutes)).replace(microsecond=0).isoformat()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Persist Ownerclan itemHistories pages.")
    parser.add_argument("--config", default=None)
    parser.add_argument("--page-limit", type=int, default=None, help="Limit pages for a small real API test run.")
    parser.add_argument("--item-limit", type=int, default=None, help="Limit changed items for a small real API test run.")
    parser.add_argument("--dry-run", action="store_true", help="Call API but do not write state/output files.")
    args = parser.parse_args(argv)

    project_root = find_project_root(Path.cwd())
    config = load_config(
        Path(args.config) if args.config else project_root / "ownerclan_API" / "config" / "config.yaml",
        project_root,
    )
    configure_logging(config.output.log_dir)
    result = sync_incremental(project_root, config, page_limit=args.page_limit, item_limit=args.item_limit, dry_run=args.dry_run)
    print(result)
    return 1 if result["failureCount"] else 0


if __name__ == "__main__":
    sys.exit(main())
