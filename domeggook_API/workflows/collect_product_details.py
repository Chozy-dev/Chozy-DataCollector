from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from product_data_collector.common.collection_state import collection_outcome, item_list_hash, resume_index, validate_limits
from ..api.client import DomeggookApiError, DomeggookClient, create_domeggook_client
from ..config import DomeggookConfig, find_project_root, load_api_keys, load_config
from ..services.logging_config import configure_logging
from ..services.parsing import parse_detail_products
from product_data_collector.persistence.postgres_storage import discovered_product_ids, require_product_save, save_products_with_raw_samples_if_enabled
from .run_budget import RunBudget

from ..persistence.storage import (
    clear_state,
    load_state,
    save_state,
)
from ..services.time_utils import now_iso


LOGGER = logging.getLogger("domeggook_API.collect_product_details")
INVALID_JSON_RETRY_DELAYS_SECONDS = (10.0, 20.0, 30.0)


def collect_details(
    project_root: Path,
    config: DomeggookConfig,
    *,
    product_limit: int | None = None,
    product_ids: list[str] | None = None,
    state_filename: str = "detail-collection-state.json",
    deadline_monotonic: float | None = None,
    run_budget: RunBudget | None = None,
    dry_run: bool = False,
    client: DomeggookClient | None = None,
) -> dict[str, int]:
    validate_limits(product_limit=product_limit)
    data_dir = project_root / "domeggook_API" / "data"
    if product_ids is None:
        product_ids = discovered_product_ids(project_root=project_root, platform="domeggook")
    product_ids = list(dict.fromkeys(product_ids))

    if client is None:
        api_keys = load_api_keys(project_root)
        client = create_domeggook_client(api_keys, config)

    state_path = data_dir / "state" / state_filename
    state = load_state(state_path)
    collected_at = str(state.get("runCollectedAt") or now_iso(config.timezone))
    list_hash = item_list_hash(product_ids)
    start_index = resume_index(product_ids, state, list_hash)
    failures: list[dict[str, object]] = []
    raw_remaining = _raw_remaining(state, config.details.raw_sample_limit)
    collected_product_count = 0
    stopped_on_runtime_limit = False
    stopped_on_daily_request_limit = False

    end_index = len(product_ids) if product_limit is None else min(start_index + max(product_limit, 0), len(product_ids))
    if not dry_run:
        state.update(runCollectedAt=collected_at, trackedListHash=list_hash, nextIndex=start_index, rawRemaining=raw_remaining)
        save_state(state_path, state)

    for index in range(start_index, end_index, config.details.batch_size):
        if _deadline_reached(deadline_monotonic):
            stopped_on_runtime_limit = True
            if not dry_run:
                save_state(
                    state_path,
                    {
                        "runCollectedAt": collected_at,
                        "trackedListHash": list_hash,
                        "nextIndex": index,
                        "lastCompletedProductId": state.get("lastCompletedProductId"),
                        "rawRemaining": raw_remaining,
                    },
                )
            break
        if run_budget is not None and not run_budget.can_call():
            stopped_on_daily_request_limit = True
            if not dry_run:
                save_state(
                    state_path,
                    {
                        "runCollectedAt": collected_at,
                        "trackedListHash": list_hash,
                        "nextIndex": index,
                        "lastCompletedProductId": state.get("lastCompletedProductId"),
                        "rawRemaining": raw_remaining,
                    },
                )
            break
        batch = product_ids[index : min(index + config.details.batch_size, end_index)]
        try:
            payload = _get_item_view_with_invalid_json_retries(
                client,
                batch,
                run_budget=run_budget,
                deadline_monotonic=deadline_monotonic,
            )
            parsed_products, parsed_failures = parse_detail_products(payload, collected_at, raw_limit=raw_remaining)
            failures.extend(parsed_failures)
        except DomeggookApiError as exc:
            LOGGER.error("failed detail batch product_ids=%s error=%s", ",".join(batch), exc)
            failures.extend({"productId": product_id, "error": str(exc)} for product_id in batch)
            break
        except Exception as exc:
            LOGGER.exception("unexpected detail batch failure product_ids=%s error=%s", ",".join(batch), exc)
            failures.extend({"productId": product_id, "error": str(exc)} for product_id in batch)
            break

        unique_products: dict[str, dict[str, object]] = {}
        for product in parsed_products:
            product_id = product.get("productId")
            if product_id is not None:
                unique_products[str(product_id)] = product

        missing_ids = set(batch) - unique_products.keys() - {str(f.get("productId")) for f in parsed_failures}
        for product_id in sorted(missing_ids):
            failure = {"productId": product_id, "error": "Requested product missing from detail response"}
            parsed_failures.append(failure)
            failures.append(failure)
        if not dry_run and unique_products:
            # On failure the durable checkpoint still points at this request.
            _save_domeggook_detail_batch(
                project_root=project_root, config=config, collected_at=collected_at,
                products=unique_products, raw_limit=raw_remaining,
            )

        collected_product_count += len(unique_products)
        raw_remaining = max(raw_remaining - sum(p.get("raw") is not None for p in unique_products.values()), 0)
        if parsed_failures:
            break
        if not dry_run:
            next_index = index + len(batch)
            state = {
                "runCollectedAt": collected_at,
                "trackedListHash": list_hash,
                "nextIndex": next_index,
                "lastCompletedProductId": batch[-1],
                "rawRemaining": raw_remaining,
            }
            save_state(
                state_path,
                state,
            )

    pause_reason = (
        "runtime_limit_reached" if stopped_on_runtime_limit else
        "daily_request_limit_reached" if stopped_on_daily_request_limit else
        "item_limit_reached" if end_index < len(product_ids) else None
    )
    if not dry_run and not failures and not pause_reason:
        clear_state(state_path)

    return {
        **collection_outcome(failed=bool(failures), pause_reason=pause_reason),
        "trackedCount": len(product_ids),
        "collectedProductCount": collected_product_count,
        "successCount": collected_product_count,
        "failureCount": len(failures),
        "runtimeLimitReached": int(stopped_on_runtime_limit),
        "dailyRequestLimitReached": int(stopped_on_daily_request_limit),
    }


def _get_item_view_with_invalid_json_retries(
    client: DomeggookClient,
    batch: list[str],
    *,
    run_budget: RunBudget | None,
    deadline_monotonic: float | None,
) -> dict[str, object]:
    for attempt in range(1, len(INVALID_JSON_RETRY_DELAYS_SECONDS) + 2):
        try:
            payload = client.get_item_view(batch)
            if run_budget is not None:
                run_budget.record_call()
            return payload
        except DomeggookApiError as exc:
            if run_budget is not None:
                run_budget.record_call()
            if not _is_invalid_json_error(exc) or attempt > len(INVALID_JSON_RETRY_DELAYS_SECONDS):
                raise
            if _deadline_reached(deadline_monotonic) or (run_budget is not None and not run_budget.can_call()):
                raise
            delay = INVALID_JSON_RETRY_DELAYS_SECONDS[attempt - 1]
            LOGGER.warning(
                "retrying detail batch after invalid_json product_ids=%s attempt=%d delaySeconds=%.1f error=%s",
                ",".join(batch),
                attempt,
                delay,
                exc,
            )
            time.sleep(delay)
    raise DomeggookApiError("request failed after invalid_json retries")


def _save_domeggook_detail_batch(
    *,
    project_root: Path,
    config: DomeggookConfig,
    collected_at: str,
    products: dict[str, dict[str, object]],
    raw_limit: int,
) -> None:
    result = save_products_with_raw_samples_if_enabled(
        project_root=project_root,
        platform="domeggook",
        collected_at=collected_at,
        products=products.values(),
        raw_sample_limit=raw_limit,
        logger=LOGGER,
    )
    require_product_save(result, products.values())


def _raw_remaining(state: dict[str, object], default: int) -> int:
    try:
        return min(max(int(state.get("rawRemaining", default)), 0), default)
    except (TypeError, ValueError):
        return default


def _deadline_reached(deadline_monotonic: float | None) -> bool:
    return deadline_monotonic is not None and time.monotonic() >= deadline_monotonic


def _is_invalid_json_error(exc: DomeggookApiError) -> bool:
    return "invalid JSON response" in str(exc)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect Domeggook/Domeme product detail snapshots.")
    parser.add_argument("--config", default=None)
    parser.add_argument("--limit", type=int, default=None, help="Limit active product ids for a small real API run.")
    parser.add_argument("--dry-run", action="store_true", help="Call API but do not write data files.")
    args = parser.parse_args(argv)

    project_root = find_project_root(Path.cwd())
    configure_logging(project_root / "domeggook_API" / "data" / "logs")
    config = load_config(Path(args.config) if args.config else project_root / "domeggook_API" / "config" / "config.yaml")
    result = collect_details(project_root, config, product_limit=args.limit, dry_run=args.dry_run)
    print(result)
    return 1 if result["failureCount"] else 0


if __name__ == "__main__":
    sys.exit(main())
