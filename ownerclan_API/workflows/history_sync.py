"""Durable history pages. API order and inclusive boundaries are deliberately unused."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import logging
import math
import time

from product_data_collector.common.collection_state import next_page_cursor, collection_outcome, validate_limits
from product_data_collector.domain.product_events import ownerclan_event, epoch, utc
from product_data_collector.persistence import postgres_storage as pg
from product_data_collector.persistence.product_events import save_events
from product_data_collector.persistence.detail_refresh import claim_refreshes, complete_refresh, fail_refresh, request_refreshes
from ..api.queries import MAX_ITEMS_BY_KEYS, item_histories_query, items_by_keys_query
from ..api.client import BULK_DETAIL_MIN_READ_TIMEOUT, OwnerclanGraphQLError, OwnerclanHttpError
from ..api.factory import make_client
from ..services.normalization import extract_connection_items, normalize_item
from ..persistence.storage import load_state, save_state, CheckpointLock

DAY = 86400000
LOGGER = logging.getLogger(__name__)


def clock():
    return datetime.now(timezone.utc).isoformat()


def millis(value):
    delta = utc(value) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return delta.days * DAY + delta.seconds * 1000 + delta.microseconds // 1000


def failure_reason(exc):
    """Useful diagnostics without logging request URLs, auth payloads or SQL values."""
    from ..api.client import OwnerclanHttpError
    if isinstance(exc, OwnerclanHttpError):
        return f"HTTP {exc.status_code}"
    if isinstance(exc, OwnerclanGraphQLError):
        return "GraphQL rate limit" if exc.is_retryable_rate_limit() else "GraphQL response rejected"
    if isinstance(exc, ValueError):
        return str(exc)[:200]
    return type(exc).__name__ + (f" SQLSTATE={exc.sqlstate}" if getattr(exc, "sqlstate", None) else "")


@contextmanager
def storage_connection(project_root):
    pg.require_database_enabled(project_root)
    config = pg.load_postgres_config(project_root)
    with pg.connect(config) as connection:
        pg.ensure_schema_initialized(config, connection)
        yield connection


def collect_history(project_root, config, *, client=None, connection=None, mode="daily",
                    date_from=None, date_to=None, page_limit=None, item_limit=None, dry_run=False):
    """Checkpoint only confirmed pages; fixed end survives pauses and ambiguous commits.

    Windows overlap 1ms on BOTH sides. Provider boundary inclusivity/order isn't
    verified. Empty terminal pages prove API traversal, never continuous coverage.
    Backfill records explicit retention rejections, without advancing daily state.
    """
    validate_limits(page_limit=page_limit, item_limit=item_limit)
    if mode not in {"daily", "backfill", "bootstrap-catchup"}:
        raise ValueError("Invalid history mode")
    if not 1 <= config.incremental.page_size <= 1000:
        raise ValueError("Invalid history page size")
    path = config.output.state_dir / ("incremental-state.json" if mode == "daily" else f"{mode}-state.json")
    with CheckpointLock(path.with_suffix(".lock")):
        if connection is None and not dry_run:
            with storage_connection(project_root) as conn:
                return _collect(config, client or make_client(project_root, config), conn, path,
                                mode, date_from, date_to, page_limit, item_limit, dry_run)
        return _collect(config, client or make_client(project_root, config), connection, path,
                        mode, date_from, date_to, page_limit, item_limit, dry_run)


def _collect(config, client, conn, path, mode, date_from, date_to, page_limit, item_limit, dry_run):
    state = load_state(path)
    end = millis(date_to or clock())
    overlap = max(config.incremental.overlap_minutes * 60000, 1)
    start = millis(date_from) if date_from else end - 90 * DAY if mode == "backfill" else (
        millis(state["lastSuccessfulHistorySyncAt"]) - overlap if state.get("lastSuccessfulHistorySyncAt")
        else end - overlap)
    if start >= end:
        raise ValueError("History start must precede end")
    pending = state.get("pendingHistory")
    if not pending:
        pending = {"from": start, "to": end, "windowFrom": start, "after": None,
                   "observedAt": clock(), "processedCount": 0, "pageCount": 0, "seenCursors": []}
        state["pendingHistory"] = pending
    elif date_from and millis(date_from) != pending["from"]:
        raise ValueError("Cannot change a pending range; resume it first")
    pages = count = saved = 0
    failure = None
    pause = None
    seen = set(pending.get("seenCursors", []))
    def checkpoint():
        if not dry_run:
            save_state(path, state)
    checkpoint()
    try:
        while pending["windowFrom"] < pending["to"]:
            if len(seen) >= 100000:
                raise ValueError("History pagination safety ceiling reached")
            lo = pending["windowFrom"]
            # Expanded request is still at most 7 days.
            hi = min(lo + 7 * DAY - 2, pending["to"])
            scope = {"mode": mode, "from": lo - 1, "to": hi + 1, "source_api": "itemHistories"}
            after = pending["after"]
            try:
                data = client.graphql(item_histories_query(first=config.incremental.page_size,
                    after=after, date_from=lo - 1, date_to=hi + 1))
            except OwnerclanGraphQLError as exc:
                # Only this provider's explicit retention error can establish a gap.
                if mode != "backfill" or "최대 3개월 이전" not in str(exc):
                    raise
                state.setdefault("gaps", []).append({**scope, "reason": "provider_retention", "error": str(exc)[:500]})
                pending.update(windowFrom=hi, after=None)
                seen.clear()
                pending["seenCursors"] = []
                checkpoint()
                continue
            nodes, info = extract_connection_items(data, "itemHistories")
            cursor = next_page_cursor(info, after, seen)
            if cursor and not nodes:
                raise ValueError("Empty non-terminal history page")
            selected = nodes if item_limit is None else nodes[:max(0, item_limit - count)]
            if len(selected) < len(nodes):
                cursor = selected[-1].get("_cursor") if selected else None
                if not cursor or cursor == after or cursor in seen:
                    raise ValueError("Cannot checkpoint partial history page")
            events, stocks = [], set()
            for node in selected:
                raw = {k: v for k, v in node.items() if k != "_cursor"}
                if not isinstance(raw.get("itemKey"), str) or not raw["itemKey"].strip():
                    raise ValueError("History missing itemKey")
                if epoch(raw.get("createdAt"), 1000) is None:
                    raise ValueError("History missing createdAt")
                event = ownerclan_event(raw, pending["observedAt"])
                if event is None:
                    stocks.add(raw["itemKey"])
                else:
                    event.record()  # Validate entire selected page before any write.
                    events.append(event)
            if not dry_run:
                receipt = save_events(conn, events, scope=scope)
                if receipt["confirmed_count"] != len(events):
                    raise RuntimeError("Event page save not confirmed")
                # Quantity signals are NOT product_events; retrying can conservatively
                # request detail again, but cannot silently lose a request.
                request_refreshes(conn, "ownerclan", stocks, observed_at=pending["observedAt"], scope=scope)
            pages += 1
            count += len(selected)
            saved += len(events)
            pending["processedCount"] += len(selected)
            pending["pageCount"] += 1
            pending["after"] = cursor
            if cursor:
                seen.add(cursor)
            else:
                state.setdefault("successfulRanges", []).append({"from": lo - 1, "to": hi + 1,
                    "status": "api_traversed", "continuousCoverageVerified": False})
                pending["windowFrom"] = hi
                seen.clear()
            pending["seenCursors"] = sorted(seen)
            checkpoint()
            if pending["windowFrom"] >= pending["to"]:
                break
            if (page_limit and pages >= page_limit) or (item_limit and count >= item_limit):
                pause = "page_limit_reached" if page_limit and pages >= page_limit else "item_limit_reached"
                break
        if not pause:
            state["lastAttemptedHistorySyncAt"] = epoch(pending["to"], 1000).isoformat()
            if not state.get("gaps"):
                state["lastSuccessfulHistorySyncAt"] = state["lastAttemptedHistorySyncAt"]
            state["completed"] = not state.get("gaps")
            state["lastRun"] = dict(pending)
            state.pop("pendingHistory", None)
            checkpoint()
    except Exception as exc:
        failure = type(exc).__name__
        state["lastFailure"] = {"type": failure, "reason": failure_reason(exc), "windowFrom": pending["windowFrom"], "after": pending["after"]}
        LOGGER.error("ownerclan history failed type=%s reason=%s", failure, failure_reason(exc))
        checkpoint()
    gaps = bool(state.get("gaps"))
    return {**collection_outcome(failed=bool(failure) or gaps, pause_reason=pause),
            "pageCount": pages, "historyCount": saved, "processedCount": count,
            "collectedProductCount": 0, "successCount": saved, "failureCount": int(bool(failure) or gaps),
            "stateUpdated": int(not failure and not pause and not dry_run),
            "eventCollectionComplete": not failure and not pause and not gaps,
            "detailRefreshComplete": False, "gapCount": len(state.get("gaps", []))}


def refresh_details(project_root, config, *, client=None, connection=None, limit=None):
    """Fetch up to 100 claimed keys, then commit each product independently.

    Response order is irrelevant. Only missing keys get one bounded supplemental
    request; API failures stop this invocation and retain unfinished versions.
    """
    validate_limits(limit=limit)
    if connection is None:
        with storage_connection(project_root) as conn:
            return refresh_details(project_root, config, client=client, connection=conn, limit=limit)
    client = client or make_client(project_root, config)
    if limit is None:
        limit = connection.execute("SELECT count(*) AS count FROM product_discovery_targets "
            "WHERE platform='ownerclan' AND requested_version>completed_version").fetchone()["count"]
    success = failed = claimed = 0
    minimal = False
    halted = False
    while claimed < limit and not halted:
        batch_size = min(MAX_ITEMS_BY_KEYS, limit - claimed)
        # One full request, one field fallback, one missing-key request. Include
        # auth retry and shared-budget pacing in each bounded request budget.
        request = config.request
        attempts = request.max_retries + 1
        http_seconds = min(5, request.timeout_seconds) + max(BULK_DETAIL_MIN_READ_TIMEOUT, request.timeout_seconds)
        request_seconds = 2 * (http_seconds * attempts
            + request.retry_after_max_seconds * request.max_retries
            + (request.interval_seconds + 61) * attempts) + request.timeout_seconds
        lease_seconds = max(300, math.ceil(3 * request_seconds + batch_size + 60))
        # Never pre-claim a later batch while the current HTTP/storage work waits.
        claims = claim_refreshes(connection, "ownerclan", limit=batch_size, lease_seconds=lease_seconds)
        if not claims:
            break
        claimed += len(claims)
        pending = {claim["external_product_id"]: claim for claim in claims}
        for attempt in range(2):
            try:
                try:
                    data = client.graphql(items_by_keys_query(list(pending), minimal=minimal))
                except OwnerclanGraphQLError as exc:
                    if minimal or not exc.looks_like_unknown_field():
                        raise
                    minimal = True
                    data = client.graphql(items_by_keys_query(list(pending), minimal=True))
                observed = clock()  # Preserve the response time across per-item commits.
                items = _batch_details(data, pending)
            except Exception as exc:
                for claim in pending.values():
                    if _fail_detail(connection, claim, exc):
                        success += 1
                    else:
                        failed += 1
                halted = True
                break
            for key, item in items.items():
                claim = pending.pop(key)
                try:
                    complete_refresh(connection, claim, normalize_item(item, observed), collected_at=observed)
                    success += 1
                except Exception as exc:
                    if _fail_detail(connection, claim, exc):
                        success += 1
                    else:
                        failed += 1
            if not pending:
                break
            if attempt == 0:
                time.sleep(1)  # Existing client/transport still reserves every HTTP attempt.
            else:
                for claim in pending.values():
                    if _fail_detail(connection, claim, ValueError("Missing detail after bounded batch retry")):
                        success += 1
                    else:
                        failed += 1
    outstanding = connection.execute("SELECT count(*) AS count FROM product_discovery_targets "
        "WHERE platform='ownerclan' AND requested_version>completed_version").fetchone()["count"]
    return {**collection_outcome(failed=bool(failed), pause_reason="refresh_pending" if outstanding else None),
            "successCount": success, "failureCount": failed, "pendingCount": outstanding,
            "detailRefreshComplete": outstanding == 0}


def _batch_details(data, requested):
    rows = data.get("itemsByKeys") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise ValueError("Missing or invalid itemsByKeys response")
    items = {}
    for item in rows:
        if item is None:
            continue  # A null entry cannot identify or complete a requested key.
        if not isinstance(item, dict) or not isinstance(item.get("key"), str):
            raise ValueError("Invalid bulk detail key")
        key = item["key"]
        if key not in requested or key in items:
            raise ValueError("Unexpected or duplicate bulk detail key")
        items[key] = item
    return items


def _fail_detail(connection, claim, exc):
    """Record only this lease's failure; reconcile an already committed result."""
    permanent = (isinstance(exc, OwnerclanHttpError) and 400 <= exc.status_code < 500
                 and exc.status_code not in {408, 429}) or (
        isinstance(exc, OwnerclanGraphQLError) and (exc.looks_like_unknown_field()
            or any(term in str(exc).lower() for term in ("unauthorized", "forbidden", "validation"))))
    retry_at = (datetime(9999, 1, 1, tzinfo=timezone.utc) if permanent else
                utc(clock()) + timedelta(seconds=min(3600, 60 * 2 ** min(claim["failure_count"], 6))))
    try:
        fail_refresh(connection, claim, error=("permanent:" if permanent else "retryable:") + failure_reason(exc),
                     next_retry_at=retry_at)
    except ValueError as stale:
        if str(stale) != "Refresh lease is stale or already completed":
            raise
        target = connection.execute("SELECT completed_version FROM product_discovery_targets WHERE id=%s",
                                    (claim["id"],)).fetchone()
        return bool(target and target["completed_version"] >= claim["processing_version"])
    return False
