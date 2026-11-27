"""Bounded catalogue and Private-sync jobs on the current product storage contract."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from copy import deepcopy
import json

from ..api.client import CallBudgetExhausted, DomeggookHttpError
from ..api.private_sync import DATE_MODIFY_FORMAT, parse_search_page
from ..services.parsing import parse_detail_products
from ..persistence.storage import FileLock, load_state, save_state
from product_data_collector.domain.product_events import ProductEvent, utc
from product_data_collector.domain.current_state import digest
from product_data_collector.persistence import postgres_storage as pg
from product_data_collector.persistence.product_events import insert_events
from product_data_collector.persistence.product_storage_schema import uses_current_storage
from product_data_collector.persistence.snapshot_payload import snapshot_row


def clock():
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def job_lock(connection, path, job):
    if not uses_current_storage():
        raise ValueError("Product sync requires migrated current product storage")
    with FileLock(path.with_suffix(".lock")):
        # Retain the namespace so existing and renamed workers still exclude each other.
        key = pg._lock_key("domeggook-stage4", job)
        acquired = connection.execute("SELECT pg_try_advisory_lock(%s) AS locked", (key,)).fetchone()["locked"]
        connection.commit()
        if not acquired:
            raise RuntimeError("Another product sync job is running")
        try:
            yield
        finally:
            connection.rollback()
            connection.execute("SELECT pg_advisory_unlock(%s)", (key,))
            connection.commit()


def detail_rows(client, ids):
    observed = clock()  # conservative request start protects against in-flight older replies
    products, failures = parse_detail_products(client.get_item_view(ids), observed)
    rows = [snapshot_row("domeggook", observed, p) for p in products]
    if failures or len(rows) != len(ids) or {r["external_product_id"] for r in rows} != set(ids):
        raise ValueError("Missing or mismatched product details")
    # An error envelope or partial empty detail must never complete a page.
    if any(not r["product_name"] or r["prices_payload"].get("supplyCurrentSupplyPrice") in (None, "") for r in rows):
        raise ValueError("Incomplete product detail")
    return rows


def sync_signal(raw, observed):
    # dateModify is a last-modification marker, NOT a source event timestamp.
    # No enum/old price/event id is documented. Preserve only a generic signal.
    return ProductEvent("domeggook", raw["no"], "POST /api/v1/product/_search",
        "modification_signal", None, utc(observed), "modification_signal", deepcopy(raw),
        value_status="signal_only", identity_discriminator=digest(raw))


def _failure(state, exc, path):
    # Never persist response messages, URLs, SQL parameters, credentials.
    state["lastFailure"] = {"type": type(exc).__name__, "at": clock()}
    if isinstance(exc, DomeggookHttpError):
        state["lastFailure"]["httpStatus"] = exc.status_code
    save_state(path, state)
    return {"status": "paused" if isinstance(exc, CallBudgetExhausted) else "failed",
            "reason": type(exc).__name__, "failureCount": 0 if isinstance(exc, CallBudgetExhausted) else 1}


def catalogue(connection, client, detail_client, state_path, *, mode="bootstrap",
              page_size=100, page_limit=None):
    """Page through the Private catalogue and commit detail before saving its cursor."""
    if mode not in {"bootstrap", "discovery", "recovery"}:
        raise ValueError("Invalid catalogue mode")
    if not 1 <= page_size <= 100 or (page_limit is not None and page_limit < 1):
        raise ValueError("Invalid catalogue limits")
    with job_lock(connection, state_path, mode):
        state = load_state(state_path)
        if state and state.get("mode") != mode:
            raise ValueError("Checkpoint belongs to another job")
        if state.get("source") == "private-search" and state.get("completed") and mode == "bootstrap":
            return _catalogue_complete(state)
        # Open category/page checkpoints cannot be translated to a Private cursor.
        if state.get("source") != "private-search" or state.get("completed"):
            state = {"startedAt": clock(), "source": "private-search", "after": None,
                     "lastProductId": None, "completed": False, "mode": mode,
                     "pageSize": page_size, "pageCount": 0, "processedCount": 0}
        if state["pageSize"] != page_size:
            raise ValueError("Cannot change page size while resuming catalogue")
        save_state(state_path, state)
        pages = 0
        try:
            while True:
                rows, after = parse_search_page(client.search(after=state["after"], size=page_size))
                ids = [row["no"] for row in rows]
                order = [int(key) for key in ids]
                if (len(ids) > page_size or
                        any(previous <= current for previous, current in zip(order, order[1:])) or
                        (order and state["lastProductId"] is not None and order[0] >= state["lastProductId"]) or
                        (after is not None and after == state["after"])):
                    raise ValueError("Private catalogue returned a repeated or out-of-order page")
                if ids:
                    pg._save_product_batch(connection, detail_rows(detail_client, ids))
                connection.commit()
                next_state = dict(state, after=after, lastProductId=order[-1] if order else state["lastProductId"],
                                  pageCount=state["pageCount"] + 1,
                                  processedCount=state["processedCount"] + len(ids), completed=after is None)
                if after is None:
                    next_state["completedAt"] = clock()
                save_state(state_path, next_state)
                state = next_state
                pages += 1
                if after is None:
                    return _catalogue_complete(state)
                if page_limit and pages >= page_limit:
                    return {"status": "paused", "reason": "page_limit", "failureCount": 0}
        except Exception as exc:
            connection.rollback()
            return _failure(state, exc, state_path)


def _catalogue_complete(state):
    return {"status": "completed", "failureCount": 0, "continuousCoverageVerified": False,
            "snapshotComplete": True, "incrementalCatchupRequired": True,
            "syncRequiredFrom": state["startedAt"]}


def _provider_time(value, provider_timezone):
    # Documentation has no timezone: require explicit operator configuration.
    return utc(value).astimezone(ZoneInfo(provider_timezone)).strftime(DATE_MODIFY_FORMAT)


def incremental(connection, private_client, detail_client, state_path, *, start=None,
                end=None, overlap_seconds=60, page_size=100, page_limit=None, provider_timezone=None):
    """Pinned inclusive range + opaque search_after; commit details/signals before checkpoint.

    First start must be explicit (use bootstrap.startedAt, even during bootstrap).
    No history retention or complete change coverage is claimed.
    """
    if overlap_seconds < 1 or not 1 <= page_size <= 100 or (page_limit is not None and page_limit < 1):
        raise ValueError("Invalid sync limits")
    with job_lock(connection, state_path, "private-sync"):
        state = load_state(state_path)
        configured_zone = provider_timezone or state.get("providerTimezone")
        if not configured_zone:
            raise ValueError("Private dateModify timezone must be explicitly configured; not measured")
        if state.get("providerTimezone") and state["providerTimezone"] != configured_zone:
            raise ValueError("Cannot change checkpoint timezone")
        state["providerTimezone"] = configured_zone
        pending = state.get("pending")
        if not pending:
            baseline = start or state.get("watermark")
            if not baseline:
                raise ValueError("First sync requires explicit initial baseline")
            lo = utc(baseline) - timedelta(seconds=overlap_seconds)
            hi = utc(end or clock())
            if lo >= hi:
                raise ValueError("Invalid sync range")
            pending = {"from": _provider_time(lo, configured_zone), "to": _provider_time(hi, configured_zone),
                       "watermark": hi.isoformat(), "after": None, "observedAt": clock(), "seen": [],
                       "pageSize": page_size, "pageCount": 0, "processedCount": 0}
            state["pending"] = pending
        elif start is not None or end is not None:
            raise ValueError("Resume pending sync without changing its range")
        if pending["pageSize"] != page_size:
            raise ValueError("Cannot change pending sync page size")
        save_state(state_path, state)
        pages = 0
        try:
            while True:
                rows, after = parse_search_page(private_client.search(pending["from"], pending["to"],
                    after=pending["after"], size=page_size))
                if len(rows) > page_size:
                    raise ValueError("Private search exceeded requested page size")
                token = json.dumps(after, sort_keys=True)
                if after is not None and (token in pending["seen"] or after == pending["after"]):
                    raise ValueError("Repeated Private search cursor")
                if len(pending["seen"]) >= 100000:
                    raise ValueError("Private search pagination safety ceiling")
                ids = list(dict.fromkeys(r["no"] for r in rows))
                # Fetch ALL details before this page writes; batch at official max 100.
                details = []
                for offset in range(0, len(ids), 100):
                    details.extend(detail_rows(detail_client, ids[offset:offset + 100]))
                if details:
                    pg._save_product_batch(connection, details)
                    insert_events(connection, [sync_signal(r, pending["observedAt"]).record() for r in rows],
                                  request_refresh=False)
                connection.commit()
                next_state = deepcopy(state)
                if after is None:
                    next_state.update(watermark=pending["watermark"], lastRange={
                        "from": pending["from"], "to": pending["to"],
                        "completedAt": clock(), "pageCount": pending["pageCount"] + 1,
                        "processedCount": pending["processedCount"] + len(rows),
                        "status": "documented_api_traversed", "continuousCoverageVerified": False})
                    next_state.pop("pending")
                else:
                    next_state["pending"]["after"] = after
                    next_state["pending"]["seen"].append(token)
                    next_state["pending"]["pageCount"] += 1
                    next_state["pending"]["processedCount"] += len(rows)
                save_state(state_path, next_state)
                state = next_state
                pages += 1
                if after is None:
                    return {"status": "completed", "failureCount": 0, "continuousCoverageVerified": False}
                pending = state["pending"]
                if page_limit and pages >= page_limit:
                    return {"status": "paused", "reason": "page_limit", "failureCount": 0}
        except Exception as exc:
            connection.rollback()
            return _failure(state, exc, state_path)
