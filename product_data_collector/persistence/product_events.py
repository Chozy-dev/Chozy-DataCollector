"""Bounded, atomic source-event + refresh-request persistence."""
from itertools import islice

from psycopg.types.json import Jsonb

from .postgres_storage import _lock_key
from .storage_values import json_safe


class EventSaveError(RuntimeError):
    def __init__(self, committed_count, pending_keys):
        self.committed_count = committed_count
        self.pending_keys = tuple(pending_keys)
        super().__init__("Event batch commit not confirmed; replay the page/range before advancing checkpoint")


def save_events(connection, events, *, request_refresh=True, scope=None, batch_size=500):
    """Owns commits. Return only after event AND requested version are durable.

    Iterable input is bounded in memory. On error the unconsumed suffix is unknown;
    replay the entire source page/range. Duplicates never increment requested_version.
    """
    if not 1 <= batch_size <= 10000:
        raise ValueError("batch_size must be between 1 and 10000")
    iterator = iter(events)
    committed_count = 0
    inserted = 0
    while True:
        keys = []
        try:
            batch = list(islice(iterator, batch_size))
            if not batch:
                break
            rows = [event.record() for event in batch if event is not None]
            keys = [r["dedupe_key"] for r in rows]
            n = insert_events(connection, rows, request_refresh=request_refresh, scope=scope)
            connection.commit()
        except Exception as exc:
            connection.rollback()
            raise EventSaveError(committed_count, keys) from exc
        committed_count += len(keys)
        inserted += n
    return {"confirmed_count": committed_count, "inserted_count": inserted}


def insert_events(connection, rows, *, request_refresh=True, scope=None):
    """Transaction primitive; caller must commit before acknowledging progress."""
    from .detail_refresh import _request_refresh
    locks = sorted({_lock_key(r["platform"], r["external_product_id"]) for r in rows})
    with connection.cursor() as cursor:
        cursor.executemany("SELECT pg_advisory_xact_lock(%s)", [(key,) for key in locks])
    json_columns = {"option_reference", "raw_before", "raw_after", "verified_before", "verified_after",
                    "verification_evidence", "raw_event"}
    inserted = 0
    for row in rows:
        product = connection.execute("SELECT id FROM products WHERE platform=%s AND external_product_id=%s",
                                     (row["platform"], row["external_product_id"])).fetchone()
        columns = tuple(row) + ("product_id",)
        values = tuple(Jsonb(json_safe(v)) if k in json_columns and v is not None else v for k, v in row.items())
        saved = connection.execute(
            f"INSERT INTO product_events ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))}) "
            "ON CONFLICT (dedupe_key) DO NOTHING RETURNING id",
            values + (product["id"] if product else None,),
        ).fetchone()
        if saved:
            inserted += 1
            if request_refresh and row["event_origin"] == "source":
                _request_refresh(connection, row["platform"], row["external_product_id"], row["observed_at"],
                                 scope or {"source_api": row["source_api"]})
        elif product:
            connection.execute("UPDATE product_events SET product_id=%s WHERE dedupe_key=%s AND product_id IS NULL",
                               (product["id"], row["dedupe_key"]))
    return inserted


def record_verification(connection, event_id, *, before, after, evidence, related_source_event_id=None):
    """Attach independently verified values; does not prove event-to-snapshot causality.

    Evidence must identify the compared observations and field. First verification
    wins; conflicting re-verification requires a separately reviewed correction.
    This primitive deliberately does not commit.
    """
    from product_data_collector.domain.product_events import utc
    if not isinstance(evidence, dict) or not all(k in evidence for k in ("field", "before_observed_at", "after_observed_at")):
        raise ValueError("Field and both observation times are required verification evidence")
    if before is None or after is None or utc(evidence["before_observed_at"]) >= utc(evidence["after_observed_at"]):
        raise ValueError("Verification requires known values and increasing observation times")
    if evidence["field"] in {"supply_price", "base_price", "option_final_price", "return_shipping_fee"}:
        from product_data_collector.domain.current_state import number
        before, after = number(before), number(after)
        if before is None or after is None:
            raise ValueError("Price verification requires interpretable numeric values")
    target = connection.execute("SELECT * FROM product_events WHERE id=%s FOR UPDATE", (event_id,)).fetchone()
    if not target:
        raise ValueError("Unknown event")
    if related_source_event_id is not None:
        source = connection.execute("SELECT * FROM product_events WHERE id=%s", (related_source_event_id,)).fetchone()
        if (not source or source["event_origin"] != "source" or target["event_origin"] != "observation"
                or (source["platform"], source["external_product_id"], source["event_scope"]) !=
                   (target["platform"], target["external_product_id"], target["event_scope"])):
            raise ValueError("Related source event must belong to the same product/scope")
    if target["value_status"] in {"observed_change", "observed_unchanged"}:
        if (target["verified_before"], target["verified_after"], target["verification_evidence"], target["related_source_event_id"]) != (
                json_safe(before), json_safe(after), evidence, related_source_event_id):
            raise ValueError("Conflicting event verification")
        return False
    connection.execute(
        "UPDATE product_events SET verified_before=%s, verified_after=%s, value_status=%s, "
        "verification_evidence=%s, related_source_event_id=%s WHERE id=%s",
        (Jsonb(json_safe(before)), Jsonb(json_safe(after)), "observed_change" if before != after else "observed_unchanged",
         Jsonb(evidence), related_source_event_id, event_id),
    )
    return True
