"""Latest reads, legacy comparison and bounded history context; no scoring."""
from product_data_collector.domain.current_state import CURRENT_COLUMNS, project_current, number

from .snapshot_payload import snapshot_row
from .storage_values import json_safe


def legacy_rows(connection, ids):
    return connection.execute(
        """SELECT p.*, h.id AS history_id, h.observed_at AS history_observed_at,
            h.prices, h.inventory, h.shipping
        FROM products p LEFT JOIN LATERAL (
            SELECT id, observed_at, prices, inventory, shipping FROM product_history
            WHERE product_id=p.id AND observed_at<=p.last_collected_at
            ORDER BY observed_at DESC, id DESC LIMIT 1
        ) h ON true WHERE p.id=ANY(%s) ORDER BY p.id""", (ids,),
    ).fetchall()


def legacy_snapshot(record):
    inventory = record.get("inventory") or {}
    prices = record.get("prices") or {}
    shipping = record.get("shipping") or {}
    inv_payload = dict(inventory.get("payload") or {})
    if "stockQuantity" not in inv_payload and inventory.get("stockQuantity") is not None:
        inv_payload["stockQuantity"] = inventory["stockQuantity"]
    return snapshot_row(record["platform"], record["last_collected_at"].isoformat(), {
        "productId": record["external_product_id"], "prices": prices.get("payload") or {},
        "inventory": inv_payload, "shipping": shipping.get("payload") or {},
        "options": inventory.get("options") or [], "status": record.get("status"),
        "optionsComplete": inventory.get("optionsComplete") is True,
        "sourceSpecific": record["source_specific"],
        "markets": record["source_specific"].get("markets", {}),
    })


def read_current_product(connection, platform, external_id, *, read_path="auto"):
    if read_path not in {"auto", "legacy"}:
        raise ValueError("read_path must be auto or legacy")
    product = connection.execute("SELECT * FROM products WHERE platform=%s AND external_product_id=%s",
                                 (platform, external_id)).fetchone()
    if not product:
        return None
    if read_path == "auto" and product.get("current_observed_at") == product["last_collected_at"]:
        return {**product, "read_path": "current"}
    legacy = legacy_rows(connection, [product["id"]])[0]
    return {**product, **project_current(legacy_snapshot(legacy)), "read_path": "legacy"}


def compare_current_batch(connection, *, after_id=0, limit=500):
    if not 1 <= limit <= 10000:
        raise ValueError("Invalid comparison limit")
    ids = [r["id"] for r in connection.execute("SELECT id FROM products WHERE id>%s ORDER BY id LIMIT %s", (after_id, limit)).fetchall()]
    differences = []
    for row in legacy_rows(connection, ids):
        expected = project_current(legacy_snapshot(row))
        mismatch = [key for key in CURRENT_COLUMNS if json_safe(row.get(key)) != json_safe(expected[key])]
        if mismatch:
            differences.append({"product_id": row["id"], "columns": mismatch})
    return {"next_id": ids[-1] if ids else after_id, "checked": len(ids), "differences": differences}


def read_product_options(connection, platform, external_id, *, include_missing=False, read_path="auto"):
    """Read retained options with their own timestamps; partial lists do not refresh absent options."""
    from product_data_collector.domain.current_state import option_states
    current = read_current_product(connection, platform, external_id, read_path=read_path)
    if current is None:
        return []
    if current["read_path"] == "legacy":
        record = legacy_rows(connection, [current["id"]])[0]
        row = legacy_snapshot(record)
        return [{**o, "last_observed_at": record["history_observed_at"], "membership_status": "present",
                 "read_path": "legacy"} for o in option_states(platform, row["options_payload"], row["prices_payload"])]
    return connection.execute("SELECT * FROM product_options WHERE product_id=%s "
        + ("" if include_missing else "AND membership_status='present' ") + "ORDER BY id", (current["id"],)).fetchall()


def iter_history_window(connection, product_id, start, end, *, batch_size=500):
    """Half-open observation window. Initial/stock-only rows remain labelled as-is."""
    from product_data_collector.domain.product_events import utc
    start, end = utc(start), utc(end)
    if start >= end or not 1 <= batch_size <= 10000:
        raise ValueError("Invalid history window/batch size")
    at, last_id = start, 0
    while True:
        rows = connection.execute(
            "SELECT * FROM product_history WHERE product_id=%s AND observed_at>=%s AND observed_at<%s "
            "AND (observed_at,id)>(%s,%s) ORDER BY observed_at,id LIMIT %s",
            (product_id, start, end, at, last_id, batch_size),
        ).fetchall()
        if not rows:
            return
        yield from rows
        at, last_id = rows[-1]["observed_at"], rows[-1]["id"]


def history_context(connection, product_id, start, *, price_scope="supply", option_identity=None):
    """Return pre-window price evidence and start of the observed status run.

    These are observations, never source occurrences or proof of continuous
    coverage. Missing intervals are not filled. No counts or 90-day scores here.
    """
    from product_data_collector.domain.product_events import utc
    if price_scope not in {"supply", "base", "option"} or (price_scope == "option" and not option_identity):
        raise ValueError("Choose supply, base, or option with a local option_identity")
    start = utc(start)
    product = connection.execute("SELECT * FROM products WHERE id=%s", (product_id,)).fetchone()
    if not product:
        return None
    at, last_id, anchor = start, 0, None
    while anchor is None:
        rows = connection.execute(
            "SELECT * FROM product_history WHERE product_id=%s AND (observed_at,id)<(%s,%s) "
            "ORDER BY observed_at DESC,id DESC LIMIT 200", (product_id, at, last_id),
        ).fetchall()
        if not rows:
            break
        for row in rows:
            payload = (row["prices"] or {}).get("payload") or {}
            options = (row["inventory"] or {}).get("options") or []
            from product_data_collector.domain.current_state import minimum_quantity_price, option_states
            if price_scope == "base":
                valid = minimum_quantity_price(payload.get("supplyCurrentSupplyPrice")) if product["platform"] == "domeggook" else number(payload.get("currentSupplyPrice"))
            elif price_scope == "option":
                valid = next((o["price"] for o in option_states(product["platform"], options, payload)
                              if o["option_identity"] == option_identity), None)
            else:
                evidence = {**product, "prices": row["prices"], "inventory": row["inventory"], "shipping": row["shipping"]}
                valid = project_current(legacy_snapshot(evidence))["current_supply_price"]
            if valid is not None:
                anchor = row
                break
        at, last_id = rows[-1]["observed_at"], rows[-1]["id"]
    last = connection.execute(
        "SELECT * FROM product_history WHERE product_id=%s AND observed_at<%s ORDER BY observed_at DESC,id DESC LIMIT 1",
        (product_id, start),
    ).fetchone()
    status_anchor = None
    if last and last["status"] is not None:
        status_anchor = connection.execute(
            """SELECT * FROM product_history WHERE product_id=%s AND status=%s AND observed_at<%s
            AND (observed_at,id)>COALESCE((SELECT ROW(observed_at,id) FROM product_history
                WHERE product_id=%s AND observed_at<%s AND status IS DISTINCT FROM %s
                ORDER BY observed_at DESC,id DESC LIMIT 1), ROW('-infinity'::timestamptz,0::bigint))
            ORDER BY observed_at,id LIMIT 1""", (product_id, last["status"], start, product_id, start, last["status"]),
        ).fetchone()
    return {"price_anchor": anchor, "status_anchor": status_anchor, "continuous_coverage_verified": False,
            "time_basis": "observed_at", "initial_is_change": False, "price_scope": price_scope,
            "option_identity": option_identity}


def iter_event_window(connection, platform, external_id, start, end, *, time_basis="occurred_at", batch_size=500):
    """Separate source-time and missing-time/observation streams; never COALESCE times."""
    from product_data_collector.domain.product_events import utc
    if time_basis not in {"occurred_at", "observed_at"} or not 1 <= batch_size <= 10000:
        raise ValueError("Invalid event query")
    start, end = utc(start), utc(end)
    if start >= end:
        raise ValueError("Invalid event window")
    at, last_id = start, 0
    predicate = "event_origin='source'" if time_basis == "occurred_at" else "occurred_at IS NULL"
    while True:
        rows = connection.execute(
            f"SELECT * FROM product_events WHERE platform=%s AND external_product_id=%s AND {predicate} "
            f"AND {time_basis}>=%s AND {time_basis}<%s AND ({time_basis},id)>(%s,%s) "
            f"ORDER BY {time_basis},id LIMIT %s", (platform, external_id, start, end, at, last_id, batch_size),
        ).fetchall()
        if not rows:
            return
        yield from rows
        at, last_id = rows[-1][time_basis], rows[-1]["id"]


def event_context(connection, platform, external_id, start, *, price_scope="supply", option_identity=None):
    """Pre-window source evidence, distinct from history_context's observations.

    A later detail comparison attached to a source signal is never backdated to
    occurred_at. Base price, final supply price and a verified option mapping
    are separate price scopes, just as in history_context.
    """
    from product_data_collector.domain.product_events import utc
    if price_scope not in {"supply", "base", "option"} or (price_scope == "option" and not option_identity):
        raise ValueError("Choose supply, base, or option with a local option_identity")
    start = utc(start)
    params = (platform, external_id, start)
    semantic = {"supply": "supply_price", "base": "base_price", "option": "option_final_price"}[price_scope]
    option_filter = ("AND e.option_link_status='verified' AND EXISTS (SELECT 1 FROM product_options o "
                     "WHERE o.id=e.option_id AND o.option_identity=%s) ") if price_scope == "option" else ""
    price = connection.execute(
        "SELECT e.* FROM product_events e WHERE platform=%s AND external_product_id=%s AND occurred_at<%s "
        "AND event_origin='source' AND event_scope=%s AND verified_after IS NOT NULL "
        "AND value_status='source_parsed' AND semantic=%s " + option_filter +
        "ORDER BY occurred_at DESC,id DESC LIMIT 1",
        params + ("option" if price_scope == "option" else "product", semantic)
        + ((option_identity,) if price_scope == "option" else ()),
    ).fetchone()
    last_status = connection.execute(
        "SELECT * FROM product_events WHERE platform=%s AND external_product_id=%s AND occurred_at<%s "
        "AND event_origin='source' AND event_scope='product' AND semantic IN ('soldout','restocked') "
        "ORDER BY occurred_at DESC,id DESC LIMIT 1", params,
    ).fetchone()
    soldout_start = None
    if last_status and last_status["semantic"] == "soldout":
        previous = connection.execute(
            "SELECT occurred_at,id FROM product_events WHERE platform=%s AND external_product_id=%s AND occurred_at<%s "
            "AND event_origin='source' AND event_scope='product' AND semantic='restocked' ORDER BY occurred_at DESC,id DESC LIMIT 1", params,
        ).fetchone()
        query = ("SELECT * FROM product_events WHERE platform=%s AND external_product_id=%s AND occurred_at<%s "
                 "AND event_origin='source' AND event_scope='product' AND semantic='soldout' ")
        if previous:
            query += "AND (occurred_at,id)>(%s,%s) "
            params += (previous["occurred_at"], previous["id"])
        soldout_start = connection.execute(query + "ORDER BY occurred_at,id LIMIT 1", params).fetchone()
    return {"price_anchor": price, "last_status_event": last_status, "soldout_start": soldout_start,
            "time_basis": "occurred_at", "continuous_coverage_verified": False,
            "price_scope": price_scope, "option_identity": option_identity}
