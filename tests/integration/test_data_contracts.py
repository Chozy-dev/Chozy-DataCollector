"""Service-spec data contracts, using only the guarded disposable PostgreSQL DB.

These tests reconstruct observations, not source occurrences or production scores.
"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from domeggook_API.services.parsing import parse_detail_product
from ownerclan_API.services.normalization import normalize_item
from product_data_collector.domain.current_state import option_states
from product_data_collector.domain.product_events import ownerclan_event
from product_data_collector.persistence import postgres_storage as storage
from product_data_collector.persistence.current_state import (
    history_context, iter_history_window, read_current_product, read_product_options,
)
from product_data_collector.persistence.product_events import save_events


END = datetime(2026, 11, 19, tzinfo=timezone.utc)


def save(db, platform, product):
    storage._save_product_batch_with_retry(
        db, [storage._snapshot_row(platform, product["collectedAt"], product)])


def owner(at, **changes):
    raw = {"key": "A", "price": 50, "status": "available", "openmarketSellable": True,
           "guaranteedShippingPeriod": 3, "metadata": {"vendorKey": "001"},
           "options": [{"key": "001", "price": 100, "quantity": 10},
                       {"key": "002", "price": 200, "quantity": 20}], **changes}
    return normalize_item(raw, at.isoformat())


@pytest.mark.parametrize("days", [7, 14, 30, 90])
def test_window_reconstructs_selected_option_without_daily_snapshots(db, days):
    start = END - timedelta(days=days)
    initial = owner(start - timedelta(days=2))
    save(db, "ownerclan", initial)
    unchanged = deepcopy(initial)
    unchanged["collectedAt"] = (start + timedelta(days=1)).isoformat()
    save(db, "ownerclan", unchanged)
    changed = owner(END - timedelta(days=2), options=[
        {"key": "001", "price": 150, "quantity": 0},
        {"key": "002", "price": 200, "quantity": 20}])
    save(db, "ownerclan", changed)
    pid = read_current_product(db, "ownerclan", "A")["id"]
    context = history_context(db, pid, start, price_scope="option", option_identity="native:001")
    changes = list(iter_history_window(db, pid, start, END, batch_size=1))
    assert len(changes) == 1
    assert db.execute("SELECT count(*) AS n FROM product_history").fetchone()["n"] == 2
    assert context["price_anchor"]["change_type"] == "initial"
    points = [context["price_anchor"], *changes]
    daily = []
    for day in range(days):
        cutoff = start + timedelta(days=day + 1)
        observation = max((p for p in points if p["observed_at"] < cutoff),
                          key=lambda p: (p["observed_at"], p["id"]))
        options = option_states("ownerclan", observation["inventory"]["options"],
                                observation["prices"]["payload"])
        daily.append(next(o["price"] for o in options if o["option_identity"] == "native:001"))
    assert daily == [100] * (days - 2) + [150, 150]
    # Carrying an observation forward does not certify source-feed coverage.
    assert context["continuous_coverage_verified"] is False


def test_option_alert_inputs_are_separate_from_product_stockout_inputs(db):
    for offset, price, qty in [(-3, 100, 10), (-2, 120, 0), (-1, 120, 8)]:
        save(db, "ownerclan", owner(END + timedelta(days=offset), options=[
            {"key": "001", "price": price, "quantity": qty},
            {"key": "002", "price": 200, "quantity": 20}]))
    current = read_current_product(db, "ownerclan", "A")
    rows = list(iter_history_window(db, current["id"], END - timedelta(days=4), END))
    selected = [next(o for o in option_states("ownerclan", h["inventory"]["options"])
                     if o["source_option_id"] == "001") for h in rows]
    assert [o["price"] for o in selected] == [100, 120, 120]
    assert [o["status"] for o in selected] == ["available", "soldout", "available"]
    assert [o["stock_quantity"] for o in selected] == [10, 0, 8]
    assert all(h["status"] == "available" for h in rows)
    assert current["status"] == "available" and current["current_supply_price"] is None
    # Observations do not manufacture source events to be counted a second time.
    assert db.execute("SELECT count(*) AS n FROM product_events").fetchone()["n"] == 0


def test_key_change_is_retained_without_inventing_option_continuity(db):
    save(db, "ownerclan", owner(END - timedelta(days=3)))
    save(db, "ownerclan", owner(END - timedelta(days=2), options=[
        {"key": "1", "price": 100, "quantity": 10}]))
    rows = read_product_options(db, "ownerclan", "A", include_missing=True)
    assert {o["option_identity"]: o["membership_status"] for o in rows} == {
        "native:001": "missing", "native:002": "missing", "native:1": "present"}
    raw = {"itemKey": "A", "kind": "optionSoldout", "title": "001",
           "createdAt": int((END - timedelta(days=2)).timestamp() * 1000)}
    save_events(db, [ownerclan_event(raw, END), ownerclan_event(raw, END)])
    event = db.execute("SELECT * FROM product_events").fetchone()
    assert event["event_scope"] == "option" and event["option_id"] is None
    assert event["option_link_status"] == "unresolved"
    assert db.execute("SELECT count(*) AS n FROM product_events").fetchone()["n"] == 1


@pytest.mark.parametrize("score,count", [("0%", 0), ("96%", 128), (None, None)])
def test_domeme_scoring_fields_survive_database_roundtrip(db, score, count):
    raw = {"no": "D", "basis": {"status": "판매중"}, "channel": {"supply": "1"},
           "price": {"supply": 3000}, "qty": {"inventory": 9999},
           "deli": {"sendAvg": "0.7", "fastDeli": "1"},
           "seller": {"id": "S", "type": "간이과세자", "good": "1", "rank": "2",
                      "score": {"avg": score, "cnt": count},
                      "vacation": {"start": "2026-11-18", "end": "2026-11-20"}}}
    save(db, "domeggook", parse_detail_product(raw, END.isoformat()))
    current = read_current_product(db, "domeggook", "D")
    assert current["seller_average_satisfaction"] == score
    assert current["seller_review_count"] == count
    assert current["seller_excellent_seller"] is True
    assert current["seller_type"] == "간이과세자" and current["seller_grade"] == "2"
    assert current["source_specific"]["seller"]["vacation"] == raw["seller"]["vacation"]
    assert str(current["average_shipping_days"]) == "0.7" and current["fast_shipping"] is True
    assert current["stock_quantity"] == 9999


def test_owner_vendor_and_shipping_are_available_without_assuming_90_day_coverage(db):
    save(db, "ownerclan", owner(END - timedelta(days=1)))
    current = read_current_product(db, "ownerclan", "A")
    assert current["source_specific"]["vendorKey"] == "001"
    assert current["source_specific"]["guaranteedShippingPeriod"] == 3
    assert history_context(db, current["id"], END - timedelta(days=90))["price_anchor"] is None
    assert history_context(db, current["id"], END)["continuous_coverage_verified"] is False
    # No source events is absence of evidence, not a verified zero stockout count.
    assert db.execute("SELECT count(*) AS n FROM product_events").fetchone()["n"] == 0


def test_domeme_base_price_and_option_sale_status_changes_remain_in_history(db):
    for offset, base, hid, sup, qty in [(-4, 3000, 0, 1, 10), (-3, 3100, 0, 1, 0),
                                       (-2, 3100, 0, 1, 8), (-1, 3100, 1, 1, 8)]:
        raw = {"no": "D", "basis": {"status": "판매중"}, "price": {"supply": base},
               "selectOpt": {"type": "combination", "data": {
                   "01": {"name": "red", "supPrice": 200, "qty": qty, "hid": hid, "sup": sup}}}}
        save(db, "domeggook", parse_detail_product(raw, (END + timedelta(days=offset)).isoformat()))
    current = read_current_product(db, "domeggook", "D")
    rows = list(iter_history_window(db, current["id"], END - timedelta(days=5), END))
    options = [option_states("domeggook", h["inventory"]["options"], h["prices"]["payload"])[0]
               for h in rows]
    assert [o["price"] for o in options] == [3200, 3300, 3300, 3300]
    assert [o["status"] for o in options] == ["available", "soldout", "available", "discontinued"]
    assert all(o["identity_status"] == "unverified" for o in options)
    assert read_product_options(db, "domeggook", "D")[0]["status"] == "discontinued"


def test_partial_list_does_not_refresh_unobserved_selected_option(db):
    save(db, "ownerclan", owner(END - timedelta(days=3)))
    selected = read_product_options(db, "ownerclan", "A")[0]
    save(db, "ownerclan", owner(END - timedelta(days=1), optionsComplete=False,
                               options=[{"key": "002", "price": 220, "quantity": 0}]))
    retained = next(o for o in read_product_options(db, "ownerclan", "A")
                    if o["source_option_id"] == "001")
    assert retained["last_observed_at"] == selected["last_observed_at"]
    assert retained["membership_status"] == "present" and retained["price"] == 100
    current = read_current_product(db, "ownerclan", "A")
    assert current["options_complete"] is False
    assert current["current_supply_price"] is None
    assert retained["last_observed_at"] < current["last_collected_at"]


def test_product_stockout_events_can_be_selected_without_option_or_replay_inflation(db):
    save(db, "ownerclan", owner(END - timedelta(days=5)))
    events = [ownerclan_event({"itemKey": "A", "kind": kind,
                              "createdAt": int((END - timedelta(days=offset)).timestamp() * 1000)}, END)
              for kind, offset in [("soldout", 4), ("restocked", 3), ("optionSoldout", 2), ("soldout", 1)]]
    save_events(db, events + events)
    rows = db.execute("SELECT * FROM product_events WHERE event_origin='source' "
                      "AND event_scope='product' AND semantic='soldout' ORDER BY occurred_at").fetchall()
    assert len(rows) == 2
    assert len({r["external_product_id"] for r in rows}) == 1
    assert all(r["occurred_at"] is not None for r in rows)
    # Supplier numerator is distinct products with stockouts, not number of events.
    # Eligible denominators, coverage and scores belong to the scoring workflow.


def test_owner_option_source_status_changes_with_positive_stock_are_preserved(db):
    # Synthetic enum values test transport/storage, not provider enum semantics.
    for offset, status in [(-2, "available"), (-1, "discontinued")]:
        save(db, "ownerclan", owner(END + timedelta(days=offset), options=[
            {"key": "001", "price": 100, "quantity": 10, "status": status}]))
    current = read_current_product(db, "ownerclan", "A")
    rows = list(iter_history_window(db, current["id"], END - timedelta(days=3), END))
    assert [h["inventory"]["options"][0]["status"] for h in rows] == ["available", "discontinued"]
    option = read_product_options(db, "ownerclan", "A")[0]
    assert option["status"] == "discontinued" and option["status_basis"] == "source"
    assert option["stock_quantity"] == 10 and current["status"] == "available"
