from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from domeggook_API.services.parsing import parse_detail_product
from ownerclan_API.services.normalization import normalize_item
from product_data_collector.domain.current_state import minimum_quantity_price, option_states, project_current
from product_data_collector.domain.product_events import ownerclan_event, domeggook_event, observation_event
from product_data_collector.persistence.postgres_storage import _snapshot_row

NOW = "2026-02-01T00:00:00Z"


def owner_raw(**changes):
    return {"itemKey": "001", "kind": "priceIncreased", "title": "price", "createdAt": 1767225600123,
            "valueBefore": "100", "valueAfter": "200", **changes}


@pytest.mark.parametrize("raw,expected", [("10+700|1+800", 800), ("1+0|10+0", 0),
    (0, 0), (None, None), ("", None), ("garbage", None), ("1+100|1+200", None), (True, None)])
def test_minimum_purchase_quantity_not_cheapest_tier(raw, expected):
    assert minimum_quantity_price(raw) == expected


def test_source_id_and_fingerprint_dedup_exclude_page_cursor_and_observation_time():
    one = ownerclan_event(owner_raw(), NOW)
    two = ownerclan_event(owner_raw(page=7, _cursor="unverified"), "2026-03-01T00:00:00Z")
    assert one.record()["dedupe_key"] == two.record()["dedupe_key"]
    assert one.record()["occurred_at"] == datetime(2026, 1, 1, 0, 0, 0, 123000, tzinfo=timezone.utc)
    assert one.record()["observed_at"] != one.record()["occurred_at"]
    assert one.record()["identity_status"] == "fingerprint"
    assert ownerclan_event(owner_raw(), NOW, verified_source_event_id="E1").record()["identity_status"] == "source_id"
    assert replace(one, source_event_id="E1").record()["dedupe_key"] != replace(one, source_event_id="E2").record()["dedupe_key"]
    assert one.record()["dedupe_key"] == ownerclan_event(owner_raw(valueBefore=100, valueAfter="200.00"), NOW).record()["dedupe_key"]


@pytest.mark.parametrize("change", [{"createdAt": 1767225600124}, {"valueAfter": "201"}, {"itemKey": "1"},
                                   {"kind": "priceDecreased"}, {"title": "other option"}])
def test_different_evidence_is_not_merged(change):
    assert ownerclan_event(owner_raw(), NOW).record()["dedupe_key"] != ownerclan_event(owner_raw(**change), NOW).record()["dedupe_key"]


@pytest.mark.parametrize("kind", ["stockChanged", "optionStockChanged"])
def test_quantity_events_excluded(kind):
    assert ownerclan_event(owner_raw(kind=kind), NOW) is None


def test_ambiguous_identity_missing_time_and_observation_contract():
    row = ownerclan_event(owner_raw(createdAt=None), NOW).record()
    assert row["occurred_at"] is None and row["identity_status"] == "ambiguous"
    assert row["identity_note"]
    event = observation_event(platform="ownerclan", external_product_id="A", kind="price_comparison", observed_at=NOW,
                              before=100, after=200, comparison_key=[1, 2], semantic="supply_price")
    assert event.record()["occurred_at"] is None
    with pytest.raises(ValueError, match="invent"):
        replace(event, occurred_at=datetime.now(timezone.utc)).record()


@pytest.mark.parametrize("kind,semantic", [("AMT", "price_signal"), ("OPTAMT", "option_price_signal"),
    ("OPEN", "listing_opened"), ("RESTART", "restocked"), ("SOLDOUT", "soldout"),
    ("OPTSOLDOUT", "soldout"), ("OPTRESTART", "restocked"), ("CLOSE", "listing_closed"), ("DEL", "listing_deleted")])
def test_dome_signal_semantics(kind, semantic):
    row = domeggook_event({"no": 1, "status": kind, "date": 1767225600, "price": 123}, NOW).record()
    assert row["semantic"] == semantic and row["value_status"] == "signal_only"
    assert row["occurred_at"].year == 2026 and row["verified_after"] is None
    assert row["raw_after"] is None
    assert row["event_scope"] == ("option" if kind.startswith("OPT") else "product")


def test_owner_final_option_cost_zero_infinite_missing_and_metadata():
    raw = {"key": "A", "price": 10, "production": "maker", "status": "available", "openmarketSellable": True,
           "shippingFee": 0, "shippingType": "free", "metadata": {"vendorKey": "V", "coupangCategoryFullName": "a>b", "smartstoreCategoryCode": "001"},
           "options": [{"key": "001", "price": 0, "quantity": 9999}, {"key": "002", "price": 0, "quantity": 4}]}
    product = normalize_item(raw, NOW)
    row = _snapshot_row("ownerclan", NOW, product)
    current = project_current(row)
    assert current["current_supply_price"] == 0 and current["stock_quantity"] == 9999
    assert current["stock_is_infinite"] is True and current["shipping_fee"] == 0
    assert current["sellable"] is True
    assert product["sourceSpecific"]["production"] == "maker"
    assert product["sourceSpecific"]["smartstoreCategoryCode"] == "001"
    raw["options"][1]["price"] = 200
    different = project_current(_snapshot_row("ownerclan", NOW, normalize_item(raw, NOW)))
    assert different["current_supply_price"] is None and different["supply_price_status"] == "option_required"
    raw["options"] = []
    assert project_current(_snapshot_row("ownerclan", NOW, normalize_item(raw, NOW)))["current_supply_price"] is None
    raw["options"] = [{"price": 100, "quantity": 1}, {"price": 100, "quantity": None}]
    assert project_current(_snapshot_row("ownerclan", NOW, normalize_item(raw, NOW)))["stock_quantity"] is None


def test_dome_unverified_options_never_supply_costs_or_verified_ids():
    raw = {"no": 1, "price": {"supply": "10+700|1+800"}, "qty": {"inventory": 0, "supplyUnit": 2},
           "selectOpt": {"data": {"00": {"domPrice": "100", "supPrice": "300", "qty": "0", "hid": "2", "hash": "abc"}}}}
    product = parse_detail_product(raw, NOW)
    state = project_current(_snapshot_row("domeggook", NOW, product))
    option = option_states("domeggook", product["options"])[0]
    assert state["current_supply_price"] is None
    assert option["price"] is None and option["price_meaning"] == "unknown"
    assert option["identity_status"] == "unverified" and option["source_option_id"] == "00"
    assert option["raw_option"]["supPrice"] == "300"
    assert option["stock_quantity"] == 0
    raw.pop("selectOpt")
    assert project_current(_snapshot_row("domeggook", NOW, parse_detail_product(raw, NOW)))["current_supply_price"] == 800


def test_unknown_option_identity_is_labelled_and_conflicting_source_keys_rejected():
    rows = option_states("ownerclan", [{"price": 100}, {"price": 200}])
    assert len(rows) == 2 and all(o["source_option_id"] is None for o in rows)
    assert all(o["identity_status"] == "unverified" for o in rows)
    with pytest.raises(ValueError, match="Conflicting"):
        option_states("ownerclan", [{"skuKey": "K", "price": 100}, {"skuKey": "K", "price": 200}])


def test_invalid_source_numeric_price_not_verified():
    for before, after in [("unknown", "200"), ("300", "200"), (None, 200)]:
        event = ownerclan_event(owner_raw(valueBefore=before, valueAfter=after), NOW).record()
        assert event["value_status"] == "unresolved" and event["verified_after"] is None


def test_missing_native_option_ids_cannot_establish_product_total():
    p = normalize_item({"key": "A", "options": [{"price": 10, "quantity": 2}, {"price": 10, "quantity": 2}]}, NOW)
    current = project_current(_snapshot_row("ownerclan", NOW, p))
    assert current["current_supply_price"] is None and current["stock_quantity"] is None


def test_sum_equal_to_9999_is_not_a_source_infinite_inventory_marker():
    p = normalize_item({"key": "A", "options": [{"key": "1", "price": 10, "quantity": 4999},
                                               {"key": "2", "price": 10, "quantity": 5000}]}, NOW)
    current = project_current(_snapshot_row("ownerclan", NOW, p))
    assert current["stock_quantity"] == 9999 and current["stock_is_infinite"] is False


def test_migration_apply_requires_explicit_dsn_variable(monkeypatch):
    from product_data_collector.persistence.migrations import main
    with pytest.raises(SystemExit) as exc:
        main(["--apply", "stage2_storage_contract"])
    assert exc.value.code == 2


def test_option_key_leading_zero_change_is_not_numeric_price_normalization():
    from product_data_collector.domain.product_history import comparable_state
    before = {"options": [{"skuKey": "001", "price": "100", "quantity": "2"}]}
    after = {"options": [{"skuKey": "1", "price": 100, "quantity": 2}]}
    assert comparable_state(before) != comparable_state(after)
