"""Offline regressions for the verified 2026-11-19 option contracts."""
from copy import deepcopy
from unittest.mock import MagicMock

import pytest

from domeggook_API.services.parsing import parse_detail_product
from product_data_collector.domain.current_state import option_states, project_current
from product_data_collector.domain.product_events import ownerclan_event
from product_data_collector.persistence.postgres_storage import _snapshot_row
from product_data_collector.persistence.product_options import save_options

NOW = "2026-11-19T00:00:00Z"


def dome_row(*, base="3000", extra="200", kind="combination", original=None,
             hid="0", sup="1", qty="1993"):
    raw = {"no": "12473769", "basis": {"status": "판매중"}, "channel": {"supply": "true"},
           "price": {"supply": base, "supplyOrg": original},
           "selectOpt": {"type": kind, "data": {"01": {
               "name": "option", "supPrice": extra, "domPrice": "999", "hash": "hash",
               "hid": hid, "sup": sup, "qty": qty}}}}
    return _snapshot_row("domeggook", NOW, parse_detail_product(raw, NOW))


def projected_option(row):
    return option_states("domeggook", row["options_payload"], row["prices_payload"])[0]


@pytest.mark.parametrize("extra,expected", [("200", 3200), ("0", 3000)])
def test_fixed_combination_final_price_preserves_evidence(extra, expected):
    row = dome_row(extra=extra)
    before = deepcopy(row)
    option = projected_option(row)
    assert option["price"] == expected
    assert option["price_meaning"] == "final"
    assert option["price_confidence"] == "source_contract"
    assert option["raw_option"]["supPrice"] == extra
    assert option["raw_option"]["domPrice"] == "999"
    assert row["prices_payload"]["supplyCurrentSupplyPrice"] == 3000
    assert row == before
    assert option["identity_status"] == "unverified"
    assert option["option_identity"] == "reference:01"
    # A selected option price does not establish a product-wide final price.
    assert project_current(row)["current_supply_price"] is None


@pytest.mark.parametrize("changes", [
    {"kind": "independent"}, {"kind": None}, {"kind": "unknown"},
    {"base": "1+3000|10+2800"}, {"base": None}, {"extra": None},
    {"extra": "-200"}, {"extra": "invalid"}, {"original": "3500"},
])
def test_unverified_prices_remain_unknown(changes):
    row = dome_row(**changes)
    option = projected_option(row)
    assert option["price"] is None
    assert option["price_meaning"] == "unknown"
    assert option["price_confidence"] == "unresolved"
    assert project_current(row)["current_supply_price"] is None


@pytest.mark.parametrize("hid,sup,qty,status,basis", [
    ("1", "1", "1993", "discontinued", "hid"),
    ("1", "1", "0", "discontinued", "hid"),
    ("2", "1", "1993", "hidden", "hid"),  # Official contract; not a measured hid=2 sample.
    ("0", "0", "1993", "unavailable", "sup"),
    ("0", "1", "0", "soldout", "quantity"),
    ("0", "1", "1993", "available", "hid_sup_quantity"),
    (None, "1", "1993", None, "unknown"),
    ("3", "1", "1993", None, "unknown"),
    ("0", None, "1993", None, "unknown"),
    ("0", "invalid", "1993", None, "unknown"),
    ("0", "1", None, None, "unknown"),
])
def test_option_status_requires_verified_signals(hid, sup, qty, status, basis):
    row = dome_row(hid=hid, sup=sup, qty=qty)
    option = projected_option(row)
    assert (option["status"], option["status_basis"]) == (status, basis)
    assert project_current(row)["sellable"] is True


def test_option_storage_receives_final_price_and_original_adjustment():
    row = dome_row(hid="1")
    connection = MagicMock()
    save_options(connection, 123, row)
    query, records = connection.cursor.return_value.__enter__.return_value.executemany.call_args.args
    record = records[0]
    assert "price_meaning" in query
    assert record[0] == 123
    assert record[6:9] == (3200, "final", "source_contract")
    assert record[11:13] == ("discontinued", "hid")
    assert record[13].obj["supPrice"] == "200"
    assert record[13].obj["domPrice"] == "999"


def test_legacy_option_reads_receive_base_price(monkeypatch):
    from product_data_collector.persistence import current_state as storage
    row = dome_row()
    monkeypatch.setattr(storage, "read_current_product", lambda *a, **k: {"id": 123, "read_path": "legacy"})
    monkeypatch.setattr(storage, "legacy_rows", lambda *a: [{"history_observed_at": NOW}])
    monkeypatch.setattr(storage, "legacy_snapshot", lambda record: row)
    assert storage.read_product_options(object(), "domeggook", "12473769")[0]["price"] == 3200


def test_option_history_anchor_uses_its_own_base_price():
    from product_data_collector.persistence.current_state import history_context
    row = dome_row()
    history = {"prices": {"payload": row["prices_payload"]},
               "inventory": {"options": row["options_payload"]},
               "observed_at": row["collected_at"], "id": 1}
    connection = MagicMock()
    connection.execute.side_effect = [
        MagicMock(fetchone=lambda: {"platform": "domeggook"}),
        MagicMock(fetchall=lambda: [history]),
        MagicMock(fetchone=lambda: None),
    ]
    result = history_context(connection, 123, "2026-11-20T00:00:00Z",
                             price_scope="option", option_identity="reference:01")
    assert result["price_anchor"] is history


@pytest.mark.parametrize("count", [0, 10, None])
def test_seller_zero_score_and_review_count_remain_distinct(count):
    raw = {"no": "1", "seller": {"score": {"avg": "0%", "cnt": count}}}
    row = _snapshot_row("domeggook", NOW, parse_detail_product(raw, NOW))
    assert row["seller"]["average_satisfaction"] == "0%"
    assert row["seller"]["review_count"] == count


def test_missing_seller_score_does_not_become_zero():
    row = _snapshot_row("domeggook", NOW, parse_detail_product({"no": "1", "seller": {}}, NOW))
    assert row["seller"]["average_satisfaction"] is None
    assert row["seller"]["review_count"] is None


@pytest.mark.parametrize("kind", ["optionPriceIncreased", "optionSoldout", "optionRestocked"])
def test_six_field_owner_history_keeps_unresolved_option(kind):
    raw = {"itemKey": "A", "kind": kind, "title": "option", "createdAt": 1795046400000,
           "valueBefore": "100", "valueAfter": "200"}
    event = ownerclan_event(raw, NOW).record()
    assert event["raw_event"] == raw
    assert event["event_scope"] == "option"
    assert event["option_link_status"] == "unresolved"
    assert event.get("option_id") is None
