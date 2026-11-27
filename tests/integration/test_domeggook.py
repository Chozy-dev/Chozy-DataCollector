"""Real isolated PostgreSQL + documentation-only Private API mocks."""
from copy import deepcopy
from unittest.mock import Mock
import pytest

from tests.unit.domeggook.test_private_sync import page, signal, detail
from domeggook_API.workflows import product_sync as s
from domeggook_API.api.client import CallBudgetExhausted
from domeggook_API.persistence.storage import load_state, save_state

START = "2026-11-19T00:00:00+00:00"
END = "2026-11-19T01:00:00+00:00"


class Details:
    def __init__(self, price=100, hidden=0):
        self.calls = []
        self.price, self.hidden = price, hidden
        self.fail = False

    def get_item_view(self, ids):
        self.calls.append(ids)
        if self.fail:
            raise RuntimeError("simulated detail failure")
        return {"domeggook": {"item": [detail(key, self.price, hidden=self.hidden) for key in ids]}}


def catalogue_search():
    return Mock(search=Mock(side_effect=[page([signal("30"), signal("20")], [20]), page([signal("10")])]))


def run(db, path, client=None, detail_client=None, **kw):
    client = client or Mock(search=Mock(return_value=page([signal()])))
    if not load_state(path).get("pending"):
        kw = {"start": START, "end": END, **kw}
    return s.incremental(db, client, detail_client or Details(), path, provider_timezone="Asia/Seoul", **kw)


def count(db, table):
    return db.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"]


def test_private_catalogue_resumes_and_replaces_open_cursor(db, tmp_path):
    private, details = catalogue_search(), Details()
    path = tmp_path / "bootstrap.json"
    save_state(path, {"mode": "bootstrap", "categoryIndex": 1, "page": 3})
    assert s.catalogue(db, private, details, path, page_size=2, page_limit=1)["status"] == "paused"
    state = load_state(path)
    assert state["after"] == [20] and state["source"] == "private-search"
    assert s.catalogue(db, private, details, path, page_size=2)["status"] == "completed"
    assert private.search.call_args.kwargs["after"] == [20]
    assert count(db, "products") == count(db, "product_history") == 3
    assert count(db, "product_events") == 0
    assert details.calls == [["30", "20"], ["10"]]
    assert load_state(path)["startedAt"] == state["startedAt"]
    assert s.catalogue(db, private, details, path)["status"] == "completed"
    assert private.search.call_count == 2


def test_sync_details_options_snapshots_signals_and_replay(db, tmp_path):
    path = tmp_path / "sync.json"
    assert run(db, path)["status"] == "completed"
    assert count(db, "products") == count(db, "product_options") == count(db, "product_events") == 1
    event = db.execute("SELECT * FROM product_events").fetchone()
    assert event["occurred_at"] is event["verified_before"] is None
    assert event["event_origin"] == "source" and event["value_status"] == "signal_only"
    assert run(db, path)["status"] == "completed"
    assert count(db, "product_events") == count(db, "product_history") == 1
    assert run(db, path, detail_client=Details(price=200, hidden=1))["status"] == "completed"
    row = db.execute("SELECT * FROM products").fetchone()
    assert row["current_supply_price"] is None and row["price_info"]["supplyCurrentSupplyPrice"] == 200
    assert row["seller_average_satisfaction"] == "95%"
    option = db.execute("SELECT * FROM product_options WHERE membership_status='present'").fetchone()
    assert option["status"] != "available"
    assert option["price"] == 200 and option["price_meaning"] == "final"
    assert count(db, "product_history") == 2 and count(db, "product_events") == 1


def test_private_cursor_resume_and_fixed_range(db, tmp_path):
    path = tmp_path / "sync.json"
    private = Mock(search=Mock(side_effect=[page([signal()], [10]), page([signal("9")])]))
    assert run(db, path, private, page_limit=1)["status"] == "paused"
    state = load_state(path)
    assert state["pending"]["after"] == [10] and "watermark" not in state
    assert run(db, path, private)["status"] == "completed"
    assert private.search.call_args.kwargs["after"] == [10]
    assert private.search.call_args.args == (state["pending"]["from"], state["pending"]["to"])
    assert count(db, "products") == 2


@pytest.mark.parametrize("failure", ["detail", "missing", "database", "budget", "api"])
def test_failed_page_does_not_advance_and_retries(db, tmp_path, monkeypatch, failure):
    path = tmp_path / "sync.json"
    private, details = Mock(search=Mock(return_value=page([signal()]))), Details()
    original = s.insert_events
    if failure == "detail":
        details.fail = True
    elif failure == "missing":
        details.get_item_view = lambda ids: {"domeggook": {"item": []}}
    elif failure == "database":
        def fail(*a, **kw):
            raise RuntimeError("injected DB error")
        monkeypatch.setattr(s, "insert_events", fail)
    else:
        private.search.side_effect = CallBudgetExhausted() if failure == "budget" else RuntimeError()
    result = run(db, path, private, details)
    assert result["status"] == ("paused" if failure == "budget" else "failed")
    assert load_state(path)["pending"]["after"] is None and "watermark" not in load_state(path)
    assert count(db, "products") == count(db, "product_events") == 0
    monkeypatch.setattr(s, "insert_events", original)
    assert run(db, path)["status"] == "completed"


def test_old_inflight_detail_cannot_overwrite_new_sync(db, tmp_path):
    old = s.pg._snapshot_row("domeggook", START, {"productId": "10", "productName": "old", "prices": {"supplyPrice": 1}})
    assert run(db, tmp_path / "sync.json", detail_client=Details(price=200))["status"] == "completed"
    s.pg._save_product_batch(db, [old])
    db.commit()
    assert db.execute("SELECT price_info FROM products").fetchone()["price_info"]["supplyCurrentSupplyPrice"] == 200


def test_catalogue_detail_failure_retries_page_and_recovery_refreshes(db, tmp_path):
    path, details = tmp_path / "bootstrap.json", Details()
    private = Mock(search=Mock(return_value=page([signal()])))
    details.fail = True
    assert s.catalogue(db, private, details, path)["status"] == "failed"
    assert load_state(path)["after"] is None
    assert count(db, "products") == 0
    assert run(db, tmp_path / "sync.json", detail_client=Details(price=200))["status"] == "completed"
    details.fail = False
    assert s.catalogue(db, private, details, path)["status"] == "completed"
    details.price = 300
    recovery = tmp_path / "recovery.json"
    assert s.catalogue(db, private, details, recovery, mode="recovery")["status"] == "completed"
    assert db.execute("SELECT price_info FROM products WHERE external_product_id='10'").fetchone()["price_info"]["supplyCurrentSupplyPrice"] == 300
    assert count(db, "product_events") == 1


def test_duplicate_page_cursor_is_failure(db, tmp_path):
    private = Mock(search=Mock(return_value=page([signal()], [10])))
    assert run(db, tmp_path / "sync.json", private)["status"] == "failed"
    assert load_state(tmp_path / "sync.json")["pending"]["after"] == [10]
    assert count(db, "product_events") == 1


def test_session_lock_prevents_duplicate_job_across_paths(db, db_factory, tmp_path):
    other = db_factory()
    with s.job_lock(db, tmp_path / "first.json", "private-sync"):
        with pytest.raises(RuntimeError, match="Another"):
            with s.job_lock(other, tmp_path / "other.json", "private-sync"):
                pass


def test_checkpoint_write_failure_after_commit_replays(db, tmp_path, monkeypatch):
    original = s.save_state
    def fail_completion(path, state):
        if "watermark" in state:
            raise OSError("checkpoint write failed")
        return original(path, state)
    monkeypatch.setattr(s, "save_state", fail_completion)
    path = tmp_path / "sync.json"
    assert run(db, path)["status"] == "failed"
    assert "pending" in load_state(path) and count(db, "product_events") == 1
    monkeypatch.setattr(s, "save_state", original)
    assert run(db, path)["status"] == "completed"
    assert count(db, "product_events") == count(db, "product_history") == 1


def test_real_database_rejection_rolls_back_entire_sync_page(db, tmp_path):
    db.execute("ALTER TABLE product_events ADD CONSTRAINT reject_test_product CHECK (external_product_id <> '10')")
    db.commit()
    path = tmp_path / "sync.json"
    assert run(db, path)["status"] == "failed"
    assert count(db, "products") == count(db, "product_options") == count(db, "product_history") == 0
    assert load_state(path)["pending"]["after"] is None
    db.execute("ALTER TABLE product_events DROP CONSTRAINT reject_test_product")
    db.commit()
    assert run(db, path)["status"] == "completed"


def test_lost_commit_ack_replay_is_idempotent(db, tmp_path, monkeypatch):
    original = s.insert_events
    def lost_ack(conn, *args, **kwargs):
        original(conn, *args, **kwargs)
        conn.commit()
        raise RuntimeError("lost commit acknowledgement")
    monkeypatch.setattr(s, "insert_events", lost_ack)
    path = tmp_path / "sync.json"
    assert run(db, path)["status"] == "failed"
    assert count(db, "product_events") == 1 and "pending" in load_state(path)
    monkeypatch.setattr(s, "insert_events", original)
    assert run(db, path)["status"] == "completed"
    assert count(db, "product_events") == count(db, "product_history") == 1


def test_catalogue_rolls_back_failed_page_and_resumes(db, tmp_path, monkeypatch):
    original = s.pg._save_product_batch
    def fail_second(conn, rows):
        original(conn, rows)
        if rows[0]["external_product_id"] == "10":
            raise RuntimeError("second page save failed")
    monkeypatch.setattr(s.pg, "_save_product_batch", fail_second)
    path, private, details = tmp_path / "bootstrap.json", catalogue_search(), Details()
    assert s.catalogue(db, private, details, path, page_size=2)["status"] == "failed"
    assert load_state(path)["after"] == [20] and count(db, "products") == 2
    monkeypatch.setattr(s.pg, "_save_product_batch", original)
    private.search.side_effect = None
    private.search.return_value = page([signal("10")])
    assert s.catalogue(db, private, details, path, page_size=2)["status"] == "completed"
    assert count(db, "products") == count(db, "product_history") == 3
    assert details.calls == [["30", "20"], ["10"], ["10"]]


def test_pending_catalogue_scope_cannot_change(db, tmp_path):
    path, private, details = tmp_path / "bootstrap.json", catalogue_search(), Details()
    assert s.catalogue(db, private, details, path, page_size=2, page_limit=1)["status"] == "paused"
    with pytest.raises(ValueError, match="page size"):
        s.catalogue(db, private, details, path, page_size=3)


def test_daily_range_overlaps_previous_watermark(db, tmp_path):
    path = tmp_path / "sync.json"
    assert run(db, path)["status"] == "completed"
    private = Mock(search=Mock(return_value=page()))
    assert s.incremental(db, private, Details(), path, end="2026-11-19T02:00:00Z")["status"] == "completed"
    assert private.search.call_args.args == ("2026-11-19 09:59:00", "2026-11-19 11:00:00")
