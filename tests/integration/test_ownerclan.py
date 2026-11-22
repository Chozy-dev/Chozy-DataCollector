from contextlib import contextmanager
from datetime import timedelta

import pytest

from tests.unit.ownerclan.test_ownerclan_workflow import _config, _item
from tests.unit.ownerclan.test_history_workflows import Client, page, history, START, END
from ownerclan_API.workflows import history_sync as hs
from ownerclan_API.workflows import bootstrap as bs
from ownerclan_API.persistence.storage import load_state
from product_data_collector.persistence.detail_refresh import claim_refreshes, complete_refresh
from product_data_collector.persistence.product_events import save_events as real_save, EventSaveError
from product_data_collector.persistence.postgres_storage import _snapshot_row, _save_product_batch_with_retry


def capture(db, tmp_path, client, **kwargs):
    return hs.collect_history(tmp_path, _config(tmp_path), connection=db, client=client,
                              date_from=START, date_to=END, **kwargs)


def test_history_to_detail_and_duplicate_replay(db, tmp_path):
    nodes = [history("priceIncreased"), history("optionSoldout"), history("stockChanged")]
    assert capture(db, tmp_path, Client(page(nodes), page(), page()))["eventCollectionComplete"]
    row = db.execute("SELECT * FROM product_events WHERE event_scope='option'").fetchone()
    assert row["product_id"] is None and row["option_link_status"] == "unresolved"
    assert db.execute("SELECT count(*) AS n FROM product_events").fetchone()["n"] == 2
    initial = db.execute("SELECT requested_version FROM product_discovery_targets").fetchone()["requested_version"]
    capture(db, tmp_path, Client(page(nodes), page(), page()))
    # New overlap observation may conservatively refresh stock, but source replay
    # never increments a version. Both map to one natural-key work target.
    assert db.execute("SELECT count(*) AS n FROM product_discovery_targets").fetchone()["n"] == 1
    result = hs.refresh_details(tmp_path, _config(tmp_path), connection=db,
                                client=Client({"itemsByKeys": [_item("A")]}), limit=1)
    assert result["detailRefreshComplete"]
    assert db.execute("SELECT count(*) AS n FROM product_events WHERE product_id IS NULL").fetchone()["n"] == 0
    assert db.execute("SELECT option_id FROM product_events WHERE event_scope='option'").fetchone()["option_id"] is None
    assert db.execute("SELECT count(*) AS n FROM product_options").fetchone()["n"] > 0


def test_detail_failure_retry_and_expired_lease_resume(db, tmp_path):
    capture(db, tmp_path, Client(page([history()]), page(), page()))
    result = hs.refresh_details(tmp_path, _config(tmp_path), connection=db, client=Client(RuntimeError()), limit=1)
    assert result["failureCount"] == 1 and not result["detailRefreshComplete"]
    work = db.execute("SELECT * FROM product_discovery_targets").fetchone()
    assert work["completed_version"] == 0 and work["failure_count"] == 1
    db.execute("UPDATE product_discovery_targets SET next_retry_at=now()-interval '1 second'")
    db.commit()
    claim_refreshes(db, "ownerclan", limit=1)
    db.execute("UPDATE product_discovery_targets SET lease_until=now()-interval '1 second'")
    db.commit()
    assert hs.refresh_details(tmp_path, _config(tmp_path), connection=db,
        client=Client({"itemsByKeys": [_item("A")]}), limit=1)["detailRefreshComplete"]


@pytest.mark.parametrize("lost_after", ["partial", "commit"])
def test_page_replay_after_partial_batch_or_lost_commit(db, tmp_path, monkeypatch, lost_after):
    nodes = [history(key="A"), history(key="B")]
    def broken(conn, events, **kwargs):
        real_save(conn, events[:1] if lost_after == "partial" else events, batch_size=1, **kwargs)
        raise EventSaveError(1, [])
    monkeypatch.setattr(hs, "save_events", broken)
    assert capture(db, tmp_path, Client(page(nodes)))["status"] == "failed"
    state = load_state(_config(tmp_path).output.state_dir / "incremental-state.json")
    assert state["pendingHistory"]["after"] is None
    monkeypatch.setattr(hs, "save_events", real_save)
    assert capture(db, tmp_path, Client(page(nodes), page(), page()))["status"] == "completed"
    assert db.execute("SELECT count(*) AS n FROM product_events").fetchone()["n"] == 2
    assert [r["requested_version"] for r in db.execute("SELECT requested_version FROM product_discovery_targets")] == [1, 1]


def test_ninety_day_backfill_resumes_and_dedupes_without_reversing_current(db, tmp_path):
    current = hs.normalize_item(_item("A"), hs.clock())
    _save_product_batch_with_retry(db, [_snapshot_row("ownerclan", current["collectedAt"], current)])
    before = db.execute("SELECT last_collected_at FROM products").fetchone()["last_collected_at"]
    lo, hi = "2026-06-18T00:00:00Z", "2026-09-16T00:00:00Z"
    config = _config(tmp_path)
    first = hs.collect_history(tmp_path, config, connection=db, client=Client(page([history()])),
        mode="backfill", date_from=lo, date_to=hi, page_limit=1)
    assert first["status"] == "paused"
    responses = [page([history()])] * 12
    assert hs.collect_history(tmp_path, config, connection=db, client=Client(*responses),
        mode="backfill", date_from=lo, date_to=hi)["status"] == "completed"
    state = load_state(config.output.state_dir / "backfill-state.json")
    assert len(state["successfulRanges"]) == 13 and state["completed"]
    assert db.execute("SELECT count(*) AS n FROM product_events").fetchone()["n"] == 1
    assert db.execute("SELECT last_collected_at FROM products").fetchone()["last_collected_at"] == before


def test_bootstrap_overlap_with_newer_detail_and_crash_recovery(db, tmp_path, monkeypatch):
    @contextmanager
    def connection(*args):
        yield db
    monkeypatch.setattr(hs, "storage_connection", connection)
    def save(**kwargs):
        products = kwargs["products"]
        _save_product_batch_with_retry(db, [_snapshot_row("ownerclan", p["collectedAt"], p) for p in products])
        return {"snapshotCount": len(products)}
    monkeypatch.setattr(bs, "save_products_with_raw_samples_if_enabled", save)
    config = _config(tmp_path)
    client = Client(page(), page([_item("A")], more=True, cursor="a", root="allItems"), page([history()]))
    assert bs.bootstrap(tmp_path, config, client=client, page_limit=1)["status"] == "paused"
    claim = claim_refreshes(db, "ownerclan", limit=1)[0]
    at = hs.clock()
    detail = hs.normalize_item({**_item("A"), "name": "newer detail"}, at)
    complete_refresh(db, claim, detail, collected_at=at)
    client = Client(page(), page([{**_item("A"), "name": "old scan"}], root="allItems"), page(), page([history()]))
    assert bs.bootstrap(tmp_path, config, client=client)["status"] == "completed"
    assert db.execute("SELECT product_name FROM products").fetchone()["product_name"] == "newer detail"
    assert db.execute("SELECT count(*) AS n FROM product_events").fetchone()["n"] == 1
    assert db.execute("SELECT completed_version=requested_version AS done FROM product_discovery_targets").fetchone()["done"]


def test_permanent_detail_error_keeps_unfinished_target(db, tmp_path):
    from ownerclan_API.api.client import OwnerclanHttpError
    capture(db, tmp_path, Client(page([history()]), page(), page()))
    result = hs.refresh_details(tmp_path, _config(tmp_path), connection=db,
        client=Client(OwnerclanHttpError(403, "forbidden")), limit=1)
    assert not result["detailRefreshComplete"]
    target = db.execute("SELECT * FROM product_discovery_targets").fetchone()
    assert target["last_error"] == "permanent:HTTP 403"
    assert target["next_retry_at"].year == 9999 and target["completed_version"] == 0


def test_bootstrap_snapshot_commit_reply_loss_replays_without_duplicate_history(db, tmp_path, monkeypatch):
    @contextmanager
    def connection(*args):
        yield db
    monkeypatch.setattr(hs, "storage_connection", connection)
    lost = [True]
    def save(**kwargs):
        products = kwargs["products"]
        _save_product_batch_with_retry(db, [_snapshot_row("ownerclan", p["collectedAt"], p) for p in products])
        if lost.pop() if lost else False:
            raise RuntimeError("snapshot commit response lost")
        return {"snapshotCount": len(products)}
    monkeypatch.setattr(bs, "save_products_with_raw_samples_if_enabled", save)
    config = _config(tmp_path)
    assert bs.bootstrap(tmp_path, config, client=Client(page(), page([_item("A")], root="allItems")))["status"] == "failed"
    state = load_state(config.output.state_dir / "bootstrap-state.json")
    assert state["after"] is None and not state["snapshotComplete"]
    assert bs.bootstrap(tmp_path, config, client=Client(page(), page([_item("A")], root="allItems"), page(), page()))["status"] == "completed"
    assert db.execute("SELECT count(*) AS n FROM product_history").fetchone()["n"] == 1
