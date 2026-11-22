from contextlib import contextmanager
from datetime import datetime, timezone
import re

import pytest

from tests.unit.ownerclan.test_ownerclan_workflow import _config, _item
from ownerclan_API.workflows import history_sync as hs
from ownerclan_API.workflows import bootstrap as bs
from ownerclan_API.persistence.storage import load_state, save_state, CheckpointLock
from ownerclan_API.api.client import OwnerclanGraphQLError
from product_data_collector.domain.product_events import OWNER_KINDS

START = "2026-09-01T00:00:00+00:00"
END = "2026-09-16T00:00:00+00:00"


def history(kind="soldout", key="A", **kwargs):
    return {"itemKey": key, "kind": kind, "createdAt": 1788220800123,
            "valueBefore": "10", "valueAfter": "20", **kwargs}


def page(nodes=(), *, more=False, cursor=None, root="itemHistories"):
    return {root: {"pageInfo": {"hasNextPage": more, "endCursor": cursor},
                   "edges": [{"cursor": str(i), "node": node} for i, node in enumerate(nodes)]}}


class Client:
    def __init__(self, *responses):
        self.responses = iter(responses)
        self.queries = []

    def graphql(self, query):
        self.queries.append(query)
        result = next(self.responses)
        if isinstance(result, Exception):
            raise result
        return result


@pytest.fixture
def sink(monkeypatch):
    records, requests = {}, []
    def save(conn, events, **kwargs):
        for event in events:
            row = event.record()
            records.setdefault(row["dedupe_key"], row)
        return {"confirmed_count": len(events)}
    monkeypatch.setattr(hs, "save_events", save)
    monkeypatch.setattr(hs, "request_refreshes", lambda c, p, keys, **kw: requests.extend(keys))
    return records, requests


def collect(tmp_path, client, **kwargs):
    return hs.collect_history(tmp_path, _config(tmp_path), client=client, connection=object(),
                              date_from=START, date_to=END, **kwargs)


def test_windows_and_cursor_complete_without_assuming_sort(tmp_path, sink):
    client = Client(page([history()], more=True, cursor="next"), page([history("restocked")]), page(), page())
    result = collect(tmp_path, client)
    assert result["eventCollectionComplete"]
    assert not result["detailRefreshComplete"]
    assert 'after: "next"' in client.queries[1]
    bounds = [tuple(map(int, re.findall(r"date(?:From|To): (\d+)", q))) for q in client.queries]
    assert all(b - a <= 7 * hs.DAY for a, b in bounds)
    assert bounds[2][0] < bounds[1][1]
    assert len(sink[0]) == 2


@pytest.mark.parametrize("kind", list(OWNER_KINDS))
def test_all_source_kinds_preserve_raw_and_unresolved_option(tmp_path, sink, kind):
    raw = history(kind, title="red", optionKey="001")
    collect(tmp_path, Client(page([raw]), page(), page()))
    row = next(iter(sink[0].values()))
    assert row["raw_event"] == raw
    assert row["occurred_at"].microsecond == 123000
    assert row["option_link_status"] == ("unresolved" if kind.startswith("option") else "not_applicable")


@pytest.mark.parametrize("kind", ["stockChanged", "optionStockChanged"])
def test_stock_excluded_but_detail_requested(tmp_path, sink, kind):
    collect(tmp_path, Client(page([history(kind)]), page(), page()))
    assert not sink[0]
    assert sink[1] == ["A"]


def test_notification_change_with_empty_values_does_not_abort_history_page(tmp_path, sink):
    raw = history("productNotificationInformationChanged", valueBefore="", valueAfter="")
    result = collect(tmp_path, Client(page([raw, history("soldout")]), page(), page()))
    assert result["status"] == "completed"
    assert result["historyCount"] == 2
    row = next(row for row in sink[0].values()
               if row["event_kind"] == "productNotificationInformationChanged")
    assert row["raw_event"] == raw
    assert row["value_status"] == "unresolved"
    assert row["verified_before"] is None and row["verified_after"] is None
    state = load_state(_config(tmp_path).output.state_dir / "incremental-state.json")
    assert "pendingHistory" not in state
    assert "lastSuccessfulHistorySyncAt" in state


def test_history_failure_logs_reason_and_retains_checkpoint(tmp_path, sink, caplog):
    result = collect(tmp_path, Client(page([history("unknown")])))
    assert result["status"] == "failed"
    assert "Unsupported ownerclan event kind: unknown" in caplog.text
    state = load_state(_config(tmp_path).output.state_dir / "incremental-state.json")
    assert state["pendingHistory"]["after"] is None


def test_replay_and_late_boundary_event(tmp_path, sink):
    collect(tmp_path, Client(page([history()]), page(), page()))
    collect(tmp_path, Client(page([history(), history("restocked", createdAt=1788220800000)]), page(), page()))
    assert len(sink[0]) == 2


@pytest.mark.parametrize("response", [
    {}, page([], more=True, cursor="c"), page([history("unknown")]),
    page([history(key="")]), page([history(createdAt=None)]),
])
def test_invalid_page_never_completes(tmp_path, sink, response):
    result = collect(tmp_path, Client(response))
    assert result["status"] == "failed"
    state = load_state(_config(tmp_path).output.state_dir / "incremental-state.json")
    assert state["pendingHistory"]["after"] is None
    assert "lastSuccessfulHistorySyncAt" not in state
    assert not sink[0]


def test_repeated_cursor_failure_and_resume_fixed_range(tmp_path, sink):
    result = collect(tmp_path, Client(page([history()], more=True, cursor="c"),
                                     page([history()], more=True, cursor="c")))
    assert result["status"] == "failed"
    client = Client(page(), page(), page())
    assert collect(tmp_path, client)["status"] == "completed"
    assert 'after: "c"' in client.queries[0]


@pytest.mark.parametrize("limit", [{"page_limit": 1}, {"item_limit": 1}])
def test_limits_resume_partial_page(tmp_path, sink, limit):
    result = collect(tmp_path, Client(page([history(), history("restocked")], more=True, cursor="c")), **limit)
    assert result["status"] == "paused"
    result = collect(tmp_path, Client(page([history("restocked")]), page(), page()))
    assert result["status"] == "completed"
    assert len(sink[0]) == 2


def test_ambiguous_commit_replays_whole_page(tmp_path, sink, monkeypatch):
    saver = hs.save_events
    def lost(*args, **kwargs):
        saver(*args, **kwargs)
        raise RuntimeError("commit reply lost")
    monkeypatch.setattr(hs, "save_events", lost)
    assert collect(tmp_path, Client(page([history()])))["status"] == "failed"
    monkeypatch.setattr(hs, "save_events", saver)
    assert collect(tmp_path, Client(page([history()]), page(), page()))["status"] == "completed"
    assert len(sink[0]) == 1


def test_backfill_retention_gap_and_separate_daily_state(tmp_path, sink):
    daily = _config(tmp_path).output.state_dir / "incremental-state.json"
    save_state(daily, {"lastSuccessfulHistorySyncAt": END})
    result = collect(tmp_path, Client(OwnerclanGraphQLError([{"message": "itemHistories 쿼리는 최대 3개월 이전 내역까지 조회할 수 있습니다."}]), page(), page()), mode="backfill")
    assert result["status"] == "failed" and result["gapCount"] == 1
    state = load_state(daily.with_name("backfill-state.json"))
    assert not state["completed"]
    assert len(state["successfulRanges"]) == 2
    assert load_state(daily) == {"lastSuccessfulHistorySyncAt": END}


def test_checkpoint_lock_exclusion_and_release(tmp_path):
    path = tmp_path / "state.lock"
    with CheckpointLock(path):
        with pytest.raises(RuntimeError):
            with CheckpointLock(path):
                pass
    with CheckpointLock(path):
        pass


def test_bootstrap_pause_failure_resume_and_history_overlap(tmp_path, monkeypatch):
    calls, saved = [], []
    monkeypatch.setattr(bs, "collect_history", lambda *a, **kw: calls.append(kw) or {"status": "completed", "eventCollectionComplete": True})
    def save(**kw):
        saved.extend(kw["products"])
        return {"snapshotCount": len(kw["products"])}
    monkeypatch.setattr(bs, "save_products_with_raw_samples_if_enabled", save)
    config = _config(tmp_path)
    first = bs.bootstrap(tmp_path, config, client=Client(page([_item("A")], more=True, cursor="a", root="allItems")), page_limit=1)
    assert first["status"] == "paused"
    failure = bs.bootstrap(tmp_path, config, client=Client(RuntimeError()))
    assert failure["status"] == "failed"
    client = Client(page([_item("B")], root="allItems"))
    assert bs.bootstrap(tmp_path, config, client=client)["status"] == "completed"
    assert 'after: "a"' in client.queries[0]
    assert saved[0]["collectedAt"] == saved[1]["collectedAt"]
    assert calls[-1]["date_from"] == saved[0]["collectedAt"]
    assert calls[-1]["mode"] == "bootstrap-catchup"


def test_daily_entry_never_scans_catalogue(tmp_path, monkeypatch):
    from ownerclan_API.workflows import main
    monkeypatch.setattr(main, "load_config", lambda *a: _config(tmp_path))
    monkeypatch.setattr(main, "collect_by_categories", lambda *a, **k: pytest.fail("category scan"))
    monkeypatch.setattr(main, "sync_incremental", lambda *a, **k: {"status": "completed", "failureCount": 0})
    monkeypatch.setattr(hs, "refresh_details", lambda *a, **k: {"status": "completed", "failureCount": 0})
    assert set(main.run(tmp_path, tmp_path / "config")) == {"incremental", "refresh"}


def test_daily_watermark_overlap_and_legacy_is_not_history_evidence(tmp_path, sink, monkeypatch):
    config = _config(tmp_path)
    path = config.output.state_dir / "incremental-state.json"
    save_state(path, {"lastSuccessfulItemSyncAt": "2000-01-01T00:00:00Z"})
    monkeypatch.setattr(hs, "clock", lambda: END)
    client = Client(page([history()]))
    hs.collect_history(tmp_path, config, client=client, connection=object())
    first_from = int(re.search(r"dateFrom: (\d+)", client.queries[0])[1])
    assert first_from == hs.millis(END) - 120 * 60000 - 1
    state = load_state(path)
    assert state["lastSuccessfulHistorySyncAt"] == END
    assert state["lastSuccessfulItemSyncAt"].startswith("2000")
    client = Client(page([history("restocked")]))
    hs.collect_history(tmp_path, config, client=client, connection=object())
    assert int(re.search(r"dateFrom: (\d+)", client.queries[0])[1]) == first_from


def test_stock_request_failure_keeps_page_replayable(tmp_path, sink, monkeypatch):
    def broken(*a, **k):
        raise RuntimeError("request commit lost")
    monkeypatch.setattr(hs, "request_refreshes", broken)
    assert collect(tmp_path, Client(page([history(), history("stockChanged")])))["status"] == "failed"
    state = load_state(_config(tmp_path).output.state_dir / "incremental-state.json")
    assert state["pendingHistory"]["after"] is None
    assert len(sink[0]) == 1  # Events already committed; replay dedupes them.


def test_backfill_network_failure_is_not_a_retention_gap(tmp_path, sink):
    assert collect(tmp_path, Client(RuntimeError()), mode="backfill")["status"] == "failed"
    state = load_state(_config(tmp_path).output.state_dir / "backfill-state.json")
    assert not state.get("gaps")
    assert state["pendingHistory"]["windowFrom"] == hs.millis(START)


def test_bootstrap_does_not_claim_latest_detail_completion(tmp_path, monkeypatch):
    monkeypatch.setattr(bs, "collect_history", lambda *a, **kw: {"status": "completed", "eventCollectionComplete": True})
    monkeypatch.setattr(bs, "save_products_with_raw_samples_if_enabled", lambda **kw: {"snapshotCount": len(kw["products"])})
    result = bs.bootstrap(tmp_path, _config(tmp_path), client=Client(page([_item("A")], root="allItems")))
    assert result["snapshotComplete"] and not result["detailRefreshComplete"]


def test_millisecond_conversion_timezone_and_first_limit():
    from ownerclan_API.api.queries import item_histories_query
    assert hs.millis("2026-09-01T09:00:00.123+09:00") == 1788220800123
    for first in (0, 1001):
        with pytest.raises(ValueError):
            item_histories_query(first=first)
