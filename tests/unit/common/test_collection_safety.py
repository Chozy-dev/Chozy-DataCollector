import json
from dataclasses import replace

import pytest

from coupang_API.workflows import collector as coupang
from tests.unit.coupang.test_collector_bulk import FakeClient as CoupangClient
from domeggook_API.workflows import collect_product_details as dome
from domeggook_API.workflows import discover_products as discovery
from tests.unit.domeggook.test_discover_collect import FakeClient as DomeClient, _config as dome_config
from ownerclan_API.workflows import collect_by_categories as owner
from ownerclan_API.workflows import sync_incremental as incremental
from tests.unit.ownerclan.test_ownerclan_workflow import _config as owner_config, _item, _category
from ownerclan_API.persistence.storage import atomic_write_json, load_state
from product_data_collector.persistence.postgres_storage import ProductSaveError
from scripts.run_daily_collector import _exit_code, _status, _reason


def page(keys, *, more=False, cursor=None):
    return {"allItems": {
        "pageInfo": {"hasNextPage": more, "endCursor": cursor},
        "edges": [{"cursor": key, "node": _item(key)} for key in keys],
    }}


class Pages:
    def __init__(self, *pages):
        self.pages = iter(pages)
        self.queries = []

    def graphql(self, query):
        self.queries.append(query)
        return next(self.pages)


class Saver:
    def __init__(self):
        self.calls = []
        self.committed = {}
        self.fail = False

    def save(self, **kwargs):
        products = list(kwargs["products"])
        if self.fail:
            raise ProductSaveError("ownerclan", [], [p["productId"] for p in products])
        self.calls.append(kwargs["collected_at"])
        for product in products:
            self.committed.setdefault((product["productId"], kwargs["collected_at"]), product)
        return {"snapshotCount": len({p["productId"] for p in products}), "rawSampleCount": 0}

    def close(self):
        pass


@pytest.fixture
def owner_setup(tmp_path, monkeypatch):
    config = owner_config(tmp_path)
    atomic_write_json(config.output.category_cache_path, {"categories": [_category("C1", children=[])]})
    saver = Saver()
    monkeypatch.setattr(owner, "ProductSnapshotSaver", lambda **kwargs: saver)
    monkeypatch.setattr(incremental, "save_products_with_raw_samples_if_enabled", saver.save)
    return config, saver


@pytest.mark.parametrize("limit", [{"page_limit": 1}, {"item_limit": 1}])
def test_owner_limits_pause_then_resume_to_completion(tmp_path, owner_setup, limit):
    config, saver = owner_setup
    result = owner.collect_by_categories(tmp_path, config, client=Pages(page(["A"], more=True, cursor="A")), **limit)
    state_path = config.output.state_dir / "category-collection-state.json"
    assert result["status"] == "paused"
    assert load_state(state_path)["after"] == "A"
    client = Pages(page(["B"]))
    result = owner.collect_by_categories(tmp_path, config, client=client)
    assert 'after: "A"' in client.queries[0]
    assert result["status"] == "completed"
    assert not state_path.exists()
    assert len(saver.committed) == 2


def test_owner_category_limit_resumes_next_category(tmp_path, owner_setup):
    config, _ = owner_setup
    atomic_write_json(config.output.category_cache_path, {"categories": [_category("C1", children=[]), _category("C2", children=[])]})
    first = owner.collect_by_categories(tmp_path, config, client=Pages(page(["A"])), category_limit=1)
    assert first["status"] == "paused"
    assert load_state(config.output.state_dir / "category-collection-state.json")["categoryKey"] == "C2"
    client = Pages(page(["B"]))
    assert owner.collect_by_categories(tmp_path, config, client=client, category_limit=1)["status"] == "completed"
    assert 'category: "C2"' in client.queries[0]


def test_owner_duplicate_product_on_terminal_page_does_not_create_false_resume(tmp_path, owner_setup):
    config, saver = owner_setup
    result = owner.collect_by_categories(tmp_path, config, client=Pages(page(["A", "A"])))
    assert result["status"] == "completed"
    assert len(saver.committed) == 1


@pytest.mark.parametrize("bad_page", [page(["B"], more=True, cursor="A"), page(["B"], more=True), {}])
def test_owner_bad_pagination_does_not_clear_state(tmp_path, owner_setup, bad_page):
    config, saver = owner_setup
    result = owner.collect_by_categories(tmp_path, config, client=Pages(page(["A"], more=True, cursor="A"), bad_page))
    assert result["status"] == "failed"
    assert load_state(config.output.state_dir / "category-collection-state.json")["after"] == "A"
    assert len(saver.committed) == 1


def test_owner_db_failure_cannot_advance_checkpoint(tmp_path, owner_setup):
    config, saver = owner_setup
    saver.fail = True
    with pytest.raises(ProductSaveError):
        owner.collect_by_categories(tmp_path, config, client=Pages(page(["A"])))
    state_path = config.output.state_dir / "category-collection-state.json"
    assert load_state(state_path)["after"] is None
    saver.fail = False
    assert owner.collect_by_categories(tmp_path, config, client=Pages(page(["A"])))["status"] == "completed"
    assert len(saver.committed) == 1


def test_owner_removed_category_does_not_transfer_its_cursor(tmp_path, owner_setup):
    config, saver = owner_setup
    path = config.output.state_dir / "category-collection-state.json"
    atomic_write_json(path, {"categoryKey": "REMOVED", "after": "old-category-cursor"})
    saver.fail = True
    with pytest.raises(ProductSaveError):
        owner.collect_by_categories(tmp_path, config, client=Pages(page(["A"])))
    assert load_state(path)["categoryKey"] == "C1"
    assert load_state(path)["after"] is None


def test_owner_commit_then_checkpoint_failure_replays_same_observation(tmp_path, monkeypatch, owner_setup):
    config, saver = owner_setup
    real_save = owner.save_state

    def failing_save(path, state):
        if state.get("after") == "A":
            assert len(saver.committed) == 1
            raise OSError("checkpoint disk failure")
        real_save(path, state)

    monkeypatch.setattr(owner, "save_state", failing_save)
    with pytest.raises(OSError):
        owner.collect_by_categories(tmp_path, config, client=Pages(page(["A"], more=True, cursor="A")))
    assert load_state(config.output.state_dir / "category-collection-state.json")["after"] is None
    monkeypatch.setattr(owner, "save_state", real_save)
    result = owner.collect_by_categories(tmp_path, config, client=Pages(page(["A"])))
    assert result["status"] == "completed"
    assert len(saver.committed) == 1
    assert len(set(saver.calls)) == 1


def test_parallel_owner_repeated_cursor_keeps_unfinished_category(tmp_path, monkeypatch, owner_setup):
    config, saver = owner_setup

    class Repeated:
        def graphql(self, query):
            return page(["A"], more=True, cursor="A")

    monkeypatch.setattr(owner, "make_client", lambda *args, **kwargs: Repeated())
    result = owner.collect_by_categories_parallel(tmp_path, config, category_workers=2)
    assert result["status"] == "failed"
    progress = load_state(config.output.state_dir / "category-collection-progress.json")
    assert progress["completedCategoryKeys"] == []
    assert progress["inProgress"]["C1"]["after"] == "A"
    assert len(saver.committed) == 1


@pytest.mark.parametrize("limit", [{"page_limit": 1}, {"item_limit": 1}])
def test_incremental_paused_range_keeps_watermark_and_resumes(tmp_path, owner_setup, limit):
    config, saver = owner_setup
    path = config.output.state_dir / "legacy-all-items-state.json"
    atomic_write_json(path, {"lastSuccessfulItemSyncAt": "2026-01-01T00:00:00+09:00"})
    result = incremental.sync_all_items_legacy(tmp_path, config, client=Pages(page(["A"], more=True, cursor="A")), **limit)
    assert result["status"] == "paused"
    paused = load_state(path)
    assert paused["lastSuccessfulItemSyncAt"] == "2026-01-01T00:00:00+09:00"
    assert paused["pendingIncremental"]["after"] == "A"
    client = Pages(page(["B"]))
    result = incremental.sync_all_items_legacy(tmp_path, config, client=client)
    assert result["status"] == "completed"
    assert 'after: "A"' in client.queries[0]
    assert load_state(path) == {"lastSuccessfulItemSyncAt": paused["pendingIncremental"]["dateTo"]}
    assert len(set(saver.calls)) == 1


def test_incremental_partial_page_uses_last_committed_edge(tmp_path, owner_setup):
    config, _ = owner_setup
    result = incremental.sync_all_items_legacy(tmp_path, config, client=Pages(page(["A", "B"])), item_limit=1)
    assert result["status"] == "paused"
    assert load_state(config.output.state_dir / "legacy-all-items-state.json")["pendingIncremental"]["after"] == "A"
    assert incremental.sync_all_items_legacy(tmp_path, config, client=Pages(page(["B"])))["status"] == "completed"


def test_incremental_db_failure_does_not_mark_page_or_range_done(tmp_path, owner_setup):
    config, saver = owner_setup
    saver.fail = True
    result = incremental.sync_all_items_legacy(tmp_path, config, client=Pages(page(["A"])))
    assert result["status"] == "failed"
    state = load_state(config.output.state_dir / "legacy-all-items-state.json")
    assert "lastSuccessfulItemSyncAt" not in state
    assert state["pendingIncremental"]["after"] is None
    assert state["pendingIncremental"]["itemsCompleted"] is False


def test_history_cursor_repetition_does_not_mark_range_complete(tmp_path, owner_setup):
    config, _ = owner_setup
    config = replace(config, incremental=replace(config.incremental, include_item_histories=True))
    history = {"itemHistories": {"pageInfo": {"hasNextPage": True, "endCursor": "same"}, "edges": []}}
    result = incremental.sync_all_items_legacy(tmp_path, config, client=Pages(page(["A"]), history, history))
    assert result["status"] == "failed"
    assert "lastSuccessfulItemSyncAt" not in load_state(config.output.state_dir / "legacy-all-items-state.json")


@pytest.fixture
def coupang_setup(tmp_path, monkeypatch):
    path = tmp_path / "coupang_API" / "config" / "keywords.txt"
    path.parent.mkdir(parents=True)
    path.write_text("first\nsecond\n", encoding="utf-8")
    CoupangClient.calls = []
    CoupangClient.failing_keywords = set()
    monkeypatch.setattr(coupang, "CoupangPartnersClient", CoupangClient)
    monkeypatch.setattr(coupang, "load_credentials", lambda _: ("test", "test"))
    saver = Saver()
    monkeypatch.setattr(coupang, "save_products_with_raw_samples_if_enabled", saver.save)
    return tmp_path / "coupang_API/data/state/product_search_checkpoint.json", saver


def test_coupang_db_failure_keeps_keyword_pending(tmp_path, monkeypatch, coupang_setup):
    path, saver = coupang_setup
    calls = []

    def save(**kwargs):
        calls.append(kwargs)
        state = load_state(path)
        assert not state["completedKeywords"] if len(calls) == 1 else state["completedKeywords"] == ["first"]
        if len(calls) == 2:
            raise ProductSaveError("coupang", [], ["100"])
        return saver.save(**kwargs)

    monkeypatch.setattr(coupang, "save_products_with_raw_samples_if_enabled", save)
    assert coupang.collect_once(tmp_path, coupang.CollectorConfig()) == 1
    assert load_state(path)["completedKeywords"] == ["first"]
    monkeypatch.setattr(coupang, "save_products_with_raw_samples_if_enabled", saver.save)
    CoupangClient.calls = []
    assert coupang.collect_once(tmp_path, coupang.CollectorConfig()) == 0
    assert [call.keyword for call in CoupangClient.calls] == ["second"]
    assert not path.exists()
    assert len(saver.committed) == 1


def test_coupang_checkpoint_failure_never_clears_pending_keywords(tmp_path, monkeypatch, coupang_setup):
    path, saver = coupang_setup
    real_save = coupang.Checkpoint.save

    def fail_after_commit(checkpoint):
        if checkpoint.completed_keywords:
            assert saver.committed
            raise OSError("checkpoint failure")
        real_save(checkpoint)

    monkeypatch.setattr(coupang.Checkpoint, "save", fail_after_commit)
    assert coupang.collect_once(tmp_path, coupang.CollectorConfig()) == 1
    assert load_state(path)["completedKeywords"] == []
    monkeypatch.setattr(coupang.Checkpoint, "save", real_save)
    assert coupang.collect_once(tmp_path, coupang.CollectorConfig()) == 0
    assert len(saver.committed) == 1
    assert len(set(saver.calls)) == 1


def test_dome_detail_partial_db_failure_replays_unconfirmed_batch(tmp_path, monkeypatch):
    saver = Saver()
    config = replace(dome_config(), details=replace(dome_config().details, batch_size=2))
    monkeypatch.setattr(dome, "save_products_with_raw_samples_if_enabled", saver.save)
    saver.fail = True
    with pytest.raises(ProductSaveError):
        dome.collect_details(tmp_path, config, product_ids=["A", "B", "C"], client=DomeClient())
    path = tmp_path / "domeggook_API/data/state/detail-collection-state.json"
    assert load_state(path)["nextIndex"] == 0
    saver.fail = False
    first = dome.collect_details(tmp_path, config, product_ids=["A", "B", "C"], product_limit=2, client=DomeClient())
    assert first["status"] == "paused"
    assert load_state(path)["nextIndex"] == 2
    client = DomeClient()
    assert dome.collect_details(tmp_path, config, product_ids=["A", "B", "C"], client=client)["status"] == "completed"
    assert client.detail_requests == [["C"]]
    assert len(saver.committed) == 3
    assert not path.exists()


def test_dome_missing_detail_response_does_not_advance_checkpoint(tmp_path, monkeypatch):
    monkeypatch.setattr(dome, "save_products_with_raw_samples_if_enabled", Saver().save)

    class Missing(DomeClient):
        def get_item_view(self, ids):
            return {"domeggook": {"item": []}}

    result = dome.collect_details(tmp_path, dome_config(), product_ids=["A"], client=Missing())
    assert result["status"] == "failed"
    assert load_state(tmp_path / "domeggook_API/data/state/detail-collection-state.json")["nextIndex"] == 0


def test_discovery_empty_page_flushes_buffer_before_state_moves(tmp_path, monkeypatch):
    config = replace(dome_config(), discovery=replace(dome_config().discovery, markets=("dome",), sorts={"ranking": "rd"}, list_page_size=2))
    monkeypatch.setattr(discovery, "require_database_enabled", lambda _: None)
    monkeypatch.setattr(discovery, "postgres_enabled", lambda _: True)
    monkeypatch.setattr(discovery, "save_discovery_targets_and_search_ranks_if_enabled", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("DB failure")))

    class EndsEmpty(DomeClient):
        def get_item_list(self, request):
            if request.page == 1:
                return super().get_item_list(request)
            return {"domeggook": {"list": {"item": []}}}

    with pytest.raises(RuntimeError, match="DB failure"):
        discovery.discover(tmp_path, config, client=EndsEmpty())
    state = load_state(tmp_path / "domeggook_API/data/state/discovery-state.json")
    assert state.get("nextPage", 1) == 1


@pytest.mark.parametrize("platform", ["ownerclan", "domeggook", "coupang"])
def test_daily_status_only_completes_successful_range(platform):
    assert _status(platform, {}, 1) == "failed"
    paused = {"stage": {"status": "paused", "reason": "item_limit_reached"}}
    assert _status(platform, paused, 0) == "paused"
    assert _reason(platform, paused, 0) == "item_limit_reached"
    assert _status(platform, {"stage": {"status": "completed"}}, 0) == "completed"


def test_disabled_product_db_is_not_a_successful_collection(tmp_path, owner_setup, monkeypatch):
    config, _ = owner_setup
    monkeypatch.setattr(incremental, "save_products_with_raw_samples_if_enabled", lambda **_: {"snapshotCount": 0, "rawSampleCount": 0})
    assert incremental.sync_all_items_legacy(tmp_path, config, client=Pages(page(["A"])))["status"] == "failed"


def test_disabled_discovery_db_does_not_advance_checkpoint(tmp_path, monkeypatch):
    monkeypatch.setenv("POSTGRES_ENABLED", "false")
    with pytest.raises(RuntimeError, match="PostgreSQL is disabled"):
        discovery.discover(tmp_path, dome_config(), client=DomeClient())
    state = load_state(tmp_path / "domeggook_API/data/state/discovery-state.json")
    assert state["nextPage"] == 1


@pytest.mark.parametrize("payload", [{}, {"data": {}}, {"data": {"productData": [None]}}, {"data": {"productData": [{}]}}])
def test_coupang_malformed_response_is_not_empty_success(payload):
    with pytest.raises(ValueError):
        coupang.parse_product_records(payload, requested_keyword="test", collected_at="2026-01-01T00:00:00Z")
