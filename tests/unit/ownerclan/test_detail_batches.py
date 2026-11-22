import json
import re
from types import SimpleNamespace

import pytest
import requests

from ownerclan_API.api.client import OwnerclanClient, OwnerclanGraphQLError, OwnerclanHttpError
from ownerclan_API.api.queries import items_by_keys_query
from ownerclan_API.api.rate_limiter import RateLimiter
from ownerclan_API.workflows import history_sync as hs
from product_data_collector.operations.budget import BudgetStopped
from tests.unit.ownerclan.test_history_workflows import Client
from tests.unit.ownerclan.test_ownerclan_auth_client import Response, StubProvider
from tests.unit.ownerclan.test_ownerclan_workflow import _config, _item


def requested(query):
    return json.loads(re.search(r"keys: (\[.*?\])", query)[1])


@pytest.mark.parametrize("keys", [[], [str(i) for i in range(101)], "A", [None], [""], ["A", "A"]])
def test_batch_query_rejects_invalid_keys(keys):
    with pytest.raises(ValueError):
        items_by_keys_query(keys)


def test_batch_query_escapes_keys_and_reuses_field_selection():
    keys = ['A"B', "한글", "C\\D"]
    query = items_by_keys_query(keys)
    assert requested(query) == keys
    assert "fixedPrice" in query
    assert "fixedPrice" not in items_by_keys_query(keys, minimal=True)


@pytest.fixture
def worker(monkeypatch, tmp_path):
    def run(keys, responses, *, limit=None, save_error_key=None):
        rows = [{"id": i, "platform": "ownerclan", "external_product_id": key,
                 "failure_count": 0, "processing_version": 1, "eligible": True, "done": False}
                for i, key in enumerate(keys)]
        saved, failures, claim_sizes, sleeps = [], [], [], []

        class Connection:
            def execute(self, statement, params=None):
                return SimpleNamespace(fetchone=lambda: {"count": sum(not row["done"] for row in rows)})

        def claim(conn, platform, *, limit, lease_seconds):
            assert platform == "ownerclan" and lease_seconds >= 300
            claim_sizes.append(limit)
            result = [row for row in rows if row["eligible"] and not row["done"]][:limit]
            for row in result:
                row["eligible"] = False
            return result

        def complete(conn, row, product, *, collected_at):
            if row["external_product_id"] == save_error_key:
                raise ValueError("invalid product")
            assert product["productId"] == row["external_product_id"]
            assert product["collectedAt"] == collected_at
            row["done"] = True
            saved.append((row["external_product_id"], collected_at))

        def fail(conn, row, *, error, next_retry_at):
            failures.append((row["external_product_id"], error, next_retry_at))

        monkeypatch.setattr(hs, "claim_refreshes", claim)
        monkeypatch.setattr(hs, "complete_refresh", complete)
        monkeypatch.setattr(hs, "fail_refresh", fail)
        monkeypatch.setattr(hs.time, "sleep", sleeps.append)
        client = Client(*responses)
        result = hs.refresh_details(tmp_path, _config(tmp_path), client=client, connection=Connection(), limit=limit)
        return SimpleNamespace(result=result, saved=saved, failures=failures, queries=client.queries,
                               claim_sizes=claim_sizes, sleeps=sleeps)

    return run


def test_worker_splits_101_products_and_matches_reordered_keys(worker):
    keys = [f"K{i}" for i in range(101)]
    out = worker(keys, [{"itemsByKeys": [_item(key) for key in reversed(keys[:100])]},
                        {"itemsByKeys": [_item(keys[-1])]}])
    assert out.result["successCount"] == 101 and out.result["detailRefreshComplete"]
    assert [len(requested(q)) for q in out.queries] == [100, 1]
    assert out.claim_sizes == [100, 1]
    assert {key for key, _ in out.saved} == set(keys)
    assert len({at for _, at in out.saved[:100]}) == 1


def test_limit_counts_products_and_leaves_other_work_pending(worker):
    out = worker(["A", "B", "C"], [{"itemsByKeys": [_item("A"), _item("B")]}], limit=2)
    assert requested(out.queries[0]) == ["A", "B"]
    assert out.result["successCount"] == 2 and out.result["pendingCount"] == 1
    assert out.result["status"] == "paused"


def test_only_missing_keys_are_retried_and_nulls_never_complete_work(worker):
    out = worker(["A", "B"], [{"itemsByKeys": [None, _item("A")]}, {"itemsByKeys": [_item("B")]}])
    assert [requested(q) for q in out.queries] == [["A", "B"], ["B"]]
    assert out.result["successCount"] == 2 and not out.failures
    assert out.sleeps == [1]


def test_exhausted_missing_key_remains_failed_and_unfinished(worker):
    out = worker(["A", "B"], [{"itemsByKeys": [_item("A")]}, {"itemsByKeys": []}])
    assert out.result["successCount"] == out.result["failureCount"] == 1
    assert out.result["pendingCount"] == 1 and not out.result["detailRefreshComplete"]
    assert out.failures[0][0] == "B" and "Missing detail" in out.failures[0][1]


def test_one_product_save_failure_does_not_rollback_other_products(worker):
    out = worker(["A", "B", "C"], [{"itemsByKeys": [_item(k) for k in ["A", "B", "C"]]}],
                 save_error_key="B")
    assert {key for key, _ in out.saved} == {"A", "C"}
    assert out.result["successCount"] == 2 and out.result["failureCount"] == 1


@pytest.mark.parametrize("response", [{}, {"itemsByKeys": None}, {"itemsByKeys": {}},
    {"itemsByKeys": [_item("A"), _item("A")]}, {"itemsByKeys": [_item("X")]},
    {"itemsByKeys": [{}]}, {"itemsByKeys": [42]}])
def test_invalid_batch_never_completes_any_claim(worker, response):
    out = worker(["A", "B"], [response])
    assert not out.saved and len(out.queries) == 1
    assert out.result["failureCount"] == out.result["pendingCount"] == 2


def test_minimal_field_fallback_is_remembered_for_the_next_batch(worker):
    keys = [str(i) for i in range(101)]
    out = worker(keys, [OwnerclanGraphQLError([{"message": "Cannot query field fixedPrice"}]),
                        {"itemsByKeys": [_item(k) for k in keys[:100]]}, {"itemsByKeys": [_item(keys[-1])]}])
    assert out.result["detailRefreshComplete"]
    assert ["fixedPrice" in q for q in out.queries] == [True, False, False]


@pytest.mark.parametrize("error", [OwnerclanHttpError(403, "forbidden"), OwnerclanHttpError(429, "limited"),
    OwnerclanGraphQLError([{"message": "Too many requests"}]), BudgetStopped("provider_restricted"),
    RuntimeError("network failure")])
def test_api_failure_stops_new_batches_and_preserves_all_pending_work(worker, error):
    out = worker([str(i) for i in range(101)], [error])
    assert len(out.queries) == 1 and out.claim_sizes == [100]
    assert out.result["failureCount"] == 100 and out.result["pendingCount"] == 101


class PostSession:
    def __init__(self, *responses):
        self.responses = iter(responses)
        self.calls = []

    def get(self, *args, **kwargs):
        pytest.fail("Bulk detail must use POST JSON")

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        result = next(self.responses)
        if isinstance(result, Exception):
            raise result
        return result


def test_bulk_client_posts_json_and_refreshes_auth_on_401():
    provider = StubProvider(["old", "new"])
    session = PostSession(Response(401), Response(payload={"data": {"itemsByKeys": []}}))
    client = OwnerclanClient(provider, "production", RateLimiter(0), 10, 0, 60, session=session)
    query = items_by_keys_query(["A"])
    assert client.graphql(query) == {"itemsByKeys": []}
    assert [call[1]["headers"]["Authorization"] for call in session.calls] == ["Bearer old", "Bearer new"]
    assert all(call[1]["json"] == {"query": query} and "params" not in call[1] for call in session.calls)
    assert all(call[1]["timeout"] == (5, 45) for call in session.calls)


@pytest.mark.parametrize("status", [403, 429])
def test_bulk_client_stops_http_rejection_without_retry(status):
    session = PostSession(Response(status))
    client = OwnerclanClient(StubProvider(["token"]), "production", RateLimiter(0), 10, 3, 60, session=session)
    with pytest.raises(OwnerclanHttpError) as error:
        client.graphql(items_by_keys_query(["A"]))
    assert error.value.status_code == status and len(session.calls) == 1


def test_bulk_client_stops_graphql_rate_limit_without_retry():
    session = PostSession(Response(payload={"errors": [{"message": "Too many requests"}]}))
    client = OwnerclanClient(StubProvider(["token"]), "production", RateLimiter(0), 10, 3, 60, session=session)
    with pytest.raises(OwnerclanGraphQLError):
        client.graphql(items_by_keys_query(["A"]))
    assert len(session.calls) == 1


def test_bulk_client_network_retry_keeps_post_transport(monkeypatch):
    monkeypatch.setattr("ownerclan_API.api.client.time.sleep", lambda _: None)
    session = PostSession(requests.ReadTimeout(), Response(payload={"data": {"itemsByKeys": []}}))
    client = OwnerclanClient(StubProvider(["token"]), "production", RateLimiter(0), 10, 1, 60, session=session)
    assert client.graphql(items_by_keys_query(["A"])) == {"itemsByKeys": []}
    assert len(session.calls) == 2
