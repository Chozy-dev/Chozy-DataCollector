import json
import re
from datetime import timedelta

import pytest

from ownerclan_API.api.client import OwnerclanClient
from ownerclan_API.api.rate_limiter import RateLimiter
from ownerclan_API.services.normalization import normalize_item
from ownerclan_API.workflows import history_sync as hs
from product_data_collector.domain.product_events import utc
from product_data_collector.persistence.detail_refresh import claim_refreshes, complete_refresh, request_refreshes
from tests.unit.ownerclan.test_history_workflows import Client
from tests.unit.ownerclan.test_ownerclan_workflow import _config, _item


def request(db, keys):
    request_refreshes(db, "ownerclan", keys, observed_at=utc(hs.clock()) - timedelta(seconds=1), scope={})


def run(db, tmp_path, client, **kwargs):
    return hs.refresh_details(tmp_path, _config(tmp_path), connection=db, client=client, **kwargs)


def work(db, key):
    return db.execute("SELECT * FROM product_discovery_targets WHERE external_product_id=%s", (key,)).fetchone()


def test_101_details_use_two_batches_and_completed_work_is_not_fetched_again(db, tmp_path):
    keys = [f"K{i:03d}" for i in range(101)]
    request(db, keys)
    class Catalogue:
        def __init__(self):
            self.queries = []

        def graphql(self, query):
            self.queries.append(query)
            # DB claim order is independent of fixture/input order.
            batch = json.loads(re.search(r"keys: (\[.*?\])", query)[1])
            return {"itemsByKeys": [_item(k) for k in reversed(batch)]}

    client = Catalogue()
    assert run(db, tmp_path, client)["detailRefreshComplete"]
    assert [len(json.loads(re.search(r"keys: (\[.*?\])", q)[1])) for q in client.queries] == [100, 1]
    assert db.execute("SELECT count(*) AS n FROM products").fetchone()["n"] == 101
    assert db.execute("SELECT count(*) AS n FROM product_history").fetchone()["n"] == 101
    assert run(db, tmp_path, Client())["successCount"] == 0


def test_partial_missing_commits_success_and_resume_fetches_only_unfinished_key(db, tmp_path, monkeypatch):
    monkeypatch.setattr(hs.time, "sleep", lambda _: None)
    request(db, ["A", "B", "C"])
    client = Client({"itemsByKeys": [_item("A"), None]}, {"itemsByKeys": [_item("B")]})
    result = run(db, tmp_path, client)
    assert result["successCount"] == 2 and result["failureCount"] == result["pendingCount"] == 1
    assert work(db, "A")["completed_version"] == work(db, "B")["completed_version"] == 1
    assert work(db, "C")["completed_version"] == 0
    assert work(db, "C")["next_retry_at"] > utc(hs.clock())
    db.execute("UPDATE product_discovery_targets SET next_retry_at=now()-interval '1 second' WHERE external_product_id='C'")
    db.commit()
    retry = Client({"itemsByKeys": [_item("C")]})
    assert run(db, tmp_path, retry)["detailRefreshComplete"]
    assert 'keys: ["C"]' in retry.queries[0]
    assert db.execute("SELECT count(*) AS n FROM product_history").fetchone()["n"] == 3


def test_request_arriving_during_bulk_fetch_stays_pending_after_old_version_completes(db, tmp_path):
    request(db, ["A", "B"])

    class MoreWork:
        def graphql(self, query):
            request_refreshes(db, "ownerclan", ["A"], observed_at=hs.clock(), scope={"new": True})
            return {"itemsByKeys": [_item("A"), _item("B")]}

    result = run(db, tmp_path, MoreWork())
    assert result["pendingCount"] == 1 and not result["detailRefreshComplete"]
    target = work(db, "A")
    assert target["requested_version"] == 2 and target["completed_version"] == 1
    assert target["lease_token"] is None and work(db, "B")["completed_version"] == 1
    assert run(db, tmp_path, Client({"itemsByKeys": [_item("A")]}))["detailRefreshComplete"]
    assert work(db, "A")["completed_version"] == 2


def test_one_db_constraint_failure_preserves_other_products_and_retry_state(db, tmp_path):
    request(db, ["A", "B", "C"])
    db.execute("ALTER TABLE products ADD CONSTRAINT reject_bad_name CHECK (product_name<>'bad')")
    db.commit()
    result = run(db, tmp_path, Client({"itemsByKeys": [_item("A"), {**_item("B"), "name": "bad"}, _item("C")]}))
    assert result["successCount"] == 2 and result["failureCount"] == 1
    assert {row["external_product_id"] for row in db.execute("SELECT external_product_id FROM products")} == {"A", "C"}
    assert work(db, "B")["completed_version"] == 0 and work(db, "B")["failure_count"] == 1


def test_lost_completion_reply_reconciles_committed_item_and_continues_batch(db, tmp_path, monkeypatch):
    request(db, ["A", "B"])

    def lost(conn, claim, product, **kwargs):
        result = complete_refresh(conn, claim, product, **kwargs)
        if claim["external_product_id"] == "A":
            raise RuntimeError("commit response lost")
        return result

    monkeypatch.setattr(hs, "complete_refresh", lost)
    result = run(db, tmp_path, Client({"itemsByKeys": [_item("A"), _item("B")]}))
    assert result["successCount"] == 2 and result["failureCount"] == 0 and result["detailRefreshComplete"]
    assert db.execute("SELECT count(*) AS n FROM product_history").fetchone()["n"] == 2


@pytest.mark.parametrize("new_owner_completes", [False, True])
def test_stale_bulk_lease_never_overwrites_or_releases_new_owners_work(db, tmp_path, new_owner_completes):
    request(db, ["A", "B"])
    new_claim = []

    class Reclaimed:
        def graphql(self, query):
            db.execute("UPDATE product_discovery_targets SET lease_until=now()-interval '1 second' WHERE external_product_id='A'")
            db.commit()
            claim = claim_refreshes(db, "ownerclan", limit=1)[0]
            new_claim.append(claim)
            if new_owner_completes:
                observed = hs.clock()
                product = normalize_item({**_item("A"), "name": "new owner"}, observed)
                complete_refresh(db, claim, product, collected_at=observed)
            return {"itemsByKeys": [{**_item("A"), "name": "stale response"}, _item("B")]}

    result = run(db, tmp_path, Reclaimed())
    target = work(db, "A")
    if new_owner_completes:
        assert result["detailRefreshComplete"]
        assert db.execute("SELECT product_name FROM products WHERE external_product_id='A'").fetchone()["product_name"] == "new owner"
    else:
        assert result["failureCount"] == result["pendingCount"] == 1
        assert target["lease_token"] == new_claim[0]["lease_token"] and target["failure_count"] == 0
        assert not db.execute("SELECT 1 FROM products WHERE external_product_id='A'").fetchone()
    assert work(db, "B")["completed_version"] == 1


def test_crash_after_partial_batch_reclaims_only_unfinished_products(db, tmp_path, monkeypatch):
    request(db, ["A", "B", "C"])

    def crash(conn, claim, product, **kwargs):
        result = complete_refresh(conn, claim, product, **kwargs)
        if claim["external_product_id"] == "A":
            raise KeyboardInterrupt("simulated process interruption")
        return result

    monkeypatch.setattr(hs, "complete_refresh", crash)
    with pytest.raises(KeyboardInterrupt):
        run(db, tmp_path, Client({"itemsByKeys": [_item(k) for k in ["A", "B", "C"]]}))
    assert work(db, "A")["completed_version"] == 1
    db.execute("UPDATE product_discovery_targets SET lease_until=now()-interval '1 second' WHERE requested_version>completed_version")
    db.commit()
    monkeypatch.setattr(hs, "complete_refresh", complete_refresh)
    client = Client({"itemsByKeys": [_item("C"), _item("B")]})
    assert run(db, tmp_path, client)["detailRefreshComplete"]
    assert json.loads(re.search(r"keys: (\[.*?\])", client.queries[0])[1]) == ["B", "C"]
    assert db.execute("SELECT count(*) AS n FROM product_history").fetchone()["n"] == 3


def test_post_bulk_and_missing_retry_share_committed_api_budget(db, db_factory, tmp_path, monkeypatch):
    from product_data_collector.operations.budget import BudgetSession
    from product_data_collector.persistence.migrations import apply_migration
    from tests.unit.ownerclan.test_ownerclan_auth_client import Response, StubProvider

    monkeypatch.setattr(hs.time, "sleep", lambda _: None)
    for name in ["stage7_supply_scoring", "stage8_operations", "runtime_budget_guard"]:
        apply_migration(db, name)
    db.execute("INSERT INTO api_budget_policy(account,api,limits) VALUES('fixture-owner','default','{\"minute\":10}')")
    db.commit()
    request(db, ["A", "B"])

    class Transport:
        def __init__(self):
            self.calls = []

        def request(self, method, url, **kwargs):
            self.calls.append((method, kwargs))
            assert method == "POST" and "json" in kwargs and "params" not in kwargs
            return Response(payload={"data": {"itemsByKeys": [_item("A" if len(self.calls) == 1 else "B")]}})

    transport = Transport()
    session = BudgetSession(transport, db_factory, "fixture-owner", "default", shared_required=False)
    client = OwnerclanClient(StubProvider(["fixture-token"]), "production", RateLimiter(0), 10, 0, 60, session=session)
    assert run(db, tmp_path, client)["detailRefreshComplete"]
    assert len(transport.calls) == 2
    assert db.execute("SELECT sum(reserved) AS n FROM api_budget_usage WHERE account='fixture-owner' AND kind='minute'").fetchone()["n"] == 2
