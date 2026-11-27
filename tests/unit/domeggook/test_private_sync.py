"""Private responses here are DOCUMENTATION-BASED fixtures, never live evidence."""
from unittest.mock import Mock
from urllib.parse import parse_qs, urlsplit
import requests
import pytest

from domeggook_API.api.private_sync import PrivateSyncClient, parse_search_page
from domeggook_API.api.client import (
    DomeggookClient, PrivateDetailClient, CallBudgetExhausted, DomeggookHttpError, DomeggookApiError,
)
from domeggook_API.api.rate_limiter import RateLimiter
from domeggook_API.workflows.run_budget import RunBudget, PersistentCallBudget
from domeggook_API.workflows.product_sync import sync_signal


START = "2026-11-19 00:00:00"
END = "2026-11-19 23:59:59"


def page(rows=(), after=None):
    return {"result": True, "data": {"totalCount": len(rows), "list": list(rows), "searchAfter": after}}


def signal(no="10", modified="2026-11-19 10:00:00"):
    return {"no": no, "dateModify": modified, "status": "판매중"}


def detail(no="10", price=100, adjustment=0, hidden=0):
    import json
    return {"basis": {"no": no, "title": "sample"}, "price": {"supply": price, "dome": 999},
            "qty": {"inventory": 20}, "seller": {"score": {"avg": "95%", "cnt": 10}},
            "selectOpt": json.dumps({"type": "combination", "data": {
                "k": {"name": "red", "supPrice": adjustment, "domPrice": 555, "qty": 20, "hid": hidden, "sup": 1}}})}


def private(status=200, payload=None, budget=None, retries=0):
    session = Mock()
    session.post.return_value = Mock(status_code=status, headers={}, json=lambda: payload or page())
    client = PrivateSyncClient("TEST_ONLY", session=session, rate_limiter=Mock(),
                              request_budget=budget or RunBudget(10), max_retries=retries)
    return client, session


def test_documented_request_and_response():
    client, session = private(payload=page([signal()], [10]))
    assert parse_search_page(client.search(START, END, after=[20])) == ([signal()], [10])
    args = session.post.call_args.kwargs
    assert args["json"]["search_after"] == [20]
    assert args["headers"] == {"Authorization": "Bearer TEST_ONLY"}
    assert args["json"]["query"]["bool"]["filter"][1]["range"]["dateModify"] == {"gte": START, "lte": END}


def test_private_catalogue_request_and_es_detail_share_budget():
    budget = RunBudget(2)
    search, session = private(budget=budget)
    search.search()
    body = session.post.call_args.kwargs["json"]
    assert body["query"]["bool"]["filter"] == [{"term": {"channel.supply": True}}]
    assert "search_after" not in body

    session.get.return_value = Mock(status_code=200, json=lambda: {"domeggook": {"item": [detail()]}})
    details = PrivateDetailClient("PRIVATE_FIXTURE", RateLimiter(100), session=session, request_budget=budget)
    details.get_item_view(["10", "9"])
    params = parse_qs(urlsplit(session.get.call_args.args[0]).query)
    assert params == {"ver": ["4.0"], "mode": ["getItemViewES"], "om": ["json"],
                      "no": ["10,9"], "market": ["supply"], "aid": ["PRIVATE_FIXTURE"]}
    with pytest.raises(CallBudgetExhausted):
        details.get_item_view(["10"])
    assert session.get.call_count == 1


@pytest.mark.parametrize(("start", "end"), [
    ("2026-11-19T00:00:00+09:00", END),
    (START, "2026-11-19 23:59:59Z"),
    ("2026-11-31 00:00:00", END),
    ("2026-11-19 0:00:00", END),
    (END, START),
])
def test_invalid_date_modify_range_does_not_send_or_consume_budget(start, end):
    client, session = private()
    with pytest.raises(ValueError, match="Private dateModify"):
        client.search(start, end)
    session.post.assert_not_called()
    client.limiter.wait.assert_not_called()
    assert client.budget.used_api_calls == 0


@pytest.mark.parametrize("payload", [{}, {"result": False}, {"result": True, "data": {}},
    page([{}]), page([], [10]), page([], "bad")])
def test_malformed_pages_fail_closed(payload):
    with pytest.raises((ValueError, DomeggookApiError)):
        parse_search_page(payload)


@pytest.mark.parametrize("status", [401, 403, 429, 500])
def test_http_failure_is_distinct(status):
    client, session = private(status=status)
    with pytest.raises(DomeggookHttpError) as caught:
        client.search(START, END)
    assert caught.value.status_code == status
    assert session.post.call_count == 1


def test_retry_consumes_budget_and_does_not_send_extra_request(monkeypatch):
    monkeypatch.setattr("domeggook_API.api.private_sync.time.sleep", lambda _: None)
    budget = RunBudget(1)
    client, session = private(status=429, budget=budget, retries=3)
    with pytest.raises(CallBudgetExhausted):
        client.search(START, END)
    assert session.post.call_count == budget.used_api_calls == 1


def test_network_retry_and_invalid_json(monkeypatch):
    monkeypatch.setattr("domeggook_API.api.private_sync.time.sleep", lambda _: None)
    client, session = private(retries=1)
    session.post.side_effect = requests.Timeout()
    with pytest.raises(DomeggookApiError, match="Timeout"):
        client.search(START, END)
    assert client.budget.used_api_calls == 2
    session.post.side_effect = None
    session.post.return_value.json = Mock(side_effect=ValueError())
    with pytest.raises(DomeggookApiError, match="JSON"):
        client.search(START, END)


def test_signal_dedupe_no_invented_time_or_prices():
    a = sync_signal(signal(), "2026-11-19T01:00:00Z").record()
    b = sync_signal(signal(), "2026-11-19T02:00:00Z").record()
    assert a["dedupe_key"] == b["dedupe_key"]
    assert a["occurred_at"] is a["verified_before"] is a["verified_after"] is None
    assert a["value_status"] == "signal_only" and a["identity_status"] == "ambiguous"


def test_persistent_budget_survives_restart(tmp_path):
    path = tmp_path / "budget.json"
    PersistentCallBudget(path, 1).reserve_call()
    with pytest.raises(CallBudgetExhausted):
        PersistentCallBudget(path, 1).reserve_call()


def test_open_detail_supply_and_failed_attempt_budget(monkeypatch):
    budget = RunBudget(1)
    session = Mock()
    session.get.side_effect = requests.Timeout()
    monkeypatch.setattr("domeggook_API.api.client.time.sleep", lambda _: None)
    client = DomeggookClient("TEST_ONLY", RateLimiter(100), session=session, request_budget=budget)
    with pytest.raises(CallBudgetExhausted):
        client.get_item_view(["10"])
    assert "market=supply" in session.get.call_args.args[0]
    assert session.get.call_count == 1
