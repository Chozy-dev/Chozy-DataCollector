import json

import pytest
import requests

from scripts.probes import ownerclan_session_probe as probe


@pytest.fixture
def transport(monkeypatch):
    sent = []
    behavior = {}
    monkeypatch.setattr(probe.RateLimiter, "wait", lambda self: None)

    def send(self, request, **kwargs):
        sent.append((self, request, kwargs))
        assert request.method == "POST" and kwargs["timeout"] == (5, 45)
        assert self.max_retries.total == 0
        response = requests.Response()
        response.status_code = 200
        if request.url == probe.AUTH_ENDPOINTS["production"]:
            payload = {"token": "fixture-token"}
        else:
            if behavior.get("error"):
                raise behavior["error"]
            response.status_code = behavior.get("status", 200)
            query = json.loads(request.body)["query"]
            api = "itemsByKeys" if "itemsByKeys" in query else "item"
            key = "WFSQRWB" if "WFSQRWB" in query else "WFM25VZ"
            node = dict(key=key, price=1, status="available")
            payload = behavior.get("payload", {"data": {api: [node] if api == "itemsByKeys" else node}})
        response._content = json.dumps(payload).encode()
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    return sent, behavior


def test_complete_crossover_uses_one_auth_eight_reads_and_four_pools(transport):
    sent, _ = transport
    result = probe.run({}, lambda: None, credentials=lambda _: ("u", "p"))
    assert result["counts"] == dict(auth=1, reads=8, total=9)
    assert result["status"] == "complete"
    assert result["completion_seconds"] is not None
    pools = [id(a) for a, _, _ in sent[1:]]
    assert pools[0] == pools[2] and pools[1] == pools[3]
    assert pools[4] == pools[6] and pools[5] == pools[7]
    assert len(set(pools)) == 4
    assert all(c["assessment"]["complete"] for c in result["cases"])


@pytest.mark.parametrize("behavior", [
    {"error": requests.ReadTimeout("fixture")},
    {"status": 401}, {"status": 403}, {"status": 429},
    {"payload": {"errors": [{"message": "quota"}]}},
    {"payload": {"data": {"item": None}}},
    {"payload": {"data": {"item": []}}},
    {"payload": {"data": {}}},
    {"payload": {"data": {"item": {"key": "WRONG", "price": 1, "status": "available"}}}},
])
def test_first_error_stops_without_auth_refresh_or_further_requests(transport, behavior):
    sent, options = transport
    options.update(behavior)
    result = probe.run({}, lambda: None, credentials=lambda _: ("u", "p"))
    assert result["status"] == "stopped"
    assert result["completion_seconds"] is None
    assert result["counts"] == dict(auth=1, reads=1, total=2)
    assert len(sent) == 2


def test_global_budget_blocks_tenth_request_and_second_auth(monkeypatch):
    monkeypatch.setattr(probe.RateLimiter, "wait", lambda self: None)
    budget = probe.WireBudget()
    budget.reserve(probe.AUTH_ENDPOINTS["production"])
    with pytest.raises(probe.StopProbe, match="auth_cap"):
        budget.reserve(probe.AUTH_ENDPOINTS["production"])
    for _ in range(8):
        budget.reserve(probe.API_ENDPOINTS["production"])
    with pytest.raises(probe.StopProbe, match="read_cap"):
        budget.reserve(probe.API_ENDPOINTS["production"])
    assert budget.auth + budget.reads == 9


def test_offline_default_never_runs(monkeypatch):
    monkeypatch.setattr(probe, "run", lambda *a: pytest.fail("execution reached"))
    assert probe.main([]) == 0


def test_cross_session_pacing_uses_high_resolution_elapsed_time(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(probe.time, "perf_counter", lambda: now[0])
    monkeypatch.setattr(probe.time, "sleep", lambda seconds: now.__setitem__(0, now[0]+seconds))
    budget = probe.WireBudget()
    starts = []
    for url in [probe.AUTH_ENDPOINTS["production"], probe.API_ENDPOINTS["production"],
                probe.API_ENDPOINTS["production"]]:
        budget.reserve(url)
        starts.append(now[0])
        now[0] += .108  # A fast response still leaves a full three-second start gap.
    assert starts == [1000.0, 1003.0, 1006.0]
