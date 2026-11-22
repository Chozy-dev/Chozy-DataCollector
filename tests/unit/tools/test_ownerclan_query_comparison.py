import pytest
import requests

from scripts.probes import ownerclan_query_comparison as probe


def test_plan_does_not_open_network_or_load_credentials(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("offline plan accessed credentials/network")
    monkeypatch.setattr(probe, "load_credentials", forbidden)
    monkeypatch.setattr(probe, "MeasuredSession", forbidden)
    assert probe.main([]) == 0


def test_invalid_and_duplicate_products_are_not_normal_acquisitions():
    result = probe.assess(["A", "B"], [
        {"key":"A", "price":1, "status":"available"},
        {"key":"A", "price":1, "status":"available"},
        {"key":"B", "price":True, "status":"available"},
        {"key":"C", "price":1, "status":"available"}, None])
    assert result["normal_products"] == 0
    assert result["duplicate_keys"] == result["unexpected_keys"] == 1
    assert not result["complete"]


def test_unordered_valid_products_are_complete():
    result = probe.assess(["A", "B"], [
        {"key":"B", "price":0, "status":"discontinued"},
        {"key":"A", "price":12.5, "status":"soldout"}])
    assert result["normal_products"] == 2
    assert result["complete"]


def test_auth_retry_is_blocked_before_transport(monkeypatch):
    calls = []
    def fake_request(self, method, url, **kwargs):
        calls.append((method, kwargs))
        response = requests.Response()
        response.status_code = 200
        response._content = b'{}'
        return response
    monkeypatch.setattr(requests.Session, "request", fake_request)
    with probe.MeasuredSession({"calls":[]}, lambda: None) as session:
        session.request("POST", probe.AUTH_ENDPOINTS["production"])
        with pytest.raises(probe.StopProbe, match="automatic_auth_retry_blocked"):
            session.request("POST", probe.AUTH_ENDPOINTS["production"])
    assert len(calls) == 1
    assert calls[0][1]["timeout"] == (5,45)
    assert calls[0][1]["allow_redirects"] is False


def test_coverage_plan_has_38_reads_without_network(monkeypatch,capsys):
    monkeypatch.setattr(probe,"load_credentials",lambda *a: pytest.fail("credentials accessed"))
    monkeypatch.setattr(probe,"MeasuredSession",lambda *a: pytest.fail("network opened"))
    assert probe.main(["--mode","coverage"]) == 0
    import json
    plan = json.loads(capsys.readouterr().out)
    assert plan["order"] == [20,50,100,5,10]
    assert sum(100//b for b in plan["order"]) == plan["reads"] == 38


def test_read_cap_and_timeout_stop_before_another_attempt(monkeypatch):
    attempts = []
    def fail(*args,**kwargs):
        attempts.append(1)
        raise requests.ReadTimeout("test")
    monkeypatch.setattr(requests.Session,"request",fail)
    with probe.MeasuredSession({"calls":[]},lambda:None,max_reads=1) as session:
        session.context = {"api":"itemsByKeys"}
        with pytest.raises(probe.StopProbe,match="timeout"):
            session.request("POST",probe.API_ENDPOINTS["production"])
        assert session.result["calls"][0]["timed_out"]
        with pytest.raises(probe.StopProbe,match="local_read_cap"):
            session.request("POST",probe.API_ENDPOINTS["production"])
    assert len(attempts) == 1


def test_partial_coverage_never_has_a_completion_time():
    case = dict(batch_size=20,planned_calls=5,batches=[],wall_seconds=45)
    calls = [dict(http_seconds=45,timed_out=True,status=None,graphql_errors=0,response_bytes=None)]
    probe.coverage_summary(case,calls)
    assert not case["complete"]
    assert case["completion_seconds"] is None
    assert case["unrequested_products"] == 80
    assert case["not_obtained_products"] == 100
    assert case["response_missing_products"] == 0


def test_null_response_is_not_confused_with_a_complete_empty_result():
    result = probe.assess(["A"],None)
    assert result["response_type"] == "NoneType"
    assert result["missing"] == 1
    assert result["normal_products"] == 0
    assert not result["complete"]


@pytest.mark.parametrize("field", ["key", "status"])
@pytest.mark.parametrize("value", [[], {}])
def test_malformed_product_fields_are_reported_instead_of_crashing(field, value):
    node = dict(key="A", price=1, status="available")
    node[field] = value
    result = probe.assess(["A"], [node])
    assert not result["complete"]
    assert result["normal_products"] == 0
    assert result["invalid_or_duplicate_rows"] == 1


@pytest.mark.parametrize("during_body", [False, True])
def test_timeout_records_header_or_body_phase_and_closes_response(monkeypatch, during_body):
    closed = []

    class Body:
        def stream(self, *args, **kwargs):
            raise requests.ConnectionError("read timed out")
            yield b""  # Make this a streaming iterator.

        def close(self):
            closed.append(True)

        def release_conn(self):
            pass

    def transport(self, method, url, **kwargs):
        assert kwargs["stream"] is True
        if not during_body:
            raise requests.ReadTimeout("before headers")
        response = requests.Response()
        response.status_code = 200
        response.raw = Body()
        return response

    monkeypatch.setattr(requests.Session, "request", transport)
    with probe.MeasuredSession({"calls": []}, lambda: None) as session:
        with pytest.raises(probe.StopProbe, match="timeout"):
            session.post(probe.API_ENDPOINTS["production"], json={"query": "test"})
        call = session.result["calls"][0]
    assert call["failure_phase"] == ("reading_body" if during_body else "awaiting_headers")
    assert call["status"] == (200 if during_body else None)
    assert call["response_bytes"] is None
    assert call["timed_out"]
    assert len(closed) == int(during_body)


def test_all_disjoint_batches_required_for_100_product_completion():
    batches = [dict(normal_products=20,missing=0,duplicate_keys=0,
                    unexpected_keys=0,complete=True) for _ in range(5)]
    case = dict(batch_size=20,planned_calls=5,batches=batches,wall_seconds=30)
    calls = [dict(http_seconds=5,timed_out=False,status=200,graphql_errors=0,
                  response_bytes=100) for _ in range(5)]
    probe.coverage_summary(case,calls)
    assert case["complete"]
    assert case["completion_seconds"] == 30
    assert case["normal_unique_products"] == 100
    assert case["unrequested_products"] == 0
