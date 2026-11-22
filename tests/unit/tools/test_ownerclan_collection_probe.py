import json

import pytest

from scripts.probes import ownerclan_collection_probe as probe


def test_default_plan_never_loads_credentials_or_opens_network(monkeypatch, capsys):
    def forbidden(*args, **kwargs):
        pytest.fail("offline plan attempted credential or network access")

    monkeypatch.setattr(probe, "load_credentials", forbidden)
    monkeypatch.setattr(probe.requests, "Session", forbidden)
    assert probe.main([]) == 0
    result = json.loads(capsys.readouterr().out)
    assert not result["plan"]["execute"]


def test_probe_refuses_unbounded_rates_before_opening_network(monkeypatch):
    monkeypatch.setattr(probe.requests, "Session", lambda: pytest.fail("network opened"))
    with pytest.raises(SystemExit, match="2"):
        probe.main(["--execute", "--rates", "200"])


def test_null_duplicate_and_unexpected_rows_do_not_inflate_product_coverage():
    assessment = probe.assess(["A", "B"], [{"key": "A"}, {"key": "A"}, None, {"key": "C"}])
    assert assessment["returned"] == 1
    assert assessment["missing_keys"] == ["B"]
    assert assessment["duplicate_keys"] == ["A"]
    assert assessment["unexpected_keys"] == ["C"]
    assert assessment["null_rows"] == 1


def test_real_rolling_minute_excludes_start_at_exact_window_end():
    calls = [{"offset_seconds": offset, "http_seconds": .1, "ok": True}
             for offset in (0, .5, 60, 60.5)]
    result = probe.summarize_rate(calls, 61)
    assert result["peak_starts_in_60_seconds"] == 2
    assert result["attempts"] == result["ok"] == 4
