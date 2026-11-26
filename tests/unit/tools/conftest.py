import hashlib
import json

import pytest

from scripts.probes import ownerclan_missing_items_probe, ownerclan_query_comparison


@pytest.fixture
def ownerclan_comparison_plan(tmp_path, monkeypatch):
    keys = [f"WTEST{index:03}" for index in range(100)]
    sample = tmp_path / "sample.json"
    sample.write_text(json.dumps({"sample_keys": keys}), encoding="utf-8")
    plan = {
        "sample": {
            "source": sample.name,
            "source_sha256": hashlib.sha256(sample.read_bytes()).hexdigest(),
            "ordered_keys_sha256": hashlib.sha256(json.dumps(keys, separators=(",", ":")).encode()).hexdigest(),
        },
        "phases": [{"requests": [{} for _ in range(8)]}],
        "transport": {"min_start_interval_seconds": 3},
    }
    (tmp_path / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
    query = ownerclan_missing_items_probe.items_by_keys_query(keys[40:60])
    measurement = {"calls": [{}, {}, {}, {"query_sha256": hashlib.sha256(query.encode()).hexdigest()}]}
    (tmp_path / "batch-measurement.json").write_text(json.dumps(measurement), encoding="utf-8")
    for probe in (ownerclan_query_comparison, ownerclan_missing_items_probe):
        monkeypatch.setattr(probe, "ROOT", tmp_path)
        monkeypatch.setattr(probe, "OUT", tmp_path)
