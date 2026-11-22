"""A persistent missing product must not stop the rest of the live sample."""
import json
import re
from datetime import datetime, timezone
from types import SimpleNamespace

from product_data_collector.persistence.detail_refresh import request_refreshes
from ownerclan_API.workflows.history_sync import refresh_details
from scripts.probes.ownerclan_live_e2e import SampleClient
from tests.unit.ownerclan.test_ownerclan_workflow import _config, _item


def test_probe_preserves_real_worker_continuation_after_missing(db, tmp_path, monkeypatch):
    monkeypatch.setattr('ownerclan_API.workflows.history_sync.time.sleep', lambda _: None)
    request_refreshes(db, 'ownerclan', [f'PROBE-{i}' for i in range(200)],
                      observed_at=datetime.now(timezone.utc), scope={'test': 'missing-continuation'})
    result = {'calls': []}
    requests = []
    missing = []

    def graphql(query):
        keys = json.loads(re.search(r'keys: (\[.*?\])', query)[1])
        requests.append(keys)
        result['calls'].append({})
        if len(requests) == 1:
            missing.append(keys[-1])
            keys = keys[:-1]
        elif len(requests) == 2:
            assert keys == missing
            keys = []
        return {'itemsByKeys': [_item(key) for key in keys]}

    session = SimpleNamespace(stopped=False)
    client = SampleClient(SimpleNamespace(graphql=graphql), session, result, lambda: None)
    outcome = refresh_details(tmp_path, _config(tmp_path), client=client, connection=db, limit=200)
    assert list(map(len, requests)) == [100, 1, 100]
    assert outcome['successCount'] == 199
    assert outcome['failureCount'] == 1
    assert outcome['pendingCount'] == 1
    assert not session.stopped
    assert db.execute('SELECT count(*) AS n FROM products').fetchone()['n'] == 199
    unresolved = db.execute('SELECT external_product_id FROM product_discovery_targets WHERE requested_version>completed_version').fetchall()
    assert unresolved == [{'external_product_id': missing[0]}]
