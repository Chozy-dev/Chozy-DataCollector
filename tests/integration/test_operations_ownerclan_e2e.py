"""Real runner/adapters/storage/scoring with an in-memory supplier transport."""
import json
import re
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone

from product_data_collector.operations.adapters import handlers, execution_fingerprint
from product_data_collector.operations.runner import Runner
from product_data_collector.persistence.migrations import apply_migration
from tests.unit.ownerclan.test_history_workflows import history, page
from tests.unit.ownerclan.test_ownerclan_workflow import _config, _item


def test_ownerclan_initial_incremental_and_missing_detail_resume(db, db_factory, tmp_path, monkeypatch):
    for name in ('stage7_supply_scoring', 'stage8_operations',
                 'stage7_history_policy_v2', 'stage10_scoring_performance'):
        apply_migration(db, name)
    import ownerclan_API.config as source_config
    import ownerclan_API.api.factory as factory
    import ownerclan_API.workflows.history_sync as hs
    monkeypatch.setattr(source_config, 'load_config', lambda *_: _config(tmp_path))
    monkeypatch.setattr(hs.time, 'sleep', lambda _: None)

    class FixtureClient:
        _session = object()
        _jwt_provider = SimpleNamespace()
        missing = False
        price = 1000

        def graphql(self, query):
            if 'itemHistories' in query:
                return page([history('stockChanged')])
            keys = json.loads(re.search(r'keys: (\[.*?\])', query)[1])
            return {'itemsByKeys': [] if self.missing else [
                {**_item(key), 'price': self.price, 'status': 'available',
                 'options': [{'key': 'fixture-option', 'optionAttributes': [], 'price': self.price, 'quantity': 100}],
                 'guaranteedShippingPeriod': 1} for key in keys]}

    client = FixtureClient()
    monkeypatch.setattr(factory, 'make_client', lambda *_: client)
    cfg = {'sources': {'ownerclan': {'config': 'ownerclan_API/config/config.yaml', 'account': 'fixture'}},
           'scoring_mode': 'initial', 'chunk_size': 1}
    runner = Runner(db_factory, handlers(cfg, db_factory), retry_seconds=0)
    slot = datetime.now(timezone.utc)

    def run(job_id):
        scope = {'scoring_mode': cfg['scoring_mode'], 'config_hash': execution_fingerprint(cfg, 'ownerclan')}
        return runner.run('ownerclan', slot, scope, job_id)

    assert run('fixture-initial') == 'completed'
    first = db.execute("SELECT read_supply_home('OWNERCLAN',10) AS r").fetchone()['r']
    assert first and len(first['items']) == 1, db.execute('SELECT current_supply_price,supply_price_status,sellable FROM products').fetchall()
    assert first['formulaVersion'] == 'supply_v2'
    assert db.execute('SELECT reliability FROM product_supply_scores').fetchone()['reliability'] is None
    assert db.execute('SELECT count(*) AS n FROM scoring_coverage').fetchone()['n'] == 0
    db.commit()

    cfg['scoring_mode'] = 'incremental'
    client.missing = True
    slot += timedelta(seconds=1)
    assert run('fixture-incremental') in ('paused', 'failed')
    assert db.execute("SELECT read_supply_home('OWNERCLAN',10) AS r").fetchone()['r'] == first
    assert db.execute("SELECT count(*) AS n FROM scoring_runs WHERE mode='incremental'").fetchone()['n'] == 0
    db.execute("UPDATE product_discovery_targets SET next_retry_at=now()-interval '1 second'")
    db.commit()
    client.missing = False
    client.price = 2000
    assert run('fixture-incremental') == 'completed'
    latest = db.execute("SELECT read_supply_home('OWNERCLAN',10) AS r").fetchone()['r']
    assert latest['batchId'] != first['batchId']
    assert len(latest['items']) == 1
    assert db.execute('SELECT current_supply_price FROM products').fetchone()['current_supply_price'] == 2000
    assert db.execute('SELECT count(*) AS n FROM scoring_queue').fetchone()['n'] == 0
    assert run('fixture-incremental') == 'completed'
