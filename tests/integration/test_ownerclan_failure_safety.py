"""Real worker/storage/fences with HTTP fixtures; no supplier or production DB access."""
from contextlib import contextmanager
from datetime import timedelta
import json

import pytest
import requests

from ownerclan_API.api.client import OwnerclanClient
from ownerclan_API.api.rate_limiter import RateLimiter
from ownerclan_API.workflows import history_sync as hs
from product_data_collector.domain.product_events import utc
from product_data_collector.persistence.migrations import apply_migration
from product_data_collector.persistence.scoring import run_scoring,read_published
from product_data_collector.operations.fence import certify_collection
from tests.integration.test_ownerclan_batches import request,run,work
from tests.unit.ownerclan.test_ownerclan_auth_client import Response,StubProvider
from tests.unit.ownerclan.test_ownerclan_workflow import _item,_config


@pytest.fixture(autouse=True)
def no_network_or_wait(monkeypatch):
    def forbidden(*args,**kwargs):
        pytest.fail('Real HTTP is forbidden in failure safety tests')
    monkeypatch.setattr(requests.Session,'request',forbidden)
    monkeypatch.setattr(hs.time,'sleep',lambda _:None)


class FixtureHTTP:
    def __init__(self,*payloads):
        self.responses=iter(payloads)
        self.calls=[]

    def post(self,url,**kwargs):
        self.calls.append(kwargs)
        value=next(self.responses)
        if isinstance(value,Exception):
            raise value
        return Response(payload=value)


def client(*payloads,retries=0):
    transport=FixtureHTTP(*payloads)
    value=OwnerclanClient(StubProvider(['fixture-token']),'production',RateLimiter(0),15,retries,300,session=transport)
    return value,transport


@pytest.mark.parametrize('data,calls',[
    ({'itemsByKeys':None},1),({'itemsByKeys':[]},2),({},1),
    ({'itemsByKeys':{}},1),({'itemsByKeys':'invalid'},1),
    ({'itemsByKeys':42},1),({'itemsByKeys':False},1),
])
def test_http200_invalid_or_empty_retains_all_versions_then_resumes(db,tmp_path,data,calls):
    request(db,['A','B'])
    c,http=client(*[{'data':data} for _ in range(calls)])
    result=run(db,tmp_path,c)
    assert len(http.calls)==calls
    assert result['failureCount']==result['pendingCount']==2
    assert not result['detailRefreshComplete'] and result['status']!='completed'
    assert db.execute('SELECT count(*) n FROM products').fetchone()['n']==0
    for key in ('A','B'):
        target=work(db,key)
        assert target['requested_version']==1 and target['completed_version']==0
        assert target['refresh_status']=='retry' and target['failure_count']==1
        assert target['lease_token'] is None and target['lease_until'] is None
        assert target['next_retry_at']>utc(hs.clock())
        assert target['last_error'].startswith('retryable:')
    db.execute("UPDATE product_discovery_targets SET next_retry_at=now()-interval '1 second'")
    db.commit()
    c,http=client({'data':{'itemsByKeys':[_item('B'),_item('A')]}})
    assert run(db,tmp_path,c)['detailRefreshComplete']
    assert db.execute('SELECT count(*) n FROM product_history').fetchone()['n']==2
    assert run(db,tmp_path,c)['successCount']==0
    assert len(http.calls)==1  # Completed work is never requested again.


def test_all_read_timeouts_preserve_pending_then_successful_resume(db,tmp_path):
    request(db,['A','B'])
    c,http=client(requests.ReadTimeout(),requests.ReadTimeout(),requests.ReadTimeout(),retries=2)
    result=run(db,tmp_path,c)
    assert len(http.calls)==3
    assert all(k['timeout']==(5,45) for k in http.calls)
    assert result['failureCount']==result['pendingCount']==2
    assert db.execute('SELECT count(*) n FROM product_history').fetchone()['n']==0
    assert all(work(db,k)['refresh_status']=='retry' for k in ('A','B'))
    db.execute("UPDATE product_discovery_targets SET next_retry_at=now()-interval '1 second'")
    db.commit()
    c,http=client({'data':{'itemsByKeys':[_item('A'),_item('B')]}})
    assert run(db,tmp_path,c)['detailRefreshComplete']
    assert db.execute('SELECT count(*) n FROM product_history').fetchone()['n']==2


def test_partial_success_survives_all_supplemental_timeouts_and_resume(db,tmp_path):
    request(db,['A','B'])
    c,http=client({'data':{'itemsByKeys':[_item('A')]}},
                  requests.ReadTimeout(),requests.ReadTimeout(),requests.ReadTimeout(),retries=2)
    result=run(db,tmp_path,c)
    assert len(http.calls)==4
    assert result['successCount']==result['failureCount']==result['pendingCount']==1
    assert work(db,'A')['completed_version']==1
    assert work(db,'B')['completed_version']==0 and work(db,'B')['refresh_status']=='retry'
    assert db.execute('SELECT count(*) n FROM product_history').fetchone()['n']==1
    db.execute("UPDATE product_discovery_targets SET next_retry_at=now()-interval '1 second' WHERE external_product_id='B'")
    db.commit()
    c,http=client({'data':{'itemsByKeys':[_item('B')]}})
    assert run(db,tmp_path,c)['detailRefreshComplete']
    assert 'keys: ["B"]' in http.calls[0]['json']['query']
    assert db.execute('SELECT count(*) n FROM product_history').fetchone()['n']==2


def test_pending_work_created_after_scoring_start_blocks_publication(db,tmp_path,monkeypatch):
    from product_data_collector.persistence import scoring
    migrate_scoring(db,False)
    request(db,['A'])
    c,_=client({'data':{'itemsByKeys':[_item('A')]}})
    assert run(db,tmp_path,c)['detailRefreshComplete']
    at=utc(hs.clock())+timedelta(seconds=1)
    assert run_scoring(db,run_id='good',mode='initial',platform='ownerclan',as_of=at)['status']=='completed'
    prior=read_published(db,'OWNERCLAN')
    db.commit()
    publish=scoring.publish
    def newly_pending(conn,run):
        request(conn,['B'])
        return publish(conn,run)
    monkeypatch.setattr(scoring,'publish',newly_pending)
    with pytest.raises(RuntimeError,match='Detail refresh incomplete'):
        run_scoring(db,run_id='late-pending',mode='initial',platform='ownerclan',as_of=at)
    db.rollback()
    assert read_published(db,'OWNERCLAN')==prior
    assert not db.execute("SELECT 1 FROM supply_home_batches WHERE batch_id='late-pending'").fetchone()


def migrate_scoring(db,operations):
    for name in ('stage7_supply_scoring','stage7_history_policy_v2','stage10_scoring_performance'):
        apply_migration(db,name)
    if operations:
        apply_migration(db,'stage8_operations')


@pytest.mark.parametrize('operations',[False,True])
def test_pending_details_block_direct_scoring_and_new_publication(db,tmp_path,operations):
    migrate_scoring(db,operations)
    request(db,['A'])
    c,_=client({'data':{'itemsByKeys':[_item('A')]}})
    assert run(db,tmp_path,c)['detailRefreshComplete']
    if operations:
        certify_collection(db,'ownerclan','fixture-complete')
    at=utc(hs.clock())+timedelta(seconds=1)
    assert run_scoring(db,run_id='prior',mode='initial',platform='ownerclan',as_of=at)['status']=='completed'
    previous=read_published(db,'OWNERCLAN')
    db.commit()
    request(db,['B'])
    c,_=client({'data':{'itemsByKeys':None}})
    assert not run(db,tmp_path,c)['detailRefreshComplete']
    if operations:
        with pytest.raises(RuntimeError,match='Detail refresh incomplete'):
            certify_collection(db,'ownerclan','must-not-certify')
    # Replaying an already committed run is safe; it must not create a new batch.
    assert run_scoring(db,run_id='prior',mode='initial',platform='ownerclan',as_of=at)['status']=='completed'
    with pytest.raises(RuntimeError,match='Detail refresh incomplete'):
        run_scoring(db,run_id='unsafe',mode='initial',platform='ownerclan',as_of=at)
    db.rollback()
    assert read_published(db,'OWNERCLAN')==previous
    assert not db.execute("SELECT 1 FROM supply_home_batches WHERE batch_id='unsafe'").fetchone()
    assert not db.execute("SELECT 1 FROM scoring_runs WHERE run_id='unsafe'").fetchone()


def test_partial_worker_stops_real_operations_pipeline_and_resume_is_idempotent(db,db_factory,tmp_path,monkeypatch):
    from product_data_collector.operations.adapters import handlers,execution_fingerprint
    from product_data_collector.operations.runner import Runner
    migrate_scoring(db,True)
    request(db,['A','B'])
    cfg=_config(tmp_path)
    c,http=client({'data':{'itemsByKeys':[_item('A')]}},
                  {'data':{'itemsByKeys':[]}},
                  {'data':{'itemsByKeys':[_item('B')]}})
    monkeypatch.setattr('ownerclan_API.config.load_config',lambda *a,**kw:cfg)
    monkeypatch.setattr('ownerclan_API.api.factory.make_client',lambda *a,**kw:c)
    monkeypatch.setattr(hs,'collect_history',lambda *a,**kw:{'status':'completed'})
    # Keep the actual adapter/Runner/certification/scoring, bypass only budget transport wrapping.
    monkeypatch.setattr('product_data_collector.operations.adapters.BudgetSession',lambda session,*a,**kw:session)
    @contextmanager
    def connect():
        with db_factory() as conn:
            yield conn
    config={'sources':{'ownerclan':{'config':'fixture','account':'fixture'}},'require_platform_budget':False}
    runner=Runner(connect,handlers(config,connect),retry_seconds=0)
    scope={'config_hash':execution_fingerprint(config,'ownerclan')}
    scheduled=utc(hs.clock())
    assert runner.run('ownerclan',scheduled,scope)=='failed'
    assert work(db,'A')['completed_version']==1 and work(db,'B')['completed_version']==0, (
        list(db.execute('SELECT error,metrics FROM ops_jobs')),http.calls)
    assert db.execute('SELECT count(*) n FROM products').fetchone()['n']==1
    assert db.execute('SELECT count(*) n FROM scoring_runs').fetchone()['n']==0
    assert db.execute('SELECT count(*) n FROM ops_collection_receipts').fetchone()['n']==0
    assert read_published(db,'OWNERCLAN') is None
    db.execute("UPDATE product_discovery_targets SET next_retry_at=now()-interval '1 second' WHERE external_product_id='B'")
    db.commit()
    assert runner.run('ownerclan',scheduled,scope)=='completed'
    assert len(http.calls)==3
    assert 'keys: ["B"]' in http.calls[-1]['json']['query']
    assert db.execute('SELECT count(*) n FROM product_history').fetchone()['n']==2
    published=read_published(db,'OWNERCLAN')
    assert published is not None
    db.commit()
    assert runner.run('ownerclan',scheduled,scope)=='completed'
    assert len(http.calls)==3
    assert read_published(db,'OWNERCLAN')==published
