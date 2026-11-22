from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

import pytest
from psycopg.types.json import Jsonb

from product_data_collector.persistence.migrations import apply_migration
from product_data_collector.persistence.scoring import run_scoring, read_published
from product_data_collector.operations.fence import scoring_boundary,certify_collection,source_collection_boundary
from product_data_collector.operations.runner import Runner

AT = datetime(2026, 11, 19, tzinfo=timezone.utc)


@pytest.fixture
def ops(db, db_factory):
    for name in ('stage5_coupang_best', 'stage6_naver_season', 'stage7_supply_scoring', 'stage8_operations', 'stage10_scoring_performance', 'runtime_budget_guard'):
        apply_migration(db, name)
    return db


def product(db, key='A', source='domeggook'):
    row = db.execute("""INSERT INTO products(platform,external_product_id,product_name,first_seen_at,last_collected_at)
        VALUES(%s,%s,%s,%s,%s) RETURNING id""", (source,key,key,AT,AT)).fetchone()
    db.commit()
    certify_collection(db,source,'fixture-only')
    return row['id']


def score(db, rid='s', **kwargs):
    if not db.execute("SELECT 1 FROM product_discovery_targets WHERE platform='domeggook' AND requested_version>completed_version LIMIT 1").fetchone():
        db.commit()
        certify_collection(db,'domeggook','fixture-only')
    return run_scoring(db, run_id=rid, mode='initial', platform='domeggook', as_of=AT+timedelta(days=1), **kwargs)


def test_external_writer_blocked_source_independent(ops, db_factory):
    pid=product(ops)
    other=db_factory()
    with scoring_boundary(ops,'f','domeggook'):
        with pytest.raises(Exception, match='fenced'):
            other.execute('UPDATE products SET product_name=%s WHERE id=%s',('changed',pid))
        other.rollback()
        product(other,'B','ownerclan')
    with source_collection_boundary(ops,'domeggook'):
        with pytest.raises(Exception, match='fenced'):
            other.execute('UPDATE products SET product_name=%s WHERE id=%s',('racing',pid))
        other.rollback()
    other.execute('UPDATE products SET product_name=%s WHERE id=%s',('changed',pid))
    other.commit()
    with pytest.raises(RuntimeError,match='unverified'):
        run_scoring(ops,run_id='uncertified',mode='initial',platform='domeggook',as_of=AT+timedelta(days=1))


def test_inflight_writer_defers_score(ops, db_factory):
    pid=product(ops)
    other=db_factory()
    other.execute('UPDATE products SET product_name=%s WHERE id=%s',('inflight',pid))
    with pytest.raises(RuntimeError, match='writers active'): score(ops)
    other.rollback()
    assert score(ops)['status']=='completed'


def test_resume_rejects_changed_inputs_preserves_publication(ops):
    pid=product(ops)
    score(ops,'good')
    prior=read_published(ops,'DOMEME')
    ops.commit()
    assert score(ops,'partial',max_chunks=1)['status']=='running'
    ops.execute('UPDATE products SET product_name=%s WHERE id=%s',('new',pid));ops.commit()
    with pytest.raises(ValueError,match='new run-id'): score(ops,'partial')
    assert read_published(ops,'DOMEME')==prior
    ops.commit()
    assert score(ops,'new')['status']=='completed'


def test_detail_pending_blocks(ops):
    from product_data_collector.persistence.detail_refresh import request_refreshes
    product(ops)
    request_refreshes(ops,'domeggook',['A'],observed_at=AT,scope='fixture')
    with pytest.raises(RuntimeError,match='incomplete'): score(ops)
    assert read_published(ops,'DOMEME') is None


def test_shutdown_does_not_publish_partial(ops):
    product(ops)
    assert score(ops,stop_requested=lambda:True)['status']=='running'
    assert read_published(ops,'DOMEME') is None
    ops.commit()
    assert score(ops)['status']=='completed'


def reserve(conn, shared=False):
    row=conn.execute("SELECT reserve_api_call('acct','api',%s) AS r",(shared,)).fetchone()['r']
    conn.commit()
    return row


def policy(db, limits, integrated=False):
    db.execute("INSERT INTO api_budget_policy(account,api,limits,platform_integrated) VALUES('acct','api',%s,%s)",
               (Jsonb(limits),integrated)); db.commit()


@pytest.mark.parametrize('kind',['second','minute','day','month'])
def test_budget_periods_and_no_refund(ops,kind):
    policy(ops,{kind:1})
    assert reserve(ops)=='reserved'
    ops.rollback()
    assert reserve(ops)=='exhausted:'+kind


def test_platform_unintegrated_fails_closed(ops):
    policy(ops,{'day':10})
    assert reserve(ops,True)=='platform_unintegrated'
    assert ops.execute('SELECT count(*) n FROM api_budget_usage').fetchone()['n']==0


def test_shared_budget_concurrency(ops,db_factory):
    policy(ops,{'day':7,'month':7},True)
    conns=[db_factory() for _ in range(20)]
    with ThreadPoolExecutor(max_workers=20) as pool:
        results=list(pool.map(lambda c:reserve(c,True),conns))
    assert results.count('reserved')==7
    assert ops.execute('SELECT sum(reserved) n FROM api_budget_usage').fetchone()['n']==14


def make_runner(db_factory, handlers, **kwargs):
    @contextmanager
    def connect():
        with db_factory() as conn: yield conn
    return Runner(connect,handlers,retry_seconds=0,**kwargs)


def test_step_resume_no_repeat_and_dedup(ops,db_factory):
    calls=[]
    def collect(c,j): calls.append('collect'); return {'status':'completed','count':2}
    def fail(c,j): calls.append('score'); raise RuntimeError('private data')
    r=make_runner(db_factory,{'domeggook':[collect,fail]})
    assert r.run('domeggook',AT,{})=='failed'
    assert ops.execute('SELECT step FROM ops_jobs').fetchone()['step']==1
    ops.commit()
    r.handlers['domeggook'][1]=lambda c,j:{'status':'completed'}
    assert r.run('domeggook',AT,{})=='completed'
    assert r.run('domeggook',AT.astimezone(timezone(timedelta(hours=9))),{})=='completed'
    assert calls==['collect','score']
    assert ops.execute('SELECT error FROM ops_jobs').fetchone()['error'] is None


def test_source_failure_independent_retry_bounded(ops,db_factory):
    def fail(c,j): raise RuntimeError('secret')
    r=make_runner(db_factory,{'domeggook':[fail],'ownerclan':[lambda c,j:'completed']},max_attempts=1)
    assert r.run('domeggook',AT,{})=='failed'
    assert r.run('ownerclan',AT,{})=='completed'
    assert r.run('domeggook',AT,{})=='failed'
    assert ops.execute("SELECT attempts,error FROM ops_jobs WHERE task='domeggook'").fetchone()=={'attempts':1,'error':'attempts_exhausted'}


def test_duplicate_lock_and_restart_running(ops,db_factory):
    r=make_runner(db_factory,{'domeggook':[lambda c,j:'completed']})
    ops.execute("SELECT pg_advisory_lock(hashtextextended('ops-task:domeggook',0))");ops.commit()
    assert r.run('domeggook',AT,{})=='paused'
    ops.execute("SELECT pg_advisory_unlock_all()");ops.commit()
    ops.execute("INSERT INTO ops_jobs(job_id,task,scheduled_at,scope,status) VALUES('dead','domeggook',%s,'{}','running')",(AT,));ops.commit()
    assert r.run('domeggook',AT,{},'dead')=='completed'


def test_old_pending_blocks_new_slot(ops,db_factory):
    r=make_runner(db_factory,{'naver':[lambda c,j:'paused']})
    assert r.run('naver',AT,{})=='paused'
    assert r.run('naver',AT+timedelta(days=1),{})=='paused'
    assert ops.execute('SELECT max(attempts) n FROM ops_jobs').fetchone()['n']==1


def test_status_read_contract(ops):
    from product_data_collector.operations.status import report
    assert 'usage' in report(ops,{})


def test_future_observation_even_unchanged_score_is_blocked(ops):
    pid=product(ops)
    score(ops,'good')
    ops.execute('UPDATE products SET last_collected_at=%s WHERE id=%s',(AT+timedelta(days=3),pid));ops.commit()
    with pytest.raises(ValueError,match='as_of precedes'): score(ops,'bad')
    assert score(ops,'good')['status']=='completed'  # committed-result replay


def test_commit_response_loss_replays_durable_step(ops,db_factory):
    calls=[]
    class CommitLost:
        def __init__(self,c): self.c=c; self.count=0
        def __getattr__(self,k): return getattr(self.c,k)
        def commit(self):
            self.c.commit();self.count+=1
            if self.count==3: raise ConnectionError('commit acknowledgement lost')
    @contextmanager
    def connect():
        with db_factory() as c: yield CommitLost(c)
    def step(c,j): calls.append(1);return 'completed'
    r=Runner(connect,{'naver':[step]},retry_seconds=0)
    assert r.run('naver',AT,{})=='failed'
    r=make_runner(db_factory,{'naver':[step]})
    assert r.run('naver',AT,{})=='completed'
    assert calls==[1]


def test_budget_timeout_and_restriction_are_durable(ops,db_factory):
    from product_data_collector.operations.budget import BudgetSession,BudgetStopped
    policy(ops,{'day':3},True)
    @contextmanager
    def connect():
        with db_factory() as c: yield c
    class HTTP:
        def request(self,*a,**kw): raise TimeoutError()
    transport=BudgetSession(HTTP(),connect,'acct','api')
    with pytest.raises(TimeoutError): transport.get('https://fixture.invalid')
    assert ops.execute('SELECT reserved FROM api_budget_usage').fetchone()['reserved']==1
    ops.commit()
    class Response: status_code=429
    class Limited:
        def request(self,*a,**kw): return Response()
    transport.session=Limited()
    with pytest.raises(BudgetStopped): transport.get('https://fixture.invalid')
    assert reserve(ops,True)=='blocked'
    assert ops.execute('SELECT reserved FROM api_budget_usage').fetchone()['reserved']==2


def test_budget_missing_helper_stops_before_reservation_or_http(ops, db_factory):
    from product_data_collector.operations.budget import BudgetSession, BudgetStopped
    policy(ops, {'day':3}, True)
    ops.execute('DROP FUNCTION block_api_account(text,text)')
    ops.commit()
    class HTTP:
        def request(self,*args,**kwargs):
            pytest.fail('Missing deployment helper must stop before HTTP')
    session = BudgetSession(HTTP(),db_factory,'acct','api')
    with pytest.raises(BudgetStopped,match='runtime_budget_guard_missing'):
        session.get('https://fixture.invalid')
    assert ops.execute('SELECT count(*) AS n FROM api_budget_usage').fetchone()['n'] == 0


def test_job_scope_cannot_change(ops,db_factory):
    r=make_runner(db_factory,{'naver':[lambda c,j:'completed']})
    assert r.run('naver',AT,{'x':1})=='completed'
    assert r.run('naver',AT,{'x':2})=='failed'
    assert ops.execute('SELECT scope FROM ops_jobs').fetchone()['scope']=={'x':1}


def test_superseded_job_does_not_block_new_work(ops,db_factory):
    r=make_runner(db_factory,{'naver':[lambda c,j:'paused']})
    r.run('naver',AT,{})
    ops.execute("UPDATE ops_jobs SET superseded_by='replacement'");ops.commit()
    r.handlers['naver']=[lambda c,j:'completed']
    assert r.run('naver',AT+timedelta(days=1),{},'replacement')=='completed'


def test_truncate_input_rejected(ops):
    with pytest.raises(Exception,match='truncated'):
        ops.execute('TRUNCATE product_events')
    ops.rollback()


def test_adapter_dependency_and_pinned_clock(ops,db_factory,tmp_path,monkeypatch):
    from product_data_collector.operations.adapters import handlers,execution_fingerprint
    cfg={'sources':{'domeggook':{'config':str(tmp_path/'source.yaml'),'state_dir':str(tmp_path/'state'),
        'account':'fixture','private_rpm':1,'provider_timezone':'Asia/Seoul','initial_start':AT.isoformat()}}}
    (tmp_path/'source.yaml').write_text('{}')
    h=handlers(cfg,db_factory)
    assert len(h['domeggook'])==2
    job={'job_id':'adapter','scope':{'config_hash':execution_fingerprint(cfg)},'metrics':{}}
    ops.execute("INSERT INTO ops_jobs(job_id,task,scheduled_at,scope) VALUES('adapter','domeggook',%s,%s)",(AT,Jsonb(job['scope'])));ops.commit()
    product(ops)
    import domeggook_API.config as source_config
    import domeggook_API.api.client as source_client
    import domeggook_API.workflows.product_sync as workflow
    monkeypatch.setattr(source_config,'load_config',lambda *_:object())
    monkeypatch.setattr(source_config,'load_api_keys',lambda *_:['fixture'])
    class Client: _session=object()
    monkeypatch.setattr(source_client,'create_domeggook_client',lambda *_:Client())
    monkeypatch.setenv('DOMEGGOOK_PRIVATE_API_KEY','fixture')
    def collect(conn,*args,**kwargs):
        conn.execute("UPDATE products SET product_name='collected' WHERE platform='domeggook'")
        conn.commit()
        return {'status':'completed'}
    monkeypatch.setattr(workflow,'incremental',collect)
    assert h['domeggook'][0](ops,job)['status']=='completed'
    assert ops.execute('SELECT job_id FROM ops_collection_receipts').fetchone()['job_id']=='adapter'
    ops.commit()
    assert h['domeggook'][1](ops,job)['status']=='completed'
    assert 'as_of' in job['metrics']


def test_adapter_scope_file_change_rejected(ops,db_factory,tmp_path):
    from product_data_collector.operations.adapters import handlers,execution_fingerprint
    cfg={'sources':{'domeggook':{'config':str(tmp_path/'source.yaml')}}}
    (tmp_path/'source.yaml').write_text('{}')
    h=handlers(cfg,db_factory)
    job={'scope':{'config_hash':'changed'}}
    with pytest.raises(ValueError,match='configuration changed'): h['domeggook'][0](ops,job)
