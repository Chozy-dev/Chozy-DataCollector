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


@pytest.mark.parametrize('kind',['second','minute','hour','day','month'])
def test_budget_periods_and_no_refund(ops,kind):
    apply_migration(ops, 'shared_api_budget')
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
    class Response:
        status_code=429

        def close(self): pass
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


def test_shared_budget_migration_preserves_usage_and_function_grants(ops):
    policy(ops, {'day': 2}, True)
    assert reserve(ops, True) == 'reserved'
    ops.execute('ALTER FUNCTION reserve_api_call(text,text,boolean) SECURITY DEFINER')
    ops.execute('REVOKE EXECUTE ON FUNCTION reserve_api_call(text,text,boolean) FROM PUBLIC')
    ops.commit()
    original = ops.execute("""SELECT proowner,proacl,prosecdef FROM pg_proc
        WHERE oid=to_regprocedure('reserve_api_call(text,text,boolean)')""").fetchone()
    apply_migration(ops, 'shared_api_budget')
    actual = ops.execute("""SELECT proowner,proacl,prosecdef FROM pg_proc
        WHERE oid=to_regprocedure('reserve_api_call(text,text,boolean)')""").fetchone()
    assert actual == original
    assert reserve(ops, True) == 'reserved'
    assert reserve(ops, True) == 'exhausted:day'


def test_coupang_best_leaves_report_capacity_and_blocks_shared_parent(ops, db_factory):
    from product_data_collector.operations.budget import BudgetSession, BudgetStopped
    from product_data_collector.operations.budget_policy import install_policies
    from requests import Response

    apply_migration(ops, 'shared_api_budget')
    install_policies(ops, [
        dict(account='coupang', api='best', limits={'hour': 3}, shared_api='default', platform_integrated=True),
        dict(account='coupang', api='default', limits={'hour': 3}, platform_integrated=True),
    ])
    ops.commit()
    url = 'https://api-gateway.coupang.com/v2/products/bestcategories/1001'

    class HTTP:
        body = b'{"rCode":0}'
        calls = 0

        def request(self, *_args, **_kwargs):
            self.calls += 1
            response = Response()
            response.status_code = 200
            response._content = self.body
            response._content_consumed = True
            return response

    http = HTTP()
    reports = BudgetSession(http, db_factory, 'coupang', 'default')
    best = BudgetSession(http, db_factory, 'coupang', 'best', parents={'best': 'default'})
    with pytest.raises(BudgetStopped, match='shared_policy_missing'):
        best.get(url)
    ops.execute("UPDATE api_budget_policy SET limits='{\"hour\":1}' WHERE api='best'")
    ops.commit()
    best.get(url)
    with pytest.raises(BudgetStopped, match='exhausted:hour'):
        best.get(url)
    reports.get('https://api-gateway.coupang.com/v2/products/search')
    ops.execute("UPDATE api_budget_policy SET limits='{\"hour\":2}' WHERE api='best'")
    ops.commit()
    http.body = b'{"rCode":403}'
    with pytest.raises(BudgetStopped, match='provider_restricted'):
        best.get(url)
    with pytest.raises(BudgetStopped, match='blocked'):
        reports.get('https://api-gateway.coupang.com/v2/products/search')
    assert http.calls == 3
    assert dict((r['api'], r['reserved']) for r in ops.execute(
        'SELECT api,reserved FROM api_budget_usage')) == {'default': 3, 'best': 2}


def test_hub_routes_share_total_under_concurrency_with_separate_usage(ops, db_factory):
    from product_data_collector.operations.budget_policy import install_policies

    apply_migration(ops, 'shared_api_budget')
    install_policies(ops, [
        dict(account='hub-key', api=api, limits=limits, shared_api=parent, platform_integrated=True)
        for api, limits, parent in [
            ('trend', {'month': 2}, 'hub'), ('shopping', {'month': 12}, 'hub'),
            ('hub', {'hour': 7}, None),
        ]
    ])
    ops.commit()
    workers = [(db_factory(), 'trend' if i % 2 else 'shopping') for i in range(20)]

    def reserve_route(worker):
        conn, api = worker
        row = conn.execute("SELECT reserve_api_call('hub-key',%s,true) AS result", (api,)).fetchone()
        conn.commit()
        return row['result']

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(reserve_route, workers))
    assert results.count('reserved') == 7
    assert all(r in ('reserved', 'exhausted:hour', 'exhausted:month') for r in results)
    usage = {r['api']: r['reserved'] for r in ops.execute('SELECT api,reserved FROM api_budget_usage')}
    assert usage['hub'] == usage.get('trend', 0) + usage.get('shopping', 0) == 7
    assert usage.get('trend', 0) <= 2


def test_standalone_supplier_session_reserves_before_wire(ops, db_factory, monkeypatch):
    import psycopg
    import requests
    from product_data_collector.operations.budget import BudgetStopped, SupplierSession

    apply_migration(ops, 'shared_api_budget')
    policy(ops, {'day': 1}, True)
    monkeypatch.setenv('COLLECTOR_REQUIRE_SHARED_BUDGET', 'true')
    monkeypatch.delenv('COLLECTOR_SHARED_BUDGET_DSN', raising=False)
    url = 'https://api.ownerclan.com/v1/graphql'
    calls = []

    def send(_, request, **kwargs):
        with db_factory() as conn:
            assert conn.execute('SELECT reserved FROM api_budget_usage').fetchone()['reserved'] == 1
        calls.append(request.url)
        assert kwargs['allow_redirects'] is False
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"data":{}}'
        response._content_consumed = True
        return response

    monkeypatch.setattr(requests.Session, 'send', send)
    with SupplierSession() as session:
        with pytest.raises(BudgetStopped, match='shared_budget_configuration_missing'):
            session.post(url)
        monkeypatch.setenv('COLLECTOR_SHARED_BUDGET_DSN', 'fixture-only')
        monkeypatch.setenv('COLLECTOR_OWNERCLAN_BUDGET_ACCOUNT', 'acct')
        ops.execute("UPDATE api_budget_policy SET api='default'")
        ops.commit()
        factory = psycopg.connect
        def connect(dsn, *args, **kwargs):
            if dsn == 'fixture-only':
                return db_factory()
            return factory(dsn, *args, **kwargs)

        monkeypatch.setattr(psycopg, 'connect', connect)
        session.post(url)
        with pytest.raises(BudgetStopped, match='exhausted:day'):
            session.post(url)
    assert calls == [url]


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
    from domeggook_API.api.client import PrivateDetailClient
    from domeggook_API.api.private_sync import PrivateSyncClient
    import domeggook_API.workflows.product_sync as workflow
    monkeypatch.setenv('DOMEGGOOK_PRIVATE_API_KEY','fixture')
    def collect(conn,private,details,*args,**kwargs):
        assert isinstance(private, PrivateSyncClient)
        assert isinstance(details, PrivateDetailClient)
        assert private.session.api == details._session.api == 'private'
        assert private.limiter is details._credentials[0].rate_limiter
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
