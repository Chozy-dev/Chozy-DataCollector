import json
from scripts.probes import ownerclan_batch_comparison as probe


def test_offline_plan_never_reads_sample_or_credentials(monkeypatch,capsys):
    monkeypatch.setattr(probe,'sample',lambda: (_ for _ in ()).throw(AssertionError('sample accessed')))
    assert probe.main([]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['reads'] == 118 and result['auth'] == 1
    assert result['order'] == [100,50,20,10,1]


def fake_session(result,save,**kwargs):
    class Session:
        context = {}
        def __enter__(self):return self
        def __exit__(self,*_):pass
    return Session()


def test_missing_does_not_prevent_remaining_comparisons(tmp_path):
    queries = []
    class Provider:
        def __init__(self,*_,**kwargs):pass
        def token(self):return 'fixture'
    class Client:
        def __init__(self,*_,**kwargs):pass
        def graphql(self,query):
            queries.append(query)
            return {'itemsByKeys':[]}
    r = probe.run([str(i) for i in range(100)],'fixture',fake_session,Client,Provider,
                  lambda *_:('fixture','fixture'),tmp_path/'measurement.json')
    assert len(queries) == 118
    assert r['status'] == 'finished_with_missing'
    assert all(c['attempts_finished'] and c['completion_seconds'] is None for c in r['cases'])


def test_transport_error_stops_all_later_requests(tmp_path):
    calls = []
    class Provider:
        def __init__(self,*_,**kwargs):pass
        def token(self):return 'fixture'
    class Client:
        def __init__(self,*_,**kwargs):pass
        def graphql(self,query):
            calls.append(query)
            raise probe.StopProbe('timeout')
    r = probe.run([str(i) for i in range(100)],'fixture',fake_session,Client,Provider,
                  lambda *_:('fixture','fixture'),tmp_path/'measurement.json')
    assert len(calls) == 1
    assert r['status'] == 'stopped' and r['stop_reason'] == 'timeout'
    assert all(c['status']=='not_started' for c in r['cases'][1:])


def test_unfinished_timing_record_does_not_mask_local_failure(tmp_path):
    saved = {}
    def session(result,save,**kwargs):
        saved['result'] = result
        return fake_session(result,save,**kwargs)
    class Provider:
        def __init__(self,*_,**kwargs):pass
        def token(self):return 'fixture'
    class Client:
        def __init__(self,*_,**kwargs):pass
        def graphql(self,query):
            saved['result']['calls'].append(dict(api='itemsByKeys',status=None,
                graphql_errors=0,response_bytes=None,timed_out=False,started_at='2026-11-22T00:00:00+00:00'))
            raise PermissionError('local persistence fixture')
    r = probe.run([str(i) for i in range(100)],'fixture',session,Client,Provider,
                  lambda *_:('fixture','fixture'),tmp_path/'measurement.json')
    assert r['stop_reason'] == 'PermissionError'
    assert r['cases'][0]['status'] == 'incomplete'
    assert r['cases'][0]['http_mean_seconds'] is None


def test_transient_atomic_replace_failure_is_retried(tmp_path,monkeypatch):
    original = probe.Path.replace
    failures = []
    def replace(path,target):
        if not failures:
            failures.append(1)
            raise PermissionError('temporary sharing violation')
        return original(path,target)
    monkeypatch.setattr(probe.Path,'replace',replace)
    monkeypatch.setattr(probe.time,'sleep',lambda _:None)
    class Provider:
        def __init__(self,*_,**kwargs):pass
        def token(self):return 'fixture'
    class Client:
        def __init__(self,*_,**kwargs):pass
        def graphql(self,query):return {'itemsByKeys':[]}
    path = tmp_path/'measurement.json'
    r = probe.run([str(i) for i in range(100)],'fixture',fake_session,Client,Provider,
                  lambda *_:('fixture','fixture'),path,order=(100,))
    assert failures == [1] and path.exists()
    assert r['status'] == 'finished_with_missing'
