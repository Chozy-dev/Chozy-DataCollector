"""Fixed-100 full-field batch comparison: <=118 reads + one auth; offline by default."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import time

from scripts.probes.ownerclan_query_comparison import (
    ROOT, OUT as SOURCE, ITEM_FIELDS, MeasuredSession, StopProbe,
    JwtProvider, OwnerclanClient, RateLimiter, load_credentials,
    items_by_keys_query, assess, coverage_summary,
)

ORDER = (100,50,20,10,1)
OUT = ROOT / 'artifacts/experiments/ownerclan-batch-comparison'


def sample():
    plan = json.loads((SOURCE/'plan.json').read_text(encoding='utf8'))
    path = ROOT / plan['sample']['source']
    assert hashlib.sha256(path.read_bytes()).hexdigest() == plan['sample']['source_sha256']
    keys = json.loads(path.read_text(encoding='utf8'))['sample_keys'][:100]
    assert len(keys) == len(set(keys)) == 100
    digest = hashlib.sha256(json.dumps(keys,separators=(',',':')).encode()).hexdigest()
    assert digest == plan['sample']['ordered_keys_sha256']
    return keys, digest


def response_summary(response):
    body = response.content
    result = {'body_sha256':hashlib.sha256(body).hexdigest()}
    try:
        payload = json.loads(body.decode('utf-8-sig'))
        data = payload.get('data') if isinstance(payload,dict) else None
        nodes = data.get('itemsByKeys') if isinstance(data,dict) else None
        if isinstance(data,dict) and nodes == [] and set(payload)=={'data'}:
            result['exact_empty_envelope_utf8'] = body.decode('utf-8-sig')
    except (ValueError,UnicodeError):
        pass
    return result


def run(keys, digest, session_factory=MeasuredSession, client_factory=OwnerclanClient,
        provider_factory=JwtProvider, credentials=load_credentials, output=OUT/'measurement.json',order=ORDER):
    if output.exists():
        raise ValueError('Result exists; refuse accidental rerun')
    output.parent.mkdir(parents=True,exist_ok=True)
    result = dict(status='running',started_at=datetime.now(timezone.utc).isoformat(),
        authorization='User requested fixed-100 comparison; this run bounded reads='+str(sum(100//b for b in order))+' and auth<=1',
        preflight={'local_collectors':'app signal.pause; no host Python collector observed',
                   'account_wide_usage':'unverifiable','provider_remaining_budget':'unverifiable'},
        sample_sha256=digest,ordered_keys=keys,
        fields_sha256=hashlib.sha256(ITEM_FIELDS.encode()).hexdigest(),
        order=order,workers=1,session_policy='one reused requests.Session and one JWT in this run',
        min_start_interval_seconds=3,timeout_connect_read=[5,45],automatic_retries=0,
        missing_repair=False,field_fallback=False,redirects=False,
        max_new_request_seconds=900,hard_wall_deadline=False,production_db_changes=0,
        limits='One descending pass; order/cache/time effects not isolated. No completion time for incomplete coverage. Read timeout is inactivity, not total wall deadline.',
        calls=[],cases=[dict(batch_size=b,planned_calls=100//b,status='not_started',
                            complete=False,completion_seconds=None,batches=[]) for b in order])
    def save():
        pending = output.with_suffix('.pending')
        for attempt in range(20):
            try:
                pending.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf8')
                pending.replace(output)
                break
            except PermissionError:
                if attempt == 19:
                    raise
                time.sleep(.05)
    save()
    started = time.perf_counter()
    try:
        username,password = credentials(ROOT)
        with session_factory(result,save,max_reads=sum(100//b for b in order),max_seconds=900) as session:
            session.limiter = RateLimiter(3,clock=time.perf_counter)
            session.response_observer = response_summary
            provider = provider_factory(username,password,'production',45,session=session)
            provider.token()
            client = client_factory(provider,'production',RateLimiter(0),45,0,300,session=session)
            for case in result['cases']:
                b = case['batch_size']
                case['status'] = 'running'
                before = len(result['calls'])
                start = time.perf_counter()
                try:
                    for offset in range(0,100,b):
                        batch = keys[offset:offset+b]
                        query = items_by_keys_query(batch)
                        session.context = dict(api='itemsByKeys',batch_size=b,fields='full',
                            sample_slice=[offset,offset+b],query_sha256=hashlib.sha256(query.encode()).hexdigest())
                        data = client.graphql(query)
                        nodes = data.get('itemsByKeys')
                        assessment = assess(batch,nodes)
                        returned = [n.get('key') for n in nodes if isinstance(n,dict)] if isinstance(nodes,list) else []
                        assessment.update(requested_keys=batch,returned_keys=returned,
                                          missing_keys=sorted(set(batch)-set(k for k in returned if isinstance(k,str))))
                        case['batches'].append(assessment)
                        save()
                        # Missing/empty valid arrays remain observations, not transport failures.
                        if not isinstance(nodes,list) or assessment['duplicate_keys'] or assessment['unexpected_keys']:
                            raise StopProbe('invalid_response_shape_or_keys')
                    case['status'] = 'finished'
                finally:
                    case['wall_seconds'] = time.perf_counter()-start
                    coverage_summary(case,result['calls'][before:])
                    case['attempts_finished'] = len(case['batches']) == case['planned_calls']
                    if not case['complete']:
                        case['status'] = 'incomplete'
                    save()
                    print(json.dumps({k:v for k,v in case.items() if k!='batches'}),flush=True)
            result['status'] = 'complete' if all(c['complete'] for c in result['cases']) else 'finished_with_missing'
    except (Exception,KeyboardInterrupt) as error:
        result.update(status='stopped',stop_reason=str(error) if isinstance(error,StopProbe) else type(error).__name__)
    finally:
        result['wall_seconds'] = time.perf_counter()-started
        result['finished_at'] = datetime.now(timezone.utc).isoformat()
        result['counts'] = dict(auth=sum(c.get('api')=='auth' for c in result['calls']),
                               reads=sum(c.get('api')!='auth' for c in result['calls']))
        starts = [datetime.fromisoformat(c['started_at']).timestamp() for c in result['calls']]
        result['minimum_observed_utc_start_gap_seconds'] = min((b-a for a,b in zip(starts,starts[1:])),default=None)
        save()
        print(json.dumps({k:result[k] for k in ('status','counts','wall_seconds')}),flush=True)
    return result


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute',action='store_true')
    p.add_argument('--single',action='store_true',help='Separate corrective single-item pass, <=100 reads + one auth')
    args = p.parse_args(argv)
    order = (1,) if args.single else ORDER
    output = OUT/('single-measurement.json' if args.single else 'measurement.json')
    if not args.execute:
        print(json.dumps(dict(execute=False,order=order,reads=sum(100//b for b in order),auth=1,
                              min_start_interval_seconds=3,missing_repair=False,output=str(output))))
        return 0
    logging.disable(logging.CRITICAL)
    keys,digest = sample()
    result = run(keys,digest,output=output,order=order)
    return 0 if result['status'] in ('complete','finished_with_missing') else 2


if __name__=='__main__':
    raise SystemExit(main())
