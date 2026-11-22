"""Read-only itemsByKeys probe; API execution requires --execute. No collector/DB changes."""
import argparse
from collections import Counter
from datetime import datetime
import csv
import hashlib
import json
import logging
from pathlib import Path
import random
import sys
import time

import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


class GraphqlPostSession(requests.Session):
    """Accept both legacy GET queries and the client's native POST JSON."""

    def request(self, method, url, **kwargs):
        if 'graphql' in url and method.upper() == 'GET':
            method = 'POST'
            kwargs['json'] = kwargs.pop('params')
        return super().request(method, url, **kwargs)


def save(path, value):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')
    for attempt in range(40):
        try:
            tmp.replace(path)
            return
        except PermissionError:
            if attempt == 39:
                raise
            time.sleep(.05)


def assess(keys, nodes):
    if not isinstance(nodes,list):
        raise ValueError('not_an_array')
    returned = [n['key'] for n in nodes if isinstance(n,dict) and isinstance(n.get('key'),str)]
    requested = set(keys)
    return dict(requested=len(keys),rows=len(nodes),null_rows=sum(n is None for n in nodes),
        unique_returned=len(set(returned)),missing_keys=sorted(requested-set(returned)),
        unexpected_keys=sorted(set(returned)-requested),duplicate_keys=sorted(k for k,n in Counter(returned).items() if n>1),
        status_counts=dict(Counter(n.get('status','not_selected') for n in nodes if isinstance(n,dict))))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',type=Path,required=True,help='Full changed-product CSV for the 10,000-product benchmark; archived inputs must be supplied explicitly')
    parser.add_argument('--execute',action='store_true')
    parser.add_argument('--resume',help='Existing experiment directory name under artifacts/experiments')
    parser.add_argument('--repair',action='store_true',help='Retry only missing keys after the first 10,000-key pass')
    args=parser.parse_args()
    source=args.source.resolve()
    if not source.is_file():
        parser.error('--source must name an existing full changed-product CSV')
    rows=list(csv.DictReader(source.open(encoding='utf-8-sig')))
    if len(rows)<10000:
        parser.error('This benchmark requires at least 10,000 products; small examples belong under artifacts/samples')
    sample=random.Random(20261120).sample(sorted(rows,key=lambda r:r['itemKey']),10000)
    keys=[r['itemKey'] for r in sample]
    parent=(ROOT/'artifacts/experiments').resolve()
    parent.mkdir(parents=True,exist_ok=True)
    out=parent/(args.resume or ('owner-items-batch-'+datetime.now().strftime('%Y%m%d-%H%M%S')))
    if out.resolve().parent != parent or not out.name.startswith('owner-items-batch-'):
        parser.error('invalid experiment directory')
    out.mkdir(exist_ok=bool(args.resume))
    previous=json.loads((out/'result.json').read_text(encoding='utf-8')) if args.resume else None
    if previous:
        assert json.loads((out/'sample.json').read_text(encoding='utf-8'))['keys']==keys
        save(out/('attempt-'+datetime.now().strftime('%H%M%S')+'.json'),previous)
    save(out/'sample.json',dict(seed=20261120,source=str(source),
        source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),keys=keys,
        discontinued_signal_products=sum('discontinued' in json.loads(r['kinds']) for r in sample)))
    result=dict(started_at=datetime.now().astimezone().isoformat(),output=str(out.relative_to(ROOT)),
        execute=args.execute,complete=False,calls=[],batches=[],notes=[],unique_target_products=10000,
        limits=dict(seconds=1800,rpm=100,connect_timeout=5,read_timeout=45,network_retries=1),
        transport='POST JSON via existing OwnerclanClient and JwtProvider; session converts graphql GET to POST')
    if previous:
        result=previous
        result.pop('termination',None)
    save(out/'result.json',result)
    if not args.execute:
        print(json.dumps(result,ensure_ascii=False))
        return
    from ownerclan_API.api.auth import JwtProvider
    from ownerclan_API.api.client import OwnerclanClient, OwnerclanApiError, OwnerclanGraphQLError
    from ownerclan_API.api.rate_limiter import RateLimiter
    from ownerclan_API.api.queries import ITEM_FIELDS, MINIMAL_ITEM_FIELDS
    from ownerclan_API.config.settings import load_credentials
    logging.disable(logging.CRITICAL)
    started=time.monotonic()
    prior_seconds=(datetime.now().astimezone()-datetime.fromisoformat(result['started_at'])).total_seconds()
    pace=RateLimiter(.61)
    class Stop(Exception):
        pass
    def guard():
        if prior_seconds+time.monotonic()-started>1680 or len(result['calls'])>=150:
            raise Stop('budget')
    class Session(GraphqlPostSession):
        def request(self,method,url,**kwargs):
            guard();pace.wait();guard()
            record=dict(api='graphql' if 'graphql' in url else 'auth',offset=round(time.monotonic()-started,4),
                method='POST' if 'graphql' in url else method)
            result['calls'].append(record)
            save(out/'result.json',result)
            t=time.monotonic()
            kwargs.update(timeout=(5,45),allow_redirects=False)
            try:
                response=super().request(method,url,**kwargs)
                record.update(http=response.status_code,response_bytes=len(response.content))
                if response.status_code in (401,403,429):
                    record['error']='access_or_rate_stop'
                    raise Stop('access_or_rate_stop')
                return response
            except Exception as e:
                record.setdefault('error',type(e).__name__)
                raise
            finally:
                record['seconds']=round(time.monotonic()-t,4)
                save(out/'result.json',result)

    try:
        username,password=load_credentials(ROOT)
        with Session() as session:
            client=OwnerclanClient(JwtProvider(username,password,'production',45,session=session),
                'production',RateLimiter(.61),45,0,90,session=session)
            def request_batch(label,batch,fields):
                query='query { itemsByKeys(keys: '+json.dumps(batch)+') { '+fields+' } }'
                record=dict(label=label,requested=len(batch),keys=batch,query_bytes=len(query.encode()),fields=fields)
                result['batches'].append(record)
                save(out/'result.json',result)
                t=time.monotonic()
                for attempt in range(2):
                    try:
                        data=client.graphql(query)
                        break
                    except OwnerclanGraphQLError as e:
                        record.update(error='OwnerclanGraphQLError',errors=e.errors)
                        save(out/'result.json',result)
                        raise
                    except OwnerclanApiError as e:
                        if attempt or not isinstance(e.__cause__,(requests.Timeout,requests.ConnectionError)):
                            record['error']=type(e).__name__
                            raise
                        result['notes'].append(dict(label=label,reason='network_retry',backoff=2))
                        time.sleep(2)
                nodes=data.get('itemsByKeys')
                save(out/(label+'.json'),data)
                record.update(assess(batch,nodes),elapsed_seconds=round(time.monotonic()-t,4),saved=True)
                save(out/'result.json',result)
                print(json.dumps({k:v for k,v in record.items() if k not in ('keys','fields','missing_keys')},ensure_ascii=False),flush=True)
                if record['unexpected_keys'] or record['duplicate_keys']:
                    raise Stop('returned_key_mismatch')
                return nodes
            try:
                if not any(b['label']=='pilot-1' and b.get('saved') for b in result['batches']):
                    request_batch('pilot-1',keys[:1],'key')
            except OwnerclanGraphQLError:
                # Verify the actual production root rather than infer support from a manual example.
                data=client.graphql('query { __schema { queryType { fields { name } } } }')
                save(out/'production-query-fields.json',data)
                result['notes'].append(dict(reason='itemsByKeys_pilot_failed',
                    itemsByKeys_in_production_schema=any(f['name']=='itemsByKeys' for f in data['__schema']['queryType']['fields'])))
                raise
            fields=ITEM_FIELDS
            try:
                if not any(b['label']=='calibration-100' and b.get('saved') for b in result['batches']):
                    request_batch('calibration-100',keys[:100],fields)
            except OwnerclanGraphQLError as e:
                if not e.looks_like_unknown_field():
                    raise
                fields=MINIMAL_ITEM_FIELDS
                request_batch('calibration-100-minimal',keys[:100],fields)
            cap100=any(any('최대 100개' in str(e.get('message','')) for e in b.get('errors',[])) for b in result['batches'])
            if not cap100:
                try:
                    request_batch('calibration-1000',keys[:1000],fields)
                except OwnerclanGraphQLError as e:
                    if not any('최대 100개' in str(x.get('message','')) for x in e.errors):
                        raise
                    cap100=True
            if cap100:
                result['actual_max_keys_reported']=100
                result['notes'].append(dict(reason='production_reports_max_100_despite_manual_5000'))
                for index in range(1,100):
                    label=f'bulk-100-{index+1:05d}'
                    if not any(b['label']==label and b.get('saved') for b in result['batches']):
                        request_batch(label,keys[index*100:(index+1)*100],fields)
                bulk=[b for b in result['batches'] if (b['label']=='calibration-100' or b['label'].startswith('bulk-100-')) and b.get('saved')]
                result['complete']=len(bulk)==100
            else:
                for index in range(2):
                    request_batch(f'bulk-5000-{index+1}',keys[index*5000:(index+1)*5000],fields)
                bulk=[b for b in result['batches'] if b['label'].startswith('bulk-5000-') and b.get('saved')]
                result['complete']=len(bulk)==2
            result['bulk_unique_returned']=sum(b['unique_returned'] for b in bulk)
            result['bulk_missing_keys']=[k for b in bulk for k in b['missing_keys']]
            result['bulk_missing_with_discontinued_signal']=sum('discontinued' in json.loads(r['kinds'])
                for r in sample if r['itemKey'] in set(result['bulk_missing_keys']))
            if args.repair and result['bulk_missing_keys']:
                missing=result['bulk_missing_keys']
                recovered=set()
                for index in range(0,len(missing),100):
                    nodes=request_batch(f'repair-100-{index//100+1:05d}',missing[index:index+100],fields)
                    recovered.update(n['key'] for n in nodes if isinstance(n,dict))
                result['repair_recovered_keys']=sorted(recovered)
                result['remaining_missing_keys']=sorted(set(missing)-recovered)
                result['unique_returned_after_repair']=result['bulk_unique_returned']+len(recovered)
                result['full_data_complete']=not result['remaining_missing_keys']
    except Exception as e:
        result['termination']=type(e).__name__
    finally:
        result.update(elapsed_seconds=round(prior_seconds+time.monotonic()-started,4),finished_at=datetime.now().astimezone().isoformat())
        save(out/'result.json',result)
        print(json.dumps({k:v for k,v in result.items() if k not in ('calls','batches','bulk_missing_keys')},ensure_ascii=False),flush=True)


if __name__=='__main__':
    main()
