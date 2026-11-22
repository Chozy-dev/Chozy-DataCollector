"""Investigate missing key responses; max five reads plus one auth, offline by default."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime,timezone
import hashlib
import json
import logging
import re
import time

from scripts.probes.ownerclan_query_comparison import (
    OUT,ROOT,ITEM_FIELDS,MeasuredSession,StopProbe,JwtProvider,OwnerclanClient,
    RateLimiter,load_credentials,item_query,items_by_keys_query,assess,
)


def describe_response(response):
    raw=response.content
    result=dict(body_sha256=hashlib.sha256(raw).hexdigest(),
                content_type=response.headers.get('Content-Type'))
    try:
        payload=json.loads(raw.decode('utf-8-sig'))
    except (ValueError,UnicodeError):
        return dict(result,json_valid=False)
    result.update(json_valid=True,root_type=type(payload).__name__)
    if not isinstance(payload,dict):
        return result
    result['root_field_types']={k:type(v).__name__ for k,v in payload.items()}
    errors=payload.get('errors')
    result['errors_count']=len(errors) if isinstance(errors,list) else int(bool(errors))
    data=payload.get('data')
    result['data_type']=type(data).__name__
    if not isinstance(data,dict):
        return result
    result['data_field_types']={k:type(v).__name__ for k,v in data.items()}
    result['operations']={}
    for op in ('item','itemsByKeys'):
        if op not in data:
            continue
        value=data[op]
        nodes=value if isinstance(value,list) else [value] if isinstance(value,dict) else []
        types=Counter(type(n).__name__ for n in nodes)
        descriptions=[]
        for node in nodes:
            if not isinstance(node,dict):
                continue
            descriptions.append(dict(key=node.get('key'),status=node.get('status'),
                price=node.get('price'),options_count=len(node['options']) if isinstance(node.get('options'),list) else None,
                serialized_value_bytes=len(json.dumps(node,ensure_ascii=False).encode()),
                field_types={k:type(v).__name__ for k,v in node.items()},
                field_value_bytes={k:len(json.dumps(v,ensure_ascii=False).encode()) for k,v in node.items()}))
        result['operations'][op]=dict(type=type(value).__name__,rows=len(nodes),
            element_types=dict(types),returned_keys=[n.get('key') for n in nodes if isinstance(n,dict)],
            products=descriptions)
    # Preserve exact bytes only for data-only null/empty envelopes, never product bodies/auth/errors.
    if set(payload)=={'data'} and set(data).issubset({'item','itemsByKeys'}) and data and all(v is None or v==[] for v in data.values()):
        result['exact_empty_envelope_utf8']=raw.decode('utf-8-sig')
    return result


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute',action='store_true')
    args=parser.parse_args(argv)
    plan=json.loads((OUT/'plan.json').read_text(encoding='utf-8'))
    source=ROOT/plan['sample']['source']
    assert hashlib.sha256(source.read_bytes()).hexdigest()==plan['sample']['source_sha256']
    keys=json.loads(source.read_text(encoding='utf-8'))['sample_keys'][:100]
    old=json.loads((OUT/'batch-measurement.json').read_text(encoding='utf-8'))
    original=items_by_keys_query(keys[40:60])
    assert hashlib.sha256(original.encode()).hexdigest()==old['calls'][3]['query_sha256']
    cases=[('control_bulk1','itemsByKeys',keys[:1]),
           ('suspect_item1','item',keys[40:41]),
           ('suspect_bulk1','itemsByKeys',keys[40:41]),
           ('suspect_bulk5','itemsByKeys',keys[40:45]),
           ('original_bulk20','itemsByKeys',keys[40:60])]
    if not args.execute:
        print(json.dumps(dict(execute=False,auth=1,reads=5,total_http_cap=6,
                             labels=[c[0] for c in cases],interval_seconds=3)))
        return 0
    logging.disable(logging.CRITICAL)
    result=dict(started_at=datetime.now(timezone.utc).isoformat(),status='running',
        authorization='User approved at most six actual API calls; including authentication here',
        account_usage_verified=False,official_daily_limit_verified=False,
        limits=dict(total_http=6,auth=1,reads=5,workers=1,start_interval_seconds=3,
                    automatic_retries=0,timeout_connect_read=[5,45]),
        historical_body_recoverable=False,historical_response_bytes=28,
        original_query_sha256=old['calls'][3]['query_sha256'],
        normal_keys=keys[:40],suspect_keys=keys[40:60],
        key_format_comparison={label:dict(count=len(group),lengths=dict(Counter(map(len,group))),
            valid_W_uppercase_alphanumeric=sum(bool(re.fullmatch(r'W[A-Z0-9]+',k)) for k in group),
            whitespace=sum(k!=k.strip() for k in group),duplicates=len(group)-len(set(group)))
            for label,group in [('normal',keys[:40]),('suspect',keys[40:60])]},
        calls=[],cases=[])
    def save():
        pending=OUT/'missing-items-measurement.pending'
        pending.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
        pending.replace(OUT/'missing-items-measurement.json')
    save()
    try:
        username,password=load_credentials(ROOT)
        with MeasuredSession(result,save,max_reads=5,max_seconds=245) as session:
            session.response_observer=describe_response
            provider=JwtProvider(username,password,'production',45,session=session)
            client=OwnerclanClient(provider,'production',RateLimiter(0),45,0,300,session=session)
            provider.token()
            for label,api,batch in cases:
                query=item_query(batch[0]) if api=='item' else items_by_keys_query(batch)
                session.context=dict(label=label,api=api,fields='full',requested_keys=batch,
                    query_sha256=hashlib.sha256(query.encode()).hexdigest())
                case=dict(label=label,api=api,requested_keys=batch)
                result['cases'].append(case)
                started=time.monotonic()
                try:
                    data=client.graphql(query)
                    value=data.get(api)
                    nodes=[value] if api=='item' and isinstance(value,dict) else value
                    case['assessment']=assess(batch,nodes)
                    # An incomplete result is the diagnostic observation, not a retry trigger.
                finally:
                    case['wall_seconds']=time.monotonic()-started
                    save()
                    print(json.dumps(case),flush=True)
            result['status']='complete'
    except Exception as exc:
        result.update(status='stopped',stop_reason=str(exc) if isinstance(exc,StopProbe) else type(exc).__name__)
    finally:
        result['finished_at']=datetime.now(timezone.utc).isoformat()
        result['counts']=dict(auth=sum(c['api']=='auth' for c in result['calls']),
                             graphql=sum(c['api']!='auth' for c in result['calls']))
        save()
        print(json.dumps(dict(status=result['status'],counts=result['counts'])),flush=True)
    return 0 if result['status']=='complete' else 2


if __name__=='__main__':
    raise SystemExit(main())
