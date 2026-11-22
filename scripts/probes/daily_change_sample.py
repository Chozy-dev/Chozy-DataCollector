"""Read-only preliminary survey. No DB/checkpoint writes. Default is offline dry-run."""
import argparse
import csv
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import random
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from collections import Counter, defaultdict
from urllib.parse import urlparse, parse_qs

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import requests
from dotenv import load_dotenv
from domeggook_API.api.client import DomeggookClient, ListRequest
from domeggook_API.api.rate_limiter import RateLimiter as DomeLimiter
from domeggook_API.config.settings import load_api_key
from domeggook_API.services.parsing import parse_list_items, parse_list_header
from ownerclan_API.api.client import OwnerclanClient
from ownerclan_API.api.auth import JwtProvider
from ownerclan_API.api.rate_limiter import RateLimiter as OwnerLimiter
from ownerclan_API.config.settings import load_credentials
from ownerclan_API.api.queries import item_histories_query

KST = timezone(timedelta(hours=9))

def read(path):
    return json.loads(path.read_text(encoding='utf-8'))

def stratified(rows, group, n, rng):
    groups = defaultdict(list)
    for row in rows:
        groups[group(row)].append(row)
    quotas = {k: n * len(v) / len(rows) for k, v in groups.items()}
    counts = {k: int(q) for k, q in quotas.items()}
    for k in sorted(groups, key=lambda k: quotas[k]-counts[k], reverse=True)[:n-sum(counts.values())]:
        counts[k] += 1
    selected = []
    for k in sorted(groups, key=str):
        selected += rng.sample(groups[k], counts[k])
    rng.shuffle(selected)
    return selected

class StopSurvey(Exception):
    pass

class Survey:
    def __init__(self, args, existing=None):
        self.args = args
        self.out = ROOT / 'artifacts/experiments' / args.output
        self.out.mkdir(parents=True, exist_ok=True)
        self.started = time.monotonic()
        self.lock = threading.RLock()
        self.rng = random.Random(args.seed)
        end = datetime.now(KST).replace(hour=0, minute=0, second=0, microsecond=0)
        self.lo, self.hi = end-timedelta(days=1), end
        self.result = {'seed': args.seed, 'started_at': datetime.now(KST).isoformat(),
                       'window': [self.lo.isoformat(), self.hi.isoformat()],
                       'limits': {'seconds': 1620, 'calls':1600, 'source_calls':800, 'rpm':100,
                                  'timeout':15, 'retries':0},
                       'calls':[], 'categories':[], 'products':[], 'events':{}, 'notes':[], 'pilot':{}}
        self.blocked = set()
        self.counts = Counter()
        self.pace = {s: OwnerLimiter(.6) for s in ['domeme','ownerclan']}
        self.clients = {}
        if existing is None:
            self.save()

    def save(self):
        with self.lock:
            self.result['elapsed_seconds'] = round(time.monotonic()-self.started,3)
            tmp = self.out/'result.tmp'
            tmp.write_text(json.dumps(self.result, ensure_ascii=False, indent=2), encoding='utf-8')
            for attempt in range(40):
                try:
                    tmp.replace(self.out/'result.json')
                    break
                except PermissionError:
                    if attempt==39: raise
                    time.sleep(.05)

    def guard(self, source):
        if (source in self.blocked or time.monotonic()-self.started > 1585
                or self.counts[source] >= 800 or sum(self.counts.values()) >=1600):
            raise StopSurvey('time_budget_or_source_stopped')

    def session(self, source):
        survey = self
        class Session(requests.Session):
            def request(self, method, url, **kwargs):
                survey.guard(source)
                survey.pace[source].wait()
                survey.guard(source)
                parsed = urlparse(url)
                params = parse_qs(parsed.query) or kwargs.get('params', {}) or kwargs.get('data', {})
                op = params.get('mode', ['graphql' if 'graphql' in url else 'auth' if '/auth' in url else 'ip'])
                if isinstance(op, list): op = op[0]
                query = str(params.get('query',''))
                if 'allItems' in query: op = 'allItems'
                elif 'itemHistories' in query: op = 'itemHistories'
                elif '__schema' in query: op = 'introspection'
                survey.counts[source] += 1
                record = {'source':source,'api':op,'offset':round(time.monotonic()-survey.started,3)}
                record['in_flight']=True
                survey.result['calls'].append(record)
                survey.save()
                t = time.monotonic()
                kwargs.update(timeout=15, allow_redirects=False)
                try:
                    response = super().request(method,url,**kwargs)
                    record['http'] = response.status_code
                    if response.status_code in (401,403,429):
                        survey.blocked.add(source)
                    return response
                except Exception as e:
                    record['error'] = type(e).__name__
                    raise
                finally:
                    record['seconds'] = round(time.monotonic()-t,4)
                    record.pop('in_flight',None)
                    survey.save()
        return Session()

    def note(self, source, e):
        # Never serialize exceptions: request URLs can contain API credentials.
        self.result['notes'].append({'source':source,'error':type(e).__name__,
                                     'code':getattr(e,'code',None), 'http':getattr(e,'status_code',None)})
        if getattr(e,'status_code',None) in (401,403,429) or str(getattr(e,'code','')) in ('401','403','429'):
            self.blocked.add(source)
        self.save()

    def plan(self):
        plans = {}
        for source, path, key, group, expected in [
            ('domeme','domeggook_API/data/state/categories.json','code',lambda c:c['depth'],4363),
            ('ownerclan','ownerclan_API/data/state/categories.json','key',lambda c:c['fullName'].split('>')[0],5762)]:
            p=ROOT/path
            rows=read(p)['categories']
            rows=list({r[key]:r for r in rows if not r.get('children')}.values())
            assert len(rows)==expected
            # Pilot itself has proportional strata; the remaining 180 fill the 200-category plan.
            selected=stratified(rows,group,200,self.rng)
            pilot=stratified(selected,group,20,self.rng)
            used={r[key] for r in pilot}
            plans[source]=pilot+[r for r in selected if r[key] not in used]
            self.result.setdefault('population',{})[source]={'size':len(rows),
                'strata':dict(Counter(str(group(r)) for r in rows)),
                'cache':path, 'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
        self.result['plan']=plans
        self.result['private_sync']={'called':False,'reason':'2026-11-19 actual IP unauthorized; approval pending',
            'evidence':'artifacts/experiments/domeggook-private-sync/20261119-223214.json',
            'filters':'existing client: dateModify range, channel.supply; no product-number argument'}
        self.save()
        return plans

    def dom_category(self,c):
        client=self.clients['domeme']
        row={'source':'domeme','category':c['code'],'stratum':c['depth'],'status':'started'}
        self.result['categories'].append(row)
        payload=client.get_item_list(ListRequest('supply','da',200,category_code=c['code']))
        h=parse_list_header(payload)
        total=int(h['numberOfItems']); size=int(h['itemsPerPage'])
        row.update(total=total,header=h)
        pages={1:parse_list_items(payload)}
        indices=self.rng.sample(range(total),min(2,total))
        row['random_indices']=indices
        chosen=[]
        for index in indices:
            pg=index//size+1; pos=index%size
            if pg not in pages:
                p=client.get_item_list(ListRequest('supply','da',size,page=pg,category_code=c['code']))
                hh=parse_list_header(p)
                if int(hh['currentPage'])!=pg or int(hh['numberOfItems'])!=total:
                    raise ValueError('pagination_changed')
                pages[pg]=parse_list_items(p)
            chosen.append(str(pages[pg][pos]['no']))
        row['status']='complete'
        self.add_products('domeme',row,chosen,'random_index_over_live_category')

    def owner_category(self,c):
        row={'source':'ownerclan','category':c['key'],'stratum':c['fullName'].split('>')[0],'status':'started'}
        self.result['categories'].append(row)
        keys=set(); after=None; cursors=set()
        # Only completed categories yield samples. A prefix is never called a random sample.
        for _ in range(5):
            args=f'first:500, category:{json.dumps(c["key"])}'+(f',after:{json.dumps(after)}' if after else '')
            d=self.clients['ownerclan'].graphql('query { allItems('+args+') { pageInfo { hasNextPage endCursor } edges { node { key } } } }')['allItems']
            keys.update(e['node']['key'] for e in d['edges'])
            pi=d['pageInfo']
            if pi['hasNextPage'] is False:
                row.update(total=len(keys),status='complete')
                self.add_products('ownerclan',row,self.rng.sample(sorted(keys),min(2,len(keys))),'random_from_complete_live_category')
                return
            after=pi['endCursor']
            if not after or after in cursors: raise ValueError('invalid_cursor')
            cursors.add(after)
        row.update(status='incomplete_no_sample',prefix_count=len(keys))

    def add_products(self,source,row,keys,method):
        seen={(p['source'],p['id']) for p in self.result['products']}
        row['selected']=keys
        for key in keys:
            if (source,key) not in seen:
                self.result['products'].append({'source':source,'id':key,'category':row['category'],
                    'total':row['total'],'method':method,'history_complete':False})
                seen.add((source,key))
        self.save()

    def owner_history(self):
        after=None; seen=set(); events=[]
        lo=int(self.lo.timestamp()*1000); hi=int(self.hi.timestamp()*1000)
        self.result['events']['ownerclan']={'api':'itemHistories','complete':False,'rows':events}
        while True:
            d=self.clients['ownerclan'].graphql(item_histories_query(first=1000,after=after,date_from=lo-1,date_to=hi+1))['itemHistories']
            for edge in d['edges']:
                e=edge['node']
                if lo <= int(e['createdAt']) < hi: events.append(e)
            pi=d['pageInfo']; self.save()
            if pi['hasNextPage'] is False:
                self.result['events']['ownerclan']['complete']=True
                return
            after=pi['endCursor']
            if not after or after in seen: raise ValueError('invalid_cursor')
            seen.add(after)

    def dom_history(self):
        """Verified alternative API; its nine signal kinds are NOT all modifications."""
        load_dotenv(ROOT/'.env', override=False)
        session=self.session('domeme')
        key=os.environ['DOMEGGOOK_PrivateAPI_KEY']
        client=DomeggookClient(key,DomeLimiter(100),timeout_seconds=15,max_retries=0,session=session)
        ip=os.getenv('DOMEGGOOK_LOGIN_IP')
        if not ip:
            ip=session.get('https://api.ipify.org',timeout=5).text.strip()
        member=os.environ['DOMEGGOOK_ID']
        # Existing auth probe uses form POST; getItemList's GET transport is not a login contract.
        import importlib.util
        spec=importlib.util.spec_from_file_location('existing_supply_probe',ROOT/'scripts/probes/domeggook_all_supply_chk_probe.py')
        probe=importlib.util.module_from_spec(spec);spec.loader.exec_module(probe)
        login=probe.post_form(session,{'ver':'4.1','mode':'setLogin','aid':key,'om':'json','id':member,
            'pw':os.environ['DOMEGGOOK_PASSWORD'],'loginKeep':'off','userAgent':'Chozy-readonly-survey',
            'ip':ip,'device':'Third Party'},probe.SSL_API_URL)
        root=login.get('domeggook',login)
        self.result['domeme_login']={'transport':'form POST','session_issued':bool(root.get('sId') or root.get('sid')),
            'error_code':(root.get('error') or {}).get('code') if isinstance(root.get('error'),dict) else None,
            'root_fields':list(root.keys())}
        sid=root.get('sId') or root.get('sid')
        if not sid:
            self.blocked.add('domeme')
            raise ValueError('login_session_not_issued')
        events=[]
        self.result['events']['domeme']={'api':'getAllSupplyChk','complete':False,'rows':events,
            'scope':'OPEN_RESTART_CLOSE_SOLDOUT_DEL_AMT_OPTAMT_OPTSOLDOUT_OPTRESTART',
            'filter':'sc=no; sw comma-separated sample IDs; date YYYYMMDD', 'batches':[]}
        products=[p for p in self.result['products'] if p['source']=='domeme']
        for offset in range(0,len(products),100):
            batch=products[offset:offset+100]; ids=[p['id'] for p in batch]; page=1
            while True:
                data=probe.post_form(session,{'ver':'1.1','mode':'getAllSupplyChk','aid':key,'om':'json','id':member,'sId':sid,
                    'type':'all','status':'OPEN_RESTART_CLOSE_SOLDOUT_DEL_AMT_OPTAMT_OPTSOLDOUT_OPTRESTART',
                    'ic':1000,'pg':page,'date':self.lo.strftime('%Y%m%d'),'sc':'no','sw':','.join(ids)},probe.SSL_API_URL)
                r=data.get('domeggook',data)
                if 'header' not in r:
                    self.result['domeme_history_rejection']={'root_fields':list(r.keys()),
                        'error_code':r.get('error',{}).get('code') if isinstance(r.get('error'),dict) else None}
                    self.blocked.add('domeme');self.save()
                    raise ValueError('history_header_missing_stop_source')
                h=r['header']; raw=(r.get('items') or {}).get('item',[])
                if isinstance(raw,dict): raw=[raw]
                if int(h['currentPage'])!=page: raise ValueError('wrong_history_page')
                for e in raw:
                    if str(e['no']) not in ids: raise ValueError('sample_filter_not_respected')
                    if self.lo.timestamp() <= int(e['date']) < self.hi.timestamp():
                        events.append({k:e.get(k) for k in ['no','status','date','price','qty']})
                self.result['events']['domeme']['batches'].append({'offset':offset,'page':page,'header':h,'returned':len(raw)})
                self.save()
                if page>=int(h['numberOfPages']):
                    for p in batch: p['history_complete']=True
                    break
                page+=1
        self.result['events']['domeme']['complete']=True
        self.save()

    def owner_sample_histories(self):
        """Use documented itemKey filter after a global feed timeout; terminal per-item pages only."""
        lo=int(self.lo.timestamp()*1000); hi=int(self.hi.timestamp()*1000)
        feed=self.result['events'].setdefault('ownerclan',{'api':'itemHistories','complete':False,'rows':[]})
        feed['per_item_fallback']=True
        consecutive_errors=0
        for p in [p for p in self.result['products'] if p['source']=='ownerclan']:
            after=None; seen=set()
            try:
                while True:
                    q=item_histories_query(first=1000,after=after,date_from=lo-1,date_to=hi+1)
                    q=q.replace('itemHistories(', 'itemHistories(itemKey:'+json.dumps(p['id'])+',',1)
                    d=self.clients['ownerclan'].graphql(q)['itemHistories']
                    for edge in d['edges']:
                        e=edge['node']
                        if e['itemKey']!=p['id']:raise ValueError('item_filter_not_respected')
                        if lo<=int(e['createdAt'])<hi:feed['rows'].append(e)
                    if d['pageInfo']['hasNextPage'] is False:
                        p['history_complete']=True;consecutive_errors=0;self.save();break
                    after=d['pageInfo']['endCursor']
                    if not after or after in seen:raise ValueError('invalid_cursor')
                    seen.add(after);self.save()
            except Exception as e:
                self.note('ownerclan',e);consecutive_errors+=1
                if consecutive_errors>=2 or isinstance(e,StopSurvey) or 'ownerclan' in self.blocked:break
        feed['sample_complete']=all(p['history_complete'] for p in self.result['products'] if p['source']=='ownerclan')
        self.save()

    def run(self, plans):
        load_dotenv(ROOT/'.env',override=False)
        self.clients['domeme']=DomeggookClient(load_api_key(ROOT),DomeLimiter(100),timeout_seconds=15,max_retries=0,session=self.session('domeme'))
        username,password=load_credentials(ROOT)
        s=self.session('ownerclan')
        provider=JwtProvider(username,password,'production',15,session=s)
        self.clients['ownerclan']=OwnerclanClient(provider,'production',OwnerLimiter(.6),15,0,15,session=s)
        failed=set()
        for start,end in [(0,20),(20,200)]:
            for source in ['domeme','ownerclan']:
                if source in failed or source in self.blocked: continue
                t=time.monotonic(); calls=self.counts[source]; errors=0
                for c in plans[source][start:end]:
                    try:
                        self.guard(source)
                        (self.dom_category if source=='domeme' else self.owner_category)(c)
                    except Exception as e:
                        self.note(source,e); errors+=1
                        if isinstance(e,StopSurvey) or source in self.blocked or errors>=2:
                            failed.add(source); break
                    finally: self.save()
                if start==0:
                    elapsed=time.monotonic()-t
                    attempted=len([c for c in self.result['categories'] if c['source']==source])
                    self.result['pilot'][source]={'seconds':elapsed,'calls':self.counts[source]-calls,
                        'attempted':attempted,'estimated_200_seconds':elapsed/max(1,attempted)*200,
                        'estimated_200_calls':(self.counts[source]-calls)/max(1,attempted)*200}
                    self.save()
                    if source=='ownerclan' and self.result['pilot'][source]['estimated_200_seconds'] > 1585-(time.monotonic()-self.started)-120:
                        failed.add(source)
                        self.result['notes'].append({'source':source,'reason':'pilot_projection_exceeds_remaining_time; reserve history calls'})
        # History is attempted independently after list timeouts, unless access was denied.
        if 'ownerclan' not in self.blocked:
            try: self.owner_history()
            except Exception as e:
                self.note('ownerclan',e)
                if 'ownerclan' not in self.blocked:self.owner_sample_histories()
        if 'domeme' not in self.blocked:
            try:self.dom_history()
            except Exception as e:self.note('domeme',e)
        self.save()

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute',action='store_true')
    p.add_argument('--fallback',action='store_true',help='Continue same survey clock, domeme signal API only')
    p.add_argument('--owner-history',action='store_true',help='Continue same survey clock; history only')
    p.add_argument('--seed',type=int,default=20261120)
    p.add_argument('--output',default='daily-change-2026-11-20')
    args=p.parse_args()
    logging.disable(logging.CRITICAL)
    prior=ROOT/'artifacts/experiments'/args.output/'result.json'
    if (args.fallback or args.owner_history) and not prior.is_file():
        p.error('Full survey inputs were archived; restore result.json before resuming')
    old=read(prior) if args.fallback or args.owner_history else None
    survey=Survey(args,existing=old)
    if old:
        survey.result=old
        survey.lo,survey.hi=(datetime.fromisoformat(x) for x in old['window'])
        survey.started=time.monotonic()-(datetime.now(KST)-datetime.fromisoformat(old['started_at'])).total_seconds()
        survey.counts=Counter(c['source'] for c in old['calls'])
        survey.blocked={c['source'] for c in old['calls'] if c.get('http') in (401,403,429)}
        survey.blocked.update(n['source'] for n in old['notes'] if str(n.get('code')) in ('401','403','429') or n.get('http') in (401,403,429))
        plans=old['plan']; survey.save()
    else: plans=survey.plan()
    if args.execute:
        def timeout():
            survey.result['termination']='hard_27_minute_stop'
            survey.save(); os._exit(0)
        timer=threading.Timer(max(0,1620-(time.monotonic()-survey.started)),timeout); timer.daemon=True; timer.start()
        try:
            if args.fallback:
                try: survey.dom_history()
                except Exception as e: survey.note('domeme',e)
            elif args.owner_history:
                load_dotenv(ROOT/'.env',override=False)
                username,password=load_credentials(ROOT)
                s=survey.session('ownerclan')
                provider=JwtProvider(username,password,'production',15,session=s)
                survey.clients['ownerclan']=OwnerclanClient(provider,'production',OwnerLimiter(.6),15,0,15,session=s)
                try: survey.owner_history()
                except Exception as e:
                    survey.note('ownerclan',e)
                    if 'ownerclan' not in survey.blocked:
                        survey.owner_sample_histories()
            else: survey.run(plans)
        finally:
            survey.result['finished_at']=datetime.now(KST).isoformat(); survey.save(); timer.cancel()
    else:
        survey.result['dry_run']=True; survey.save()
    print(json.dumps({'output':str(survey.out),'execute':args.execute,'calls':dict(survey.counts)},ensure_ascii=False))

if __name__=='__main__': main()
