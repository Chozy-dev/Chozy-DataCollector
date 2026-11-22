"""Bounded offline fixture benchmark. Only its own local Docker instance is accepted.
No .env, API clients or production connection discovery. See docs/VALIDATION.md.
"""
from __future__ import annotations
import argparse, json, os, random, shutil, subprocess, sys, threading, time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import psutil
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from product_data_collector.persistence.postgres_storage import init_schema, _snapshot_row, _save_product_batch_with_retry
from product_data_collector.persistence.migrations import apply_migration
from product_data_collector.persistence.product_events import save_events
from product_data_collector.domain.product_events import ProductEvent
from product_data_collector.operations.fence import certify_collection
from product_data_collector.persistence import scoring
from domeggook_API.services.parsing import parse_detail_product
from ownerclan_API.services.normalization import normalize_item

CONTAINER='chozy-collection-pipeline-benchmark'
DSN='host=127.0.0.1 port=55449 dbname=collector_stage1_test user=postgres password=collector_benchmark_only'
AT=datetime(2026,11,20,tzinfo=timezone.utc)
OUT=Path('artifacts/experiments/collection-pipeline-benchmark')
START=time.monotonic()
RESOURCE_STOP=threading.Event()

def docker(*args):
    return subprocess.check_output(['docker',*args],text=True,timeout=15).strip()

def guard():
    if time.monotonic()-START>1500: raise RuntimeError('25 minute benchmark budget exhausted')
    if shutil.disk_usage('C:/').free<30*2**30: raise RuntimeError('Host disk reserve <30GiB')
    if psutil.virtual_memory().available<4*2**30: raise RuntimeError('Host RAM reserve <4GiB')
    info=json.loads(docker('inspect',CONTAINER))[0]
    if info['Config']['Labels'].get('chozy.collector-benchmark')!='true': raise RuntimeError('Dedicated container required')
    bindings=info['HostConfig']['PortBindings'].get('5432/tcp')
    if bindings!=[{'HostIp':'127.0.0.1','HostPort':'55449'}]: raise RuntimeError('Dedicated localhost port mapping required')
    if int(docker('exec',CONTAINER,'df','-B1','--output=avail','/var/lib/postgresql/data').splitlines()[-1])<10*2**30:
        raise RuntimeError('Docker filesystem reserve <10GiB')
    used=int(docker('exec',CONTAINER,'du','-s','-B1','/var/lib/postgresql/data').split()[0])
    if used>4*2**30: raise RuntimeError('Fixture volume budget >4GiB')
    return {'host_free':shutil.disk_usage('C:/').free,'host_available':psutil.virtual_memory().available,'volume_bytes':used}

def connect(schema=None):
    c=psycopg.connect(DSN,row_factory=dict_row)
    c.execute("SET statement_timeout='60s'")
    if schema: c.execute(f'SET search_path TO {schema},public')
    c.commit();return c

def fixture(i, platform, at, cfg, phase=0):
    """Reproducible scenarios; distributions are inputs, not empirical claims."""
    r=random.Random(cfg.seed+i)
    scenario=i%10
    price=round(min(300000,max(500,r.lognormvariate(9.2,1.05)))/100)*100
    if scenario in (4,5): price+=phase*100
    if scenario==6: price=None
    n=r.randrange(cfg.max_options+1)
    qty=0 if scenario==2 or (scenario in (3,5) and phase==0) or (scenario==4 and phase%2) else r.choice([3,10,25,100,9999])
    status='soldout' if qty==0 else ('discontinued' if scenario==9 else 'available')
    vendor='SYNTHETIC_ONLY_V'+str(0 if r.random()<cfg.skew else 1+i%max(2,cfg.vendors))
    if scenario==9: vendor=None
    options=[{'key':f'{j:03d}','name':f'색상 {j}','price':None if price is None else price+(j*500 if scenario==7 else 0),
              'quantity':qty if j%3 else 0,'status':'soldout' if j%3==0 or qty==0 else 'available'} for j in range(n)]
    if platform=='ownerclan':
        raw={'key':f'S9-{i}','name':f'합성 {i} 생활/주방 {scenario}','price':price,'status':status,
             'options':options,'openmarketSellable':True,'shippingFee':r.choice([0,2500,3000,5000]),'shippingType':'paid',
             'guaranteedShippingPeriod':r.choice([1,2,3,5,None]),'metadata':{'vendorKey':vendor},
             'images':[f'https://fixture.invalid/{i}.jpg']}
        p=normalize_item(raw,at.isoformat())
    else:
        raw={'no':f'S9-{i}','basis':{'no':f'S9-{i}','title':f'합성 {i} 생활/주방 {scenario}','status':'품절' if qty==0 else '판매중'},
             'channel':{'supply':'1'},'price':{'supply':price},'qty':{'inventory':qty},
             'seller':{'score':{'avg':r.choice(['96%','89%','0%',None]),'cnt':r.choice([0,10,100,500])}},
             'selectOpt':json.dumps({'type':'combination','data':{str(j):{'name':f'색상 {j}','supPrice':j*500 if scenario==7 else 0,'qty':o['quantity'],'hid':1 if j%5==4 else 0,'sup':1} for j,o in enumerate(options)}})}
        p=parse_detail_product(raw,at.isoformat())
    p.setdefault('sourceSpecific',{})['fixture_only']=True
    return _snapshot_row(platform,at.isoformat(),p)

def stock_semantic(scenario,phase):
    return 'soldout' if scenario==2 or (scenario==4 and phase%2==1) or (scenario in (3,5) and phase==0) else 'restocked'

def dbsize(c):
    return c.execute("SELECT pg_database_size(current_database()) db_bytes, (SELECT coalesce(sum(pg_indexes_size(c.oid)),0) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relkind='r') index_bytes, pg_current_wal_insert_lsn() lsn").fetchone()

class Meter:
    def __init__(self,c): self.c=c;self.stats=defaultdict(lambda:[0,0.0])
    def __getattr__(self,k): return getattr(self.c,k)
    def execute(self,q,*a,**kw):
        t=time.perf_counter()
        try:return self.c.execute(q,*a,**kw)
        finally:
            group='events' if 'product_events' in q else 'history' if 'product_history' in q else 'supplier' if 'scoring_supplier' in q else 'other'
            self.stats[group][0]+=1; self.stats[group][1]+=time.perf_counter()-t

def monitor(schema,platform,stop,record,pid,sampling_only=False):
    with connect(schema) as c:
        while not stop.is_set():
            active=c.execute("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=%s AND locktype='advisory' AND mode='ExclusiveLock' AND granted) ok",(pid,)).fetchone()['ok'];c.rollback()
            if not active and not sampling_only:
                stop.wait(.1);continue
            t=time.perf_counter(); c.execute('SELECT count(*) FROM products').fetchone(); c.rollback()
            record['select_ms'].append((time.perf_counter()-t)*1000)
            for source in (() if sampling_only else (platform, 'ownerclan' if platform=='domeggook' else 'domeggook')):
                t=time.perf_counter()
                try:
                    c.execute("UPDATE products SET product_name=product_name WHERE id=(SELECT min(id) FROM products WHERE platform=%s)",(source,));c.rollback()
                    code='ok'
                except psycopg.Error as e: code=e.sqlstate;c.rollback()
                record['writes'].append({'source':source,'code':code,'ms':(time.perf_counter()-t)*1000})
            record['python_rss_peak']=max(record['python_rss_peak'],psutil.Process().memory_info().rss)
            record['host_available_min']=min(record['host_available_min'],psutil.virtual_memory().available)
            try:
                stat=json.loads(docker('stats',CONTAINER,'--no-stream','--format','{{json .}}'))
                record['docker_memory'].append(stat['MemUsage'])
            except Exception: pass
            stop.wait(1)

def measure_run(c,schema,size,platform,mode,tag,at,resume=False):
    rec={'platform':platform,'mode':mode,'tag':tag,'select_ms':[],'writes':[],'python_rss_peak':0,'host_available_min':psutil.virtual_memory().available,'docker_memory':[]}
    certify_collection(c,platform,'SYNTHETIC_FIXTURE_ONLY')
    c.execute('SELECT pg_stat_statements_reset()'); c.commit()
    pub=scoring.publish
    def timed_publish(conn,run):
        before=dbsize(c);t=time.perf_counter();pub(conn,run)
        rec['publish_seconds']=time.perf_counter()-t
        rec['publish_wal_bytes']=int(c.execute('SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),%s) n',(before['lsn'],)).fetchone()['n'])
    scoring.publish=timed_publish
    stop=threading.Event();thread=threading.Thread(target=monitor,args=(schema,platform,stop,rec,c.info.backend_pid),daemon=True);thread.start()
    t=time.perf_counter();meter=Meter(c)
    try:
        if resume:
            partial=scoring.run_scoring(meter,run_id=tag,mode=mode,platform=platform,as_of=at,chunk_size=100,max_chunks=2)
            rec['interrupted_status']=partial['status']
        result=scoring.run_scoring(meter,run_id=tag,mode=mode,platform=platform,as_of=at,chunk_size=100,
                stop_requested=lambda: RESOURCE_STOP.is_set() or shutil.disk_usage('C:/').free<30*2**30 or time.monotonic()-START>1500 or psutil.virtual_memory().available<4*2**30)
        rec['total_seconds']=time.perf_counter()-t
        rec['processed']=result['processed'];rec['status']=result['status']
        if result['status']!='completed': raise RuntimeError('Resource/time stop: run checkpointed')
    finally:
        stop.set();thread.join(timeout=20);scoring.publish=pub
    rec['calculation_seconds']=rec['total_seconds']-rec.get('publish_seconds',0)
    rec['products_per_second']=rec['processed']/rec['calculation_seconds']
    rec['queries']=dict(meter.stats)
    rec['server_queries']=c.execute('SELECT query,calls,total_exec_time,rows,shared_blks_hit,shared_blks_read,wal_bytes FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 12').fetchall()
    rec['nulls']=c.execute("SELECT count(*) total,count(*) FILTER(WHERE stability IS NULL) null_stability,count(*) FILTER(WHERE reliability IS NULL) null_reliability,count(*) FILTER(WHERE home IS NOT NULL) calculable FROM product_supply_scores WHERE platform=%s",(platform,)).fetchone()
    rec['snapshot_rows']=c.execute('SELECT count(*) n FROM supply_home_items WHERE batch_id=%s',(tag,)).fetchone()['n']
    c.commit()
    OUT.mkdir(parents=True,exist_ok=True);(OUT/f'{size}-{tag}.json').write_text(json.dumps(rec,default=str,indent=2),encoding='utf8')
    print(json.dumps({k:rec[k] for k in ['tag','processed','total_seconds','publish_seconds','products_per_second']}),flush=True)
    return rec

def validate_config(cfg):
    if not 0<=cfg.max_options<=20 or not 2<=cfg.vendors<=1000 or not 0<=cfg.skew<=1 or not 0<cfg.change_rate<=.1:
        raise ValueError('Bounded options/vendors/skew/change-rate required')
    if cfg.size not in (1000,10000,50000,100000): raise ValueError('Only staged sizes <=100000')

def missing_evidence_probe(c,schema,size,result,at=None):
    # Isolated fixture mutation, never a claim about an actual vendor.
    c.execute("UPDATE products SET source_specific=source_specific-'vendorKey' WHERE platform='ownerclan' AND external_product_id='S9-8'")
    c.execute("UPDATE scoring_supplier_identity SET verified=false WHERE vendor_key=(SELECT source_specific->>'vendorKey' FROM products WHERE platform='ownerclan' AND external_product_id='S9-4')")
    c.commit()
    result['runs'].append(measure_run(c,schema,size,'ownerclan','incremental','missing-evidence-ownerclan',at or AT+timedelta(days=1,hours=3)))
    result['missing_supplier_reasons']=c.execute("SELECT result->'reasons' AS reasons,count(*) n FROM product_supply_scores WHERE platform='ownerclan' GROUP BY 1").fetchall()
    assert c.execute("SELECT reliability FROM product_supply_scores s JOIN products p ON p.id=s.product_id WHERE p.platform='ownerclan' AND p.external_product_id='S9-8'").fetchone()['reliability'] is None
    c.commit()


def run(cfg):
    validate_config(cfg)
    resources_before=guard()
    if cfg.size>1000:
        previous_size={10000:1000,50000:10000,100000:50000}[cfg.size]
        previous_path=OUT/f'{previous_size}.json'
        if not previous_path.exists(): raise RuntimeError('Previous scale must complete first')
        previous=json.loads(previous_path.read_text(encoding='utf8'))
        if any(previous['config'][k]!=v for k,v in vars(cfg).items() if k!='size'):
            raise RuntimeError('Previous scale must use identical fixture configuration')
        projected_seconds=(previous['load_wall_seconds']+sum(r['total_seconds'] for r in previous['runs']))*cfg.size/previous_size*1.5
        projected_bytes=(previous['after_resources']['volume_bytes']-previous['before_resources']['volume_bytes'])*cfg.size/previous_size*2
        if projected_seconds>1200 or projected_bytes>4*2**30 or resources_before['host_free']-projected_bytes<30*2**30:
            raise RuntimeError(f'Next scale unsafe/unnecessary: seconds={projected_seconds:.0f}, bytes={projected_bytes:.0f}')
    schema=f'collection_pipeline_{cfg.size}_v3'
    with connect() as c:
        c.execute('CREATE EXTENSION IF NOT EXISTS pg_stat_statements');c.execute(f'CREATE SCHEMA {schema}');c.commit()
    result={'size':cfg.size,'config':vars(cfg),'before_resources':guard(),'external_api_calls':0}
    with connect(schema) as c:
        init_schema(c)
        for name in ('stage7_supply_scoring','stage8_operations','stage7_history_policy_v2'):apply_migration(c,name)
        result['before']=dbsize(c); gen=load=0.;wall=time.perf_counter()
        load_rec={'select_ms':[],'writes':[],'python_rss_peak':0,'host_available_min':psutil.virtual_memory().available,'docker_memory':[]}
        load_stop=threading.Event();load_worker=threading.Thread(target=monitor,args=(schema,'domeggook',load_stop,load_rec,c.info.backend_pid,True),daemon=True);load_worker.start()
        # Batch across a common chronological schedule. Stable products only get initial/current observations.
        for phase,day in enumerate([-100,-89.5,-60,-30,-10,-1]):
            for platform,offset in [('domeggook',0),('ownerclan',1)]:
                for first in range(1+offset,cfg.size+1,400):
                    guard()
                    t=time.perf_counter(); rows=[];events=[]
                    for i in range(first,min(first+400,cfg.size+1),2):
                        scenario=i%10
                        if scenario==1 and phase<5:continue
                        if phase not in (0,5) and (scenario not in (3,4,5) or phase>cfg.history_changes):continue
                        row=fixture(i,platform,AT+timedelta(days=day),cfg,phase)
                        rows.append(row)
                        previous=max([p for p in range(phase) if p==0 or (p<=cfg.history_changes and scenario in (3,4,5))],default=None)
                        if scenario in (2,3,4,5) and (previous is None or stock_semantic(scenario,phase)!=stock_semantic(scenario,previous)):
                            sem=stock_semantic(scenario,phase)
                            events.append(ProductEvent(platform,f'S9-{i}','SYNTHETIC_FIXTURE_ONLY',sem,AT+timedelta(days=day),AT,sem,{'fixture_only':True,'scenario':scenario}))
                    gen+=time.perf_counter()-t;t=time.perf_counter()
                    if rows:_save_product_batch_with_retry(c,rows)
                    if events:save_events(c,events,request_refresh=False)
                    load+=time.perf_counter()-t
            print(f'load {cfg.size}: phase {phase+1}/6 elapsed={time.perf_counter()-wall:.1f}s',flush=True)
        c.execute("INSERT INTO scoring_coverage(product_id,metric,valid_from,valid_until,verified,evidence) SELECT id,m,%s,%s,true,jsonb_build_object('fixture_only',true,'stream','observed_supply_v1','stockout_state_at_valid_from','available') FROM products CROSS JOIN unnest(ARRAY['stockout','price']) m WHERE substring(external_product_id from 4)::int %% 10 NOT IN (1,8)",(AT-timedelta(days=120),AT+timedelta(days=30)))
        c.execute("INSERT INTO scoring_supplier_identity SELECT DISTINCT source_specific->>'vendorKey',%s,%s,true,'{\"fixture_only\":true}'::jsonb FROM products WHERE platform='ownerclan' AND source_specific->>'vendorKey' IS NOT NULL AND substring(external_product_id from 4)::int %% 10 <> 7",(AT-timedelta(days=120),AT+timedelta(days=30)));c.commit()
        c.execute('ANALYZE');c.commit()
        result.update(generation_seconds=gen,load_seconds=load,load_wall_seconds=time.perf_counter()-wall,after_load=dbsize(c))
        result['rows']={tbl:c.execute(f'SELECT count(*) n FROM {tbl}').fetchone()['n'] for tbl in ['products','product_options','product_history','product_events','scoring_coverage','scoring_supplier_identity']};c.commit()
        load_stop.set();load_worker.join(timeout=20);result['load_monitor']=load_rec
        result['runs']=[]
        for platform in ['domeggook','ownerclan']:
            result['runs'].append(measure_run(c,schema,cfg.size,platform,'initial','initial-'+platform,AT,resume=True))
        # 1% actual price changes, preserve snapshot/option/history consistency via the real writer.
        t=time.perf_counter();changed=[]
        for platform in ['domeggook','ownerclan']:
            rows=[]
            candidates=[i for i in range(1,cfg.size+1) if i%10 in (4,5) and ('domeggook' if i%2 else 'ownerclan')==platform]
            selected=random.Random(cfg.seed).sample(candidates, min(len(candidates),round(cfg.size*cfg.change_rate/2)))
            for i in sorted(selected):
                rows.append(fixture(i,platform,AT+timedelta(hours=1),cfg,6));changed.append(i)
            if rows:
                _save_product_batch_with_retry(c,rows)
                save_events(c,[ProductEvent(platform,f'S9-{i}','SYNTHETIC_FIXTURE_ONLY','restocked',AT+timedelta(hours=1),AT+timedelta(hours=1),'restocked',{'fixture_only':True}) for i in selected if i%10==4],request_refresh=False)
        result['changed_count']=len(changed);result['change_write_seconds']=time.perf_counter()-t
        for platform in ['domeggook','ownerclan']:
            result['runs'].append(measure_run(c,schema,cfg.size,platform,'incremental','changed-'+platform,AT+timedelta(hours=2)))
        for platform in ['domeggook','ownerclan']:
            result['runs'].append(measure_run(c,schema,cfg.size,platform,'incremental','expiry-'+platform,AT+timedelta(days=1)))
        for platform in ['domeggook','ownerclan']:
            r=measure_run(c,schema,cfg.size,platform,'incremental','unchanged-'+platform,AT+timedelta(days=1,seconds=1));result['runs'].append(r)
            assert r['processed']==0,'Unchanged products recalculated'
        peer=c.execute("SELECT * FROM products WHERE platform='ownerclan' AND external_product_id='S9-4'").fetchone()
        vendor=peer['source_specific']['vendorKey']
        result['supplier_affected_expected']=c.execute("SELECT count(*) n FROM products WHERE platform='ownerclan' AND source_specific->>'vendorKey'=%s",(vendor,)).fetchone()['n']
        _save_product_batch_with_retry(c,[fixture(4,'ownerclan',AT+timedelta(days=1,hours=1),cfg,7)])
        save_events(c,[ProductEvent('ownerclan','S9-4','SYNTHETIC_FIXTURE_ONLY','soldout',AT+timedelta(days=1,hours=1),AT+timedelta(days=1,hours=1),'soldout',{'fixture_only':True})],request_refresh=False)
        result['runs'].append(measure_run(c,schema,cfg.size,'ownerclan','incremental','supplier-ownerclan',AT+timedelta(days=1,hours=2)))
        missing_evidence_probe(c,schema,cfg.size,result)
        result['sort_plan']=c.execute("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT row_number() OVER(ORDER BY home DESC,product_id),product_id,payload FROM product_supply_scores WHERE platform='ownerclan' AND home IS NOT NULL AND (payload->>'eligible')::boolean").fetchone()
        result['after']=dbsize(c);result['after_resources']=guard();c.commit()
    (OUT/f'{cfg.size}.json').write_text(json.dumps(result,default=str,indent=2),encoding='utf8')
    print('SCALE COMPLETE '+str(cfg.size),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--size',type=int,required=True);p.add_argument('--seed',type=int,default=917);p.add_argument('--max-options',type=int,default=5);p.add_argument('--history-changes',type=int,choices=range(5),default=4);p.add_argument('--vendors',type=int,default=30);p.add_argument('--skew',type=float,default=.2);p.add_argument('--change-rate',type=float,default=.01)
    cfg=p.parse_args()
    telemetry={'rss_peak':0,'host_available_min':psutil.virtual_memory().available,'docker_memory':[]}
    finished=threading.Event()
    def resources():
        while not finished.is_set():
            telemetry['rss_peak']=max(telemetry['rss_peak'],psutil.Process().memory_info().rss)
            telemetry['host_available_min']=min(telemetry['host_available_min'],psutil.virtual_memory().available)
            try:
                telemetry['docker_memory'].append(docker('stats',CONTAINER,'--no-stream','--format','{{.MemUsage}}'))
                if int(docker('exec',CONTAINER,'du','-s','-B1','/var/lib/postgresql/data').split()[0])>4*2**30: RESOURCE_STOP.set()
            except Exception: pass
            finished.wait(2)
    worker=threading.Thread(target=resources,daemon=True);worker.start()
    try: run(cfg)
    finally:
        finished.set();worker.join(timeout=20)
        OUT.mkdir(parents=True,exist_ok=True)
        (OUT/f'{cfg.size}-resources.json').write_text(json.dumps(telemetry,indent=2),encoding='utf8')
