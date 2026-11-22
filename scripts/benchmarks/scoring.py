"""Scoring optimization comparison; pipeline guards and fixtures unchanged."""
import argparse
import importlib.util
from pathlib import Path
from scripts.benchmarks import pipeline as b
p=argparse.ArgumentParser();p.add_argument('--variant',choices=['before','after'],required=True);p.add_argument('--size',type=int,choices=[1000,10000],required=True);a=p.parse_args()
b.CONTAINER='chozy-scoring-optimization-benchmark'
b.OUT=Path('artifacts/experiments/scoring-optimization-benchmark')/a.variant
if a.variant=='before':
    spec=importlib.util.spec_from_file_location('product_data_collector.persistence.baseline_scoring',Path('artifacts/experiments/scoring-optimization-benchmark/baseline_scoring.py'))
    mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod);b.scoring=mod
import inspect
source=inspect.getsource(b.run).replace("schema=f'collection_pipeline_{cfg.size}_v3'", "schema=f'scoring_"+a.variant+"_{cfg.size}_v3'")
exec(compile(source,'scoring_benchmark_run','exec'),b.__dict__)
original_connect=b.connect
def connect(schema=None):
    c=original_connect(schema)
    if schema and c.execute('SELECT current_schema() AS s').fetchone()['s']!=schema:
        c.close();raise RuntimeError('Benchmark schema missing; refusing public fallback')
    c.commit();return c
b.connect=connect
original_apply=b.apply_migration
def apply(c,name):
    result=original_apply(c,name)
    if a.variant=='after' and name=='stage7_history_policy_v2':original_apply(c,'stage10_scoring_performance')
    return result
b.apply_migration=apply
cfg=argparse.Namespace(size=a.size,seed=917,max_options=5,history_changes=4,vendors=30,skew=.2,change_rate=.01)
import json, threading, psutil
telemetry={'rss_peak':0,'host_available_min':psutil.virtual_memory().available,'docker_memory':[]}
finished=threading.Event()
def resources():
    while not finished.is_set():
        telemetry['rss_peak']=max(telemetry['rss_peak'],psutil.Process().memory_info().rss)
        telemetry['host_available_min']=min(telemetry['host_available_min'],psutil.virtual_memory().available)
        try:
            telemetry['docker_memory'].append(b.docker('stats',b.CONTAINER,'--no-stream','--format','{{.MemUsage}}'))
            if int(b.docker('exec',b.CONTAINER,'du','-s','-B1','/var/lib/postgresql/data').split()[0])>4*2**30:b.RESOURCE_STOP.set()
        except Exception: pass
        finished.wait(2)
worker=threading.Thread(target=resources,daemon=True);worker.start()
try:b.run(cfg)
finally:
    finished.set();worker.join(timeout=20)
    b.OUT.mkdir(parents=True,exist_ok=True)
    (b.OUT/f'{cfg.size}-resources.json').write_text(json.dumps(telemetry,indent=2),encoding='utf8')
