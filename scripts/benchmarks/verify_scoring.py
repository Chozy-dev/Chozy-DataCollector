"""Paired business snapshot comparison and first-stockout probe, after both scales."""
import argparse, importlib.util, json
from pathlib import Path
from datetime import timedelta
from scripts.benchmarks import pipeline as b
p=argparse.ArgumentParser();p.add_argument('--size',type=int,choices=[1000,10000],required=True);a=p.parse_args()
b.CONTAINER='chozy-scoring-optimization-benchmark'
report={'size':a.size,'snapshot_comparison':{}}
with b.connect() as c:
    for tag in ['initial-domeggook','initial-ownerclan','changed-domeggook','changed-ownerclan','expiry-domeggook','expiry-ownerclan','supplier-ownerclan','missing-evidence-ownerclan']:
        snapshots=[]
        for variant in ['before','after']:
            payload='payload' if tag.startswith('initial-') else "payload-'asOf'-'observedAt'"
            rows=c.execute(f"SELECT rank,product_id,{payload} payload FROM scoring_{variant}_{a.size}_v3.supply_home_items WHERE batch_id=%s ORDER BY rank",(tag,)).fetchall()
            snapshots.append(rows)
        assert snapshots[0]==snapshots[1],tag
        report['snapshot_comparison'][tag]={'equal':True,'rows':len(snapshots[0]),'excluded_metadata':[] if tag.startswith('initial-') else ['asOf','observedAt']}
for variant in ['before','after']:
    b.OUT=Path('artifacts/experiments/scoring-optimization-benchmark')/variant
    if variant=='before':
        spec=importlib.util.spec_from_file_location('product_data_collector.persistence.baseline_scoring','artifacts/experiments/scoring-optimization-benchmark/baseline_scoring.py');mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod);b.scoring=mod
    else:
        from product_data_collector.persistence import scoring
        b.scoring=scoring
    schema=f'scoring_{variant}_{a.size}_v3'
    # Reuse the pipeline's real-writer first-stockout probe, restricting its scale loop.
    source=Path('scripts/benchmarks/supplier_probe.py').read_text(encoding='utf8')
    source=source.replace('for size in (1000,10000):',f'for size in ({a.size},):').replace("schema=f'collection_pipeline_{size}_v3'", "schema=f'scoring_"+variant+"_{size}_v3'")
    exec(compile(source,'supplier_stockout_probe.py','exec'),{})
    path=b.OUT/f'{a.size}.json';result=json.loads(path.read_text(encoding='utf8'))
    from argparse import Namespace
    with b.connect(schema) as c:
        changed=0
        for platform,remainder in [('domeggook',1),('ownerclan',0)]:
            rows=[]
            peer_id=result['supplier_rate_change']['peer_id']
            candidates=c.execute("SELECT external_product_id FROM products WHERE platform=%s AND id<>%s AND substring(external_product_id from 4)::int %% 10=%s ORDER BY id LIMIT %s",(platform,peer_id,remainder,round(a.size*.01/2))).fetchall()
            for candidate in candidates:
                i=int(candidate['external_product_id'][3:])
                row=b.fixture(i,platform,b.AT+timedelta(days=7),Namespace(**result['config']),5)
                row['primary_price']+=100
                for key in ('currentSupplyPrice','supplyCurrentSupplyPrice'):
                    if row['prices_payload'].get(key) is not None:row['prices_payload'][key]+=100
                for price in row['price_rows']:price['amount']+=100
                if platform=='ownerclan':
                    for option in row['options_payload']:
                        if option.get('price') is not None:option['price']+=100
                rows.append(row)
            b._save_product_batch_with_retry(c,rows);changed+=len(rows)
        assert changed==round(a.size*.01)
        result['pure_price_changed_count']=changed
        for platform in ['domeggook','ownerclan']:
            result['runs'].append(b.measure_run(c,schema,a.size,platform,'incremental','pure-price-'+platform,b.AT+timedelta(days=7,hours=1)))
        result['after_probes']=b.dbsize(c);c.commit()
    path.write_text(json.dumps(result,default=str,indent=2),encoding='utf8')
with b.connect() as c:
    for tag in ['supplier-first-stockout-v2','pure-price-domeggook','pure-price-ownerclan']:
        snapshots=[]
        for variant in ['before','after']:
            snapshots.append(c.execute(f"SELECT rank,product_id,payload-'asOf'-'observedAt' payload FROM scoring_{variant}_{a.size}_v3.supply_home_items WHERE batch_id=%s ORDER BY rank",(tag,)).fetchall())
        assert snapshots[0]==snapshots[1],tag
        report['snapshot_comparison'][tag]={'equal':True,'rows':len(snapshots[0]),'excluded_metadata':['asOf','observedAt']}
Path(f'artifacts/experiments/scoring-optimization-benchmark/{a.size}-business-comparison.json').write_text(json.dumps(report,indent=2),encoding='utf8')
