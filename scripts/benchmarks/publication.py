"""Bounded publication capacity on the operations benchmark server, never production."""
import json,time
from pathlib import Path
from scripts.benchmarks import pipeline as b
from product_data_collector.persistence.scoring import publish

def run():
    b.CONTAINER='chozy-operations-performance'
    b.guard()
    results=[]
    with b.connect() as c:
        c.execute('CREATE SCHEMA publication_capacity');c.execute('SET search_path TO publication_capacity,collection_pipeline_1000_v3,public')
        for table in ('product_supply_scores','scoring_runs','supply_home_batches','supply_home_items','scoring_queue','product_discovery_targets'):
            c.execute(f'CREATE TABLE publication_capacity.{table} (LIKE collection_pipeline_1000_v3.{table} INCLUDING ALL)')
        c.commit()
        template=c.execute("SELECT * FROM collection_pipeline_1000_v3.product_supply_scores WHERE platform='ownerclan' AND home IS NOT NULL AND (payload->>'eligible')::boolean LIMIT 1").fetchone()
        c.commit()
        for size in (100000,500000):
            b.guard()
            c.execute('TRUNCATE publication_capacity.product_supply_scores')
            c.execute('''INSERT INTO product_supply_scores(product_id,platform,as_of,formula_version,home,result,input_fingerprint,payload)
              SELECT i,'ownerclan',%s,'supply_v2',(i %% 101),%s,'synthetic-only',
              %s::jsonb || jsonb_build_object('productId',i,'score',i %% 101,'fixture_only',true)
              FROM generate_series(1,%s) i''',(template['as_of'],b.Jsonb(template['result']),b.Jsonb(template['payload']),size))
            c.execute('ANALYZE product_supply_scores');c.commit()
            for memory in ('8MB','64MB'):
                c.execute('SET work_mem TO '+"'"+memory+"'")
                plan=c.execute("EXPLAIN (ANALYZE,BUFFERS,SETTINGS,FORMAT JSON) SELECT row_number() OVER(ORDER BY home DESC,product_id),product_id,payload FROM product_supply_scores WHERE platform='ownerclan' AND home IS NOT NULL AND (payload->>'eligible')::boolean AND as_of<=%s AND formula_version IN ('supply_v1','supply_v2')",(template['as_of'],)).fetchone()
                rid=f'capacity-{size}-{memory}'
                c.execute("INSERT INTO scoring_runs(run_id,mode,platform,as_of,high_id,formula_version,scanned) VALUES(%s,'initial','ownerclan',%s,%s,'supply_v2',true)",(rid,template['as_of'],size))
                c.commit()
                run=c.execute('SELECT * FROM scoring_runs WHERE run_id=%s',(rid,)).fetchone()
                start_lsn=c.execute('SELECT pg_current_wal_insert_lsn() AS lsn').fetchone()['lsn'];c.commit()
                start=time.perf_counter();publish(c,run);c.commit();seconds=time.perf_counter()-start
                wal=c.execute('SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),%s) AS n',(start_lsn,)).fetchone()['n']
                valid=c.execute('''SELECT count(*) AS n,min(rank) AS lo,max(rank) AS hi,
                    count(*) FILTER(WHERE (payload->>'productId')::bigint<>product_id) AS mismatches
                    FROM supply_home_items WHERE batch_id=%s''',(rid,)).fetchone()
                assert valid=={'n':size,'lo':1,'hi':size,'mismatches':0}
                c.commit()
                results.append({'rows':size,'work_mem':memory,'seconds_including_commit':seconds,'wal_bytes':int(wal),'sort_plan':plan,'integrity':valid})
                print(json.dumps({k:v for k,v in results[-1].items() if k!='sort_plan'}),flush=True)
    Path('artifacts/experiments/operations-readiness/publication-capacity.json').write_text(json.dumps(results,default=str,indent=2),encoding='utf8')


if __name__ == "__main__":
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute",action="store_true")
    args=parser.parse_args()
    if args.execute: run()
    else: print(json.dumps({"execute":False,"sizes":[100000,500000],"work_mem":["8MB","64MB"]}))
