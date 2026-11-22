"""Compare conflict handling with identical publication FKs/indexes after publication_costs."""
import argparse
import json
import time
from pathlib import Path
from scripts.benchmarks import pipeline as b
from scripts.benchmarks.publication_profile import TimedConnection
from product_data_collector.persistence import scoring

OUT = Path('artifacts/experiments/publication-costs/conflicts.json')


def run():
    b.CONTAINER = 'chozy-publication-costs'
    result = {'external_api_calls':0, 'rows':500000, 'runs':[],
              'limits':'Same synthetic source as publication_costs. Every trial starts after explicit CHECKPOINT; source/OS cache is not cold. Plain INSERT is experimental, not a production change.'}
    b.guard()
    payload = json.loads(OUT.with_name('results.json').read_text(encoding='utf8'))['payload']
    with b.connect('publication_costs') as c:
        c.execute('CREATE TABLE conflict_items (LIKE supply_home_items INCLUDING ALL)')
        c.execute('ALTER TABLE conflict_items ADD FOREIGN KEY(batch_id) REFERENCES supply_home_batches(batch_id)')
        c.commit()
        for repeat, case in enumerate(('conflict','plain','plain','conflict','plain','conflict')):
            b.guard()
            c.execute('TRUNCATE conflict_items')
            c.commit()
            start = time.perf_counter()
            c.execute('CHECKPOINT')
            c.commit()
            checkpoint_seconds = time.perf_counter()-start
            start = time.perf_counter()
            plan = c.execute("""EXPLAIN (ANALYZE,BUFFERS,WAL,SETTINGS,FORMAT JSON)
                INSERT INTO conflict_items(batch_id,rank,product_id,payload)
                SELECT 'cost-500000',row_number() OVER(ORDER BY home DESC,product_id),product_id,payload
                FROM product_supply_scores WHERE platform='ownerclan' AND home IS NOT NULL
                AND (payload->>'eligible')::boolean AND as_of<=%s AND formula_version IN ('supply_v1','supply_v2')"""
                + (' ON CONFLICT DO NOTHING' if case=='conflict' else ''), (b.AT,)).fetchone()
            insert_seconds = time.perf_counter()-start
            start = time.perf_counter()
            c.commit()
            commit_seconds = time.perf_counter()-start
            valid = c.execute("""SELECT count(*) AS n,count(*) FILTER(WHERE
                product_id NOT BETWEEN 1 AND 500000 OR
                rank <> (100-product_id%%101)*(500000/101) +
                  greatest(0,least(100,500000%%101)-product_id%%101)+(product_id-1)/101+1 OR
                payload IS DISTINCT FROM (%s::jsonb || jsonb_build_object('productId',product_id,'score',product_id%%101)))
                AS mismatches FROM conflict_items""", (b.Jsonb(payload),)).fetchone()
            assert valid == {'n':500000,'mismatches':0}, valid
            c.commit()
            record = dict(case=case,repeat=repeat,insert_seconds=insert_seconds,commit_seconds=commit_seconds,
                          checkpoint_before_seconds=checkpoint_seconds,integrity=valid,plan=plan)
            result['runs'].append(record)
            OUT.write_text(json.dumps(result,default=str,indent=2),encoding='utf8')
            print(json.dumps({k:v for k,v in record.items() if k!='plan'}),flush=True)
        c.execute('DROP TABLE conflict_items')
        c.commit()


def verify_publish():
    """Exercise the adopted real function three times on fresh fixture batches."""
    b.CONTAINER = 'chozy-publication-costs'
    result = json.loads(OUT.read_text(encoding='utf8'))
    payload = json.loads(OUT.with_name('results.json').read_text(encoding='utf8'))['payload']
    result['adopted_publish'] = []
    with b.connect('publication_costs') as c:
        for repeat in range(3):
            b.guard()
            c.execute('TRUNCATE supply_home_items,supply_home_batches,scoring_runs')
            rid = 'adopted-'+str(repeat)
            c.execute("""INSERT INTO scoring_runs(run_id,mode,platform,as_of,high_id,formula_version,scanned)
                VALUES(%s,'initial','ownerclan',%s,500000,'supply_v2',true)""", (rid,b.AT))
            c.commit()
            state = c.execute('SELECT * FROM scoring_runs WHERE run_id=%s',(rid,)).fetchone()
            c.commit()
            c.execute('CHECKPOINT')
            c.commit()
            meter = TimedConnection(c)
            start = time.perf_counter()
            scoring.publish(meter,state)
            c.commit()
            seconds = time.perf_counter()-start
            scoring.publish(c,state)
            c.commit()
            valid = c.execute("""SELECT count(*) AS n,count(*) FILTER(WHERE
                product_id NOT BETWEEN 1 AND 500000 OR
                rank <> (100-product_id%%101)*(500000/101) +
                  greatest(0,least(100,500000%%101)-product_id%%101)+(product_id-1)/101+1 OR
                payload IS DISTINCT FROM (%s::jsonb || jsonb_build_object('productId',product_id,'score',product_id%%101)))
                AS mismatches FROM supply_home_items""", (b.Jsonb(payload),)).fetchone()
            assert valid == {'n':500000,'mismatches':0}, valid
            c.commit()
            record = dict(repeat=repeat,seconds_including_commit=seconds,integrity_after_retry=valid,calls=meter.calls)
            result['adopted_publish'].append(record)
            OUT.write_text(json.dumps(result,default=str,indent=2),encoding='utf8')
            print(json.dumps({k:v for k,v in record.items() if k!='calls'}),flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute',action='store_true')
    p.add_argument('--verify-publish',action='store_true')
    args = p.parse_args()
    if args.execute:
        verify_publish() if args.verify_publish else run()
    else:
        print(json.dumps({'execute':False,'rows':500000,'repeats_per_case':3,'external_api_calls':0}))


if __name__=='__main__':
    main()
