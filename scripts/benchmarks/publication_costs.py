"""Separate publication write costs on a disposable, bounded PostgreSQL server."""
import argparse
import json
import time
from pathlib import Path

from scripts.benchmarks import pipeline as b
from scripts.benchmarks.publication_profile import TimedConnection
from product_data_collector.persistence import scoring
from product_data_collector.persistence.supply_scoring_schema import DDL

OUT = Path('artifacts/experiments/publication-costs/results.json')
CASES = ('heap', 'indexes', 'unique', 'conflict', 'foreign_key', 'checkpoint')


def run():
    b.CONTAINER = 'chozy-publication-costs'
    result = {'external_api_calls': 0, 'resources': b.guard(), 'runs': [],
              'limits': 'Synthetic eligible payload; no source FK/input triggers or concurrent readers. Warm source, rotated order; no cold-cache claim. Component differences are paired comparisons, not additive percentages.'}
    with b.connect() as c:
        c.execute('CREATE SCHEMA publication_costs')
        c.execute('SET search_path TO publication_costs,public')
        c.execute('CREATE TABLE products(id bigint PRIMARY KEY)')
        for ddl in DDL:
            if any(ddl.startswith('CREATE TABLE IF NOT EXISTS ' + t + ' (') for t in
                   ('product_supply_scores', 'scoring_runs', 'supply_home_batches', 'supply_home_items', 'scoring_queue')):
                c.execute(ddl)
        c.execute('ALTER TABLE product_supply_scores DROP CONSTRAINT product_supply_scores_product_id_fkey')
        c.execute('CREATE INDEX idx_scoring_rank ON product_supply_scores(platform,home DESC,product_id)')
        c.execute('CREATE TABLE product_discovery_targets(platform text,requested_version bigint,completed_version bigint)')
        payload = {'productId': 1, 'externalProductId': 'SYNTHETIC_ONLY', 'source': 'OWNERCLAN',
                   'name': '합성 생활 주방 상품', 'url': 'https://fixture.invalid/1',
                   'imageUrl': 'https://fixture.invalid/1.jpg', 'supplyPrice': 10000,
                   'score': 70, 'stability': 70, 'reliability': None, 'eligible': True,
                   'asOf': b.AT.isoformat(), 'observedAt': b.AT.isoformat(),
                   'formulaVersion': 'supply_v2', 'missingReasons': {'supplier': 'unverified'}, 'fixture_only': True}
        result['payload'] = payload
        result['settings'] = c.execute("SELECT name,setting,unit FROM pg_settings WHERE name IN ('work_mem','shared_buffers','max_wal_size','checkpoint_timeout','synchronous_commit','fsync','track_io_timing','server_version')").fetchall()
        c.commit()
        for size in (100000, 500000):
            c.execute('TRUNCATE product_supply_scores')
            c.execute("""INSERT INTO product_supply_scores(product_id,platform,as_of,formula_version,home,result,input_fingerprint,payload)
              SELECT i,'ownerclan',%s,'supply_v2',i %% 101,'{}','fixture',
              %s::jsonb || jsonb_build_object('productId',i,'score',i %% 101)
              FROM generate_series(1,%s) i""", (b.AT, b.Jsonb(payload), size))
            c.execute('ANALYZE product_supply_scores')
            c.execute('TRUNCATE supply_home_items,supply_home_batches,scoring_runs')
            rid = f'cost-{size}'
            c.execute("INSERT INTO scoring_runs(run_id,mode,platform,as_of,high_id,formula_version,scanned) VALUES(%s,'initial','ownerclan',%s,%s,'supply_v2',true)", (rid, b.AT, size))
            c.execute("INSERT INTO supply_home_batches(batch_id,platform,as_of,formula_version) VALUES(%s,'ownerclan',%s,'supply_v2')", (rid, b.AT))
            c.commit()
            for repeat in range(3):
                # Rotate/reverse to expose time/cache order effects.
                order = CASES[repeat * 2:] + CASES[:repeat * 2]
                if repeat == 1:
                    order = tuple(reversed(order))
                for case in order:
                    b.guard()
                    c.execute('CREATE TABLE cost_items (LIKE supply_home_items INCLUDING STORAGE INCLUDING CONSTRAINTS)')
                    if case != 'heap':
                        unique = '' if case == 'indexes' else 'UNIQUE '
                        c.execute(f'CREATE {unique}INDEX cost_rank ON cost_items(batch_id,rank)')
                        c.execute(f'CREATE {unique}INDEX cost_product ON cost_items(batch_id,product_id)')
                    if case in ('foreign_key', 'checkpoint'):
                        c.execute('ALTER TABLE cost_items ADD FOREIGN KEY(batch_id) REFERENCES supply_home_batches(batch_id)')
                    c.commit()
                    checkpoint_seconds = 0
                    if case == 'checkpoint':
                        start = time.perf_counter()
                        c.execute('CHECKPOINT')
                        c.commit()
                        checkpoint_seconds = time.perf_counter() - start
                    before = c.execute('SELECT pg_current_wal_insert_lsn() AS lsn').fetchone()['lsn']
                    bg_before = c.execute('SELECT * FROM pg_stat_bgwriter').fetchone()
                    c.commit()
                    conflict = ' ON CONFLICT DO NOTHING' if case in ('conflict', 'foreign_key', 'checkpoint') else ''
                    start = time.perf_counter()
                    plan = c.execute("""EXPLAIN (ANALYZE,BUFFERS,WAL,SETTINGS,FORMAT JSON)
                      INSERT INTO cost_items(batch_id,rank,product_id,payload)
                      SELECT %s,row_number() OVER(ORDER BY home DESC,product_id),product_id,payload
                      FROM product_supply_scores WHERE platform='ownerclan' AND home IS NOT NULL
                      AND (payload->>'eligible')::boolean AND as_of<=%s AND formula_version IN ('supply_v1','supply_v2')""" + conflict, (rid, b.AT)).fetchone()
                    insert_seconds = time.perf_counter() - start
                    start = time.perf_counter()
                    c.commit()
                    commit_seconds = time.perf_counter() - start
                    wal = int(c.execute('SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),%s) AS n', (before,)).fetchone()['n'])
                    sizes = c.execute("SELECT pg_table_size('cost_items') AS heap_bytes,pg_indexes_size('cost_items') AS index_bytes").fetchone()
                    valid = c.execute("""SELECT count(*) AS n,count(DISTINCT product_id) AS ids,
                      count(DISTINCT rank) AS ranks,count(*) FILTER (WHERE
                        product_id NOT BETWEEN 1 AND %s OR
                        rank <> (100-product_id%%101)*(%s/101) +
                          greatest(0,least(100,%s%%101)-product_id%%101) + (product_id-1)/101+1 OR
                        payload IS DISTINCT FROM (%s::jsonb || jsonb_build_object('productId',product_id,'score',product_id%%101))
                      ) AS mismatches FROM cost_items""", (size,size,size,b.Jsonb(payload))).fetchone()
                    assert valid == {'n':size,'ids':size,'ranks':size,'mismatches':0}, valid
                    differences = valid['mismatches']
                    c.commit()
                    # Force a fresh statistics snapshot after commit, not the pre-insert one.
                    c.execute('SELECT pg_stat_clear_snapshot()')
                    bg_after = c.execute('SELECT * FROM pg_stat_bgwriter').fetchone()
                    rec = dict(rows=size, case=case, repeat=repeat, insert_seconds=insert_seconds,
                               commit_seconds=commit_seconds, wal_bytes=wal, sizes=sizes,
                               checkpoint_before_seconds=checkpoint_seconds,
                               rank_payload_differences=differences, plan=plan,
                               bgwriter_before=bg_before, bgwriter_after=bg_after)
                    result['runs'].append(rec)
                    OUT.parent.mkdir(parents=True, exist_ok=True)
                    OUT.write_text(json.dumps(result, default=str, indent=2), encoding='utf8')
                    print(json.dumps({k: rec[k] for k in ('rows','case','repeat','insert_seconds','commit_seconds','wal_bytes')}), flush=True)
                    c.execute('DROP TABLE cost_items')
                    c.commit()
            # The unchanged real publication path, including completion and retry no-op.
            state = c.execute('SELECT * FROM scoring_runs WHERE run_id=%s', (rid,)).fetchone()
            meter = TimedConnection(c)
            start = time.perf_counter()
            scoring.publish(meter, state)
            c.commit()
            wall = time.perf_counter() - start
            scoring.publish(c, state)
            c.commit()
            n = c.execute('SELECT count(*) AS n FROM supply_home_items').fetchone()['n']
            assert n == size
            result.setdefault('real_publish', []).append(dict(rows=size, seconds=wall, retry_rows=n, calls=meter.calls))
            c.commit()
        result['resources_after'] = b.guard()
        OUT.write_text(json.dumps(result, default=str, indent=2), encoding='utf8')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute', action='store_true')
    args = p.parse_args()
    if args.execute:
        run()
    else:
        print(json.dumps({'execute': False, 'sizes': [100000,500000], 'cases': CASES, 'repeats': 3, 'external_api_calls': 0}))


if __name__ == '__main__':
    main()
