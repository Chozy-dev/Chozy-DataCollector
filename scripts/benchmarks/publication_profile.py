"""Profile real publication on a dedicated, disposable PostgreSQL server only."""
import argparse
import json
import time
from pathlib import Path
from types import SimpleNamespace

from scripts.benchmarks import pipeline as b
from product_data_collector.persistence import scoring
from product_data_collector.persistence.supply_scoring_schema import DDL

OUT = Path('artifacts/experiments/publication-profile')


class TimedConnection:
    def __init__(self, conn):
        self.conn = conn
        self.calls = []

    def execute(self, query, params=None):
        start = time.perf_counter()
        result = self.conn.execute(query, params)
        self.calls.append({'sql': query, 'seconds': time.perf_counter() - start})
        return result


def save(result):
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / 'profile.json').write_text(json.dumps(result, default=str, indent=2), encoding='utf8')


def run():
    b.CONTAINER = 'chozy-publication-profile'
    result = {'external_api_calls': 0, 'resources': b.guard(), 'runs': [],
              'limits': 'Synthetic eligible payload; source-product FK/input triggers omitted; publication FKs retained; no production concurrency; uncontrolled cache/checkpoint state.'}
    with b.connect() as c:
        # Build only the tables used by publish; names/checks/indexes come from real DDL.
        c.execute('CREATE SCHEMA publication_profile')
        c.execute('SET search_path TO publication_profile,public')
        c.execute('CREATE TABLE products(id bigint PRIMARY KEY)')
        for ddl in DDL:
            if ddl.startswith('CREATE TABLE IF NOT EXISTS ') and any(
                ddl.startswith('CREATE TABLE IF NOT EXISTS ' + name + ' (')
                for name in ('product_supply_scores', 'scoring_runs', 'supply_home_batches', 'supply_home_items', 'scoring_queue')):
                c.execute(ddl)
        c.execute('ALTER TABLE product_supply_scores DROP CONSTRAINT product_supply_scores_product_id_fkey')
        c.execute('CREATE INDEX idx_scoring_rank ON product_supply_scores(platform,home DESC,product_id)')
        c.execute('CREATE TABLE product_discovery_targets(platform text,requested_version bigint,completed_version bigint)')
        c.commit()
        # Derive a small synthetic payload from a fixture; uncertified metrics stay NULL.
        cfg = SimpleNamespace(seed=917, max_options=5, vendors=30, skew=.2)
        product = b.fixture(8, 'ownerclan', b.AT, cfg)
        # Representative payload subset; values are synthetic, never certification.
        payload = {'productId': 1, 'externalProductId': 'SYNTHETIC_ONLY', 'source': 'OWNERCLAN',
                   'name': product.get('product_name'), 'url': 'https://fixture.invalid/1',
                   'imageUrl': 'https://fixture.invalid/1.jpg', 'supplyPrice': 10000,
                   'score': 70, 'stability': 70, 'reliability': None, 'eligible': True,
                   'asOf': b.AT.isoformat(), 'observedAt': b.AT.isoformat(),
                   'formulaVersion': 'supply_v2', 'missingReasons': {'supplier': 'unverified'},
                   'fixture_only': True}
        result['payload'] = payload
        result['settings'] = c.execute("SELECT name,setting,unit FROM pg_settings WHERE name IN ('work_mem','shared_buffers','max_wal_size','synchronous_commit','fsync','track_io_timing','server_version')").fetchall()
        for size in (100000, 500000):
            b.guard()
            c.execute('TRUNCATE product_supply_scores')
            c.execute("""INSERT INTO product_supply_scores(product_id,platform,as_of,formula_version,home,result,input_fingerprint,payload)
              SELECT i,'ownerclan',%s,'supply_v2',i %% 101,'{}','fixture',
              %s::jsonb || jsonb_build_object('productId',i,'score',i %% 101)
              FROM generate_series(1,%s) i""", (b.AT, b.Jsonb(payload), size))
            c.execute('ANALYZE product_supply_scores')
            c.commit()
            # ABBA counters fixed order bias; no global planner/memory setting changes.
            for repeat, variant in enumerate(('default', 'index', 'index', 'default')):
                b.guard()
                c.execute('TRUNCATE supply_home_items,supply_home_batches,scoring_runs')
                c.execute('SET enable_seqscan TO ' + ('off' if variant == 'index' else 'on'))
                rid = f'profile-{size}-{repeat}'
                c.execute("INSERT INTO scoring_runs(run_id,mode,platform,as_of,high_id,formula_version,scanned) VALUES(%s,'initial','ownerclan',%s,%s,'supply_v2',true)", (rid, b.AT, size))
                c.commit()
                state = c.execute('SELECT * FROM scoring_runs WHERE run_id=%s', (rid,)).fetchone()
                before = c.execute('SELECT pg_current_wal_insert_lsn() AS lsn').fetchone()['lsn']
                c.commit()
                meter = TimedConnection(c)
                start = time.perf_counter()
                scoring.publish(meter, state)
                commit_start = time.perf_counter()
                c.commit()
                total = time.perf_counter() - start
                commit_seconds = time.perf_counter() - commit_start
                valid = c.execute("""SELECT count(*) AS n,min(rank) AS lo,max(rank) AS hi,
                  count(*) FILTER(WHERE payload->>'productId'<>product_id::text) AS mismatches
                  FROM supply_home_items""").fetchone()
                assert (valid['n'], valid['lo'], valid['hi'], valid['mismatches']) == (size, 1, size, 0)
                # Exact expected rank for ties and every score, not just row count.
                diffs = c.execute("""WITH expected AS (SELECT product_id,payload,row_number() OVER(ORDER BY home DESC,product_id) AS rank FROM product_supply_scores)
                  SELECT count(*) AS n FROM expected e FULL JOIN supply_home_items i USING(product_id)
                  WHERE e.rank IS DISTINCT FROM i.rank OR e.payload IS DISTINCT FROM i.payload""").fetchone()['n']
                assert diffs == 0
                wal = c.execute('SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),%s) AS n', (before,)).fetchone()['n']
                sizes = c.execute("SELECT pg_table_size('supply_home_items') AS heap_bytes,pg_indexes_size('supply_home_items') AS index_bytes").fetchone()
                c.commit()
                rec = {'rows': size, 'variant': variant, 'repeat': repeat, 'seconds_including_commit': total,
                       'commit_seconds': commit_seconds, 'wal_bytes': int(wal), 'calls': meter.calls,
                       'rank_payload_differences': diffs, 'sizes': sizes}
                result['runs'].append(rec)
                save(result)
                print(json.dumps({k: v for k, v in rec.items() if k != 'calls'}), flush=True)
            # Actual INSERT plan (rolled back); heap-only target also omits FKs.
            # This bounds combined index/conflict/FK overhead, not each component.
            c.execute('SET enable_seqscan TO on')
            c.execute('TRUNCATE supply_home_items')
            c.execute('CREATE TABLE items_heap (LIKE supply_home_items INCLUDING STORAGE)')
            c.commit()
            result.setdefault('insert_plans', [])
            for target in ('supply_home_items', 'items_heap'):
                b.guard()
                plan = c.execute(f"""EXPLAIN (ANALYZE,BUFFERS,WAL,SETTINGS,FORMAT JSON)
                  INSERT INTO {target}(batch_id,rank,product_id,payload)
                  SELECT %s,row_number() OVER(ORDER BY home DESC,product_id),product_id,payload
                  FROM product_supply_scores WHERE platform='ownerclan' AND home IS NOT NULL
                  AND (payload->>'eligible')::boolean AND as_of<=%s AND formula_version IN ('supply_v1','supply_v2')
                  ON CONFLICT DO NOTHING""", (rid, b.AT)).fetchone()
                result['insert_plans'].append({'rows': size, 'target': target, 'plan': plan})
                c.rollback()
                save(result)
            c.execute('DROP TABLE items_heap')
            c.commit()
    result['resources_after'] = b.guard()
    save(result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    if args.execute:
        run()
    else:
        print(json.dumps({'execute': False, 'sizes': [100000, 500000], 'repeats': 'ABBA', 'external_api_calls': 0}))


if __name__ == '__main__':
    main()
