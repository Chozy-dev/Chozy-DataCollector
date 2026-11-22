"""Offline 5000-product scoring/publication replay from an external logical backup.

Keeps original observations/evidence; never proves live API completion.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time
from datetime import datetime, timezone

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
from scripts.benchmarks import pipeline as b
from product_data_collector.operations.fence import certify_collection
from product_data_collector.persistence import scoring

OUT = Path('artifacts/experiments/ownerclan-live-e2e')


def run(dump):
    b.CONTAINER = 'chozy-ownerclan-live-e2e'
    b.guard()
    root = Path(__file__).resolve().parents[2]
    dump = dump.resolve()
    if root == dump or root in dump.parents or not dump.is_file():
        raise ValueError('An external existing backup is required')
    result = {'scope': 'offline 5000 previously stored snapshots, not the current live-key sample',
              'api_calls': 0, 'source_dump_sha256': hashlib.sha256(dump.read_bytes()).hexdigest(),
              'started_at': datetime.now(timezone.utc).isoformat(), 'copied_rows': {}}
    subprocess.run(['docker','exec',b.CONTAINER,'createdb','-U','postgres','collector_replay_source'], check=True)
    with dump.open('rb') as source:
        subprocess.run(['docker','exec','-i',b.CONTAINER,'pg_restore','--exit-on-error','--no-owner','--no-acl','-U','postgres','-d','collector_replay_source'], stdin=source, check=True, timeout=120)
    with psycopg.connect(b.DSN.replace('dbname=collector_stage1_test','dbname=collector_replay_source'), row_factory=b.dict_row) as source, b.connect() as target:
        source.execute('SET default_transaction_read_only=on'); source.commit()
        products = source.execute("SELECT id,external_product_id FROM products WHERE platform='ownerclan' ORDER BY id LIMIT 5000").fetchall()
        assert len(products) == 5000
        ids = [p['id'] for p in products]
        target.execute('CREATE SCHEMA ownerclan_snapshot_replay')
        target.execute('SET search_path TO ownerclan_snapshot_replay');target.commit()
        b.init_schema(target)
        for name in ('stage7_supply_scoring','stage8_operations','stage7_history_policy_v2','stage10_scoring_performance'):
            b.apply_migration(target,name)
        t = time.perf_counter()
        # Exact existing rows, ids and observations; no pending requests are
        # modified/copied/marked complete. This is a separate snapshot replay.
        for table in ('products','product_events','product_history','product_options'):
            columns = source.execute("SELECT column_name,data_type FROM information_schema.columns WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position",(table,)).fetchall()
            names = [x['column_name'] for x in columns]
            json_names = {x['column_name'] for x in columns if x['data_type'] in ('json','jsonb')}
            condition = sql.SQL('id=ANY(%s)') if table=='products' else sql.SQL('product_id=ANY(%s)')
            rows = source.execute(sql.SQL('SELECT * FROM {} WHERE {} ORDER BY id').format(sql.Identifier(table),condition),(ids,))
            n = 0
            with target.cursor().copy(sql.SQL('COPY {} ({}) FROM STDIN').format(sql.Identifier(table),sql.SQL(',').join(map(sql.Identifier,names)))) as copy:
                for row in rows:
                    copy.write_row([Jsonb(row[k]) if k in json_names and row[k] is not None else row[k] for k in names]);n += 1
            target.commit();result['copied_rows'][table] = n
        result['copy_seconds'] = time.perf_counter()-t
        certify_collection(target,'ownerclan','offline-original-snapshot-replay-only')
        original_publish = scoring.publish
        def timed_publish(conn, state):
            start = time.perf_counter();original_publish(conn,state)
            result['publish_seconds_excluding_final_commit'] = time.perf_counter()-start
        scoring.publish = timed_publish
        t = time.perf_counter()
        try:
            state = scoring.run_scoring(target,run_id='snapshot-initial',mode='initial',platform='ownerclan',as_of=datetime.now(timezone.utc),chunk_size=100)
        finally:
            scoring.publish = original_publish
        result['score_and_publish_seconds'] = time.perf_counter()-t
        assert state['status']=='completed' and state['processed']==5000
        result['processed'] = state['processed']
        result['scores'] = target.execute('SELECT count(*) AS n,count(*) FILTER(WHERE reliability IS NULL) AS null_reliability,count(*) FILTER(WHERE stability IS NULL) AS null_stability FROM product_supply_scores').fetchone()
        result['batch'] = target.execute('SELECT batch_id,item_count FROM supply_home_published').fetchone()
        result['rank_payload_differences'] = target.execute("""WITH expected AS (SELECT product_id,payload,row_number() OVER(ORDER BY home DESC,product_id) AS rank FROM product_supply_scores WHERE home IS NOT NULL AND (payload->>'eligible')::boolean)
            SELECT count(*) AS n FROM expected e FULL JOIN supply_home_items i USING(product_id) WHERE e.rank IS DISTINCT FROM i.rank OR e.payload IS DISTINCT FROM i.payload""").fetchone()['n']
        assert result['rank_payload_differences']==0
        t = time.perf_counter()
        result['home_items'] = len(target.execute("SELECT read_supply_home('OWNERCLAN',10) AS home").fetchone()['home']['items'])
        result['home_read_seconds'] = time.perf_counter()-t
        result['evidence'] = target.execute('SELECT (SELECT count(*) FROM scoring_coverage) AS coverage,(SELECT count(*) FROM scoring_supplier_identity) AS identity').fetchone()
        assert result['evidence']=={'coverage':0,'identity':0}
        target.commit()
    result['finished_at'] = datetime.now(timezone.utc).isoformat()
    (OUT/'snapshot-replay.json').write_text(json.dumps(result,indent=2,default=str),encoding='utf8')
    print(json.dumps(result,default=str),flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute',action='store_true')
    parser.add_argument('--dump',type=Path)
    args=parser.parse_args()
    if not args.execute:
        print(json.dumps({'execute':False,'products':5000,'api_calls':0,'source':'explicit external backup'}))
    elif args.dump is None:
        parser.error('--dump is required')
    else:
        run(args.dump)


if __name__=='__main__':
    main()
