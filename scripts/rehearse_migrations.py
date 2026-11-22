"""Verify additive operations migrations on a restored, isolated local database.

Never discovers production credentials or restores over an existing database.
Use a dedicated PostgreSQL server: scoring advisory locks are database-wide.
"""
import argparse
import hashlib
import json
import os
import time
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row

from product_data_collector.persistence.migrations import apply_migration
from product_data_collector.persistence.product_storage_schema import validate_product_storage
from product_data_collector.persistence.schema import validate_schema
from product_data_collector.persistence.backfill import backfill_batch, compare_current_batch, integrity_counts

MIGRATIONS = ('stage7_supply_scoring', 'stage8_operations',
              'stage7_history_policy_v2', 'stage10_scoring_performance')


def sequences(conn):
    result = {}
    names = conn.execute('SELECT sequencename FROM pg_sequences WHERE schemaname=current_schema()').fetchall()
    for row in names:
        name = row['sequencename']
        result[name] = conn.execute(sql.SQL('SELECT last_value,is_called FROM {}')
                                    .format(sql.Identifier(name))).fetchone()
    conn.commit()
    return result


def fingerprints(conn, tables):
    result = {}
    for table in tables:
        digest = hashlib.sha256()
        count = 0
        with conn.cursor(name='rehearsal_fingerprint') as cursor:
            cursor.itersize = 5000
            cursor.execute(sql.SQL('SELECT to_jsonb(t)::text AS row FROM {} t ORDER BY to_jsonb(t)::text')
                           .format(sql.Identifier(table)))
            for row in cursor:
                digest.update(row['row'].encode('utf8') + b'\n')
                count += 1
        result[table] = {'rows': count, 'sha256': digest.hexdigest()}
    conn.commit()
    return result


def rehearse(conn):
    validate_schema(conn)
    validate_product_storage(conn, indexes=True)
    ledger = conn.execute('SELECT name FROM schema_migrations ORDER BY name').fetchall()
    tables = [r['tablename'] for r in conn.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname=current_schema() AND tablename<>'schema_migrations' ORDER BY tablename")]
    before = fingerprints(conn, tables)
    sequence_before = sequences(conn)
    result = {'ledger_before': ledger, 'before': before, 'sequences_before': sequence_before, 'migrations': []}
    for name in MIGRATIONS:
        start = time.perf_counter()
        lsn = conn.execute('SELECT pg_current_wal_insert_lsn() AS lsn').fetchone()['lsn']
        conn.commit()
        applied = apply_migration(conn, name)
        wal = conn.execute('SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),%s) AS n', (lsn,)).fetchone()['n']
        conn.commit()
        result['migrations'].append({'name': name, 'applied': applied,
                                     'seconds': time.perf_counter()-start, 'wal_bytes': int(wal)})
        assert not apply_migration(conn, name), 'Migration replay must be a no-op'
    result['after'] = fingerprints(conn, tables)
    assert before == result['after'], 'Existing rows changed during additive migration'
    sequence_after = sequences(conn)
    result['sequences_after'] = sequence_after
    assert all(sequence_after[k] == v for k, v in sequence_before.items()), 'Existing sequence changed'
    result['backfill'] = backfill_batch(conn, name='operations-rehearsal', batch_size=500)
    result['comparison'] = compare_current_batch(conn, after_id=0, limit=500)
    result['integrity'] = integrity_counts(conn)
    conn.commit()
    result['preserved_after_backfill'] = fingerprints(conn, tables)
    result['pending_details'] = conn.execute(
        'SELECT platform,count(*) AS count FROM product_discovery_targets WHERE requested_version>completed_version GROUP BY platform').fetchall()
    result['uncertified'] = conn.execute(
        'SELECT (SELECT count(*) FROM scoring_coverage) AS coverage,(SELECT count(*) FROM scoring_supplier_identity) AS identities,(SELECT count(*) FROM ops_collection_receipts) AS receipts').fetchone()
    conn.commit()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dsn-env', required=True)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--output', type=Path, default=Path('artifacts/validation/migration-rehearsal.json'))
    args = parser.parse_args()
    if not args.execute:
        print(json.dumps({'execute': False, 'migrations': MIGRATIONS}))
        return
    dsn = os.environ[args.dsn_env]
    target = conninfo_to_dict(dsn)
    if (target.get('host') != '127.0.0.1' or target.get('dbname') != 'collector_operations_copy'
            or target.get('port') in (None, '5432') or target.get('hostaddr') not in (None, '127.0.0.1')):
        parser.error('Only isolated localhost collector_operations_copy on a nonstandard port is allowed')
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("SET statement_timeout='120s'")
        conn.execute("SET lock_timeout='5s'")
        conn.commit()
        result = rehearse(conn)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, default=str, ensure_ascii=False, indent=2), encoding='utf8')
    print(json.dumps({'preserved': result['before'] == result['after'], 'migrations': result['migrations']}))


if __name__ == '__main__':
    main()
