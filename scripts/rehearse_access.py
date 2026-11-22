"""Rehearse runtime access on an already restored dedicated local copy, never production."""
import argparse
import json
import os
import secrets
import shutil
import subprocess
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

from product_data_collector.persistence.access import access_plan
from product_data_collector.persistence.migrations import apply_migration
from product_data_collector.persistence.postgres_storage import init_schema

MIGRATIONS = ('stage7_supply_scoring', 'stage8_operations', 'stage7_history_policy_v2',
              'stage10_scoring_performance', 'runtime_budget_guard')
OUT = Path('artifacts/validation/database-access.json')


def guard(dsn):
    target = conninfo_to_dict(dsn)
    if (target.get('host') != '127.0.0.1' or target.get('port') != '55450'
            or target.get('dbname') != 'collector_operations_copy'
            or target.get('hostaddr') not in (None, '127.0.0.1')):
        raise ValueError('Only isolated localhost:55450/collector_operations_copy allowed')
    info = json.loads(subprocess.check_output(['docker','inspect','chozy-access-rehearsal'], text=True))[0]
    if (info['Config']['Labels'].get('chozy.collector-access') != 'true'
            or info['HostConfig']['PortBindings']['5432/tcp'] != [{'HostIp':'127.0.0.1','HostPort':'55450'}]):
        raise ValueError('Dedicated access rehearsal container required')


def provision(c, schema, prefix, dsn):
    roles = dict(collector=prefix+'_collector', spring=prefix+'_spring', executor=prefix+'_functions')
    plan = access_plan(c, schema=schema, **roles)
    rendered = '\n'.join(stmt.as_string(c)+';' for stmt in plan)
    for stmt in plan:
        c.execute(stmt)
    c.commit()
    connects = {}
    for kind in ('collector','spring'):
        login = prefix+'_'+kind+'_login'
        password = secrets.token_urlsafe(24)
        c.execute(sql.SQL('CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS')
                  .format(sql.Identifier(login), sql.Literal(password)))
        c.execute(sql.SQL('GRANT {} TO {}').format(sql.Identifier(roles[kind]), sql.Identifier(login)))
        c.commit()
        login_dsn = make_conninfo(dsn, user=login, password=password)
        def connect(login_dsn=login_dsn):
            conn = psycopg.connect(login_dsn, row_factory=dict_row)
            conn.execute(sql.SQL('SET search_path TO {},public').format(sql.Identifier(schema)))
            conn.commit()
            return conn
        connects[kind] = connect
    return connects, rendered


def denied(connect, query):
    with connect() as c:
        try:
            c.execute(query)
        except psycopg.errors.InsufficientPrivilege:
            c.rollback()
            return
        c.rollback()
    raise AssertionError('Expected permission denial: '+query)


def check_access(admin, connects):
    collector, spring = connects['collector'], connects['spring']
    for connect in (collector, spring):
        for query in ("UPDATE api_budget_policy SET blocked=false", 'DELETE FROM api_budget_usage',
                      'CREATE TABLE forbidden(id int)', 'ALTER TABLE products DISABLE TRIGGER ALL',
                      "SELECT setval('ops_ownerclan_revision',1)", 'TRUNCATE products',
                      'DROP TABLE products', 'CREATE ROLE forbidden_role',
                      'UPDATE schema_migrations SET name=name'):
            denied(connect, query)
    for query in ('SELECT * FROM products', 'UPDATE products SET product_name=product_name',
                  'SELECT * FROM supply_home_items', 'UPDATE ops_jobs SET status=status'):
        denied(spring, query)
    denied(collector, 'UPDATE supply_home_items SET payload=payload')
    denied(collector, 'UPDATE scoring_coverage SET verified=true')
    admin.execute("INSERT INTO api_budget_policy(account,api,limits,platform_integrated) VALUES('access-fixture','default','{\"day\":5}',true)")
    admin.commit()
    def reserve(i):
        with (collector if i%2 else spring)() as c:
            r = c.execute("SELECT reserve_api_call('access-fixture','default',true) AS r").fetchone()['r']
            c.commit()
            return r
    with ThreadPoolExecutor(max_workers=4) as pool:
        reservations = list(pool.map(reserve, range(8)))
    assert reservations.count('reserved') == 5 and reservations.count('exhausted:day') == 3
    with collector() as c:
        # Input and temporary objects must not mask definer-owned budget tables.
        c.execute('CREATE TEMP TABLE api_budget_policy(account text,api text,blocked boolean)')
        c.execute("SELECT block_api_account('access-fixture','default')")
        c.commit()
    with spring() as c:
        assert c.execute("SELECT reserve_api_call('access-fixture','default',true) AS r").fetchone()['r'] == 'blocked'
        c.execute('CREATE TEMP TABLE supply_home_items(payload jsonb)')
        c.execute("SELECT read_supply_home('OWNERCLAN',10)")
    assert admin.execute("SELECT blocked FROM api_budget_policy WHERE account='access-fixture'").fetchone()['blocked']
    admin.commit()
    return {'permission_denials': 24, 'concurrent_reservations': reservations,
            'temp_shadowing': 'passed', 'block_without_unblock': 'passed'}


def run(dsn):
    guard(dsn)
    result = {'external_api_calls': 0, 'production_changes': 0}
    with psycopg.connect(dsn, row_factory=dict_row) as c:
        pending_before = c.execute('SELECT count(*) AS n FROM product_discovery_targets WHERE requested_version>completed_version').fetchone()['n']
        c.commit()
        for name in MIGRATIONS:
            apply_migration(c,name)
        # Reviewable production-shaped plan; only prefixed rehearsal roles are applied here.
        plan = '\n'.join(stmt.as_string(c)+';' for stmt in access_plan(c))
        connects, _ = provision(c,'public','copy_access',dsn)
        plan_path = Path('artifacts/validation/database-access-plan.sql')
        plan_path.parent.mkdir(parents=True,exist_ok=True)
        plan_path.write_text('-- Requires existing four migrations + runtime_budget_guard; new roles only.\nBEGIN;\n'+plan+'\nCOMMIT;\n',encoding='utf8')
        result['copy'] = check_access(c,connects)
        with connects['collector']() as runtime:
            init_schema(runtime)
            from product_data_collector.persistence.scoring import run_scoring
            from datetime import datetime,timezone
            try:
                run_scoring(runtime,run_id='access-pending-check',mode='initial',platform='ownerclan',as_of=datetime.now(timezone.utc),chunk_size=1)
            except RuntimeError as error:
                assert 'Detail refresh incomplete' in str(error)
            else:
                raise AssertionError('Restored pending must block scoring')
        result['pending_before'] = pending_before
        result['pending_after'] = c.execute('SELECT count(*) AS n FROM product_discovery_targets WHERE requested_version>completed_version').fetchone()['n']
        assert result['pending_before'] == result['pending_after']
        c.commit()
        # Repeat the real Runner regression with a non-owner login as its connection factory.
        schema = 'access_fixture_'+uuid.uuid4().hex
        c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
        c.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
        c.commit()
        init_schema(c)
        for name in MIGRATIONS:
            apply_migration(c,name)
        fixture_connects, _ = provision(c,schema,'fixture_access',dsn)
        scratch = Path('artifacts/experiments/database-access/scratch').resolve()
        scratch.mkdir(parents=True,exist_ok=False)
        from tests.integration.test_operations_ownerclan_e2e import test_ownerclan_initial_incremental_and_missing_detail_resume
        try:
            with pytest.MonkeyPatch.context() as patch:
                test_ownerclan_initial_incremental_and_missing_detail_resume(c,fixture_connects['collector'],scratch,patch)
            expected = c.execute("SELECT read_supply_home('OWNERCLAN',10) AS r").fetchone()['r']
            c.commit()
            with fixture_connects['spring']() as spring:
                assert spring.execute("SELECT read_supply_home('OWNERCLAN',10) AS r").fetchone()['r'] == expected
                spring.execute("UPDATE ops_alerts SET acknowledged_at=clock_timestamp()")
                spring.commit()
            result['runtime_initial_incremental_resume'] = 'passed'
            result['spring_same_completed_batch'] = 'passed'
            result['fixture'] = check_access(c,fixture_connects)
        finally:
            # Only this explicitly named workspace scratch directory is removed.
            assert scratch == Path.cwd().resolve() / 'artifacts/experiments/database-access/scratch'
            shutil.rmtree(scratch)
        OUT.parent.mkdir(parents=True,exist_ok=True)
        OUT.write_text(json.dumps(result,indent=2,ensure_ascii=False),encoding='utf8')
    print(json.dumps(result,ensure_ascii=False))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute',action='store_true')
    p.add_argument('--dsn-env')
    a = p.parse_args()
    if not a.execute:
        print(json.dumps({'execute':False,'production_changes':0,'migrations':MIGRATIONS,
                          'roles':['collector_runtime','spring_runtime','collector_function_owner']}))
        return
    if not a.dsn_env or not os.environ.get(a.dsn_env):
        p.error('Explicit isolated DSN required')
    run(os.environ[a.dsn_env])


if __name__ == '__main__':
    main()
