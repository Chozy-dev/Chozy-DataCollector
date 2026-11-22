"""Reviewable access plan for fresh runtime roles; never called at startup.

Existing owner/migration credentials stay offline. Runtime roles own no objects.
Plan application is an explicit administrative operation after schema migrations.
"""
from psycopg import sql

BUDGET_GUARD_DDL = ("""CREATE OR REPLACE FUNCTION block_api_account(acct TEXT, endpoint TEXT)
RETURNS VOID LANGUAGE SQL AS $$
  UPDATE api_budget_policy SET blocked=true WHERE account=acct AND api=endpoint
$$""",)

WRITE_TABLES = (
    'products', 'product_options', 'product_events', 'product_discovery_targets',
    'product_search_ranks', 'product_raw_samples', 'scoring_supplier_samples',
    'scoring_queue', 'scoring_supplier_jobs', 'product_supply_scores', 'scoring_runs',
    'supply_home_batches', 'supply_home_items', 'ops_collection_receipts',
    'ops_score_inputs', 'ops_jobs', 'ops_alerts',
)
READ_TABLES = ('schema_migrations', 'scoring_coverage', 'scoring_supplier_identity',
               'api_budget_policy', 'api_budget_usage')
APP_FUNCTIONS = (
    'scoring_enqueue', 'scoring_vendor', 'scoring_product_input',
    'scoring_product_changed', 'scoring_evidence_changed',
    'scoring_membership_deleted', 'scoring_observation_ready',
    'ops_input_write', 'ops_no_truncate', 'reserve_api_call',
    'block_api_account', 'read_supply_home',
)


def access_plan(conn, *, schema='public', collector='collector_runtime',
                spring='spring_runtime', executor='collector_function_owner'):
    """Return transactional SQL for NEW NOLOGIN roles. Existing role names fail closed.

    Deliberately scoped to supply collection/scoring/operations. Market snapshot
    tables are a separate rollout. No account isolation within a runtime role.
    """
    if len({collector, spring, executor}) != 3:
        raise ValueError('Three distinct fresh roles required')
    for role in (collector, spring, executor):
        if not role or len(role.encode('utf8')) > 63:
            raise ValueError('Role name must fit PostgreSQL identifier limit')
        if conn.execute('SELECT 1 FROM pg_roles WHERE rolname=%s', (role,)).fetchone():
            raise ValueError('Access plan requires fresh roles: ' + role)
    required = set(WRITE_TABLES + READ_TABLES + ('product_history', 'supply_home_published'))
    present = {r['relname'] for r in conn.execute(
        'SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s', (schema,))}
    if required - present:
        raise ValueError('Apply supply/operations migrations first: ' + ','.join(sorted(required-present)))
    funcs = conn.execute("""SELECT p.proname, pg_get_function_identity_arguments(p.oid) AS args
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname=%s AND p.proname=ANY(%s) ORDER BY p.proname""", (schema, list(APP_FUNCTIONS))).fetchall()
    if not {'reserve_api_call', 'block_api_account', 'read_supply_home'} <= {f['proname'] for f in funcs}:
        raise ValueError('Apply runtime_budget_guard first')
    ident = sql.Identifier
    qual = lambda name: ident(schema, name)
    roles = sql.SQL(',').join(map(ident, (collector, spring, executor)))
    plan = []
    def add(template, *args):
        plan.append(sql.SQL(template).format(*args))
    for role in (collector, spring, executor):
        add('CREATE ROLE {} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS', ident(role))
    add('REVOKE CREATE ON SCHEMA {} FROM PUBLIC', ident(schema))
    add('GRANT USAGE ON SCHEMA {} TO {}', ident(schema), roles)
    add('ALTER DEFAULT PRIVILEGES IN SCHEMA {} REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC', ident(schema))
    for table in sorted(required):
        add('REVOKE ALL ON TABLE {} FROM PUBLIC', qual(table))
    for table in WRITE_TABLES:
        if table in ('supply_home_items','ops_score_inputs','ops_alerts'):
            add('GRANT SELECT,INSERT ON TABLE {} TO {}', qual(table), ident(collector))
        else:
            add('GRANT SELECT,INSERT,UPDATE ON TABLE {} TO {}', qual(table), ident(collector))
    for table in READ_TABLES:
        add('GRANT SELECT ON TABLE {} TO {}', qual(table), ident(collector))
    add('GRANT SELECT ON TABLE {} TO {}', qual('supply_home_published'), ident(collector))
    add('GRANT SELECT,INSERT ON TABLE {} TO {}', qual('product_history'), ident(collector))
    for table in ('product_raw_samples','scoring_queue','scoring_supplier_jobs'):
        add('GRANT DELETE ON TABLE {} TO {}', qual(table), ident(collector))
    for table in ('ops_jobs','ops_alerts','api_budget_policy','api_budget_usage'):
        add('GRANT SELECT ON TABLE {} TO {}', qual(table), ident(spring))
    add('GRANT UPDATE(acknowledged_at) ON TABLE {} TO {}', qual('ops_alerts'), ident(spring))
    # A function owner can reserve/block, but cannot change schema or application inputs.
    for table in ('api_budget_policy','api_budget_usage'):
        add('GRANT SELECT,INSERT,UPDATE ON TABLE {} TO {}', qual(table), ident(executor))
    for table in ('supply_home_published','supply_home_items'):
        add('GRANT SELECT ON TABLE {} TO {}', qual(table), ident(executor))
    for table in WRITE_TABLES + ('product_history',):
        for r in conn.execute("""SELECT a.attname,pg_get_serial_sequence(format('%%I.%%I',%s::text,%s::text),a.attname) AS seq
            FROM pg_attribute a WHERE a.attrelid=to_regclass(format('%%I.%%I',%s::text,%s::text))
            AND a.attnum>0 AND NOT a.attisdropped""", (schema,table,schema,table)):
            if r['seq']:
                add('REVOKE ALL ON SEQUENCE {} FROM PUBLIC', sql.SQL(r['seq']))
                add('GRANT USAGE ON SEQUENCE {} TO {}', sql.SQL(r['seq']), ident(collector))
    for sequence in ('ops_domeggook_revision','ops_ownerclan_revision'):
        add('REVOKE ALL ON SEQUENCE {} FROM PUBLIC', qual(sequence))
        add('GRANT USAGE,SELECT ON SEQUENCE {} TO {}', qual(sequence), ident(collector))
    # Temporary CREATE is needed only to assign function ownership; revoked in this transaction.
    add('GRANT CREATE ON SCHEMA {} TO {}', ident(schema), ident(executor))
    for f in funcs:
        signature = sql.SQL('{}({})').format(qual(f['proname']), sql.SQL(f['args']))
        add('REVOKE ALL ON FUNCTION {} FROM PUBLIC', signature)
        if f['proname'] in ('reserve_api_call','block_api_account','read_supply_home'):
            add('ALTER FUNCTION {} SECURITY DEFINER', signature)
            add('ALTER FUNCTION {} SET search_path TO pg_catalog,{},pg_temp', signature, ident(schema))
            add('ALTER FUNCTION {} OWNER TO {}', signature, ident(executor))
            add('GRANT EXECUTE ON FUNCTION {} TO {},{}', signature, ident(collector), ident(spring))
        else:
            add('GRANT EXECUTE ON FUNCTION {} TO {}', signature, ident(collector))
    add('REVOKE CREATE ON SCHEMA {} FROM {}', ident(schema), ident(executor))
    return plan
