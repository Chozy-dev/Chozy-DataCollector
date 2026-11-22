"""Session input fence survives chunk commits, never locks the products table."""
from contextlib import contextmanager


@contextmanager
def source_collection_boundary(conn,platform):
    key=724639118801 if platform=='domeggook' else 724639118802
    if not conn.execute('SELECT pg_try_advisory_lock(%s) AS ok',(key,)).fetchone()['ok']:
        conn.rollback()
        raise RuntimeError('Input writers active; defer source collection')
    conn.commit()
    try:
        yield
    finally:
        if not conn.closed:
            conn.rollback()
            conn.execute('SELECT pg_advisory_unlock(%s)',(key,));conn.commit()


@contextmanager
def scoring_boundary(conn, run_id, platform):
    migrated = conn.execute("SELECT to_regclass('ops_score_inputs') AS t").fetchone()['t']
    if not migrated:
        pending=conn.execute("SELECT 1 FROM product_discovery_targets WHERE platform=%s AND requested_version>completed_version LIMIT 1",(platform,)).fetchone()
        completed=(conn.execute("SELECT 1 FROM scoring_runs WHERE run_id=%s AND status='completed'",(run_id,)).fetchone() if pending else None)
        if pending and not completed:
            raise RuntimeError('Detail refresh incomplete')
        yield
        return
    if platform not in ('domeggook','ownerclan'):
        raise ValueError('Invalid source')
    key = 724639118801 if platform=='domeggook' else 724639118802
    if not conn.execute('SELECT pg_try_advisory_lock(%s) AS ok',(key,)).fetchone()['ok']:
        conn.rollback()
        raise RuntimeError('Input writers active; defer scoring')
    try:
        revision=conn.execute('SELECT CASE WHEN is_called THEN last_value ELSE 0 END AS last_value FROM ops_'+platform+'_revision').fetchone()['last_value']
        conn.execute('INSERT INTO ops_score_inputs VALUES(%s,%s,%s) ON CONFLICT DO NOTHING',
                     (run_id,platform,revision))
        saved=conn.execute('SELECT * FROM ops_score_inputs WHERE run_id=%s',(run_id,)).fetchone()
        completed=conn.execute("SELECT 1 FROM scoring_runs WHERE run_id=%s AND status='completed'",(run_id,)).fetchone()
        if not completed and (saved['platform']!=platform or saved['revision']!=revision):
            raise ValueError('Inputs changed since interruption; use a new run-id and as-of')
        pending=conn.execute("SELECT 1 FROM product_discovery_targets WHERE platform=%s AND requested_version>completed_version LIMIT 1",(platform,)).fetchone()
        if pending and not completed:
            raise RuntimeError('Detail refresh incomplete')
        receipt=conn.execute('SELECT revision FROM ops_collection_receipts WHERE platform=%s',(platform,)).fetchone()
        if not completed and (not receipt or receipt['revision']!=revision):
            raise RuntimeError('Collection completion unverified for current inputs')
        conn.commit()
        yield
    finally:
        if not conn.closed:
            conn.rollback()
            conn.execute('SELECT pg_advisory_unlock(%s)',(key,))
            conn.commit()


def certify_collection(conn,platform,job_id):
    """Only after the complete source workflow and detail persistence succeeded."""
    if platform not in ('domeggook','ownerclan'): raise ValueError('Invalid source')
    key=724639118801 if platform=='domeggook' else 724639118802
    if not conn.execute('SELECT pg_try_advisory_lock(%s) AS ok',(key,)).fetchone()['ok']:
        conn.rollback()
        raise RuntimeError('Input writers active; defer collection receipt')
    try:
        if conn.execute('SELECT 1 FROM product_discovery_targets WHERE platform=%s AND requested_version>completed_version LIMIT 1',(platform,)).fetchone():
            raise RuntimeError('Detail refresh incomplete')
        revision=conn.execute('SELECT CASE WHEN is_called THEN last_value ELSE 0 END AS revision FROM ops_'+platform+'_revision').fetchone()['revision']
        conn.execute('''INSERT INTO ops_collection_receipts(platform,revision,job_id) VALUES(%s,%s,%s)
            ON CONFLICT(platform) DO UPDATE SET revision=EXCLUDED.revision,job_id=EXCLUDED.job_id,
            completed_at=clock_timestamp()''',(platform,revision,job_id))
        conn.commit()
    finally:
        conn.rollback()
        conn.execute('SELECT pg_advisory_unlock(%s)',(key,));conn.commit()
