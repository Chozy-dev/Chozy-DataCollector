"""Offline plan by default. Explicit DSN and --execute required for operations."""
import argparse
import json
import os
import signal
import threading
from datetime import datetime, timezone
from pathlib import Path
import yaml
import psycopg
from psycopg.rows import dict_row
from .runner import Runner,TASKS,slots


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode',choices=('plan','run','schedule','status','retry','supersede','policy'))
    p.add_argument('--config',type=Path,required=True)
    p.add_argument('--dsn-env')
    p.add_argument('--execute',action='store_true')
    p.add_argument('--task',choices=TASKS)
    p.add_argument('--job-id')
    p.add_argument('--replacement-id')
    p.add_argument('--at',type=datetime.fromisoformat)
    a=p.parse_args(argv)
    cfg=yaml.safe_load(a.config.read_text(encoding='utf8'))
    if not a.execute:
        print(json.dumps({'execute':False,'automatic_enabled':cfg.get('automatic_enabled',False),
            'slots':[(t,d.isoformat()) for t,d in slots(cfg,datetime.now(timezone.utc))]},ensure_ascii=False))
        return 0
    if not a.dsn_env or not os.environ.get(a.dsn_env): p.error('Explicit DSN env required')
    if a.mode=='schedule' and not cfg.get('automatic_enabled',False): p.error('automatic_enabled must be true')
    if a.mode=='run' and (not a.task or not a.at or not a.at.tzinfo): p.error('run requires --task and timezone-aware --at')
    connect=lambda:psycopg.connect(os.environ[a.dsn_env],row_factory=dict_row)
    with connect() as conn:
        if not conn.execute("SELECT to_regclass('ops_jobs') AS t").fetchone()['t']: p.error('Apply stage8_operations explicitly first')
    if a.mode=='status':
        from .status import report
        with connect() as conn: print(json.dumps(report(conn,cfg),default=str,ensure_ascii=False))
        return 0
    if a.mode=='policy':
        from psycopg.types.json import Jsonb
        with connect() as conn:
            for policy in cfg['budgets']:
                conn.execute("""INSERT INTO api_budget_policy(account,api,limits,min_interval,platform_integrated)
                    VALUES(%s,%s,%s,%s,%s) ON CONFLICT(account,api) DO UPDATE SET
                    limits=EXCLUDED.limits,min_interval=EXCLUDED.min_interval,
                    platform_integrated=EXCLUDED.platform_integrated""",
                    (policy['account'],policy['api'],Jsonb(policy['limits']),policy.get('min_interval',0),policy.get('platform_integrated',False)))
        return 0
    if a.mode in ('retry','supersede'):
        if not a.job_id: p.error('retry requires --job-id')
        with connect() as conn:
            row=conn.execute('SELECT * FROM ops_jobs WHERE job_id=%s',(a.job_id,)).fetchone()
            if not row: p.error('Unknown job-id')
            if not conn.execute('SELECT pg_try_advisory_xact_lock(hashtextextended(%s,0)) AS ok',
                                ('ops-task:'+row['task'].split('-')[0],)).fetchone()['ok']:
                p.error('Task still running')
            if a.mode=='supersede':
                if row['status']=='completed' or row['superseded_by']: p.error('Only unfinished current jobs can be superseded')
                if not a.replacement_id or a.replacement_id==a.job_id: p.error('Distinct --replacement-id required')
                from .adapters import execution_fingerprint
                from psycopg.types.json import Jsonb
                scope={'scoring_mode':cfg.get('scoring_mode','incremental'),'config_hash':execution_fingerprint(cfg,row['task'])}
                conn.execute('INSERT INTO ops_jobs(job_id,task,scheduled_at,scope) VALUES(%s,%s,%s,%s)',
                    (a.replacement_id,row['task'],datetime.now(timezone.utc),Jsonb(scope)))
                conn.execute("UPDATE ops_jobs SET superseded_by=%s,status='failed',error='superseded' WHERE job_id=%s",
                    (a.replacement_id,a.job_id))
                return 0
            conn.execute("UPDATE ops_jobs SET attempts=0,status='paused',updated_at=clock_timestamp()-interval '1 day' WHERE job_id=%s AND status<>'completed'",(a.job_id,))
        return 0
    from .adapters import handlers,execution_fingerprint
    def scope_for(task):
        return {'scoring_mode':cfg.get('scoring_mode','incremental'),'config_hash':execution_fingerprint(cfg,task)}
    stop=threading.Event()
    for sig in (signal.SIGINT,signal.SIGTERM): signal.signal(sig,lambda *_:stop.set())
    runner=Runner(connect,handlers(cfg,connect,stop.is_set),cfg.get('max_attempts',3),cfg.get('retry_seconds',300),stop.is_set)
    if a.mode=='run':
        result=runner.run(a.task,a.at,scope_for(a.task),a.job_id)
        print(result); return 0 if result=='completed' else 2
    if a.mode=='plan': return 0
    while not stop.is_set():
        # Resume persisted scope first, including jobs older than catch-up horizon.
        with connect() as conn:
            pending=conn.execute("SELECT * FROM ops_jobs WHERE status<>'completed' AND superseded_by IS NULL ORDER BY scheduled_at,job_id LIMIT 100").fetchall()
        for job in pending:
            if stop.is_set(): break
            try: runner.run(job['task'],job['scheduled_at'],job['scope'],job['job_id'])
            except Exception as exc: print(json.dumps({'task':job['task'],'error':type(exc).__name__}))
        for task,at in slots(cfg,datetime.now(timezone.utc)):
            if stop.is_set(): break
            try: runner.run(task,at,scope_for(task))
            except Exception as exc: print(json.dumps({'task':task,'error':type(exc).__name__}))
        try:
            from .status import report
            with connect() as conn: report(conn,cfg)
        except Exception as exc: print(json.dumps({'monitor_error':type(exc).__name__}))
        stop.wait(min(60,max(1,cfg.get('poll_seconds',30))))
    return 0


if __name__=='__main__': raise SystemExit(main())
