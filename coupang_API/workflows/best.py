"""Coupang category best snapshots with resumable batches and atomic publication."""
import argparse
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from coupang_API.api.client import BestCategoryRequest, CoupangPartnersClient
from coupang_API.api.rate_limiter import RateLimiter
from coupang_API.config.best import load_best_config
from coupang_API.config.settings import find_project_root, load_credentials
from coupang_API.persistence.best import BestStore
from coupang_API.services.best import parse_best_records

LOGGER = logging.getLogger(__name__)


def collect_best(connection, client, config, day):
    if connection.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise ValueError("Best collection requires an idle dedicated DB connection")
    lock_key = 'coupang_best:' + day.isoformat()
    with connection.transaction():
        locked = connection.execute("SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS locked", (lock_key,)).fetchone()["locked"]
    if not locked:
        raise RuntimeError("This daily batch is already running")
    store = BestStore(connection)
    started = False
    try:
        started = store.begin(day, config)
        if not started:
            return 'completed'
        for category in store.pending(day):
            if not store.reserve(day,config.max_calls):
                store.stop(day,'paused','call_budget_exhausted')
                return 'paused'
            cid = category["category_id"]
            payload = client.best_products(BestCategoryRequest(cid,config.limit))
            records = parse_best_records(payload,cid,config.limit)
            store.save_category(day,cid,records,payload,datetime.now(timezone.utc))
        store.publish(day)
        return 'completed'
    except Exception as exc:
        if started:
            # A disconnected DB can prevent failure recording; committed checkpoints survive.
            try:
                store.stop(day,'failed',type(exc).__name__)
            except Exception:
                LOGGER.exception("Could not record failed status; resume from DB checkpoints")
        raise
    finally:
        if not connection.closed:
            with connection.transaction():
                connection.execute("SELECT pg_advisory_unlock(hashtextextended(%s,0))", (lock_key,))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('best',), default='best', help=argparse.SUPPRESS)
    parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--date',type=date.fromisoformat,default=datetime.now(timezone(timedelta(hours=9))).date())
    parser.add_argument('--dsn-env',required=True,help='Explicit target DSN env var; no automatic migration')
    parser.add_argument('--execute',action='store_true',help='Perform real API calls; default only validates config')
    args = parser.parse_args(argv)
    config = load_best_config(args.config)
    if not args.execute:
        print({'snapshot_date':str(args.date),'scope':config.scope(),'max_calls':config.max_calls,
               'requests_per_minute':config.requests_per_minute,'execute':False})
        return 0
    if not os.environ.get(args.dsn_env):
        parser.error('Explicit DSN environment variable is required')
    root = find_project_root(Path.cwd())
    access, secret = load_credentials(root)
    client = CoupangPartnersClient(access,secret,
        rate_limiter=RateLimiter(config.requests_per_minute,60))
    with psycopg.connect(os.environ[args.dsn_env],row_factory=dict_row) as connection:
        status = collect_best(connection,client,config,args.date)
    print(status)
    return 0 if status == 'completed' else 2


if __name__ == '__main__':
    raise SystemExit(main())
