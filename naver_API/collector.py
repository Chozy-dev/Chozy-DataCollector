"""Naver seasonal keyword collection, resumable through its own batch ledger."""
import logging
from datetime import datetime, timedelta, timezone
import psycopg
from .store import SeasonStore
from .services import trend_body
from .client import NaverRateLimited

LOGGER = logging.getLogger(__name__)


def collect_season(connection, client, config, day):
    if connection.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise ValueError('Requires an idle dedicated DB connection')
    key = 'naver_season:' + str(day)
    # A shared provider lock also prevents independent date jobs bypassing pacing.
    with connection.transaction():
        locked = connection.execute('SELECT pg_try_advisory_lock(724639118607) AS locked').fetchone()['locked']
    if not locked:
        raise RuntimeError('Naver collector already running')
    store, started = SeasonStore(connection), False
    try:
        with connection.transaction():
            locked = connection.execute('SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS locked', (key,)).fetchone()['locked']
        if not locked:
            raise RuntimeError('Daily batch already running')
        started = store.begin(day, config)
        if not started:
            return 'completed'
        for code, _, month in config.themes:
            for part in range(3):
                if store.done(day, code, part):
                    continue
                api = 'ad' if part == 0 else 'trend'
                if not store.reserve(api, config):
                    store.stop(day, 'paused', 'call_budget_exhausted')
                    return 'paused'
                if part == 0:
                    request = {'event':code,'month':month,'showDetail':1}
                    response = client.keywords(code,month)
                else:
                    request = trend_body(store.words(day,code,part), day-timedelta(days=56), day-timedelta(days=1))
                    response = client.trend(request)
                store.save(day,code,part,request,response,datetime.now(timezone.utc))
        store.publish(day)
        return 'completed'
    except Exception as exc:
        if started:
            try:
                store.stop(day, 'paused' if isinstance(exc,NaverRateLimited) else 'failed', type(exc).__name__)
            except Exception:
                LOGGER.error('Could not record failure; resume from committed DB checkpoints')
        if isinstance(exc,NaverRateLimited):
            return 'paused'
        raise
    finally:
        if not connection.closed:
            with connection.transaction():
                connection.execute('SELECT pg_advisory_unlock(hashtextextended(%s,0))', (key,))
                connection.execute('SELECT pg_advisory_unlock(724639118607)')
