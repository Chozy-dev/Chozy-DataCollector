import argparse
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import psycopg
from psycopg.rows import dict_row
from .client import NaverClient
from .collector import collect_season
from .config import load_config


def main(argv=None):
    parser = argparse.ArgumentParser(description='Naver seasonal keywords; offline plan by default')
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--date', type=date.fromisoformat, default=datetime.now(timezone(timedelta(hours=9))).date())
    parser.add_argument('--dsn-env', required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args(argv)
    config = load_config(args.config,args.date)
    if not args.execute:
        print({'snapshotDate':str(args.date),'scope':config.scope(),'limits':vars(config),'execute':False})
        return 0
    if not os.environ.get(args.dsn_env):
        parser.error('Explicit DSN env required')
    client = NaverClient.from_env(config)
    try:
        with psycopg.connect(os.environ[args.dsn_env], row_factory=dict_row) as connection:
            status = collect_season(connection,client,config,args.date)
    except Exception as exc:
        # CLI never renders DB DSNs or provider data in tracebacks.
        print('Collection failed: ' + type(exc).__name__)
        return 1
    print(status)
    return 0 if status == 'completed' else 2


if __name__ == '__main__':
    raise SystemExit(main())
