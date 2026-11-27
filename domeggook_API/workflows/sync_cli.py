"""Catalogue and Private product sync; never schedules a full scan implicitly."""
import argparse
from pathlib import Path

from dotenv import load_dotenv

from ..api.client import PrivateDetailClient
from ..api.private_sync import PrivateSyncClient, private_api_key
from ..api.rate_limiter import RateLimiter
from .run_budget import PersistentCallBudget
from .product_sync import catalogue, incremental
from product_data_collector.persistence import postgres_storage as pg
from product_data_collector.persistence.schema import validate_schema


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["bootstrap", "discovery", "recovery", "sync"])
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--open-daily-budget", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--private-daily-budget", type=int, required=True)
    parser.add_argument("--private-rpm", type=int, required=True, help="Operator allowance for search and detail combined")
    parser.add_argument("--provider-timezone", help="Explicit dateModify timezone; requires provider verification")
    parser.add_argument("--start", help="Initial ISO timestamp with offset; use bootstrap startedAt")
    parser.add_argument("--end")
    parser.add_argument("--page-limit", type=int)
    args = parser.parse_args(argv)
    if args.private_daily_budget < 1 or args.private_rpm < 1:
        parser.error("Private budget and RPM must be positive")
    root = Path(__file__).resolve().parents[2]
    load_dotenv(root / ".env", override=False)
    try:
        key = private_api_key()
    except ValueError as exc:
        parser.error(str(exc))
    limiter = RateLimiter(args.private_rpm)
    budget = PersistentCallBudget(args.state_dir / "private-budget.json", args.private_daily_budget)
    private = PrivateSyncClient(key, rate_limiter=limiter, request_budget=budget)
    details = PrivateDetailClient(key, limiter, request_budget=budget)
    pg.require_database_enabled(root)
    db_config = pg.load_postgres_config(root)
    with pg.connect(db_config) as connection:
        validate_schema(connection)  # read-only; schema creation/migration is a separate command
        path = args.state_dir / (args.mode + ".json")
        result = (incremental(connection, private, details, path, start=args.start, end=args.end,
                              page_limit=args.page_limit, provider_timezone=args.provider_timezone)
                  if args.mode == "sync" else
                  catalogue(connection, private, details, path, mode=args.mode, page_limit=args.page_limit))
    print(result)
    return 1 if result["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
