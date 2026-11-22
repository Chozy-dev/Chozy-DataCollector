"""Catalogue and Private product sync; never schedules a full scan implicitly."""
import argparse
import os
from pathlib import Path

from ..api.client import create_domeggook_client
from ..api.private_sync import PrivateSyncClient
from ..api.rate_limiter import RateLimiter
from ..config import load_config, load_api_keys
from .run_budget import PersistentCallBudget
from .product_sync import catalogue, incremental
from product_data_collector.persistence import postgres_storage as pg
from product_data_collector.persistence.schema import validate_schema


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["bootstrap", "discovery", "recovery", "sync"])
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--open-daily-budget", type=int, required=True)
    parser.add_argument("--private-daily-budget", type=int)
    parser.add_argument("--private-rpm", type=int, help="Operator allowance, NOT verified provider limit")
    parser.add_argument("--provider-timezone", help="Explicit dateModify timezone; requires provider verification")
    parser.add_argument("--start", help="Initial ISO timestamp with offset; use bootstrap startedAt")
    parser.add_argument("--end")
    parser.add_argument("--page-limit", type=int)
    args = parser.parse_args(argv)
    if args.mode == "sync" and (not args.private_daily_budget or not args.private_rpm):
        parser.error("sync requires explicit private budget and operational RPM")
    root = Path(__file__).resolve().parents[2]
    config = load_config(root / "domeggook_API/config/config.yaml")
    client = create_domeggook_client(load_api_keys(root), config)
    client.request_budget = PersistentCallBudget(args.state_dir / "open-budget.json", args.open_daily_budget)
    private = None
    if args.mode == "sync":
        key = os.environ.get("DOMEGGOOK_PRIVATE_API_KEY") or os.environ.get("DOMEGGOOK_PrivateAPI_KEY")
        if not key:
            parser.error("Private API key is required")
        private = PrivateSyncClient(key, rate_limiter=RateLimiter(args.private_rpm),
            request_budget=PersistentCallBudget(args.state_dir / "private-budget.json", args.private_daily_budget))
    pg.require_database_enabled(root)
    db_config = pg.load_postgres_config(root)
    with pg.connect(db_config) as connection:
        validate_schema(connection)  # read-only; schema creation/migration is a separate command
        path = args.state_dir / (args.mode + ".json")
        result = (incremental(connection, private, client, path, start=args.start, end=args.end,
                              page_limit=args.page_limit, provider_timezone=args.provider_timezone) if private else
                  catalogue(connection, client, path, mode=args.mode, page_limit=args.page_limit))
    print(result)
    return 1 if result["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
