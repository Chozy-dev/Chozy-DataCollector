"""Explicit, reviewed migrations. Never invoked by collector startup."""
from __future__ import annotations

import argparse
import os

from .schema import CREATE_TABLES
from .product_storage_schema import ADDITIVE, validate_product_storage

MIGRATIONS = {
    "stage1_additive_columns": (
        'ALTER TABLE products ADD COLUMN IF NOT EXISTS backup_image_url TEXT NULL',
        "ALTER TABLE products ADD COLUMN IF NOT EXISTS source_specific JSONB NOT NULL DEFAULT '{}'::jsonb",
        'ALTER TABLE products ADD COLUMN IF NOT EXISTS status TEXT NULL',
        'ALTER TABLE products ADD COLUMN IF NOT EXISTS seller_external_id TEXT NULL',
        'ALTER TABLE products ADD COLUMN IF NOT EXISTS seller_nickname TEXT NULL',
        'ALTER TABLE products ADD COLUMN IF NOT EXISTS seller_type TEXT NULL',
        'ALTER TABLE products ADD COLUMN IF NOT EXISTS seller_grade TEXT NULL',
        'ALTER TABLE products ADD COLUMN IF NOT EXISTS seller_excellent_seller BOOLEAN NULL',
        'ALTER TABLE products ADD COLUMN IF NOT EXISTS seller_average_satisfaction TEXT NULL',
        'ALTER TABLE products ADD COLUMN IF NOT EXISTS seller_review_count NUMERIC(18, 2) NULL',
        "ALTER TABLE product_search_ranks ADD COLUMN IF NOT EXISTS keyword TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE product_search_ranks ADD COLUMN IF NOT EXISTS category_code TEXT NOT NULL DEFAULT ''",
        'ALTER TABLE product_history ADD COLUMN IF NOT EXISTS observed_at TIMESTAMPTZ',
        'ALTER TABLE product_history ADD COLUMN IF NOT EXISTS changed_fields TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]',
        "ALTER TABLE product_history ADD COLUMN IF NOT EXISTS prices JSONB NOT NULL DEFAULT '{}'::jsonb",
        "ALTER TABLE product_history ADD COLUMN IF NOT EXISTS inventory JSONB NOT NULL DEFAULT '{}'::jsonb",
        "ALTER TABLE product_history ADD COLUMN IF NOT EXISTS shipping JSONB NOT NULL DEFAULT '{}'::jsonb",
        'ALTER TABLE product_history ADD COLUMN IF NOT EXISTS status TEXT NULL',
    ),
}
MIGRATIONS["stage2_storage_contract"] = ADDITIVE
from .coupang_best_schema import DDL as COUPANG_BEST_DDL
MIGRATIONS["stage5_coupang_best"] = COUPANG_BEST_DDL
from .naver_season_schema import DDL as NAVER_SEASON_DDL
MIGRATIONS["stage6_naver_season"] = NAVER_SEASON_DDL
from .supply_scoring_schema import DDL as SUPPLY_SCORING_DDL
MIGRATIONS["stage7_supply_scoring"] = SUPPLY_SCORING_DDL

from .operations_schema import DDL as OPERATIONS_DDL
MIGRATIONS["stage8_operations"] = OPERATIONS_DDL
from .scoring_policy_schema import DDL as SCORING_POLICY_DDL
MIGRATIONS["stage7_history_policy_v2"] = SCORING_POLICY_DDL

from .scoring_invalidation_schema import DDL as SCORING_INVALIDATION_DDL
MIGRATIONS["stage10_scoring_performance"] = SCORING_INVALIDATION_DDL
from .access import BUDGET_GUARD_DDL
MIGRATIONS["runtime_budget_guard"] = BUDGET_GUARD_DDL
from .shared_budget_schema import DDL as SHARED_BUDGET_DDL
MIGRATIONS["shared_api_budget"] = SHARED_BUDGET_DDL


def apply_migration(connection, name):
    statements = MIGRATIONS[name]
    try:
        connection.execute("SET LOCAL lock_timeout = '5s'")
        connection.execute("SELECT pg_advisory_xact_lock(724639118504)")
        connection.execute(next(sql for sql in CREATE_TABLES if "EXISTS schema_migrations " in sql))
        if connection.execute("SELECT 1 FROM schema_migrations WHERE name = %s", (name,)).fetchone():
            if name == "stage2_storage_contract":
                validate_product_storage(connection, indexes=True)
                from .schema import validate_schema
                validate_schema(connection)
            connection.rollback()
            return False
        for statement in statements:
            connection.execute(statement)
        if name == "stage2_storage_contract":
            validate_product_storage(connection, indexes=True)
            from .schema import validate_schema
            validate_schema(connection)
        connection.execute("INSERT INTO schema_migrations (name) VALUES (%s)", (name,))
        connection.commit()
        return True
    except Exception:
        connection.rollback()
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--plan", action="store_true", help="Print SQL without opening a DB connection")
    group.add_argument("--apply", choices=tuple(MIGRATIONS), help="Apply a reviewed migration to the configured DB")
    parser.add_argument("--dsn-env", help="Explicit environment variable containing target DSN; .env is never read")
    args = parser.parse_args(argv)
    if args.plan:
        for name, statements in MIGRATIONS.items():
            print(f"-- {name}")
            for statement in statements:
                print(statement + ";")
        return 0
    if not args.dsn_env or not os.environ.get(args.dsn_env):
        parser.error("--apply requires --dsn-env with an explicitly supplied target DSN")
    import psycopg
    from psycopg.rows import dict_row
    with psycopg.connect(os.environ[args.dsn_env], row_factory=dict_row) as connection:
        applied = apply_migration(connection, args.apply)
    print("applied" if applied else "already applied")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
