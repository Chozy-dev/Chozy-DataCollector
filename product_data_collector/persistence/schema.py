"""Non-destructive schema creation and read-only compatibility checks."""
from __future__ import annotations

CREATE_TABLES = (
    """
        CREATE TABLE IF NOT EXISTS products (
            id BIGSERIAL PRIMARY KEY,
            platform TEXT NOT NULL,
            external_product_id TEXT NOT NULL,
            product_name TEXT NULL,
            product_url TEXT NULL,
            image_url TEXT NULL,
            backup_image_url TEXT NULL,
            source_specific JSONB NOT NULL DEFAULT '{}'::jsonb,
            status TEXT NULL,
            seller_external_id TEXT NULL,
            seller_nickname TEXT NULL,
            seller_type TEXT NULL,
            seller_grade TEXT NULL,
            seller_excellent_seller BOOLEAN NULL,
            seller_average_satisfaction TEXT NULL,
            seller_review_count NUMERIC(18, 2) NULL,
            first_seen_at TIMESTAMPTZ NOT NULL,
            last_collected_at TIMESTAMPTZ NOT NULL,
            UNIQUE (platform, external_product_id)
        )
        """,
    """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            name TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
    """
        CREATE TABLE IF NOT EXISTS product_raw_samples (
            id BIGSERIAL PRIMARY KEY,
            platform TEXT NOT NULL,
            external_product_id TEXT NOT NULL,
            collected_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT product_raw_samples_platform_collected_product_key UNIQUE (platform, collected_at, external_product_id)
        )
        """,
    """
        CREATE TABLE IF NOT EXISTS product_search_ranks (
            id BIGSERIAL PRIMARY KEY,
            platform TEXT NOT NULL,
            collected_at TIMESTAMPTZ NOT NULL,
            keyword TEXT NOT NULL DEFAULT '',
            category_code TEXT NOT NULL DEFAULT '',
            category_name TEXT NULL,
            category_path JSONB NOT NULL DEFAULT '[]'::jsonb,
            market TEXT NOT NULL DEFAULT 'default',
            sort TEXT NOT NULL DEFAULT '',
            reason TEXT NULL,
            external_product_id TEXT NOT NULL,
            rank INTEGER NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT product_search_ranks_history_key
                UNIQUE (platform, collected_at, keyword, category_code, market, sort, external_product_id, rank)
        )
        """,
    """
        CREATE TABLE IF NOT EXISTS product_discovery_targets (
            id BIGSERIAL PRIMARY KEY,
            platform TEXT NOT NULL,
            external_product_id TEXT NOT NULL,
            active BOOLEAN NOT NULL DEFAULT true,
            first_discovered_at TIMESTAMPTZ NOT NULL,
            last_discovered_at TIMESTAMPTZ NOT NULL,
            keyword TEXT NULL,
            category_code TEXT NULL,
            category_name TEXT NULL,
            market TEXT NULL,
            reason TEXT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (platform, external_product_id)
        )
        """,
    """
        CREATE TABLE IF NOT EXISTS product_history (
            id BIGSERIAL PRIMARY KEY,
            product_id BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            observed_at TIMESTAMPTZ NOT NULL,
            change_type TEXT NOT NULL,
            changed_fields TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
            prices JSONB NOT NULL DEFAULT '{}'::jsonb,
            inventory JSONB NOT NULL DEFAULT '{}'::jsonb,
            shipping JSONB NOT NULL DEFAULT '{}'::jsonb,
            status TEXT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
)

CREATE_INDEXES = (
    """CREATE INDEX IF NOT EXISTS idx_products_platform_external_id ON products(platform, external_product_id)""",
    """CREATE INDEX IF NOT EXISTS idx_products_last_collected_at ON products(last_collected_at)""",
    """CREATE INDEX IF NOT EXISTS idx_product_history_product_observed_at ON product_history(product_id, observed_at)""",
    """CREATE INDEX IF NOT EXISTS idx_product_history_changed_fields_gin ON product_history USING GIN(changed_fields)""",
    """CREATE INDEX IF NOT EXISTS idx_product_raw_samples_platform_collected_at ON product_raw_samples(platform, collected_at)""",
    """CREATE INDEX IF NOT EXISTS idx_product_search_ranks_platform_collected_at ON product_search_ranks(platform, collected_at)""",
    """CREATE INDEX IF NOT EXISTS idx_product_discovery_targets_platform_active ON product_discovery_targets(platform, active, first_discovered_at)""",
)

REQUIRED_COLUMNS = {
    "products": (
        "id",
        "platform",
        "external_product_id",
        "product_name",
        "product_url",
        "image_url",
        "backup_image_url",
        "source_specific",
        "status",
        "seller_external_id",
        "seller_nickname",
        "seller_type",
        "seller_grade",
        "seller_excellent_seller",
        "seller_average_satisfaction",
        "seller_review_count",
        "first_seen_at",
        "last_collected_at",
    ),
    "schema_migrations": (
        "name",
        "applied_at",
    ),
    "product_raw_samples": (
        "id",
        "platform",
        "external_product_id",
        "collected_at",
        "payload",
        "created_at",
    ),
    "product_search_ranks": (
        "id",
        "platform",
        "collected_at",
        "keyword",
        "category_code",
        "category_name",
        "category_path",
        "market",
        "sort",
        "reason",
        "external_product_id",
        "rank",
        "created_at",
    ),
    "product_discovery_targets": (
        "id",
        "platform",
        "external_product_id",
        "active",
        "first_discovered_at",
        "last_discovered_at",
        "keyword",
        "category_code",
        "category_name",
        "market",
        "reason",
        "created_at",
        "updated_at",
    ),
    "product_history": (
        "id",
        "product_id",
        "observed_at",
        "change_type",
        "changed_fields",
        "prices",
        "inventory",
        "shipping",
        "status",
        "created_at",
    ),
}


class SchemaCompatibilityError(RuntimeError):
    pass


def validate_schema(connection):
    from .product_storage_schema import uses_current_storage, validate_product_storage
    problems = []
    for table, required in REQUIRED_COLUMNS.items():
        columns = connection.execute(
            "SELECT attname, attnotnull, atthasdef FROM pg_attribute WHERE attrelid = to_regclass(%s) "
            "AND attnum > 0 AND NOT attisdropped", (table,),
        ).fetchall()
        actual = {row["attname"] for row in columns}
        problems.extend(f"missing {table}.{column}" for column in required if column not in actual)
        for column in columns:
            if column["attname"] not in required and column["attnotnull"] and not column["atthasdef"]:
                problems.append(f"legacy required column without default: {table}.{column['attname']}")
            if (table, column["attname"]) in {
                ("products", "last_collected_at"), ("product_history", "observed_at"),
            } and not column["attnotnull"]:
                problems.append(f"nullable observation time needs reviewed backfill: {table}.{column['attname']}")
    unique_keys = dict(UNIQUE_KEYS)
    if uses_current_storage():
        validate_product_storage(connection)
        unique_keys.update({"product_options": ("product_id", "option_identity"),
                            "product_events": ("dedupe_key",), "storage_backfill_progress": ("name",)})
    for table, expected in unique_keys.items():
        indexes = connection.execute(
            """
            SELECT array_agg(a.attname ORDER BY k.ordinality) AS columns
            FROM pg_index i
            CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ordinality)
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
            WHERE i.indrelid = to_regclass(%s) AND i.indisunique AND i.indisvalid
                AND i.indpred IS NULL AND i.indexprs IS NULL AND k.ordinality <= i.indnkeyatts
            GROUP BY i.indexrelid
            """, (table,),
        ).fetchall()
        keys = [set(index["columns"]) for index in indexes]
        if set(expected) not in keys:
            problems.append(f"missing upsert unique key: {table}({', '.join(expected)})")
        if any(key < set(expected) for key in keys):
            problems.append(f"legacy narrower unique key needs review: {table}")
    if problems:
        raise SchemaCompatibilityError(
            "Existing schema requires explicit review/migration; no data was changed. "
            + "; ".join(problems)
            + ". Inspect: python -m product_data_collector.persistence.migrations --plan"
        )


UNIQUE_KEYS = {
    "products": ("platform", "external_product_id"),
    "product_raw_samples": ("platform", "collected_at", "external_product_id"),
    "product_discovery_targets": ("platform", "external_product_id"),
    "product_search_ranks": (
        "platform", "collected_at", "keyword", "category_code", "market", "sort", "external_product_id", "rank",
    ),
    "schema_migrations": ("name",),
}

# Keep the legacy storage path available for a data-preserving application rollback.
BASE_CREATE_TABLES = CREATE_TABLES
BASE_CREATE_INDEXES = CREATE_INDEXES
from .product_storage_schema import extend_creation, INDEXES
CREATE_TABLES = extend_creation(BASE_CREATE_TABLES)
CREATE_INDEXES = BASE_CREATE_INDEXES + INDEXES
