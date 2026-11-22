"""Current product storage DDL shared by empty-DB creation and explicit migration."""
import os
import re

from product_data_collector.domain.current_state import CURRENT_COLUMNS


def uses_current_storage():
    version = os.environ.get("COLLECTOR_STORAGE_VERSION", "2")
    if version not in {"1", "2"}:
        raise ValueError("COLLECTOR_STORAGE_VERSION must be 1 or 2")
    return version == "2"

TARGET_COLUMNS = {
    "requested_version": "BIGINT NOT NULL DEFAULT 0 CHECK (requested_version >= 0)",
    "completed_version": "BIGINT NOT NULL DEFAULT 0 CHECK (completed_version >= 0 AND completed_version <= requested_version)",
    "refresh_status": "TEXT NOT NULL DEFAULT 'idle' CHECK (refresh_status IN ('idle','pending','processing','retry','completed'))",
    "refresh_requested_at": "TIMESTAMPTZ",
    "refresh_completed_at": "TIMESTAMPTZ",
    "refresh_scope": "JSONB",
    "failure_count": "INTEGER NOT NULL DEFAULT 0 CHECK (failure_count >= 0)",
    "next_retry_at": "TIMESTAMPTZ",
    "last_error": "TEXT",
    "lease_token": "UUID",
    "lease_until": "TIMESTAMPTZ",
    "processing_version": "BIGINT",
}

TABLES = (
    """CREATE TABLE IF NOT EXISTS product_options (
        id BIGSERIAL PRIMARY KEY,
        product_id BIGINT NOT NULL REFERENCES products(id) ON DELETE RESTRICT,
        option_identity TEXT NOT NULL,
        source_option_id TEXT,
        identity_status TEXT NOT NULL CHECK (identity_status IN ('source_key','unverified')),
        option_name TEXT,
        attributes JSONB NOT NULL,
        price NUMERIC,
        price_meaning TEXT NOT NULL CHECK (price_meaning IN ('final','unknown')),
        price_confidence TEXT NOT NULL CHECK (price_confidence IN ('source_contract','unresolved')),
        stock_quantity NUMERIC,
        stock_is_infinite BOOLEAN,
        status TEXT,
        status_basis TEXT NOT NULL,
        raw_option JSONB NOT NULL,
        last_observed_at TIMESTAMPTZ NOT NULL,
        membership_status TEXT NOT NULL DEFAULT 'present' CHECK (membership_status IN ('present','missing','deleted')),
        membership_observed_at TIMESTAMPTZ NOT NULL,
        UNIQUE(product_id, option_identity)
    )""",
    """CREATE TABLE IF NOT EXISTS product_events (
        id BIGSERIAL PRIMARY KEY,
        platform TEXT NOT NULL,
        external_product_id TEXT NOT NULL,
        product_id BIGINT REFERENCES products(id) ON DELETE SET NULL,
        source_api TEXT NOT NULL,
        event_kind TEXT NOT NULL,
        source_event_id TEXT,
        occurred_at TIMESTAMPTZ,
        observed_at TIMESTAMPTZ NOT NULL,
        event_origin TEXT NOT NULL CHECK (event_origin IN ('source','observation')),
        event_scope TEXT NOT NULL CHECK (event_scope IN ('product','option')),
        semantic TEXT NOT NULL,
        option_reference JSONB,
        option_id BIGINT REFERENCES product_options(id) ON DELETE SET NULL,
        option_link_status TEXT NOT NULL CHECK (option_link_status IN ('not_applicable','unresolved','verified')),
        option_link_evidence JSONB,
        raw_before JSONB,
        raw_after JSONB,
        verified_before JSONB,
        verified_after JSONB,
        value_status TEXT NOT NULL CHECK (value_status IN ('unresolved','source_parsed','signal_only','observed_change','observed_unchanged')),
        verification_evidence JSONB,
        related_source_event_id BIGINT REFERENCES product_events(id) ON DELETE SET NULL,
        raw_event JSONB NOT NULL,
        dedupe_key TEXT NOT NULL UNIQUE,
        identity_status TEXT NOT NULL CHECK (identity_status IN ('source_id','fingerprint','ambiguous')),
        identity_note TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CHECK (event_origin <> 'observation' OR occurred_at IS NULL),
        CHECK (event_kind NOT IN ('stockChanged','optionStockChanged'))
    )""",
    """CREATE TABLE IF NOT EXISTS storage_backfill_progress (
        name TEXT PRIMARY KEY,
        last_product_id BIGINT NOT NULL DEFAULT 0,
        high_product_id BIGINT NOT NULL,
        scanned_count BIGINT NOT NULL DEFAULT 0,
        updated_count BIGINT NOT NULL DEFAULT 0,
        completed BOOLEAN NOT NULL DEFAULT false,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
)

INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_product_events_natural_time ON product_events(platform, external_product_id, occurred_at, id)",
    "CREATE INDEX IF NOT EXISTS idx_product_events_observed ON product_events(platform, external_product_id, observed_at, id) WHERE occurred_at IS NULL",
    "CREATE INDEX IF NOT EXISTS idx_product_events_unlinked ON product_events(platform, external_product_id) WHERE product_id IS NULL",
    "CREATE INDEX IF NOT EXISTS idx_product_events_product ON product_events(product_id)",
    "CREATE INDEX IF NOT EXISTS idx_product_events_option ON product_events(option_id) WHERE option_id IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_product_events_related ON product_events(related_source_event_id) WHERE related_source_event_id IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_discovery_refresh_due ON product_discovery_targets(platform, next_retry_at, id) WHERE requested_version > completed_version",
)

ADDITIVE = tuple(
    f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {definition}"
    for table, columns in (("products", CURRENT_COLUMNS), ("product_discovery_targets", TARGET_COLUMNS))
    for column, definition in columns.items()
) + TABLES + INDEXES


def extend_creation(base):
    result = []
    for statement in base:
        for table, columns in (("products", CURRENT_COLUMNS), ("product_discovery_targets", TARGET_COLUMNS)):
            if f"EXISTS {table} (" in statement:
                statement = statement.replace("UNIQUE (platform, external_product_id)",
                    ",\n".join(f"{key} {value}" for key, value in columns.items()) + ",\nUNIQUE (platform, external_product_id)")
        result.append(statement)
    return tuple(result) + TABLES


def column_contracts():
    contracts = {"products": CURRENT_COLUMNS, "product_discovery_targets": TARGET_COLUMNS}
    for ddl in TABLES:
        table = re.search(r"EXISTS (\w+)", ddl)[1]
        columns = {}
        for line in ddl.splitlines()[1:]:
            match = re.match(r"\s*(\w+) (BIGSERIAL|BIGINT|INTEGER|TEXT|JSONB|NUMERIC|BOOLEAN|TIMESTAMPTZ|UUID)(.*)", line)
            if match:
                columns[match[1]] = (match[2] + match[3]).rstrip(",")
        contracts[table] = columns
    return contracts


def _expression_key(expression):
    """Compare this migration's simple checks/defaults with pg_get_expr output.

    PostgreSQL deparses IN as ANY/ALL arrays and adds text casts/parentheses.
    Preserve quoted literals; this is a strict drift check, not an SQL evaluator.
    """
    expression = re.sub(r"\bNOT IN\s*\(([^)]+)\)", r"<> ALL (ARRAY[\1])", expression, flags=re.I)
    expression = re.sub(r"\bIN\s*\(([^)]+)\)", r"= ANY (ARRAY[\1])", expression, flags=re.I)
    parts = re.split(r"('(?:[^']|'')*')", expression)
    for index in range(0, len(parts), 2):
        parts[index] = re.sub(r"::(?:text|bigint|integer)\b", "", parts[index], flags=re.I)
        parts[index] = re.sub(r"[\s()]", "", parts[index]).lower()
    return "".join(parts)


def check_contracts():
    contracts = {"product_discovery_targets": [v.split("CHECK (", 1)[1][:-1]
                 for v in TARGET_COLUMNS.values() if "CHECK (" in v]}
    for ddl in TABLES:
        table = re.search(r"EXISTS (\w+)", ddl)[1]
        contracts[table] = [line.split("CHECK (", 1)[1].rstrip().rstrip(",")[:-1]
                            for line in ddl.splitlines() if "CHECK (" in line]
    return contracts


def validate_product_storage(connection, *, indexes=False):
    from .schema import SchemaCompatibilityError
    problems = []
    aliases = {"BIGSERIAL": "bigint", "TIMESTAMPTZ": "timestamp with time zone"}
    for table, columns in column_contracts().items():
        actual = {r["attname"]: r for r in connection.execute(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS type, a.attnotnull, a.atthasdef, "
            "pg_get_expr(d.adbin,d.adrelid) AS default_expression "
            "FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum "
            "WHERE a.attrelid=to_regclass(%s) AND a.attnum>0 AND NOT a.attisdropped", (table,)
        ).fetchall()}
        for key, definition in columns.items():
            expected = aliases.get(definition.split()[0], definition.split()[0].lower())
            if key not in actual or actual[key]["type"] != expected:
                problems.append(f"product storage column/type mismatch: {table}.{key} ({expected})")
            elif ("NOT NULL" in definition or "PRIMARY KEY" in definition) and not actual[key]["attnotnull"]:
                problems.append(f"product storage required column nullable: {table}.{key}")
            elif ("DEFAULT" in definition or "BIGSERIAL" in definition) and not actual[key]["atthasdef"]:
                problems.append(f"product storage missing default: {table}.{key}")
            elif "DEFAULT " in definition:
                expected_default = definition.split("DEFAULT ", 1)[1].split(" CHECK", 1)[0]
                if _expression_key(actual[key]["default_expression"] or "") != _expression_key(expected_default):
                    problems.append(f"product storage default mismatch: {table}.{key}")
        if table not in {"products", "product_discovery_targets"}:
            for key, column in actual.items():
                if key not in columns and column["attnotnull"] and not column["atthasdef"]:
                    problems.append(f"product storage extra required column without default: {table}.{key}")
            pk = "name" if table == "storage_backfill_progress" else "id"
            if not connection.execute(
                "SELECT 1 FROM pg_constraint c JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=c.conkey[1] "
                "WHERE c.conrelid=to_regclass(%s) AND c.contype='p' AND array_length(c.conkey,1)=1 AND a.attname=%s", (table, pk)
            ).fetchone():
                problems.append(f"product storage missing primary key: {table}.{pk}")
    for table, checks in check_contracts().items():
        actual_checks = {_expression_key(row["expression"]) for row in connection.execute(
            "SELECT pg_get_expr(conbin,conrelid) AS expression FROM pg_constraint "
            "WHERE conrelid=to_regclass(%s) AND contype='c' AND convalidated", (table,),
        ).fetchall()}
        for expression in checks:
            if _expression_key(expression) not in actual_checks:
                problems.append(f"product storage missing/mismatched check: {table} ({expression})")
    if indexes:
        for ddl in INDEXES:
            name = re.search(r"EXISTS (\w+)", ddl)[1]
            table, columns = re.search(r"ON (\w+)\(([^)]+)\)", ddl).groups()
            row = connection.execute(
                "SELECT i.indisvalid, t.relname, pg_get_expr(i.indpred,i.indrelid) AS predicate, "
                "ARRAY(SELECT a.attname FROM unnest(i.indkey) WITH ORDINALITY k(num,ord) "
                "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.num ORDER BY k.ord) AS columns "
                "FROM pg_index i JOIN pg_class t ON t.oid=i.indrelid WHERE i.indexrelid=to_regclass(%s)", (name,),
            ).fetchone()
            clean = lambda value: re.sub(r"[\s()]", "", value or "").lower()
            predicate = ddl.split(" WHERE ", 1)[1] if " WHERE " in ddl else ""
            if (not row or not row["indisvalid"] or row["relname"] != table
                    or list(row["columns"]) != [c.strip() for c in columns.split(",")]
                    or clean(row["predicate"]) != clean(predicate)):
                problems.append(f"product storage missing/invalid index: {name}")
    for table, columns in {"products": ["platform", "external_product_id"],
                           "product_discovery_targets": ["platform", "external_product_id"],
                           "product_options": ["product_id", "option_identity"],
                           "product_events": ["dedupe_key"], "storage_backfill_progress": ["name"]}.items():
        keys = connection.execute(
            "SELECT ARRAY(SELECT a.attname FROM unnest(i.indkey) WITH ORDINALITY k(num,ord) "
            "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.num "
            "WHERE k.ord<=i.indnkeyatts ORDER BY k.ord) AS columns FROM pg_index i "
            "WHERE i.indrelid=to_regclass(%s) AND i.indisunique AND i.indisvalid "
            "AND i.indpred IS NULL AND i.indexprs IS NULL", (table,),
        ).fetchall()
        if set(columns) not in [set(k["columns"]) for k in keys]:
            problems.append(f"product storage missing unique key: {table}({','.join(columns)})")
    for table, column, target, delete in (
        ("product_options", "product_id", "products", "r"),
        ("product_events", "product_id", "products", "n"),
        ("product_events", "option_id", "product_options", "n"),
        ("product_events", "related_source_event_id", "product_events", "n"),
    ):
        row = connection.execute(
            "SELECT 1 FROM pg_constraint c JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=ANY(c.conkey) "
            "JOIN pg_attribute ref ON ref.attrelid=c.confrelid AND ref.attnum=c.confkey[1] "
            "WHERE c.contype='f' AND c.conrelid=to_regclass(%s) AND c.confrelid=to_regclass(%s) "
            "AND a.attname=%s AND c.confdeltype=%s AND c.convalidated "
            "AND array_length(c.conkey,1)=1 AND array_length(c.confkey,1)=1 AND ref.attname='id'",
            (table, target, column, delete)
        ).fetchone()
        if not row:
            problems.append(f"product storage FK mismatch: {table}.{column}")
    if problems:
        raise SchemaCompatibilityError("; ".join(problems) + "; run explicit product storage migration/review")
