"""PostgreSQL configuration, connections and schema initialization."""
from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Iterator

import psycopg
from dotenv import load_dotenv
from psycopg import Connection
from psycopg.rows import dict_row

from .product_storage_schema import uses_current_storage
from .storage_values import TRUE_VALUES


_SCHEMA_INIT_LOCK = Lock()
_SCHEMA_INIT_KEYS: set[tuple[str, int, str, str, bool]] = set()


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


def load_postgres_config(project_root: Path | None = None) -> PostgresConfig:
    if project_root is not None:
        load_dotenv(project_root / ".env", override=False)
    else:
        load_dotenv(override=False)

    values = {
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
    }
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise RuntimeError(f"missing required PostgreSQL environment variables: {', '.join(missing)}")

    return PostgresConfig(
        host=str(values["POSTGRES_HOST"]),
        port=int(str(values["POSTGRES_PORT"])),
        database=str(values["POSTGRES_DB"]),
        user=str(values["POSTGRES_USER"]),
        password=str(values["POSTGRES_PASSWORD"]),
    )


def postgres_enabled(project_root: Path | None = None) -> bool:
    if project_root is not None:
        load_dotenv(project_root / ".env", override=False)
    else:
        load_dotenv(override=False)
    return str(os.getenv("POSTGRES_ENABLED", "")).strip().lower() in TRUE_VALUES


def open_connection(config: PostgresConfig) -> Connection[Any]:
    return psycopg.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.password,
        connect_timeout=10,
        row_factory=dict_row,
    )


@contextmanager
def connect(config: PostgresConfig) -> Iterator[Connection[Any]]:
    connection = open_connection(config)
    try:
        yield connection
    finally:
        connection.close()


def test_connection(project_root: Path | None = None) -> dict[str, str]:
    config = load_postgres_config(project_root)
    with connect(config) as connection:
        row = connection.execute(
            "SELECT current_database() AS database, version() AS version"
        ).fetchone() or {}
    return {"database": str(row.get("database") or ""), "version": str(row.get("version") or "")}


def init_schema(connection: Connection[Any]) -> None:
    """Create an empty schema or validate an existing one; never repair it implicitly."""
    from .schema import CREATE_TABLES, CREATE_INDEXES, validate_schema
    if not uses_current_storage():
        from .schema import BASE_CREATE_TABLES as CREATE_TABLES, BASE_CREATE_INDEXES as CREATE_INDEXES

    try:
        # Serialize first-time creation across collector processes.
        connection.execute("SELECT pg_advisory_xact_lock(724639118504)")
        existing = connection.execute("SELECT to_regclass('products') AS existing").fetchone()
        if existing and existing["existing"]:
            # Runtime credentials cannot run DDL on administrator-owned objects.
            # Existing schemas are validated; repairs remain explicit migrations.
            validate_schema(connection)
            if uses_current_storage():
                from .product_storage_schema import validate_product_storage
                validate_product_storage(connection, indexes=True)
            connection.commit()
            return
        for statement in CREATE_TABLES:
            connection.execute(statement)
        validate_schema(connection)
        for statement in CREATE_INDEXES:
            connection.execute(statement)
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def ensure_schema_initialized(config: PostgresConfig, connection: Connection[Any]) -> None:
    key = (config.host, config.port, config.database, config.user, uses_current_storage())
    if key in _SCHEMA_INIT_KEYS:
        return
    with _SCHEMA_INIT_LOCK:
        if key in _SCHEMA_INIT_KEYS:
            return
        init_schema(connection)
        _SCHEMA_INIT_KEYS.add(key)
