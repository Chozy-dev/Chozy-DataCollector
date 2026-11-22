"""Opt-in tests against a disposable DB. Never read the project's .env."""
import os
import uuid

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row


@pytest.fixture
def db_factory():
    dsn = os.environ.get("COLLECTOR_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("Set COLLECTOR_TEST_POSTGRES_DSN to a disposable PostgreSQL DB")
    config = conninfo_to_dict(dsn)
    if config.get("dbname") != "collector_stage1_test" or config.get("host") != "127.0.0.1":
        pytest.fail("Integration tests require local disposable DB collector_stage1_test")
    schema = "collector_test_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    connections = []

    def factory():
        connection = psycopg.connect(dsn, row_factory=dict_row)
        connection.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        connection.commit()
        connections.append(connection)
        return connection

    try:
        yield factory
    finally:
        for connection in connections:
            connection.close()
        # This generated schema is the only deletion target, on the guarded test DB.
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture
def db(db_factory):
    from product_data_collector.persistence.postgres_storage import init_schema
    connection = db_factory()
    init_schema(connection)
    return connection
