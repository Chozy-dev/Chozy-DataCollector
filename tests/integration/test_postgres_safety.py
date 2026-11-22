from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event

import psycopg
import pytest

from product_data_collector.persistence import postgres_storage as storage
from product_data_collector.persistence.migrations import apply_migration
from product_data_collector.persistence.schema import SchemaCompatibilityError


OLD = "2026-01-01T00:00:00Z"
NOW = "2026-02-01T00:00:00Z"
NEW = "2026-03-01T00:00:00Z"


def row(key="A", at=NOW, price=100):
    return storage._snapshot_row("ownerclan", at, {"productId": key, "productName": str(price), "productPrice": price})


def count(db, table):
    # Only constant table names in this test module are used.
    return db.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"]


def test_empty_schema_and_repeated_initialization_preserve_all_legacy_data(db):
    storage._save_product_batch_with_retry(db, [row()])
    for column in ("current_payload", "comparable_payload", "comparable_fingerprint", "created_at", "updated_at"):
        db.execute(f"ALTER TABLE products ADD COLUMN {column} TEXT DEFAULT 'keep'")
    for table in ("product_change_history", "product_latest_fields", "product_prices", "product_inventory", "product_shipping_fees"):
        db.execute(f"CREATE TABLE {table} (payload TEXT)")
        db.execute(f"INSERT INTO {table} VALUES ('keep')")
    for table in ("product_search_ranks", "product_discovery_targets"):
        db.execute(f"ALTER TABLE {table} ADD COLUMN payload TEXT DEFAULT 'keep'")
    db.execute("INSERT INTO product_search_ranks(platform,collected_at,external_product_id,rank,sort) VALUES ('domeggook',now(),'A',0,'da')")
    db.commit()
    storage.init_schema(db)
    storage.init_schema(db)
    assert db.execute("SELECT current_payload,comparable_payload,created_at,updated_at FROM products").fetchone() == dict.fromkeys(
        ["current_payload", "comparable_payload", "created_at", "updated_at"], "keep"
    )
    for table in ("product_change_history", "product_latest_fields", "product_prices", "product_inventory", "product_shipping_fees"):
        assert db.execute(f"SELECT payload FROM {table}").fetchone()["payload"] == "keep"
    assert db.execute("SELECT payload,rank,sort FROM product_search_ranks").fetchone() == {"payload": "keep", "rank": 0, "sort": "da"}
    assert count(db, "product_history") == 1
    assert count(db, "schema_migrations") == 0


def test_incompatible_schema_fails_without_changing_data(db):
    storage._save_product_batch_with_retry(db, [row()])
    db.execute("ALTER TABLE products RENAME COLUMN backup_image_url TO legacy_backup_image_url")
    db.commit()
    with pytest.raises(SchemaCompatibilityError, match="backup_image_url"):
        storage.init_schema(db)
    assert count(db, "products") == 1
    assert count(db, "product_history") == 1
    assert apply_migration(db, "stage1_additive_columns") is True
    assert apply_migration(db, "stage1_additive_columns") is False
    storage.init_schema(db)
    assert count(db, "products") == 1
    assert count(db, "product_history") == 1


def test_failed_migration_rolls_back_and_is_not_marked_applied(db, monkeypatch):
    from product_data_collector.persistence import migrations
    monkeypatch.setitem(migrations.MIGRATIONS, "test_failure", (
        "ALTER TABLE products ADD COLUMN temporary_test_column TEXT", "SELECT missing_column FROM products",
    ))
    with pytest.raises(psycopg.Error):
        apply_migration(db, "test_failure")
    assert count(db, "schema_migrations") == 0
    assert not db.execute("SELECT 1 FROM pg_attribute WHERE attrelid='products'::regclass AND attname='temporary_test_column'").fetchone()


@pytest.mark.parametrize("replay_at,price", [(OLD, 1), (NOW, 100), (NOW, 500)])
def test_old_or_equal_observations_do_not_change_latest_or_history(db, replay_at, price):
    storage._save_product_batch_with_retry(db, [row()])
    assert storage._save_product_batch_with_retry(db, [row(at=replay_at, price=price)]) == 1
    assert db.execute("SELECT product_name FROM products").fetchone()["product_name"] == "100"
    assert count(db, "product_history") == 1
    storage._save_product_batch_with_retry(db, [row(at=NEW, price=200)])
    assert db.execute("SELECT product_name FROM products").fetchone()["product_name"] == "200"
    assert count(db, "product_history") == 2


def test_product_observation_time_is_preserved_instead_of_save_time(db):
    storage._save_product_batch_with_retry(db, [row()])
    storage._save_products_with_raw_samples(connection=db, batch_size=100, platform="ownerclan", collected_at=NEW,
        products=[{"productId": "A", "collectedAt": OLD, "productPrice": 1}], raw_sample_limit=0)
    assert db.execute("SELECT product_name FROM products").fetchone()["product_name"] == "100"
    assert count(db, "product_history") == 1


def test_partial_failure_reports_commits_and_pending_suffix_then_retries(db):
    db.execute("ALTER TABLE products ADD CONSTRAINT test_reject_b CHECK (external_product_id <> 'B')")
    db.commit()
    rows = [row("A"), row("B"), row("C")]
    with pytest.raises(storage.ProductSaveError) as error:
        storage._save_product_batches(db, rows, 1)
    assert error.value.committed_ids == ("A",)
    assert error.value.pending_ids == ("B", "C")
    assert count(db, "products") == count(db, "product_history") == 1
    # Remove only the fault injection constraint in this isolated test schema.
    db.execute("ALTER TABLE products DROP CONSTRAINT test_reject_b")
    db.commit()
    assert storage._save_product_batches(db, rows, 1) == 3
    assert count(db, "products") == count(db, "product_history") == 3


def test_history_failure_rolls_back_product_update_too(db):
    storage._save_product_batch_with_retry(db, [row()])
    db.execute("ALTER TABLE product_history ADD CONSTRAINT reject_test_updates CHECK (change_type='initial')")
    db.commit()
    with pytest.raises(storage.ProductSaveError):
        storage._save_product_batches(db, [row(at=NEW, price=999)], 100)
    assert db.execute("SELECT product_name FROM products").fetchone()["product_name"] == "100"
    assert count(db, "product_history") == 1


def test_lost_commit_acknowledgement_is_safe_to_retry(db):
    class LostAck:
        first = True

        def __getattr__(self, key):
            return getattr(db, key)

        def commit(self):
            db.commit()
            if self.first:
                self.first = False
                raise psycopg.OperationalError("lost commit acknowledgement")

    assert storage._save_product_batch_with_retry(LostAck(), [row()]) == 1
    assert count(db, "products") == count(db, "product_history") == 1


def test_concurrent_first_insert_waits_and_does_not_duplicate_history(db, db_factory):
    # Hold the first insert uncommitted while a second session tries the same ID.
    storage._save_product_batch(db, [row()])
    started = Event()

    def replay():
        connection = db_factory()
        started.set()
        return storage._save_product_batch_with_retry(connection, [row()])

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(replay)
        assert started.wait(5)
        db.commit()
        assert future.result(timeout=10) == 1
    assert count(db, "products") == count(db, "product_history") == 1


def test_concurrent_new_then_old_observation_cannot_regress(db, db_factory):
    storage._save_product_batch_with_retry(db, [row(at=OLD)])
    storage._save_product_batch(db, [row(at=NEW, price=300)])
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(storage._save_product_batch_with_retry, db_factory(), [row(price=200)])
        db.commit()
        future.result(timeout=10)
    assert db.execute("SELECT product_name FROM products").fetchone()["product_name"] == "300"
    assert count(db, "product_history") == 2


def test_raw_insertion_budget_zero_retention_and_platform_isolation(db):
    def save(platform, at, ids, budget):
        return storage._save_products_with_raw_samples(connection=db, batch_size=100, platform=platform,
            collected_at=at, products=[{"productId": str(i), "raw": {"i": i}} for i in ids], raw_sample_limit=budget)

    save("coupang", NOW, range(5), 5)
    assert save("ownerclan", OLD, range(105), 20)["rawSampleCount"] == 20
    save("ownerclan", NOW, range(20, 120), 100)
    before = db.execute("SELECT * FROM product_raw_samples ORDER BY id").fetchall()
    save("ownerclan", NEW, [120], 0)
    assert db.execute("SELECT * FROM product_raw_samples ORDER BY id").fetchall() == before
    assert db.execute("SELECT count(*) AS n FROM product_raw_samples WHERE platform='ownerclan'").fetchone()["n"] == 100
    assert db.execute("SELECT count(*) AS n FROM product_raw_samples WHERE platform='coupang'").fetchone()["n"] == 5
    assert not db.execute("SELECT 1 FROM product_raw_samples WHERE collected_at=%s", (OLD,)).fetchone()


def test_public_snapshot_save_propagates_partial_failure(db, monkeypatch, tmp_path):
    @contextmanager
    def connection(_):
        yield db

    monkeypatch.setattr(storage, "connect", connection)
    monkeypatch.setattr(storage, "load_postgres_config", lambda _: None)
    monkeypatch.setattr(storage, "ensure_schema_initialized", lambda *_: None)
    monkeypatch.setattr(storage, "_product_batch_size", lambda _: 1)
    db.execute("ALTER TABLE products ADD CONSTRAINT test_reject_b CHECK (external_product_id <> 'B')")
    db.commit()
    with pytest.raises(storage.ProductSaveError) as error:
        storage.save_product_snapshots(project_root=tmp_path, platform="ownerclan", collected_at=NOW,
            products=[{"productId": "A"}, {"productId": "B"}])
    assert error.value.committed_ids == ("A",)
    assert error.value.pending_ids == ("B",)


@pytest.mark.parametrize("legacy_ddl,match", [
    ("ALTER TABLE products ADD COLUMN legacy_payload JSONB NOT NULL", "legacy required column"),
    ("ALTER TABLE product_history ALTER COLUMN observed_at DROP NOT NULL", "nullable observation"),
    ("ALTER TABLE product_search_ranks ADD UNIQUE (platform,collected_at,market,sort,external_product_id,rank)", "narrower unique"),
])
def test_incompatible_legacy_constraints_are_reported_without_cleanup(db, legacy_ddl, match):
    db.execute(legacy_ddl)
    db.commit()
    with pytest.raises(SchemaCompatibilityError, match=match):
        storage.init_schema(db)
    assert count(db, "schema_migrations") == 0


def test_concurrent_raw_sample_writers_keep_platform_cap(db, db_factory):
    def save(ids):
        connection = db_factory()
        return storage._save_products_with_raw_samples(connection=connection, batch_size=100,
            platform="ownerclan", collected_at=NOW,
            products=[{"productId": str(i), "raw": {"i": i}} for i in ids], raw_sample_limit=100)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(save, range(start, start + 100)) for start in (0, 100)]
        for future in futures:
            assert future.result(timeout=15)["snapshotCount"] == 100
    assert count(db, "product_raw_samples") == 100
    assert count(db, "products") == 200


def test_dome_commit_checkpoint_failure_and_resume_use_real_db(db, monkeypatch, tmp_path):
    from domeggook_API.workflows import collect_product_details as workflow
    from tests.unit.domeggook.test_discover_collect import FakeClient, _config
    from domeggook_API.persistence.storage import load_state

    def save(**kwargs):
        return storage._save_products_with_raw_samples(connection=db, batch_size=1,
            platform=kwargs["platform"], collected_at=kwargs["collected_at"],
            products=kwargs["products"], raw_sample_limit=kwargs["raw_sample_limit"])

    monkeypatch.setattr(workflow, "save_products_with_raw_samples_if_enabled", save)
    real_save_state = workflow.save_state

    def fail_after_commit(path, state):
        if state["nextIndex"] > 0:
            assert count(db, "products") == count(db, "product_history") == 2
            raise OSError("checkpoint write failed after commit")
        real_save_state(path, state)

    monkeypatch.setattr(workflow, "save_state", fail_after_commit)
    with pytest.raises(OSError):
        workflow.collect_details(tmp_path, _config(), product_ids=["A", "B"], client=FakeClient())
    path = tmp_path / "domeggook_API/data/state/detail-collection-state.json"
    assert load_state(path)["nextIndex"] == 0
    monkeypatch.setattr(workflow, "save_state", real_save_state)
    result = workflow.collect_details(tmp_path, _config(), product_ids=["A", "B"], client=FakeClient())
    assert result["status"] == "completed"
    assert not path.exists()
    assert count(db, "products") == count(db, "product_history") == 2
    assert count(db, "product_raw_samples") == 2


def test_discovery_commit_then_checkpoint_failure_does_not_duplicate_ranks(db, monkeypatch, tmp_path):
    from dataclasses import replace
    from domeggook_API.workflows import discover_products as workflow
    from tests.unit.domeggook.test_discover_collect import FakeClient, _config
    from domeggook_API.persistence.storage import load_state

    @contextmanager
    def connection(_):
        yield db

    monkeypatch.setattr(storage, "connect", connection)
    monkeypatch.setattr(storage, "load_postgres_config", lambda _: None)
    monkeypatch.setattr(storage, "ensure_schema_initialized", lambda *_: None)
    monkeypatch.setattr(storage, "postgres_enabled", lambda _: True)
    monkeypatch.setattr(workflow, "postgres_enabled", lambda _: True)
    real_clear = workflow.clear_state

    def fail_after_commit(path):
        assert count(db, "product_search_ranks") == 2
        raise OSError("checkpoint cleanup failed after commit")

    monkeypatch.setattr(workflow, "clear_state", fail_after_commit)
    config = replace(_config(), discovery=replace(_config().discovery, markets=("dome",), sorts={"ranking": "rd"}))
    with pytest.raises(OSError):
        workflow.discover(tmp_path, config, client=FakeClient())
    path = tmp_path / "domeggook_API/data/state/discovery-state.json"
    assert load_state(path)["nextPage"] == 1
    monkeypatch.setattr(workflow, "clear_state", real_clear)
    assert workflow.discover(tmp_path, config, client=FakeClient())["status"] == "completed"
    assert count(db, "product_search_ranks") == 2
    assert count(db, "product_discovery_targets") == 2
    assert not path.exists()
