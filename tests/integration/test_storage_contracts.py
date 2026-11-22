from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from threading import Event

import psycopg
import pytest

from ownerclan_API.services.normalization import normalize_item
from product_data_collector.domain.product_events import ownerclan_event, domeggook_event, observation_event
from product_data_collector.persistence import postgres_storage as storage
from product_data_collector.persistence.backfill import backfill_batch, integrity_counts
from product_data_collector.persistence.current_state import (
    read_current_product, read_product_options, compare_current_batch, history_context, iter_history_window, iter_event_window, event_context,
)
from product_data_collector.persistence.detail_refresh import claim_refreshes, complete_refresh, fail_refresh
from product_data_collector.persistence.migrations import apply_migration
from product_data_collector.persistence.product_events import save_events, insert_events, EventSaveError, record_verification
from product_data_collector.persistence.product_options import link_event_option
from product_data_collector.persistence.schema import SchemaCompatibilityError

OLD, NOW, NEW = "2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z", "2026-03-01T00:00:00Z"


def product(key="A", price=100, at=NOW, options=None, **changes):
    return normalize_item({"key": key, "price": 10, "status": "available", "openmarketSellable": True,
        "options": [{"key": "001", "price": price, "quantity": 2}] if options is None else options, **changes}, at)


def save(db, p):
    return storage._save_product_batch_with_retry(db, [storage._snapshot_row("ownerclan", p["collectedAt"], p)])


def event(key="A", **changes):
    raw = {"itemKey": key, "createdAt": 1767225600000, "kind": "priceIncreased", "valueBefore": "10", "valueAfter": "100", **changes}
    return ownerclan_event(raw, NOW)


def count(db, table):
    return db.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"]


def test_event_first_duplicates_then_detail_links_without_changing_original_time(db):
    e = event()
    assert save_events(db, [e, e])["inserted_count"] == 1
    assert save_events(db, [replace(e, observed_at=datetime(2026, 3, 1, tzinfo=timezone.utc), raw_event={**e.raw_event, "page": 2})])["inserted_count"] == 0
    stored = db.execute("SELECT * FROM product_events").fetchone()
    assert stored["product_id"] is None and stored["occurred_at"] < stored["observed_at"]
    assert count(db, "products") == 0
    target = db.execute("SELECT * FROM product_discovery_targets").fetchone()
    assert target["requested_version"] == 1 and target["completed_version"] == 0
    save(db, product())
    assert db.execute("SELECT product_id FROM product_events").fetchone()["product_id"] is not None
    assert db.execute("SELECT completed_version FROM product_discovery_targets").fetchone()["completed_version"] == 0
    save_events(db, [event(createdAt=1760000000000, valueAfter="11")])
    assert count(db, "product_events") == 2
    assert read_current_product(db, "ownerclan", "A")["current_supply_price"] == 100


def test_different_events_and_ids_are_preserved_missing_time_is_nullable(db):
    e = event()
    save_events(db, [e, event(createdAt=1767225600001), event(valueAfter="101"),
                     replace(e, source_event_id="E1"), replace(e, source_event_id="E2"), event(createdAt=None)])
    assert count(db, "product_events") == 6
    no_time = db.execute("SELECT * FROM product_events WHERE occurred_at IS NULL").fetchone()
    assert no_time["identity_status"] == "ambiguous"
    assert len(list(iter_event_window(db, "ownerclan", "A", OLD, NEW))) == 5
    assert len(list(iter_event_window(db, "ownerclan", "A", OLD, NEW, time_basis="observed_at"))) == 1


def test_event_failure_rolls_back_queue_and_replay_is_safe(db):
    db.execute("ALTER TABLE product_events ADD CONSTRAINT reject_b CHECK (external_product_id<>'B')")
    db.commit()
    with pytest.raises(EventSaveError):
        save_events(db, [event(), event("B")])
    assert count(db, "product_events") == count(db, "product_discovery_targets") == 0
    db.execute("ALTER TABLE product_events DROP CONSTRAINT reject_b")
    db.commit()
    save_events(db, [event(), event("B")])
    assert count(db, "product_events") == count(db, "product_discovery_targets") == 2


def test_event_lost_commit_ack_is_deduplicated_with_request_version(db):
    class LostAck:
        first = True
        def __getattr__(self, name):
            return getattr(db, name)
        def commit(self):
            db.commit()
            if self.first:
                self.first = False
                raise psycopg.OperationalError("lost acknowledgement")
    with pytest.raises(EventSaveError):
        save_events(LostAck(), [event()])
    assert save_events(db, [event()])["inserted_count"] == 0
    assert db.execute("SELECT requested_version FROM product_discovery_targets").fetchone()["requested_version"] == 1


def test_current_options_old_equal_and_partial_observations(db):
    save(db, product(options=[{"key": "001", "price": 100, "quantity": 0}, {"key": "002", "price": 200, "quantity": 4}]))
    current = read_current_product(db, "ownerclan", "A")
    assert current["current_supply_price"] is None and current["supply_price_status"] == "option_required"
    assert current["status"] == "available"
    for at in (OLD, NOW):
        save(db, product(price=999, at=at))
    assert count(db, "product_history") == 1
    assert db.execute("SELECT price FROM product_options WHERE source_option_id='001'").fetchone()["price"] == 100
    save(db, product(at=NEW, options=[{"key": "001", "price": 150, "quantity": 7}], optionsComplete=False))
    assert read_current_product(db, "ownerclan", "A")["current_supply_price"] is None
    assert count(db, "product_options") == 2
    assert db.execute("SELECT membership_status FROM product_options WHERE source_option_id='002'").fetchone()["membership_status"] == "present"
    p = product(at="2026-04-01T00:00:00Z", options=[{"key": "003", "price": 250, "quantity": 9}])
    p["optionsComplete"] = True
    save(db, p)
    assert count(db, "product_options") == 3
    assert db.execute("SELECT count(*) AS n FROM product_options WHERE membership_status='missing'").fetchone()["n"] == 2
    save(db, product(at=NEW))
    assert db.execute("SELECT membership_status FROM product_options WHERE source_option_id='001'").fetchone()["membership_status"] == "missing"


def test_unresolved_option_event_preserved_and_mapping_must_match_product(db):
    save_events(db, [event(kind="optionPriceIncreased", title="red"), event("B", kind="optionSoldout")])
    save(db, product())
    save(db, product("B"))
    e = db.execute("SELECT * FROM product_events WHERE external_product_id='A'").fetchone()
    assert e["option_id"] is None and e["option_link_status"] == "unresolved"
    other = db.execute("SELECT o.id FROM product_options o JOIN products p ON p.id=o.product_id WHERE p.external_product_id='B'").fetchone()["id"]
    with pytest.raises(ValueError, match="belong"):
        link_event_option(db, e["id"], other, evidence={"verified_by": "test fixture"})
    own = db.execute("SELECT id FROM product_options WHERE product_id=%s", (e["product_id"],)).fetchone()["id"]
    link_event_option(db, e["id"], own, evidence={"verified_by": "explicit reviewed fixture"})
    db.commit()
    assert db.execute("SELECT option_link_status FROM product_events WHERE id=%s", (e["id"],)).fetchone()["option_link_status"] == "verified"


def test_signal_verification_and_related_observation_are_separate(db):
    e = domeggook_event({"no": 1, "date": 1767225600, "status": "AMT", "price": 100}, NOW)
    save_events(db, [e])
    source = db.execute("SELECT * FROM product_events").fetchone()
    assert source["verified_after"] is None and source["value_status"] == "signal_only"
    evidence = {"field": "supply_price", "before_observed_at": OLD, "after_observed_at": NOW}
    record_verification(db, source["id"], before="100.00", after=100, evidence=evidence)
    db.commit()
    assert db.execute("SELECT value_status FROM product_events").fetchone()["value_status"] == "observed_unchanged"
    observed = observation_event(platform="domeggook", external_product_id="1", kind="price_comparison", observed_at=NEW,
                                 before=100, after=200, comparison_key=[10, 11], semantic="supply_price")
    save_events(db, [observed], request_refresh=False)
    oid = db.execute("SELECT id FROM product_events WHERE event_origin='observation'").fetchone()["id"]
    record_verification(db, oid, before=100, after=200, evidence=evidence, related_source_event_id=source["id"])
    db.commit()
    assert db.execute("SELECT occurred_at FROM product_events WHERE id=%s", (oid,)).fetchone()["occurred_at"] is None
    assert record_verification(db, oid, before=100, after=200, evidence=evidence, related_source_event_id=source["id"]) is False


def test_refresh_versions_failure_retry_and_atomic_completion(db):
    save_events(db, [event()], scope={"from": OLD, "to": NOW})
    claim = claim_refreshes(db, "ownerclan")[0]
    assert claim_refreshes(db, "ownerclan") == []
    fail_refresh(db, claim, error="temporary error", next_retry_at=datetime.now(timezone.utc)-timedelta(seconds=1))
    assert count(db, "products") == 0
    retry = claim_refreshes(db, "ownerclan")[0]
    assert retry["failure_count"] == 1 and retry["lease_token"] != claim["lease_token"]
    with pytest.raises(ValueError, match="stale"):
        complete_refresh(db, claim, product(), collected_at=NOW)
    # New event while this request is being processed must survive its completion.
    save_events(db, [event(valueAfter="200")])
    result = complete_refresh(db, retry, product(), collected_at=NOW)
    assert result["completed_version"] == 1 and result["requested_version"] == 2
    assert result["refresh_status"] == "pending"
    assert complete_refresh(db, retry, product(), collected_at=NOW)["completed_version"] == 1
    last = claim_refreshes(db, "ownerclan")[0]
    done = complete_refresh(db, last, product(at=NEW, price=200), collected_at=NEW)
    assert done["refresh_status"] == "completed" and done["completed_version"] == 2
    assert read_current_product(db, "ownerclan", "A")["current_supply_price"] == 200


def test_failed_detail_or_expired_lease_cannot_mark_work_done(db):
    save_events(db, [event()])
    claim = claim_refreshes(db, "ownerclan")[0]
    with pytest.raises(ValueError, match="predates"):
        complete_refresh(db, claim, product(at=OLD), collected_at=OLD)
    db.execute("ALTER TABLE product_options ADD CONSTRAINT reject_option CHECK (price<50)")
    db.commit()
    with pytest.raises(psycopg.Error):
        complete_refresh(db, claim, product(), collected_at=NOW)
    assert count(db, "products") == count(db, "product_history") == 0
    assert db.execute("SELECT completed_version FROM product_discovery_targets").fetchone()["completed_version"] == 0
    db.execute("ALTER TABLE product_options DROP CONSTRAINT reject_option")
    db.execute("UPDATE product_discovery_targets SET lease_until=now()-interval '1 second'")
    db.commit()
    with pytest.raises(ValueError, match="stale"):
        complete_refresh(db, claim, product(), collected_at=NOW)
    new_claim = claim_refreshes(db, "ownerclan")[0]
    complete_refresh(db, new_claim, product(), collected_at=NOW)
    assert count(db, "product_history") == 1


def test_concurrent_same_time_latest_options_and_event_first_race(db, db_factory):
    save_events(db, [event()])
    storage._save_product_batch(db, [storage._snapshot_row("ownerclan", NOW, product(price=100))])
    started = Event()
    def second():
        other = db_factory()
        started.set()
        save(other, product(price=999))
        save_events(other, [event(valueAfter="999")])
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(second)
        assert started.wait(5)
        db.commit()
        future.result(timeout=10)
    assert count(db, "product_history") == count(db, "product_options") == 1
    assert read_current_product(db, "ownerclan", "A")["current_supply_price"] == 100
    assert db.execute("SELECT count(*) AS n FROM product_events WHERE product_id IS NULL").fetchone()["n"] == 0


def test_migration_legacy_preservation_resumable_backfill_and_read_rollback(db_factory, monkeypatch):
    db = db_factory()
    monkeypatch.setenv("COLLECTOR_STORAGE_VERSION", "1")
    storage.init_schema(db)
    save(db, product("A", 100))
    save(db, product("B", 200))
    before = db.execute("SELECT * FROM product_history ORDER BY id").fetchall()
    db.commit()
    monkeypatch.setenv("COLLECTOR_STORAGE_VERSION", "2")
    with pytest.raises(SchemaCompatibilityError):
        storage.init_schema(db)
    assert apply_migration(db, "stage2_storage_contract") is True
    assert apply_migration(db, "stage2_storage_contract") is False
    storage.init_schema(db)
    assert integrity_counts(db) == {"products": 2, "history": 2, "orphan_history": 0, "wrong_event_links": 0}
    assert read_current_product(db, "ownerclan", "A")["read_path"] == "legacy"
    first = backfill_batch(db, batch_size=1)
    assert first["updated_count"] == 1 and not first["completed"]
    assert backfill_batch(db, batch_size=1)["completed"]
    assert backfill_batch(db, batch_size=1)["updated_count"] == 2
    assert compare_current_batch(db)["differences"] == []
    assert db.execute("SELECT * FROM product_history ORDER BY id").fetchall() == before
    assert read_current_product(db, "ownerclan", "A")["current_supply_price"] == read_current_product(db, "ownerclan", "A", read_path="legacy")["current_supply_price"]
    monkeypatch.setenv("COLLECTOR_STORAGE_VERSION", "1")
    storage.init_schema(db)
    save(db, product("A", 300, at=NEW))
    # Old writer leaves new columns behind; auto reads detect their stale watermark.
    assert read_current_product(db, "ownerclan", "A")["read_path"] == "legacy"
    assert read_current_product(db, "ownerclan", "A")["current_supply_price"] == 300
    assert count(db, "product_events") == 0 and count(db, "product_options") == 2


def test_backfill_failure_rolls_back_progress_and_new_collection_wins(db, monkeypatch):
    monkeypatch.setenv("COLLECTOR_STORAGE_VERSION", "1")
    save(db, product("A", 100))
    save(db, product("B", 200))
    monkeypatch.setenv("COLLECTOR_STORAGE_VERSION", "2")
    backfill_batch(db, batch_size=1)
    db.execute("ALTER TABLE products ADD CONSTRAINT reject_backfill CHECK (current_supply_price<>200)")
    db.commit()
    with pytest.raises(psycopg.Error):
        backfill_batch(db, batch_size=1)
    assert db.execute("SELECT scanned_count FROM storage_backfill_progress").fetchone()["scanned_count"] == 1
    db.execute("ALTER TABLE products DROP CONSTRAINT reject_backfill")
    db.commit()
    save(db, product("B", 999, at=NEW))
    assert backfill_batch(db, batch_size=1)["updated_count"] == 1
    assert read_current_product(db, "ownerclan", "B")["current_supply_price"] == 999
    assert count(db, "product_history") == 3
    assert compare_current_batch(db)["differences"] == []


def test_repeated_migration_detects_schema_drift(db):
    apply_migration(db, "stage2_storage_contract")
    db.execute("ALTER TABLE products ALTER COLUMN current_supply_price TYPE TEXT")
    db.commit()
    with pytest.raises(SchemaCompatibilityError, match="current_supply_price"):
        apply_migration(db, "stage2_storage_contract")


def test_90_day_context_keeps_price_baseline_and_original_soldout_observation(db):
    save(db, product(at=OLD, status="soldout"))
    save(db, product(at="2026-01-15T00:00:00Z", status="soldout", options=[{"key": "001", "price": 100, "quantity": 0}]))
    save(db, product(at=NOW, status="soldout", price=200))
    save(db, product(at=NEW, status="available", price=200))
    pid = db.execute("SELECT id FROM products").fetchone()["id"]
    context = history_context(db, pid, NOW)
    assert context["status_anchor"]["observed_at"].isoformat().startswith("2026-01-01")
    assert context["price_anchor"]["observed_at"].isoformat().startswith("2026-01-15")
    assert context["continuous_coverage_verified"] is False
    rows = list(iter_history_window(db, pid, NOW, "2026-04-01T00:00:00Z", batch_size=1))
    assert len(rows) == 2 and all(r["change_type"] == "update" for r in rows)
    initial = db.execute("SELECT * FROM product_history ORDER BY id LIMIT 1").fetchone()
    assert initial["change_type"] == "initial"
    stock_only = db.execute("SELECT * FROM product_history ORDER BY id OFFSET 1 LIMIT 1").fetchone()
    assert not any(path.startswith("prices.") for path in stock_only["changed_fields"])
    assert count(db, "product_events") == 0


def test_source_context_does_not_replace_occurrence_by_observation(db):
    save_events(db, [event(kind="soldout", createdAt=1767312000000), event(kind="soldout"),
                     event(kind="optionRestocked", createdAt=1767398400000)])
    context = event_context(db, "ownerclan", "A", NOW)
    assert context["soldout_start"]["occurred_at"].isoformat().startswith("2026-01-01")
    assert context["time_basis"] == "occurred_at" and not context["continuous_coverage_verified"]
    assert [r["occurred_at"].day for r in iter_event_window(db, "ownerclan", "A", OLD, NOW, batch_size=1)] == [1, 2, 3]


def test_event_detail_concurrent_absent_product_and_duplicate_event(db, db_factory):
    insert_events(db, [event().record()])
    started = Event()
    def detail():
        other = db_factory()
        started.set()
        save(other, product())
        save_events(other, [event()])
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(detail)
        assert started.wait(5)
        db.commit()
        future.result(timeout=10)
    assert count(db, "product_events") == 1
    assert db.execute("SELECT product_id FROM product_events").fetchone()["product_id"] is not None
    assert db.execute("SELECT requested_version FROM product_discovery_targets").fetchone()["requested_version"] == 1


def test_concurrent_claims_do_not_lease_same_product(db, db_factory):
    save_events(db, [event(), event("B")])
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(lambda: claim_refreshes(db_factory(), "ownerclan", limit=1)) for _ in range(2)]
        claims = [claim for future in futures for claim in future.result(timeout=10)]
    assert len(claims) == 2 and len({c["id"] for c in claims}) == 2


def test_equal_time_previous_detail_cannot_complete_new_request(db):
    save(db, product())
    save_events(db, [event()])
    claim = claim_refreshes(db, "ownerclan")[0]
    with pytest.raises(ValueError, match="equal-time"):
        complete_refresh(db, claim, product(price=999), collected_at=NOW)
    assert db.execute("SELECT completed_version FROM product_discovery_targets").fetchone()["completed_version"] == 0
    complete_refresh(db, claim, product(price=999, at=NEW), collected_at=NEW)
    assert read_current_product(db, "ownerclan", "A")["current_supply_price"] == 999


def test_product_storage_migration_partial_failure_preserves_legacy_schema(db_factory, monkeypatch):
    from product_data_collector.persistence import migrations
    db = db_factory()
    monkeypatch.setenv("COLLECTOR_STORAGE_VERSION", "1")
    storage.init_schema(db)
    save(db, product())
    monkeypatch.setenv("COLLECTOR_STORAGE_VERSION", "2")
    monkeypatch.setitem(migrations.MIGRATIONS, "stage2_storage_contract",
                        migrations.MIGRATIONS["stage2_storage_contract"] + ("SELECT stage2_injected_failure",))
    with pytest.raises(psycopg.Error):
        apply_migration(db, "stage2_storage_contract")
    assert count(db, "products") == count(db, "product_history") == 1
    assert count(db, "schema_migrations") == 0
    assert db.execute("SELECT to_regclass('product_events') AS name").fetchone()["name"] is None
    assert not db.execute("SELECT 1 FROM pg_attribute WHERE attrelid='products'::regclass AND attname='current_supply_price'").fetchone()


def test_backfill_waiting_for_new_observation_does_not_overwrite_it(db, db_factory, monkeypatch):
    monkeypatch.setenv("COLLECTOR_STORAGE_VERSION", "1")
    save(db, product())
    monkeypatch.setenv("COLLECTOR_STORAGE_VERSION", "2")
    storage._save_product_batch(db, [storage._snapshot_row("ownerclan", NEW, product(price=999, at=NEW))])
    started = Event()
    def backfill():
        other = db_factory()
        started.set()
        return backfill_batch(other)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(backfill)
        assert started.wait(5)
        db.commit()
        result = future.result(timeout=10)
    assert result["updated_count"] == 0
    assert read_current_product(db, "ownerclan", "A")["current_supply_price"] == 999
    assert read_product_options(db, "ownerclan", "A")[0]["price"] == 999
    xmin = db.execute("SELECT xmin FROM products").fetchone()["xmin"]
    backfill_batch(db, name="second_pass")
    assert db.execute("SELECT xmin FROM products").fetchone()["xmin"] == xmin


def test_legacy_missing_completeness_and_missing_values_not_guessed(db, monkeypatch):
    monkeypatch.setenv("COLLECTOR_STORAGE_VERSION", "1")
    p = product(options=[{"key": "K", "price": 123, "quantity": None}])
    p.pop("optionsComplete")
    save(db, p)
    monkeypatch.setenv("COLLECTOR_STORAGE_VERSION", "2")
    backfill_batch(db)
    current = read_current_product(db, "ownerclan", "A")
    assert current["current_supply_price"] is None and current["stock_quantity"] is None
    assert read_product_options(db, "ownerclan", "A")[0]["price"] == 123


def test_event_retained_when_parent_removed_without_options(db):
    save_events(db, [event()])
    save(db, product(options=[]))
    db.execute("DELETE FROM products")  # Isolated fixture only; verifies new FK retention behavior.
    db.commit()
    assert count(db, "product_events") == 1
    assert db.execute("SELECT product_id FROM product_events").fetchone()["product_id"] is None


def test_migration_replay_detects_same_named_wrong_index(db):
    apply_migration(db, "stage2_storage_contract")
    db.execute("DROP INDEX idx_product_events_natural_time")
    db.execute("CREATE INDEX idx_product_events_natural_time ON product_events(semantic)")
    db.commit()
    with pytest.raises(SchemaCompatibilityError, match="index"):
        apply_migration(db, "stage2_storage_contract")


def test_explicit_migration_and_backfill_cli_on_isolated_legacy_schema(db_factory, monkeypatch):
    import os
    import subprocess
    import sys
    from psycopg.conninfo import make_conninfo
    db = db_factory()
    monkeypatch.setenv("COLLECTOR_STORAGE_VERSION", "1")
    storage.init_schema(db)
    save(db, product())
    schema = db.execute("SELECT current_schema() AS name").fetchone()["name"]
    db.commit()
    env = {**os.environ, "COLLECTOR_STORAGE_VERSION": "2", "STORAGE_CLI_TEST_DSN": make_conninfo(
        os.environ["COLLECTOR_TEST_POSTGRES_DSN"], options=f"-c search_path={schema}")}
    def run(module, *args):
        result = subprocess.run([sys.executable, "-m", "product_data_collector.persistence." + module,
                                 *args, "--dsn-env", "STORAGE_CLI_TEST_DSN"], env=env, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stderr
        return result.stdout
    assert run("migrations", "--apply", "stage2_storage_contract").strip() == "applied"
    assert run("migrations", "--apply", "stage2_storage_contract").strip() == "already applied"
    assert '"completed": true' in run("backfill", "--batch-size", "1")
    assert '"differences": []' in run("backfill", "--verify")
    assert '"history": 1' in run("backfill", "--integrity-counts")


def test_option_identity_change_survives_history_and_backfill(db):
    save(db, product(options=[{"key": "001", "price": 100, "quantity": 2}]))
    save(db, product(at=NEW, options=[{"key": "1", "price": 100, "quantity": 2}]))
    assert count(db, "product_history") == 2
    assert read_product_options(db, "ownerclan", "A", read_path="legacy")[0]["source_option_id"] == "1"
    assert read_product_options(db, "ownerclan", "A")[0]["source_option_id"] == "1"


def test_observed_price_verification_is_not_backdated_to_source_event(db):
    e = domeggook_event({"no": "A", "status": "AMT", "date": 1767225600}, NEW)
    save_events(db, [e])
    eid = db.execute("SELECT id FROM product_events").fetchone()["id"]
    record_verification(db, eid, before=100, after=200, evidence={
        "field": "supply_price", "before_observed_at": NOW, "after_observed_at": NEW})
    db.commit()
    # January's signal does not prove that March's observed price existed in January.
    assert event_context(db, "domeggook", "A", NOW)["price_anchor"] is None


def test_source_base_price_is_not_a_final_supply_price_anchor(db):
    save_events(db, [event()])
    assert event_context(db, "ownerclan", "A", NOW)["price_anchor"] is None
    anchor = event_context(db, "ownerclan", "A", NOW, price_scope="base")["price_anchor"]
    assert anchor["verified_after"] == 100


@pytest.mark.parametrize("ddl,reason", [
    ("ALTER TABLE product_discovery_targets ALTER COLUMN completed_version SET DEFAULT 1", "default"),
    ("ALTER TABLE product_events DROP CONSTRAINT product_events_event_origin_check", "check"),
])
def test_migration_replay_rejects_default_and_check_drift(db, ddl, reason):
    apply_migration(db, "stage2_storage_contract")
    db.execute(ddl)
    db.commit()
    with pytest.raises(SchemaCompatibilityError, match=reason):
        apply_migration(db, "stage2_storage_contract")


def test_option_source_price_anchor_requires_verified_mapping(db):
    save_events(db, [event(kind="optionPriceIncreased", optionKey="001")])
    save(db, product())
    event_id = db.execute("SELECT id FROM product_events").fetchone()["id"]
    option_id = db.execute("SELECT id FROM product_options").fetchone()["id"]
    context = event_context(db, "ownerclan", "A", NOW, price_scope="option", option_identity="native:001")
    assert context["price_anchor"] is None
    link_event_option(db, event_id, option_id, evidence={"verified_by": "explicit fixture"})
    db.commit()
    context = event_context(db, "ownerclan", "A", NOW, price_scope="option", option_identity="native:001")
    assert context["price_anchor"]["id"] == event_id
    assert context["price_scope"] == "option"


def test_event_partial_batch_failure_reports_only_confirmed_commits(db):
    db.execute("ALTER TABLE product_events ADD CONSTRAINT reject_b CHECK (external_product_id<>'B')")
    db.commit()
    with pytest.raises(EventSaveError) as failure:
        save_events(db, iter([event(), event("B"), event("C")]), batch_size=1)
    assert failure.value.committed_count == 1
    assert failure.value.pending_keys == (event("B").record()["dedupe_key"],)
    assert count(db, "product_events") == count(db, "product_discovery_targets") == 1
    db.execute("ALTER TABLE product_events DROP CONSTRAINT reject_b")
    db.commit()
    assert save_events(db, [event(), event("B"), event("C")], batch_size=1)["inserted_count"] == 2
    assert count(db, "product_events") == 3
    assert all(r["requested_version"] == 1 for r in db.execute("SELECT requested_version FROM product_discovery_targets").fetchall())


def test_migration_rejects_fk_to_wrong_column_on_correct_table(db):
    apply_migration(db, "stage2_storage_contract")
    db.execute("ALTER TABLE products ADD COLUMN alternative_id BIGINT UNIQUE")
    db.execute("ALTER TABLE product_events DROP CONSTRAINT product_events_product_id_fkey")
    db.execute("ALTER TABLE product_events ADD FOREIGN KEY(product_id) REFERENCES products(alternative_id) ON DELETE SET NULL")
    db.commit()
    with pytest.raises(SchemaCompatibilityError, match="FK mismatch"):
        apply_migration(db, "stage2_storage_contract")
