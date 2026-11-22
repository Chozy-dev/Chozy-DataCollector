"""Explicit, resumable local projections from existing observations; no API calls."""
import argparse
import json
import os

from product_data_collector.domain.current_state import CURRENT_COLUMNS, project_current
from .current_state import legacy_rows, legacy_snapshot, compare_current_batch
from .postgres_storage import _lock_key, current_params
from .product_options import save_options


def backfill_batch(connection, *, name="stage2_current_v1", batch_size=500):
    if not 1 <= batch_size <= 10000:
        raise ValueError("Invalid batch size")
    try:
        connection.execute("SELECT pg_advisory_xact_lock(%s)", (_lock_key("backfill", name),))
        connection.execute(
            "INSERT INTO storage_backfill_progress(name, high_product_id) SELECT %s, COALESCE(max(id),0) FROM products "
            "ON CONFLICT(name) DO NOTHING", (name,),
        )
        progress = connection.execute("SELECT * FROM storage_backfill_progress WHERE name=%s FOR UPDATE", (name,)).fetchone()
        if progress["completed"]:
            connection.commit()
            return progress
        keys = connection.execute(
            "SELECT id, platform, external_product_id FROM products WHERE id>%s AND id<=%s ORDER BY id LIMIT %s",
            (progress["last_product_id"], progress["high_product_id"], batch_size),
        ).fetchall()
        with connection.cursor() as cursor:
            cursor.executemany("SELECT pg_advisory_xact_lock(%s)",
                [(key,) for key in sorted({_lock_key(r["platform"], r["external_product_id"]) for r in keys})])
        # Re-read after locks. New collection may have committed while this batch waited.
        rows = legacy_rows(connection, [r["id"] for r in keys])
        updated = 0
        for row in rows:
            if row["current_observed_at"] is not None and row["current_observed_at"] >= row["last_collected_at"]:
                continue
            snapshot = legacy_snapshot(row)
            result = connection.execute(
                "UPDATE products SET " + ",".join(f"{c}=%s" for c in CURRENT_COLUMNS)
                + " WHERE id=%s AND last_collected_at=%s AND (current_observed_at IS NULL OR current_observed_at<%s) RETURNING id",
                current_params(project_current(snapshot)) + (row["id"], row["last_collected_at"], row["last_collected_at"]),
            ).fetchone()
            if result:
                updated += 1
                if row["history_observed_at"] is not None:
                    snapshot["collected_at"] = row["history_observed_at"]
                    save_options(connection, row["id"], snapshot)
        if rows:
            connection.execute(
                "UPDATE product_events e SET product_id=p.id FROM products p "
                "WHERE p.id=ANY(%s) AND e.product_id IS NULL AND e.platform=p.platform "
                "AND e.external_product_id=p.external_product_id", ([r["id"] for r in rows],),
            )
        last = rows[-1]["id"] if rows else progress["last_product_id"]
        result = connection.execute(
            "UPDATE storage_backfill_progress SET last_product_id=%s, scanned_count=scanned_count+%s, "
            "updated_count=updated_count+%s, completed=%s, updated_at=now() WHERE name=%s RETURNING *",
            (last, len(rows), updated, len(rows) < batch_size or last >= progress["high_product_id"], name),
        ).fetchone()
        connection.commit()
        return result
    except Exception:
        connection.rollback()
        raise


def integrity_counts(connection):
    """Explicit full scan for offline validation, never run on collector startup."""
    return connection.execute("""SELECT
        (SELECT count(*) FROM products) AS products,
        (SELECT count(*) FROM product_history) AS history,
        (SELECT count(*) FROM product_history h LEFT JOIN products p ON p.id=h.product_id WHERE p.id IS NULL) AS orphan_history,
        (SELECT count(*) FROM product_events e JOIN products p ON p.id=e.product_id
          WHERE e.platform<>p.platform OR e.external_product_id<>p.external_product_id) AS wrong_event_links""").fetchone()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-env", required=True)
    parser.add_argument("--name", default="stage2_current_v1")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-batches", type=int, default=1, help="Bound work per invocation; resume with the same name")
    parser.add_argument("--verify", action="store_true", help="Read-only, bounded keyset comparison")
    parser.add_argument("--after-id", type=int, default=0)
    parser.add_argument("--integrity-counts", action="store_true", help="Explicit full counts (can be slow)")
    args = parser.parse_args(argv)
    if not os.environ.get(args.dsn_env) or args.max_batches < 1:
        parser.error("Explicit DSN variable and positive max-batches required; .env is never loaded")
    import psycopg
    from psycopg.rows import dict_row
    with psycopg.connect(os.environ[args.dsn_env], row_factory=dict_row) as connection:
        if args.integrity_counts:
            print(json.dumps(integrity_counts(connection)))
        elif args.verify:
            last = args.after_id
            for _ in range(args.max_batches):
                result = compare_current_batch(connection, after_id=last, limit=args.batch_size)
                print(json.dumps(result))
                last = result["next_id"]
                if result["checked"] < args.batch_size:
                    break
        else:
            for _ in range(args.max_batches):
                result = backfill_batch(connection, name=args.name, batch_size=args.batch_size)
                print(json.dumps(result, default=str))
                if result["completed"]:
                    break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
