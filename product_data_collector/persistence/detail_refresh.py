"""Versioned detail work on the existing discovery table; no API worker here."""
from datetime import timedelta
from uuid import uuid4

from psycopg.types.json import Jsonb

from product_data_collector.domain.product_events import utc


def _request_refresh(connection, platform, external_id, observed_at, scope):
    # Called only for newly inserted durable events, under the natural product lock.
    connection.execute(
        """INSERT INTO product_discovery_targets(platform, external_product_id, first_discovered_at,
            last_discovered_at, requested_version, refresh_status, refresh_requested_at, refresh_scope)
        VALUES (%s,%s,%s,%s,1,'pending',%s,%s)
        ON CONFLICT (platform, external_product_id) DO UPDATE SET
            requested_version=product_discovery_targets.requested_version+1,
            refresh_requested_at=GREATEST(product_discovery_targets.refresh_requested_at, EXCLUDED.refresh_requested_at),
            refresh_scope=EXCLUDED.refresh_scope,
            refresh_status=CASE WHEN product_discovery_targets.refresh_status='processing' THEN 'processing' ELSE 'pending' END,
            next_retry_at=NULL, updated_at=now()""",
        (platform, external_id, observed_at, observed_at, observed_at, Jsonb(scope)),
    )


def claim_refreshes(connection, platform, *, limit=100, lease_seconds=300):
    """Commit a bounded lease. Crash/expired lease is claimable again with a new token."""
    if not 1 <= limit <= 1000 or lease_seconds <= 0:
        raise ValueError("Invalid claim limit/lease")
    try:
        rows = connection.execute(
            "SELECT * FROM product_discovery_targets WHERE platform=%s AND requested_version>completed_version "
            "AND (next_retry_at IS NULL OR next_retry_at<=now()) AND (lease_until IS NULL OR lease_until<=now()) "
            "ORDER BY next_retry_at NULLS FIRST, id LIMIT %s FOR UPDATE SKIP LOCKED", (platform, limit),
        ).fetchall()
        claims = []
        for row in rows:
            claim = connection.execute(
                "UPDATE product_discovery_targets SET lease_token=%s, lease_until=now()+%s, "
                "processing_version=requested_version, refresh_status='processing', updated_at=now() "
                "WHERE id=%s RETURNING *", (uuid4(), timedelta(seconds=lease_seconds), row["id"]),
            ).fetchone()
            claims.append(claim)
        connection.commit()
        return claims
    except Exception:
        connection.rollback()
        raise


def request_refreshes(connection, platform, external_ids, *, observed_at, scope):
    """Commit coalesced non-event signals (e.g. quantity changes).

    Replays at the same observation time do not create extra versions. A later
    overlap run may conservatively request detail again without storing a stock
    event. Uses the same natural-key lock as event/product writers.
    """
    from .postgres_storage import _lock_key
    observed_at = utc(observed_at)
    ids = sorted(set(external_ids), key=lambda key: _lock_key(platform, key))
    try:
        for key in ids:
            connection.execute("SELECT pg_advisory_xact_lock(%s)", (_lock_key(platform, key),))
            previous = connection.execute("SELECT refresh_requested_at FROM product_discovery_targets "
                "WHERE platform=%s AND external_product_id=%s", (platform, key)).fetchone()
            if not previous or previous["refresh_requested_at"] is None or previous["refresh_requested_at"] < observed_at:
                _request_refresh(connection, platform, key, observed_at, scope)
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def complete_refresh(connection, claim, product, *, collected_at):
    """Save detail + history + options + completion in one transaction.

    A request arriving during the lease stays pending after completion. Never use
    a source event occurred_at as collected_at. Returned success is commit-confirmed.
    """
    from .postgres_storage import _save_product_batch, _lock_key
    from .snapshot_payload import snapshot_row
    row = snapshot_row(claim["platform"], collected_at, product)
    if row["external_product_id"] != claim["external_product_id"]:
        raise ValueError("Detail does not match claimed product")
    if row["collected_at"] < claim["refresh_requested_at"]:
        raise ValueError("Detail observation predates the claimed refresh request")
    try:
        connection.execute("SELECT pg_advisory_xact_lock(%s)", (_lock_key(row["platform"], row["external_product_id"]),))
        valid = connection.execute(
            "SELECT id FROM product_discovery_targets WHERE id=%s AND lease_token=%s "
            "AND processing_version=%s AND lease_until>now() FOR UPDATE",
            (claim["id"], claim["lease_token"], claim["processing_version"]),
        ).fetchone()
        if not valid:
            done = connection.execute("SELECT * FROM product_discovery_targets WHERE id=%s", (claim["id"],)).fetchone()
            if done and done["completed_version"] >= claim["processing_version"]:
                connection.commit()
                return done
            raise ValueError("Refresh lease is stale or already completed")
        previous = connection.execute(
            "SELECT last_collected_at FROM products WHERE platform=%s AND external_product_id=%s",
            (row["platform"], row["external_product_id"]),
        ).fetchone()
        if previous and previous["last_collected_at"] == row["collected_at"] == claim["refresh_requested_at"]:
            raise ValueError("Existing equal-time state requires a newer detail observation to complete a refresh")
        _save_product_batch(connection, [row])
        result = connection.execute(
            "UPDATE product_discovery_targets SET completed_version=%s, refresh_completed_at=%s, "
            "refresh_status=CASE WHEN requested_version=%s THEN 'completed' ELSE 'pending' END, "
            "lease_token=NULL, lease_until=NULL, processing_version=NULL, failure_count=0, "
            "next_retry_at=NULL, last_error=NULL, updated_at=now() WHERE id=%s RETURNING *",
            (claim["processing_version"], row["collected_at"], claim["processing_version"], claim["id"]),
        ).fetchone()
        connection.commit()
        return result
    except Exception:
        connection.rollback()
        raise


def fail_refresh(connection, claim, *, error, next_retry_at):
    try:
        result = connection.execute(
            "UPDATE product_discovery_targets SET failure_count=failure_count+1, last_error=%s, next_retry_at=%s, "
            "refresh_status='retry', lease_token=NULL, lease_until=NULL, processing_version=NULL, updated_at=now() "
            "WHERE id=%s AND lease_token=%s AND processing_version=%s RETURNING id",
            (str(error)[:2000], utc(next_retry_at), claim["id"], claim["lease_token"], claim["processing_version"]),
        ).fetchone()
        if not result:
            raise ValueError("Refresh lease is stale or already completed")
        connection.commit()
        return True
    except Exception:
        connection.rollback()
        raise
