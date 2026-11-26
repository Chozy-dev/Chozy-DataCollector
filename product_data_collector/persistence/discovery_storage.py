"""Discovery target and search rank rows within a caller-owned transaction."""
from collections.abc import Iterable
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from .storage_values import json_safe, parse_datetime, text_or_none


DOMEGGOOK_RANKED_SORTS = {"ha", "rd"}


def _search_rank_rows(platform: str, records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in records:
        product_id = record.get("productId")
        collected_at = record.get("collectedAt")
        sort = text_or_none(record.get("sort") or record.get("sortBy")) or ""
        rank = _positive_int(record.get("rank"))
        if product_id in (None, "") or collected_at in (None, "") or rank is None:
            continue
        if platform == "domeggook" and sort not in DOMEGGOOK_RANKED_SORTS:
            continue
        rows.append(
            {
                "platform": platform,
                "collected_at": parse_datetime(str(collected_at)),
                "keyword": text_or_none(record.get("keyword")) or "",
                "category_code": text_or_none(record.get("categoryCode")) or "",
                "category_name": text_or_none(record.get("categoryName")),
                "category_path": record.get("categoryPath") if isinstance(record.get("categoryPath"), list) else [],
                "market": text_or_none(record.get("market")) or "default",
                "sort": sort,
                "reason": text_or_none(record.get("reason")),
                "external_product_id": str(product_id),
                "rank": rank,
            }
        )
    return rows

def _bulk_upsert_search_ranks(connection: Connection[Any], rows: list[dict[str, Any]]) -> None:
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO product_search_ranks (
                platform,
                collected_at,
                keyword,
                category_code,
                category_name,
                category_path,
                market,
                sort,
                reason,
                external_product_id,
                rank
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (platform, collected_at, keyword, category_code, market, sort, external_product_id, rank) DO UPDATE SET
                category_name = EXCLUDED.category_name,
                category_path = EXCLUDED.category_path,
                reason = EXCLUDED.reason
            """,
            [
                (
                    row["platform"],
                    row["collected_at"],
                    row["keyword"],
                    row["category_code"],
                    row["category_name"],
                    Jsonb(json_safe(row["category_path"])),
                    row["market"],
                    row["sort"],
                    row["reason"],
                    row["external_product_id"],
                    row["rank"],
                )
                for row in rows
            ],
        )

def _discovery_target_rows(platform: str, records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for record in records:
        product_id = record.get("productId") or record.get("productKey") or record.get("externalProductId")
        discovered_at = record.get("collectedAt") or record.get("discoveredAt")
        if product_id in (None, "") or discovered_at in (None, ""):
            continue
        external_id = str(product_id)
        if external_id in seen:
            continue
        seen.add(external_id)
        rows.append(
            {
                "platform": platform,
                "external_product_id": external_id,
                "discovered_at": parse_datetime(str(discovered_at)),
                "keyword": text_or_none(record.get("keyword")),
                "category_code": text_or_none(record.get("categoryCode")),
                "category_name": text_or_none(record.get("categoryName")),
                "market": text_or_none(record.get("market")),
                "reason": text_or_none(record.get("reason")),
            }
        )
    return rows

def _count_new_discovery_targets(
    connection: Connection[Any],
    platform: str,
    rows: list[dict[str, Any]],
) -> int:
    external_ids = [row["external_product_id"] for row in rows]
    existing = connection.execute(
        """
        SELECT external_product_id
        FROM product_discovery_targets
        WHERE platform = %s
          AND external_product_id = ANY(%s)
        """,
        (platform, external_ids),
    ).fetchall()
    existing_ids = {str(row["external_product_id"]) for row in existing}
    return sum(1 for external_id in external_ids if external_id not in existing_ids)

def _bulk_upsert_discovery_targets(connection: Connection[Any], rows: list[dict[str, Any]]) -> None:
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO product_discovery_targets (
                platform,
                external_product_id,
                first_discovered_at,
                last_discovered_at,
                keyword,
                category_code,
                category_name,
                market,
                reason
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (platform, external_product_id) DO UPDATE SET
                last_discovered_at = EXCLUDED.last_discovered_at,
                keyword = COALESCE(EXCLUDED.keyword, product_discovery_targets.keyword),
                category_code = COALESCE(EXCLUDED.category_code, product_discovery_targets.category_code),
                category_name = COALESCE(EXCLUDED.category_name, product_discovery_targets.category_name),
                market = COALESCE(EXCLUDED.market, product_discovery_targets.market),
                reason = COALESCE(EXCLUDED.reason, product_discovery_targets.reason),
                active = true,
                updated_at = now()
            """,
            [
                (
                    row["platform"],
                    row["external_product_id"],
                    row["discovered_at"],
                    row["discovered_at"],
                    row["keyword"],
                    row["category_code"],
                    row["category_name"],
                    row["market"],
                    row["reason"],
                )
                for row in rows
            ],
        )

def _positive_int(value: Any) -> int | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None
