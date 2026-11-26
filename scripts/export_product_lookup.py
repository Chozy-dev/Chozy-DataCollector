"""Export the supplier catalogue and the latest published Coupang best products."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, unquote, urlsplit

from psycopg import Cursor

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from product_data_collector.persistence.postgres_storage import connect, load_postgres_config


URL_ID_QUERY_KEYS = (
    "pageKey",
    "productId",
    "productNo",
    "product_no",
    "itemNo",
    "itemNoOrigin",
    "goodsNo",
    "no",
    "itemKey",
    "selfcode",
    "key",
)
SUPPLIERS = ("ownerclan", "domeggook")


def main() -> int:
    args = _parse_args()
    config = load_postgres_config(PROJECT_ROOT)
    with connect(config) as connection:
        with connection.cursor() as cursor:
            rows = list(_db_rows(cursor, args.platform, args.limit))

    deduped = _dedupe(rows)
    output_path = PROJECT_ROOT / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.suffix.lower() == ".jsonl":
        _write_jsonl(output_path, deduped)
    else:
        _write_csv(output_path, deduped)

    print(f"exported {len(deduped)} product lookup rows: {output_path}")
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export platform product id/name lookup data.")
    parser.add_argument(
        "--output",
        default="artifacts/exports/product_lookup.csv",
        help="Output path relative to project root. Use .csv or .jsonl.",
    )
    parser.add_argument(
        "--platform",
        choices=(*SUPPLIERS, "coupang"),
        default=None,
        help="Export one supplier or the latest completed Coupang best batch. Default: all.",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Optional row limit for the supplier catalogue and for Coupang, separately.",
    )
    return parser.parse_args()


def _db_rows(cursor: Cursor[dict[str, Any]], platform: str | None, limit: int | None) -> Iterable[dict[str, Any]]:
    if platform != "coupang":
        cursor.execute(
            """SELECT platform, external_product_id, product_name, product_url
                FROM products
                WHERE platform = ANY(%s)
                ORDER BY platform, external_product_id
                LIMIT %s""",
            (list(SUPPLIERS) if platform is None else [platform], limit),
        )
        for row in cursor:
            yield _lookup_row(**row, source="postgres.products")

    if platform not in (None, "coupang"):
        return
    # Supplier-only databases remain exportable before the optional best migration.
    cursor.execute("SELECT to_regclass('coupang_best_published') AS published_view")
    if cursor.fetchone()["published_view"] is None:
        return

    cursor.execute(
        """WITH latest AS (
                SELECT snapshot_date FROM coupang_best_batches
                WHERE status = 'completed' ORDER BY snapshot_date DESC LIMIT 1
            )
            SELECT DISTINCT ON (p.product_id) p.product_id AS external_product_id,
                   p.product_name, p.product_url
            FROM coupang_best_published AS p JOIN latest USING (snapshot_date)
            ORDER BY p.product_id, p.rank, p.category_id
            LIMIT %s""",
        (limit,),
    )
    for row in cursor:
        yield _lookup_row(platform="coupang", **row, source="postgres.coupang_best_published")


def _lookup_row(
    *,
    platform: Any,
    external_product_id: Any,
    product_name: Any,
    product_url: Any,
    source: str,
) -> dict[str, Any]:
    external_id = "" if external_product_id is None else str(external_product_id).strip()
    url = "" if product_url is None else str(product_url).strip()
    url_code = _url_product_code(url)
    return {
        "platform": "" if platform is None else str(platform),
        "external_product_id": external_id,
        "product_name": "" if product_name is None else str(product_name),
        "product_url": url,
        "url_product_code": url_code,
        "url_code_matches_external_id": "" if not url_code else str(url_code == external_id).lower(),
        "source": source,
    }


def _url_product_code(url: str) -> str:
    if not url:
        return ""
    parts = urlsplit(url)
    query = parse_qs(parts.query)
    for key in URL_ID_QUERY_KEYS:
        values = query.get(key)
        if values and values[0]:
            return values[0].strip()

    path_parts = [unquote(part).strip() for part in parts.path.split("/") if part.strip()]
    for part in reversed(path_parts):
        if any(char.isdigit() for char in part) or part.upper().startswith("W"):
            return part
    return ""


def _dedupe(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["platform"], row["external_product_id"])
        if not key[1]:
            continue
        existing = deduped.get(key)
        if existing is None or _is_better(row, existing):
            deduped[key] = row
    return sorted(deduped.values(), key=lambda row: (row["platform"], row["external_product_id"]))


def _is_better(candidate: dict[str, Any], existing: dict[str, Any]) -> bool:
    if candidate["product_url"] and not existing["product_url"]:
        return True
    if candidate["product_name"] and not existing["product_name"]:
        return True
    return False


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "platform",
        "external_product_id",
        "product_name",
        "product_url",
        "url_product_code",
        "url_code_matches_external_id",
        "source",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
