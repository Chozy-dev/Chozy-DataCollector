from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, unquote, urlsplit

from psycopg.rows import dict_row

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


def main() -> int:
    args = _parse_args()
    rows = list(_db_rows(args.platform, args.limit))
    if args.platform in (None, "coupang"):
        rows.extend(_coupang_jsonl_rows(args.limit))

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
        choices=("ownerclan", "domeggook", "coupang"),
        default=None,
        help="Limit DB export to one platform. Coupang JSONL rows are still included only for coupang/all.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional per-source row limit.")
    return parser.parse_args()


def _db_rows(platform: str | None, limit: int | None) -> Iterable[dict[str, Any]]:
    if platform == "coupang":
        return []

    query = """
        SELECT platform, external_product_id, product_name, product_url
        FROM products
    """
    params: list[Any] = []
    clauses: list[str] = []
    if platform:
        clauses.append("platform = %s")
        params.append(platform)
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY platform, external_product_id"
    if limit is not None:
        query += " LIMIT %s"
        params.append(limit)

    config = load_postgres_config(PROJECT_ROOT)
    with connect(config) as connection:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, params)
            for row in cursor:
                yield _lookup_row(
                    platform=row["platform"],
                    external_product_id=row["external_product_id"],
                    product_name=row["product_name"],
                    product_url=row["product_url"],
                    source="postgres.products",
                )


def _coupang_jsonl_rows(limit: int | None) -> Iterable[dict[str, Any]]:
    count = 0
    for path in sorted((PROJECT_ROOT / "coupang_API" / "data" / "processed").glob("*_products.jsonl")):
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                product = json.loads(line)
                yield _lookup_row(
                    platform="coupang",
                    external_product_id=product.get("productId"),
                    product_name=product.get("productName"),
                    product_url=product.get("productUrl"),
                    source=str(path.relative_to(PROJECT_ROOT)),
                )
                count += 1
                if limit is not None and count >= limit:
                    return


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
