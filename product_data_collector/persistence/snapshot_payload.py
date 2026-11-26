"""Normalize supplier products into rows for current state and history storage."""
from decimal import Decimal
from typing import Any

from product_data_collector.common.numeric_utils import parse_decimal
from product_data_collector.domain.product_history import comparable_state, external_product_id, normalize_current_product
from product_data_collector.domain.shipping_fees import parse_shipping_fee

from .storage_values import TRUE_VALUES, parse_datetime, text_or_none


def _has_inventory_snapshot(row: dict[str, Any]) -> bool:
    if row["stock_quantity"] is not None:
        return True
    return _has_meaningful_payload_value(row["inventory_payload"], ignored_keys={"stockQuantitySource"})

def _has_shipping_snapshot(shipping: dict[str, Any], row: dict[str, Any]) -> bool:
    if shipping["fee"] is not None or row["is_free_shipping"] is not None:
        return True
    if shipping["shipping_type"] not in (None, "", "unknown"):
        return True
    payload = shipping.get("payload", row["shipping_payload"])
    return _has_meaningful_payload_value(
        payload,
        ignored_keys={
            "market",
            "shipping_fee",
            "shipping_type",
            "shipping_payment",
            "shipping_fee_raw",
            "shipping_fee_type_raw",
            "requires_quantity_calculation",
            "sourceFields",
            "source_fields",
        },
    )

def _has_meaningful_payload_value(value: Any, *, ignored_keys: set[str] | None = None) -> bool:
    ignored = ignored_keys or set()
    if isinstance(value, dict):
        return any(
            _has_meaningful_payload_value(child, ignored_keys=ignored)
            for key, child in value.items()
            if str(key) not in ignored
        )
    if isinstance(value, list):
        return any(_has_meaningful_payload_value(child, ignored_keys=ignored) for child in value)
    return _has_value(value)

def snapshot_row(platform: str, collected_at: str, product: dict[str, Any]) -> dict[str, Any] | None:
    external_id = external_product_id(product)
    if not external_id:
        raise ValueError("Product is missing its external product ID")
    current = normalize_current_product(product)
    comparable = comparable_state(product)
    prices = _section_or_empty(comparable, current, "prices")
    inventory = _section_or_empty(comparable, current, "inventory")
    shipping = _section_or_empty(comparable, current, "shipping")
    shipping_payload = _shipping_payload(comparable, current)
    shipping_rows = _shipping_rows(platform, shipping_payload)
    seller = _seller_row(_object_or_empty(current.get("seller")))
    return {
        "platform": platform,
        "external_product_id": external_id,
        "collected_at": parse_datetime(str(product.get("collectedAt") or collected_at)),
        "product_name": _first_text(current, "productName", "name", "title"),
        "product_url": _first_text(current, "productUrl", "affiliateUrl", "url"),
        "image_url": _first_text(current, "imageUrl", "productImage"),
        "backup_image_url": _first_text(current, "backupImageUrl"),
        "source_specific": _source_specific_row(current),
        "status": text_or_none(current.get("status")),
        "seller": seller,
        "prices_payload": prices,
        "inventory_payload": inventory,
        "options_payload": current.get("options") if isinstance(current.get("options"), list) else [],
        "options_complete": current.get("optionsComplete") is True,
        "markets": _object_or_empty(current.get("markets")),
        "shipping_payload": shipping_payload,
        "primary_price": _decimal_or_none(_extract_primary_price(prices, current)),
        "price_rows": _price_rows(platform, prices, current),
        "stock_quantity": _decimal_or_none(_first_available_value(inventory, current, "stockQuantity")),
        "shipping_fee": shipping_rows[0]["fee"],
        "shipping_type": text_or_none(_first_value(shipping_payload, "type", "feeType", "domeFeeType", "supplyFeeType")),
        "shipping_rows": shipping_rows,
        "is_free_shipping": _bool_or_none(_first_available_value(shipping, current, "isFreeShipping")),
    }

def _seller_row(seller: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": text_or_none(seller.get("id")),
        "nickname": text_or_none(seller.get("nickname")),
        "type": text_or_none(seller.get("type")),
        "grade": text_or_none(seller.get("grade")),
        "excellent_seller": _bool_or_none(seller.get("excellentSeller")),
        "average_satisfaction": text_or_none(seller.get("averageSatisfaction")),
        "review_count": _decimal_or_none(seller.get("reviewCount")),
    }

def _source_specific_row(current: dict[str, Any]) -> dict[str, Any]:
    source_specific = current.get("sourceSpecific")
    row = dict(source_specific) if isinstance(source_specific, dict) else {}
    category = current.get("category")
    if isinstance(category, dict) and any(_has_value(value) for value in category.values()):
        row.setdefault("category", category)
    if isinstance(current.get("markets"), dict):
        row["markets"] = current["markets"]
    return row

def _object_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}

def _section_or_empty(comparable: dict[str, Any], current: dict[str, Any], key: str) -> dict[str, Any]:
    comparable_section = _object_or_empty(comparable.get(key))
    if any(_has_value(value) for value in comparable_section.values()):
        return comparable_section
    return _object_or_empty(current.get(key)) or comparable_section

def _shipping_payload(comparable: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    current_shipping = _object_or_empty(current.get("shipping"))
    comparable_shipping = _object_or_empty(comparable.get("shipping"))
    payload = {
        key: value
        for key, value in comparable_shipping.items()
        if _has_value(value)
    }
    payload.update(
        {
            key: value
            for key, value in current_shipping.items()
            if _has_value(value)
        }
    )
    return payload or comparable_shipping

def _first_text(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None

def _first_value(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if _has_value(value):
            return value
    return None

def _first_available_value(primary: dict[str, Any], fallback: dict[str, Any], key: str) -> Any:
    value = primary.get(key)
    if _has_value(value):
        return value
    value = fallback.get(key)
    return value if _has_value(value) else None

def _has_value(value: Any) -> bool:
    return value not in (None, "") and value != {"__value__": "__MISSING__"}

def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in TRUE_VALUES:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    return None

def _extract_primary_price(prices: dict[str, Any], current: dict[str, Any]) -> Any:
    for key in ("productPrice", "salePrice", "price", "supplyPrice"):
        value = prices.get(key) if key in prices else current.get(key)
        if value not in (None, ""):
            return value
    for value in prices.values():
        if isinstance(value, (int, float, str)) and not isinstance(value, bool):
            return value
    return None

def _price_rows(platform: str, prices: dict[str, Any], current: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if _has_value(prices.get("domeCurrentSupplyPrice")):
        rows.append(
            {
                "market": "dome",
                "price_type": "current_supply",
                "amount": _decimal_or_none(prices.get("domeCurrentSupplyPrice")),
            }
        )
    if _has_value(prices.get("supplyCurrentSupplyPrice")):
        rows.append(
            {
                "market": "supply",
                "price_type": "current_supply",
                "amount": _decimal_or_none(prices.get("supplyCurrentSupplyPrice")),
            }
        )
    if _has_value(prices.get("minimumRetailPrice")):
        rows.append(
            {
                "market": "retail",
                "price_type": "minimum_retail",
                "amount": _decimal_or_none(prices.get("minimumRetailPrice")),
            }
        )
    if _has_value(prices.get("recommendedRetailPrice")):
        rows.append(
            {
                "market": "retail",
                "price_type": "recommended_retail",
                "amount": _decimal_or_none(prices.get("recommendedRetailPrice")),
            }
        )
    if _has_value(prices.get("resaleMinimumPrice")):
        rows.append(
            {
                "market": "resale",
                "price_type": "minimum",
                "amount": _decimal_or_none(prices.get("resaleMinimumPrice")),
            }
        )
    if _has_value(prices.get("resaleRecommendedPrice")):
        rows.append(
            {
                "market": "resale",
                "price_type": "recommended",
                "amount": _decimal_or_none(prices.get("resaleRecommendedPrice")),
            }
        )
    if _has_value(prices.get("currentSupplyPrice")):
        rows.append(
            {
                "market": platform,
                "price_type": "current_supply",
                "amount": _decimal_or_none(prices.get("currentSupplyPrice")),
            }
        )
    if _has_value(prices.get("fixedPrice")):
        rows.append(
            {
                "market": platform,
                "price_type": "fixed",
                "amount": _decimal_or_none(prices.get("fixedPrice")),
            }
        )
    if rows:
        return rows
    return [
        {
            "market": platform,
            "price_type": "primary",
            "amount": _decimal_or_none(_extract_primary_price(prices, current)),
        }
    ]

def _shipping_rows(platform: str, shipping: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if _has_value(shipping.get("domeFee")) or _has_value(shipping.get("domeFeeType")):
        parsed = parse_shipping_fee(
            _first_value(shipping, "domeFeeRaw", "domeFee"),
            shipping.get("domeFeeType"),
            fee_payer=_first_value(shipping, "domeFeePayer", "feePayer", "deliWho", "who"),
        )
        rows.append(
            {
                "market": "dome",
                "fee": parsed["shipping_fee"],
                "shipping_type": text_or_none(shipping.get("domeFeeType")),
                **parsed,
                "payload": _shipping_row_payload(shipping, "dome", parsed),
            }
        )
    if _has_value(shipping.get("supplyFee")) or _has_value(shipping.get("supplyFeeType")):
        parsed = parse_shipping_fee(
            _first_value(shipping, "supplyFeeRaw", "supplyFee"),
            shipping.get("supplyFeeType"),
            fee_payer=_first_value(shipping, "supplyFeePayer", "feePayer", "deliWho", "who"),
        )
        rows.append(
            {
                "market": "supply",
                "fee": parsed["shipping_fee"],
                "shipping_type": text_or_none(shipping.get("supplyFeeType")),
                **parsed,
                "payload": _shipping_row_payload(shipping, "supply", parsed),
            }
        )
    if rows:
        return rows
    parsed = parse_shipping_fee(
        _first_value(shipping, "feeRaw", "fee"),
        _first_value(shipping, "type", "feeType"),
        fee_payer=_first_value(shipping, "shippingPayment", "feePayer", "deliWho", "who", "type", "feeType"),
    )
    return [
        {
            "market": platform,
            "fee": parsed["shipping_fee"],
            "shipping_type": text_or_none(_first_value(shipping, "type", "feeType")),
            **parsed,
            "payload": _shipping_row_payload(shipping, platform, parsed),
        }
    ]

def _shipping_row_payload(shipping: dict[str, Any], market: str, parsed: dict[str, Any]) -> dict[str, Any]:
    payload = dict(shipping)
    payload["market"] = market
    payload.update(parsed)
    remote_area_fee = _remote_area_fee(shipping)
    if remote_area_fee:
        payload["remote_area_fee"] = remote_area_fee
    payload["source_fields"] = _shipping_source_fields(shipping, market)
    return payload

def _remote_area_fee(shipping: dict[str, Any]) -> dict[str, Any]:
    fees: dict[str, Any] = {}
    jeju = _first_value(shipping, "feeExtraJeju")
    islands = _first_value(shipping, "feeExtraIslands")
    nested = shipping.get("remoteAreaFee") if isinstance(shipping.get("remoteAreaFee"), dict) else {}
    jeju = jeju if _has_value(jeju) else _first_value(nested, "jeju")
    islands = islands if _has_value(islands) else _first_value(nested, "islands")
    if _has_value(jeju):
        fees["jeju"] = jeju
    if _has_value(islands):
        fees["islands"] = islands
    return fees

def _shipping_source_fields(shipping: dict[str, Any], market: str) -> dict[str, Any]:
    if market == "dome":
        return {
            "fee": _first_value(shipping, "domeFeeRaw", "domeFee"),
            "type": shipping.get("domeFeeType"),
            "tbl": shipping.get("domeFeeTable"),
            "pay": _first_value(shipping, "domeFeePayer", "feePayer", "deliWho", "who"),
        }
    if market == "supply":
        return {
            "fee": _first_value(shipping, "supplyFeeRaw", "supplyFee"),
            "type": shipping.get("supplyFeeType"),
            "tbl": shipping.get("supplyFeeTable"),
            "pay": _first_value(shipping, "supplyFeePayer", "feePayer", "deliWho", "who"),
        }
    source_fields = shipping.get("sourceFields")
    if isinstance(source_fields, dict):
        return dict(source_fields)
    return {
        "fee": _first_value(shipping, "feeRaw", "fee"),
        "type": _first_value(shipping, "typeRaw", "type", "feeType"),
        "pay": _first_value(shipping, "shippingPayment", "feePayer", "deliWho", "who"),
    }

def _decimal_or_none(value: Any) -> Decimal | None:
    return parse_decimal(value)
