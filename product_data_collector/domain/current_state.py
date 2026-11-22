"""Current product projections. Raw prices are evidence, not necessarily usable costs."""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal

from product_data_collector.common.numeric_utils import parse_decimal


CURRENT_COLUMNS = {
    "current_supply_price": "NUMERIC",
    "supply_price_basis": "TEXT",
    "supply_price_status": "TEXT",
    "price_info": "JSONB",
    "stock_quantity": "NUMERIC",
    "stock_is_infinite": "BOOLEAN",
    "stock_basis": "TEXT",
    "options_complete": "BOOLEAN",
    "supply_unit": "NUMERIC",
    "shipping_fee": "NUMERIC",
    "shipping_type": "TEXT",
    "shipping_payment": "TEXT",
    "shipping_method": "TEXT",
    "shipping_info": "JSONB",
    "average_shipping_days": "NUMERIC",
    "fast_shipping": "BOOLEAN",
    "sellable": "BOOLEAN",
    "current_observed_at": "TIMESTAMPTZ",
}


def number(value):
    value = parse_decimal(value)
    return value if value is not None and value.is_finite() and value >= 0 else None


def flag(value):
    if isinstance(value, bool):
        return value
    if value is not None:
        text = str(value).lower().strip()
        if text in {"1", "true", "y", "yes"}:
            return True
        if text in {"0", "false", "n", "no"}:
            return False
    return None


def canonical_json(value):
    """Preserve string identity and list ordering; never coerce option IDs to numbers."""
    def default(obj):
        if isinstance(obj, Decimal):
            return str(obj.normalize())
        raise TypeError(type(obj).__name__)
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=default)


def digest(value):
    return hashlib.sha256(canonical_json(value).encode("utf8")).hexdigest()


def minimum_quantity_price(raw):
    scalar = number(raw)
    if scalar is not None:
        return scalar
    if not isinstance(raw, str):
        return None
    tiers = []
    for part in raw.split("|"):
        pair = part.split("+")
        if len(pair) != 2:
            return None
        quantity, price = map(number, pair)
        if quantity is None or quantity <= 0 or price is None:
            return None
        tiers.append((quantity, price))
    if len({qty for qty, _ in tiers}) != len(tiers):
        return None
    return min(tiers)[1] if tiers else None


def known_payload(value):
    """Drop missing placeholders, preserving real zero and false values."""
    if value is None or value == "" or value == {"__value__": "__MISSING__"}:
        return None
    if isinstance(value, dict):
        return {key: cleaned for key, child in value.items()
                if (cleaned := known_payload(child)) not in (None, {}, [])}
    if isinstance(value, list):
        return [cleaned for child in value if (cleaned := known_payload(child)) is not None]
    return value


def domeggook_option_status(option, qty):
    hidden = number(option.get("hiddenStatus"))
    supply = flag(option.get("supplyOnSale"))
    if hidden == 1:
        return "discontinued", "hid"
    if hidden == 2:
        return "hidden", "hid"
    if supply is False:
        return "unavailable", "sup"
    if qty == 0:
        return "soldout", "quantity"
    if hidden == 0 and supply is True and qty is not None and qty > 0:
        return "available", "hid_sup_quantity"
    return None, "unknown"


def option_states(platform, options, prices=None):
    result = {}
    for option in options:
        if not isinstance(option, dict):
            raise ValueError("Option must be an object")
        raw_id = option.get("skuKey")
        raw_id = str(raw_id) if raw_id not in (None, "") else None
        raw = option.get("sourceOption", option)
        # The fingerprint is explicitly a local observation identity, not a native ID.
        identity = (("native:" if platform == "ownerclan" else "reference:") + raw_id
                    if raw_id is not None else "unresolved:" + digest(raw))
        attrs = option.get("optionAttributes") or []
        qty = number(option.get("quantity"))
        owner = platform == "ownerclan"
        price = number(option.get("price")) if owner else None
        if platform == "domeggook":
            prices = prices or {}
            base = number(prices.get("supplyCurrentSupplyPrice"))
            extra = number(option.get("supplyPrice"))
            # Only the measured fixed-price combination contract is verified.
            # Tiered/discounted bases and negative adjustments remain unresolved.
            if (option.get("skuType") == "combination" and base is not None
                    and extra is not None and prices.get("supplyOriginalSupplyPrice") is None):
                price = base + extra
            status, status_basis = domeggook_option_status(option, qty)
        else:
            status = option.get("status") or ("soldout" if qty == 0 else "available" if qty is not None else None)
            status_basis = "source" if option.get("status") else "quantity" if qty is not None else "unknown"
        state = {
            "option_identity": identity,
            "source_option_id": raw_id,
            "identity_status": "source_key" if owner and raw_id is not None else "unverified",
            "option_name": option.get("name"),
            "attributes": attrs,
            "price": price,
            "price_meaning": "final" if owner or price is not None else "unknown",
            "price_confidence": "source_contract" if price is not None else "unresolved",
            "stock_quantity": qty,
            "stock_is_infinite": None if qty is None else qty == 9999,
            "status": status,
            "status_basis": status_basis,
            "raw_option": raw,
        }
        if identity in result and result[identity] != state:
            raise ValueError("Conflicting options with the same source reference")
        result[identity] = state
    return list(result.values())


def project_current(row):
    platform = row["platform"]
    prices = row["prices_payload"]
    inventory = row["inventory_payload"]
    shipping = row["shipping_payload"]
    specific = row["source_specific"]
    options = option_states(platform, row.get("options_payload", []), prices)
    complete = row.get("options_complete") is True
    reliable_owner_list = complete and len(options) == len(row.get("options_payload", [])) and all(
        o["identity_status"] == "source_key" for o in options)
    cost, basis, confidence = None, "unknown", "unresolved"
    if platform == "domeggook":
        cost = minimum_quantity_price(prices.get("supplyCurrentSupplyPrice"))
        basis = "minimum_quantity_supply"
        confidence = "source_contract" if cost is not None else "unresolved"
        if options:
            # Preserve the base in price_info; final option costs still require selection.
            cost, basis, confidence = None, "option_selection", "unresolved"
    elif platform == "ownerclan":
        valid = [o["price"] for o in options if o["price"] is not None]
        if options and reliable_owner_list and len(valid) == len(options) and len(set(valid)) == 1:
            cost, basis, confidence = valid[0], "uniform_option_final", "source_contract"
        elif options:
            basis = "option_selection"
            confidence = "option_required" if reliable_owner_list and len(valid) == len(options) else "unresolved"
        # A product base price alone is deliberately not promoted to an option final cost.
        else:
            basis = "base_price_only"
    stock = number(row.get("stock_quantity"))
    stock_basis = "source_quantity"
    infinite = None if stock is None else stock == 9999
    if platform == "ownerclan" and options:
        quantities = [o["stock_quantity"] for o in options]
        stock = (Decimal(9999) if Decimal(9999) in quantities else
                 sum(quantities, Decimal(0)) if all(q is not None for q in quantities) else None) if basis == "uniform_option_final" else None
        stock_basis = "option_sum" if basis == "uniform_option_final" else "option_selection_or_incomplete"
        infinite = None if stock is None else Decimal(9999) in quantities
    selected_shipping = next((s for s in row["shipping_rows"] if s["market"] == "supply"), None)
    if platform != "domeggook":
        selected_shipping = row["shipping_rows"][0]
    selected_shipping = selected_shipping or {}
    status = row.get("status")
    if platform == "ownerclan":
        permission = flag(specific.get("openmarketSellable"))
        sellable = False if permission is False or status in {"soldout", "discontinued", "unavailable", "prohibited"} else (
            True if status == "available" and permission is True else None)
    elif platform == "domeggook":
        permission = flag(row.get("markets", {}).get("supplyOnSale"))
        sellable = False if permission is False or status in {"판매종료", "기간종료", "판매중지", "판매중단", "품절", "승인거부", "아웃벌점 제재"} else (
            True if permission is True and status == "판매중" else None)
    else:
        sellable = None
    return {
        "current_supply_price": cost,
        "supply_price_basis": basis,
        "supply_price_status": confidence,
        "price_info": prices,
        "stock_quantity": stock,
        "stock_is_infinite": infinite,
        "stock_basis": stock_basis,
        "options_complete": complete,
        "supply_unit": number(inventory.get("supplyOrderUnit")),
        "shipping_fee": selected_shipping.get("fee"),
        "shipping_type": selected_shipping.get("shipping_type"),
        "shipping_payment": selected_shipping.get("shipping_payment"),
        "shipping_method": shipping.get("method"),
        "shipping_info": known_payload(shipping),
        "average_shipping_days": number(shipping.get("averageShippingDays")),
        "fast_shipping": flag(shipping.get("fastShipping")),
        "sellable": sellable,
        "current_observed_at": row["collected_at"],
    }
