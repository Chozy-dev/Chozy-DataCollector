"""Best's $.data array is intentionally separate from search's productData."""
from decimal import Decimal, InvalidOperation

from coupang_API.api.client import CoupangResponseError


def positive_int(value):
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError("Expected positive integer")
    number = int(value)
    if number <= 0:
        raise ValueError("Expected positive integer")
    return number


def parse_best_records(payload, category_id, limit=100):
    category_id = positive_int(category_id)
    if not isinstance(payload, dict):
        raise ValueError("Invalid best envelope")
    if str(payload.get("rCode")) != "0":
        raise CoupangResponseError(payload.get("rCode"), payload.get("rMessage"))
    data = payload.get("data")
    # Empty responses remain unresolved until the provider's empty-category policy is verified.
    if not isinstance(data, list) or not data or len(data) > limit:
        raise ValueError("Best data must be a nonempty array within requested limit")
    records, ids, ranks = [], set(), set()
    for product in data:
        if not isinstance(product, dict):
            raise ValueError("Invalid best product")
        product_id = positive_int(product.get("productId"))
        rank = positive_int(product.get("rank"))
        if product_id in ids or rank in ranks:
            raise ValueError("Duplicate best product or rank")
        ids.add(product_id)
        ranks.add(rank)
        try:
            price = Decimal(str(product.get("productPrice")))
        except InvalidOperation as exc:
            raise ValueError("Invalid best price") from exc
        if not price.is_finite() or price < 0 or price >= Decimal('1e16') or price.as_tuple().exponent < -2:
            raise ValueError("Invalid best price")
        if type(product.get("isRocket")) is not bool:
            raise ValueError("Missing boolean isRocket")
        for field in ("productName", "productUrl", "productImage"):
            if not isinstance(product.get(field), str) or not product[field].strip():
                raise ValueError(f"Missing {field}")
        records.append(dict(product_id=product_id, rank=rank, price=price,
                            product_name=product["productName"], product_url=product["productUrl"],
                            image_url=product["productImage"], is_rocket=product["isRocket"], raw=product))
    return records


def rank_movement(rank, previous_rank, baseline_available):
    if not baseline_available:
        return None, None
    if previous_rank is None:
        return None, "NEW"
    change = previous_rank - rank
    return change, "UP" if change > 0 else "DOWN" if change < 0 else "SAME"
