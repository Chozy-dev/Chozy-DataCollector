"""Supply scoring v2: Decimal arithmetic, missing is never zero.

Keep unrounded component values until the final displayed integer (ROUND_HALF_UP).
History uses a certified stream per metric; source and observations are never added.
"""
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

from .current_state import number

VERSION = "supply_v2"
KST = ZoneInfo("Asia/Seoul")


def rounded(value):
    return None if value is None else int(max(Decimal(0), min(Decimal(100), value)).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def weighted(items):
    valid = [(Decimal(str(v)), Decimal(w)) for v, w in items if v is not None]
    return sum((v*w for v, w in valid), Decimal(0))/sum(w for _, w in valid) if valid else None


def stock_score(quantity):
    q = number(quantity)
    if q is None:
        return None
    return next(score for minimum, score in [(1000, 100), (300, 80), (100, 60), (30, 40), (1, 20), (0, 0)] if q >= minimum)


def shipping_score(platform, days, fast=False):
    d = number(days)
    if d is None or (platform == "ownerclan" and d < 1):
        return None
    if platform == "domeggook":
        return min(100, (100 if d <= Decimal('.5') else 85 if d <= 1 else 60 if d <= 2 else 30) + (5 if fast is True else 0))
    return 100 if d <= 1 else 75 if d <= 2 else 50 if d <= 3 else 25


def dome_reliability(satisfaction, reviews, excellent=False, seller_type=None):
    s = number(str(satisfaction).strip().removesuffix('%')) if satisfaction is not None else None
    n = number(reviews)
    if s is None or s > 100 or n is None:
        return None
    w = min(n/50, Decimal(1))
    result = s*w + 70*(1-w)
    result += 5 if excellent is True else 0
    result -= 10 if seller_type in {'간이과세자', '개인판매자'} else 0
    return max(Decimal(0),min(Decimal(100),result))


def owner_reliability(total, soldout):
    if not total:
        return None
    if not 0 <= soldout <= total <= 1000:
        raise ValueError("Invalid supplier sample")
    rate = Decimal(soldout)/total
    base = next(score for ceiling, score in [('.05', 100), ('.15', 80), ('.30', 55), ('.50', 30), ('1', 10)] if rate <= Decimal(ceiling))
    w = min(Decimal(total)/20, Decimal(1))
    return base*w + 70*(1-w)


def vacation_bounds(product):
    vacation = (product.get('source_specific') or {}).get('seller', {}).get('vacation') or {}
    try:
        start = datetime.fromisoformat(vacation['start']).replace(tzinfo=KST)
        # Date-only end includes the last calendar day.
        end = datetime.fromisoformat(vacation['end']).replace(tzinfo=KST)
        if len(vacation['end']) == 10:
            end += timedelta(days=1)
        return (start, end) if start < end else (None, None)
    except (ValueError, TypeError, KeyError):
        return None, None


def calculate(product, as_of, history, supplier=None):
    platform = product['platform']
    days = product.get('average_shipping_days') if platform == 'domeggook' else (product.get('source_specific') or {}).get('guaranteedShippingPeriod')
    components = {'stock': stock_score(product.get('stock_quantity')),
                  'shipping': shipping_score(platform, days, product.get('fast_shipping')),
                  'stockout': None, 'price': None}
    if history['stockout_count'] is not None:
        components['stockout'] = [100, 70, 40, 10][min(history['stockout_count'], 3)]
    if history['price_count'] is not None:
        components['price'] = [100, 80, 50, 20][min(history['price_count'], 3)]
    stability = weighted([(components[k], w) for k, w in [('stock', 20), ('shipping', 25), ('stockout', 35), ('price', 20)]])
    start, end = vacation_bounds(product) if platform == 'domeggook' else (None, None)
    on_vacation = start is not None and start <= as_of < end
    if on_vacation and stability is not None:
        stability = min(stability, Decimal(50))
    reliability = dome_reliability(product.get('seller_average_satisfaction'), product.get('seller_review_count'), product.get('seller_excellent_seller'), product.get('seller_type')) if platform == 'domeggook' else (supplier or {}).get('score')
    home = weighted([(stability, 30), (reliability, 25)])
    reasons = dict(history['reasons'])
    for key in ('stock', 'shipping'):
        if components[key] is None:
            reasons[key] = 'missing_or_unverified_input'
    if reliability is None:
        reasons['supplier'] = (supplier or {}).get('reason', 'missing_satisfaction_or_review_count')
    due = [t for t in (start, end, history.get('next_due'), (supplier or {}).get('next_due')) if t is not None and t > as_of]
    return {'stability': rounded(stability), 'reliability': rounded(reliability), 'home': rounded(home),
            'components': components, 'reasons': reasons, 'history': history, 'supplier': supplier,
            'shipping_days': number(days), 'stock_is_infinite': product.get('stock_is_infinite'),
            'on_vacation': on_vacation, 'next_due': min(due) if due else None,
            'unrounded': {'stability': stability, 'reliability': reliability, 'home': home}}
