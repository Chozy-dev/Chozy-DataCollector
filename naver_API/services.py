"""Pure parsing; relative indices never cross request boundaries."""
from datetime import timedelta
import math
import re


def compact(value):
    return re.sub(r'\s+', '', value)


def count(value):
    if isinstance(value, str):
        value = compact(value)
        if value == '<10':
            return None
        if value.isascii() and value.isdigit():
            return int(value)
    if type(value) is int and value >= 0:
        return value
    raise ValueError('Invalid monthly count')


def select_keywords(payload):
    if not isinstance(payload, dict) or not isinstance(payload.get('keywordList'), list):
        raise ValueError('Invalid keyword response')
    selected = []
    seen = set()
    for item in payload['keywordList']:
        word = item['relKeyword']
        if not isinstance(word, str) or not compact(word) or word in seen:
            raise ValueError('Invalid or duplicate keyword')
        seen.add(word)
        pc = count(item['monthlyPcQcCnt'])
        mobile = count(item['monthlyMobileQcCnt'])
        if pc is not None and mobile is not None:
            selected.append({'keyword': word, 'pc': pc, 'mobile': mobile, 'total': pc + mobile})
    selected.sort(key=lambda r: (-r['total'], r['keyword']))
    if len(selected) < 10:
        raise ValueError('Fewer than ten exact-count keywords; refuse incomplete theme')
    return [dict(r, rank=i + 1) for i, r in enumerate(selected[:10])]


def trend_body(words, start, end):
    return {'startDate': str(start), 'endDate': str(end), 'timeUnit': 'date',
            'keywordGroups': [{'groupName': w, 'keywords': [w]} for w in words]}


def growth_results(payload, words, start, end):
    if not isinstance(payload, dict) or not isinstance(payload.get('results'), list):
        raise ValueError('Invalid trend response')
    if any(payload.get(k) != v for k, v in [('startDate', str(start)), ('endDate', str(end)), ('timeUnit', 'date')]):
        raise ValueError('Trend period mismatch')
    groups = {}
    for result in payload['results']:
        title = result['title']
        if title not in words or title in groups or result.get('keywords') != [title]:
            raise ValueError('Trend group mismatch')
        groups[title] = result
    expected = [str(start + timedelta(days=i)) for i in range(56)]
    if end != start + timedelta(days=55):
        raise ValueError('Expected 56 days')
    output = {}
    for word in words:
        if word not in groups:
            output[word] = (None, 'missing_group')
            continue
        points = groups[word].get('data')
        if not isinstance(points, list):
            raise ValueError('Invalid trend data')
        values = {}
        invalid = False
        for point in points:
            period, ratio = point.get('period'), point.get('ratio')
            if period in values or period not in expected or type(ratio) not in (int, float) or not math.isfinite(ratio) or not 0 <= ratio <= 100:
                invalid = True
                break
            values[period] = ratio
        if invalid or set(values) != set(expected):
            output[word] = (None, 'invalid_dates_or_values')
            continue
        old = sum(values[d] for d in expected[:28]) / 28
        new = sum(values[d] for d in expected[28:]) / 28
        output[word] = ((new / old - 1) * 100, 'available') if old else (None, 'zero_baseline')
    return output
