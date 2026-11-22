"""Read-only continuation of the 2026-11-19 survey; no database or collector state access."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime
import hashlib
import json
import logging
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
BASE = ROOT / 'artifacts/experiments/daily-change-2026-11-20'
OUT = BASE / 'owner-history-followup'


def read(p):
    return json.loads(p.read_text(encoding='utf-8'))


def write(p, value):
    tmp = p.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    for attempt in range(40):
        try:
            tmp.replace(p)
            return
        except PermissionError:
            if attempt == 39:
                raise
            time.sleep(.05)


def identity(e):
    # API has no event ID: collapse indistinguishable same-product events explicitly.
    return json.dumps(e, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def validate_page(d, after, seen, previous_time):
    edges, pi = d['edges'], d['pageInfo']
    if type(pi['hasNextPage']) is not bool:
        raise ValueError('missing_terminal_flag')
    if pi['hasNextPage'] and not edges:
        raise ValueError('empty_nonterminal_page')
    if edges:
        if pi['endCursor'] != edges[-1]['cursor'] or pi['startCursor'] != edges[0]['cursor']:
            raise ValueError('edge_page_cursor_mismatch')
        cursors = [e['cursor'] for e in edges]
        if any(not c or c == after or c in seen for c in cursors) or len(set(cursors)) != len(cursors):
            raise ValueError('repeated_cursor')
        times = [int(e['node']['createdAt']) for e in edges]
        if times[0] > previous_time or any(a < b for a, b in zip(times, times[1:])):
            raise ValueError('non_descending_history')
    if pi['hasNextPage'] and not pi.get('endCursor'):
        raise ValueError('missing_cursor')


def load_chain():
    old = read(BASE / 'result.json')
    rows = old['events']['ownerclan']['rows']
    lo, hi = [int(datetime.fromisoformat(t).timestamp()*1000) for t in old['window']]
    assert len(rows) == 59000
    assert all(lo <= int(e['createdAt']) < hi for e in rows)
    assert all(a['createdAt'] >= b['createdAt'] for a, b in zip(rows, rows[1:]))
    boundary = int(rows[-1]['createdAt'])
    after, seen, previous_time, terminal = None, set(), boundary+1, False
    pages = []
    recoveries = read(OUT/'recoveries.json') if (OUT/'recoveries.json').exists() else []
    for p in sorted(OUT.glob('page-*.json')):
        page = read(p)
        recovery = next((r for r in recoveries if r['start_page'] == len(pages)+1),None)
        upper = boundary+1
        for r in recoveries:
            if r['start_page'] <= len(pages)+1:
                upper = r['dateTo']
        if recovery:
            after, seen, previous_time = None, set(), upper
        assert not terminal and page['after'] == after
        assert page['dateFrom'] == lo-1 and page['dateTo'] == upper
        validate_page(page['data'], after, seen, previous_time)
        pages.append(page)
        edges = page['data']['edges']
        if edges:
            previous_time = int(edges[-1]['node']['createdAt'])
            seen.update(e['cursor'] for e in edges)
        after = page['data']['pageInfo']['endCursor']
        terminal = page['data']['pageInfo']['hasNextPage'] is False
    return old, rows, lo, hi, boundary, pages, after, seen, previous_time, terminal


def report():
    from scripts.probes.daily_change_report import classify
    old, rows, lo, hi, boundary, pages, after, seen, previous_time, terminal = load_chain()
    tail = [e['node'] for p in pages for e in p['data']['edges']]
    expected = Counter(identity(e) for e in rows if int(e['createdAt']) == boundary)
    actual = Counter(identity(e) for e in tail if int(e['createdAt']) == boundary)
    overlap = bool(pages) and all(actual[k] >= n for k, n in expected.items())
    # Crossing below the boundary ensures its entire tie group was traversed.
    overlap_closed = overlap and (terminal or any(int(e['createdAt']) < boundary for e in tail))
    recovery_checks = []
    recoveries = read(OUT/'recoveries.json') if (OUT/'recoveries.json').exists() else []
    for recovery in recoveries:
        index = recovery['start_page']-1
        before = rows+[e['node'] for p in pages[:index] for e in p['data']['edges']]
        following = [e['node'] for p in pages[index:] for e in p['data']['edges']]
        expected_recovery = {identity(e) for e in before if recovery['boundary'] <= int(e['createdAt']) < recovery['dateTo']}
        captured = {identity(e) for e in following}
        verified = bool(expected_recovery) and expected_recovery <= captured and (terminal or
            any(int(e['createdAt']) < recovery['boundary'] for e in following))
        recovery_checks.append(dict(start_page=recovery['start_page'],expected_unique_events=len(expected_recovery),verified=verified))
    original_counts = Counter(identity(e) for e in rows)
    tail_counts = Counter(identity(e) for e in tail if lo <= int(e['createdAt']) < hi)
    # Preserve same-page multiplicity, but do not count overlap retries as new raw events.
    raw_union = original_counts.copy()
    for page in pages:
        raw_union |= Counter(identity(e['node']) for e in page['data']['edges'] if lo <= int(e['node']['createdAt']) < hi)
    merged = {identity(e): e for e in rows}
    for e in tail:
        if lo <= int(e['createdAt']) < hi:
            merged[identity(e)] = e
    mapping = defaultdict(list)
    for e in merged.values():
        mapping[e['itemKey']].append(e)
    classes = [classify('ownerclan', es) for es in mapping.values()]
    metrics = {k: sum(c[k] for c in classes) for k in ['base_price_verified', 'product_stock_signal',
        'option_signal', 'shipping_signal', 'other_signal', 'score_possible']}
    calls = [c for p in sorted(OUT.glob('calls-*.json')) for c in read(p)]
    result = dict(window=old['window'], complete=terminal and overlap_closed and all(r['verified'] for r in recovery_checks),
        recovery_checks=recovery_checks,
        api_terminal=terminal, boundary_overlap_verified=overlap_closed, boundary=boundary,
        original_cursor_available=False, recovery='timestamp overlap; original dateFrom; dateTo=boundary+1ms',
        original_raw_events=len(rows), fetched_raw_events=len(tail), unique_events=len(merged),
        max_observed_multiplicity_event_rows=sum(raw_union.values()),
        cross_run_overlap_rows=sum((original_counts & tail_counts).values()),
        indistinguishable_duplicate_rows=sum(raw_union.values())-len(merged),
        duplicate_observations=len(rows)+sum(lo <= int(e['createdAt']) < hi for e in tail)-len(merged),
        unique_changed_products=len(mapping), metrics=metrics,
        supplier_propagation_trigger_products=metrics['product_stock_signal'],
        final_supplier_peer_recalculation_products=None, daily_change_rate=None, denominator=None,
        pages=len(pages), last_success_cursor=after,
        next_after=None if terminal else after, remaining_date_range=None if terminal else [lo, previous_time],
        calls=len(calls), average_response_seconds=sum(c['seconds'] for c in calls)/len(calls) if calls else None,
        history_calls=sum(c['api']=='itemHistories' for c in calls),
        errors=dict(Counter(c.get('error') for c in calls if c.get('error'))),
        kind_events=dict(Counter(e['kind'] for e in merged.values())),
        limitations=['API terminal plus timestamp overlap, not provider snapshot/late-arrival completeness',
            'No event ID; identical full-node events collapsed; raw pages and original data retained',
            'No verified current full-catalog denominator, vendorKey, 90-day coverage or peer membership'])
    write(OUT/'analysis.json', result)
    write(OUT/'checkpoint.json', {k: result[k] for k in ['complete','api_terminal','last_success_cursor',
        'next_after','boundary','boundary_overlap_verified','remaining_date_range']})
    return result


def execute():
    import requests
    from ownerclan_API.api.auth import JwtProvider
    from ownerclan_API.api.client import OwnerclanClient, OwnerclanApiError, OwnerclanGraphQLError
    from ownerclan_API.api.rate_limiter import RateLimiter
    from ownerclan_API.api.queries import item_histories_query
    from ownerclan_API.config.settings import load_credentials
    started = time.monotonic()
    previous_starts = [datetime.strptime(p.stem.removeprefix('calls-'),'%Y%m%d-%H%M%S') for p in OUT.glob('calls-*.json')]
    prior_seconds = (datetime.now()-min(previous_starts)).total_seconds() if previous_starts else 0
    runid = datetime.now().strftime('%Y%m%d-%H%M%S')
    callpath = OUT/f'calls-{runid}.json'
    calls = []
    pace = RateLimiter(.61)

    class Stop(Exception):
        pass

    def guard():
        # Leave room for connect+read timeouts and disk persistence before 30 minutes.
        if prior_seconds+time.monotonic()-started > 1740 or len(calls) >= 2800:
            raise Stop('budget')

    class Session(requests.Session):
        def request(self, method, url, **kwargs):
            guard()
            pace.wait()
            guard()
            c = dict(api='itemHistories' if 'graphql' in url else 'auth', offset=time.monotonic()-started)
            calls.append(c)
            write(callpath, calls)
            t = time.monotonic()
            kwargs.update(timeout=(5, 20), allow_redirects=False)
            try:
                response = super().request(method, url, **kwargs)
                c['http'] = response.status_code
                if response.status_code in (401,403,429):
                    c['error'] = 'access_or_rate_stop'
                    raise Stop('access_or_rate_stop')
                return response
            except Exception as e:
                c.setdefault('error', type(e).__name__)
                raise
            finally:
                c['seconds'] = round(time.monotonic()-t,4)
                write(callpath, calls)

    status = 'stopped'
    try:
        old, rows, lo, hi, boundary, pages, after, seen, previous_time, terminal = load_chain()
        recoveries = read(OUT/'recoveries.json') if (OUT/'recoveries.json').exists() else []
        upper = recoveries[-1]['dateTo'] if recoveries else boundary+1
        if recoveries and recoveries[-1]['start_page'] == len(pages)+1:
            after, seen, previous_time = None, set(), upper
        if terminal:
            status = 'already_terminal'
            return
        username, password = load_credentials(ROOT)
        with Session() as session:
            provider = JwtProvider(username,password,'production',20,session=session)
            client = OwnerclanClient(provider,'production',RateLimiter(.61),20,0,90,session=session)
            while not terminal:
                guard()
                query = item_histories_query(first=1000,after=after,date_from=lo-1,date_to=upper)
                for attempt in range(3):
                    try:
                        d = client.graphql(query)['itemHistories']
                        break
                    except OwnerclanGraphQLError:
                        raise  # Includes GraphQL rate limits and invalid/expired cursors: stop safely.
                    except OwnerclanApiError as e:
                        if attempt == 2 or not isinstance(e.__cause__, (requests.Timeout, requests.ConnectionError)):
                            raise
                        delay = 2 if attempt == 0 else 90
                        if prior_seconds+time.monotonic()-started+delay > 1740:
                            raise Stop('retry_budget')
                        time.sleep(delay)
                try:
                    validate_page(d, after, seen, previous_time)
                except ValueError as e:
                    write(OUT/f'rejected-{runid}.json',dict(after=after,dateFrom=lo-1,dateTo=upper,
                        validation_error=str(e),data=d))
                    raise
                page = dict(after=after,dateFrom=lo-1,dateTo=upper,data=d)
                write(OUT/f'page-{len(pages)+1:05d}.json',page)
                pages.append(page)
                edges = d['edges']
                if edges:
                    previous_time = int(edges[-1]['node']['createdAt'])
                    seen.update(e['cursor'] for e in edges)
                after = d['pageInfo']['endCursor']
                terminal = d['pageInfo']['hasNextPage'] is False
                # Fail before moving further if the first recovery page cannot reconnect.
                if len(pages) == 1 and not report()['boundary_overlap_verified']:
                    raise Stop('boundary_overlap_unverified')
                report()
                print(json.dumps(dict(pages=len(pages),tail_rows=sum(len(p['data']['edges']) for p in pages),
                    seconds=round(time.monotonic()-started,1),terminal=terminal)),flush=True)
            status = 'terminal'
    except Exception as e:
        status = type(e).__name__
        # Never persist exception text or request URL/headers/credentials.
    finally:
        write(OUT/f'run-{runid}.json',dict(status=status,elapsed_seconds=round(time.monotonic()-started,3),
            calls=len(calls),limits=dict(seconds=1800,new_request_cutoff=1740,rpm_max=100,timeout=[5,20],retries=2)))
        print(json.dumps(report(),ensure_ascii=False),flush=True)


def export_products():
    """Offline, reviewable product-level deduplicated classification evidence."""
    import csv
    from scripts.probes.daily_change_report import classify
    old, rows, lo, hi, boundary, pages, *_ = load_chain()
    merged = {identity(e): e for e in rows}
    for page in pages:
        for edge in page['data']['edges']:
            e = edge['node']
            if lo <= int(e['createdAt']) < hi:
                merged[identity(e)] = e
    mapping = defaultdict(list)
    for e in merged.values():
        mapping[e['itemKey']].append(e)
    with (OUT/'products.csv').open('w',encoding='utf-8-sig',newline='') as f:
        writer = None
        for key, events in sorted(mapping.items()):
            record = dict(itemKey=key,unique_events=len(events),**classify('ownerclan',events))
            record['kinds'] = json.dumps(record['kinds'])
            if writer is None:
                writer = csv.DictWriter(f,fieldnames=list(record))
                writer.writeheader()
            writer.writerow(record)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir',type=Path,help='Restored full survey directory containing result.json and owner-history-followup/')
    parser.add_argument('--execute',action='store_true')
    parser.add_argument('--export',action='store_true',help='Offline product classification CSV')
    args = parser.parse_args()
    if args.input_dir:
        BASE=args.input_dir.resolve()
        OUT=BASE/'owner-history-followup'
    if not (BASE/'result.json').is_file():
        parser.error('Full survey inputs were archived; restore them and pass --input-dir')
    if args.execute and args.export:
        parser.error('--export is offline only')
    OUT.mkdir(exist_ok=True)
    logging.disable(logging.CRITICAL)
    if args.execute:
        execute()
    else:
        print(json.dumps(report(),ensure_ascii=False))
        if args.export:
            export_products()
