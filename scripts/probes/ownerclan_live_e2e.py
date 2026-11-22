"""Bounded live history-key sample -> detail -> isolated score/publication/read.

Not a complete daily history traversal or an operations/Spring deployment test.
No production DB discovery, operational checkpoint changes, or invented evidence.
"""
import argparse
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
from pathlib import Path
import time

from scripts.benchmarks import pipeline as b
from scripts.migration_preflight import query as audit_query
from scripts.probes.ownerclan_query_comparison import MeasuredSession, StopProbe, assess
from scripts.probes.ownerclan_session_probe import FrozenToken
from ownerclan_API.api.auth import JwtProvider
from ownerclan_API.api.client import OwnerclanClient
from ownerclan_API.api.queries import item_histories_query
from ownerclan_API.api.rate_limiter import RateLimiter
from ownerclan_API.config.settings import load_config, load_credentials
from ownerclan_API.services.normalization import extract_connection_items
from ownerclan_API.workflows.history_sync import millis, refresh_details
from product_data_collector.common.collection_state import next_page_cursor
from product_data_collector.domain.product_events import ownerclan_event
from product_data_collector.operations.fence import certify_collection
from product_data_collector.persistence import scoring
from product_data_collector.persistence.detail_refresh import request_refreshes
from product_data_collector.persistence.product_events import save_events

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'artifacts/experiments/ownerclan-workflow-e2e'
SCHEMA = 'ownerclan_live_sample'
LIMIT = 5000
READ_CAP = 100


class ProbeSession(MeasuredSession):
    def __init__(self, result, save):
        super().__init__(result, save, max_reads=READ_CAP, max_seconds=1200)
        self.limiter = RateLimiter(3, clock=time.perf_counter)
        self.stopped = False

    def request(self, *args, **kwargs):
        if self.stopped:
            raise StopProbe('previous_transport_error_stop')
        try:
            return super().request(*args, **kwargs)
        except Exception:
            self.stopped = True
            raise


class SampleClient:
    def __init__(self, client, session, result, save):
        self.client, self.session, self.result, self.save = client, session, result, save

    def graphql(self, query):
        api = 'itemHistories' if 'itemHistories(' in query else 'itemsByKeys'
        keys = None
        if api == 'itemsByKeys':
            import re
            keys = json.loads(re.search(r'keys: (\[.*?\])', query)[1])
        self.session.context = {'api': api, 'requested': len(keys) if keys else None,
                                'query_sha256': hashlib.sha256(query.encode()).hexdigest()}
        data = self.client.graphql(query)
        if keys:
            assessment = assess(keys, data.get('itemsByKeys'))
            self.result['calls'][-1]['assessment'] = assessment
            self.save()
            # Observe only. The real worker owns missing-key supplementation,
            # retry state, and continuation to other batches. A missing product
            # must never poison this transport for the remaining catalogue.
        return data


def run():
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / 'measurement.json'
    if path.exists():
        raise RuntimeError('Existing measurement; refuse accidental live rerun')
    b.CONTAINER = 'chozy-ownerclan-live-e2e'
    resources = b.guard()
    import subprocess
    app = json.loads(subprocess.check_output(['docker', 'inspect', 'product-data-collector-app']))[0]
    if app['Config']['Cmd'] != ['python', '-c', 'import signal; signal.pause()']:
        raise RuntimeError('Collector must be idle')
    relations = audit_query('product-data-collector-postgres', "SELECT coalesce(json_agg(t),'[]') FROM (SELECT schemaname,tablename FROM pg_tables WHERE tablename IN ('api_budget_policy','api_budget_usage')) t")
    if relations:
        raise RuntimeError('Budget tables present: review current policy before live calls')
    result = {'status': 'running', 'calls': [], 'pages': [], 'started_at': datetime.now(timezone.utc).isoformat(),
              'preflight': {'collector': 'idle', 'local_other_python_collectors': 'none observed',
                            'account_wide_usage': 'unverifiable', 'provider_remaining_budget': 'unverifiable'},
              'limits': {'products': LIMIT, 'minimum_sample_products': 4500, 'history_pages': 12, 'reads': READ_CAP, 'auth': 1,
                         'start_interval_seconds': 3, 'total_seconds': 1200, 'retry': 0, 'timeout': [5, 45]},
              'missing_key_supplemental_once': True,
              'continue_other_batches_after_missing': True,
              'scope': 'first 5000 unique keys in bounded history prefix; isolated sample only; not whole-day completion',
              'resources_before': resources}

    def save():
        pending = path.with_suffix('.pending')
        pending.write_text(json.dumps(result, ensure_ascii=False, default=str, indent=2), encoding='utf8')
        pending.replace(path)

    save()
    pipeline_start = None
    try:
        with b.connect() as c:
            c.execute('CREATE SCHEMA ' + SCHEMA)
            c.execute('SET search_path TO ' + SCHEMA)
            c.commit()
            b.init_schema(c)
            for name in ('stage7_supply_scoring', 'stage8_operations', 'stage7_history_policy_v2', 'stage10_scoring_performance'):
                b.apply_migration(c, name)
            cfg = load_config(ROOT / 'ownerclan_API/config/config.yaml', ROOT)
            cfg = replace(cfg, request=replace(cfg.request, max_retries=0, interval_seconds=3),
                          output=replace(cfg.output, state_dir=OUT / 'scratch/state', log_dir=OUT / 'scratch/logs', raw_sample_limit=0))
            with ProbeSession(result, save) as session:
                username, password = load_credentials(ROOT)
                token = JwtProvider(username, password, 'production', 45, session=session).token()
                client = SampleClient(OwnerclanClient(FrozenToken(token), 'production', RateLimiter(0), 45, 0, 300, session=session), session, result, save)
                pipeline_start = time.perf_counter()
                # Last closed KST calendar day; fixed range and opaque cursor.
                kst = timezone(timedelta(hours=9))
                end = datetime.now(kst).replace(hour=0, minute=0, second=0, microsecond=0)
                start = end - timedelta(days=1)
                result['history_range'] = {'from': start.isoformat(), 'to': end.isoformat()}
                observed = datetime.now(timezone.utc)
                selected, nodes = {}, []
                after, seen = None, set()
                history_start = time.perf_counter()
                for page in range(12):
                    b.guard()
                    t = time.perf_counter()
                    data = client.graphql(item_histories_query(first=1000, after=after, date_from=millis(start)-1, date_to=millis(end)+1))
                    rows, info = extract_connection_items(data, 'itemHistories')
                    cursor = next_page_cursor(info, after, seen)
                    if cursor and not rows:
                        raise StopProbe('empty_nonterminal_history')
                    for row in rows:
                        key = row.get('itemKey')
                        if not isinstance(key, str) or not key.strip():
                            raise StopProbe('invalid_history_key')
                        if key in selected or len(selected) < LIMIT:
                            selected.setdefault(key, None)
                            nodes.append({k: v for k, v in row.items() if k != '_cursor'})
                    result['pages'].append({'page': page+1, 'signals': len(rows), 'sample_unique': len(selected),
                                            'has_next': cursor is not None, 'wall_seconds': time.perf_counter()-t})
                    save()
                    if len(selected) == LIMIT or cursor is None:
                        break
                    seen.add(cursor)
                    after = cursor
                result['history_seconds'] = time.perf_counter()-history_start
                result['history_traversal_complete'] = cursor is None
                result['sample_keys'] = len(selected)
                result['sample_key_sha256'] = hashlib.sha256('\n'.join(selected).encode()).hexdigest()
                if len(selected) < 4500:
                    raise StopProbe('insufficient_unique_keys_within_history_budget')
                sample_size = len(selected)
                scope = {'kind': 'diagnostic_explicit_key_sample', 'size': sample_size, 'sha256': result['sample_key_sha256'],
                         'whole_day_history_complete': False}
                t = time.perf_counter()
                events = [e for raw in nodes if (e := ownerclan_event(raw, observed)) is not None]
                result['event_save'] = save_events(c, events, scope=scope)
                request_refreshes(c, 'ownerclan', selected, observed_at=observed, scope=scope)
                result['event_request_save_seconds'] = time.perf_counter()-t
                # Real worker: claims/versioned per-product commit and pending preservation.
                t = time.perf_counter()
                result['detail'] = refresh_details(ROOT, cfg, client=client, connection=c, limit=sample_size)
                result['detail_and_storage_seconds'] = time.perf_counter()-t
                result['saved_products'] = c.execute('SELECT count(*) AS n FROM products').fetchone()['n']
                c.commit()
                if not result['detail']['detailRefreshComplete']:
                    try:
                        scoring.run_scoring(c, run_id='blocked-sample', mode='initial', platform='ownerclan', as_of=datetime.now(timezone.utc))
                    except RuntimeError as exc:
                        result['incomplete_scoring_blocked'] = 'Detail refresh incomplete' in str(exc)
                    assert result.get('incomplete_scoring_blocked')
                    raise StopProbe('sample_detail_incomplete')
                assert result['saved_products'] == sample_size
                # Completion is for this isolated explicit-key sample only. No
                # daily checkpoint/90d coverage/supplier identity is fabricated.
                certify_collection(c, 'ownerclan', 'diagnostic-explicit-key-sample')
                as_of = datetime.now(timezone.utc)
                original_publish = scoring.publish
                def timed_publish(conn, state):
                    t = time.perf_counter()
                    original_publish(conn, state)
                    result['publish_seconds_excluding_final_commit'] = time.perf_counter()-t
                scoring.publish = timed_publish
                t = time.perf_counter()
                try:
                    state = scoring.run_scoring(c, run_id='live-sample-initial', mode='initial', platform='ownerclan', as_of=as_of, chunk_size=100,
                                                stop_requested=lambda: time.perf_counter()-pipeline_start>1200)
                finally:
                    scoring.publish = original_publish
                result['score_and_publish_seconds'] = time.perf_counter()-t
                assert state['status'] == 'completed'
                result['processed'] = state['processed']
                result['scored'] = c.execute("SELECT count(*) AS n,count(*) FILTER(WHERE reliability IS NULL) AS null_reliability FROM product_supply_scores").fetchone()
                result['batch'] = c.execute('SELECT batch_id,item_count FROM supply_home_published').fetchone()
                result['rank_payload_differences'] = c.execute("""WITH expected AS (SELECT product_id,payload,row_number() OVER(ORDER BY home DESC,product_id) AS rank FROM product_supply_scores WHERE platform='ownerclan' AND home IS NOT NULL AND (payload->>'eligible')::boolean AND as_of<=%s AND formula_version IN ('supply_v1','supply_v2'))
                  SELECT count(*) AS n FROM expected e FULL JOIN supply_home_items i USING(product_id)
                  WHERE e.rank IS DISTINCT FROM i.rank OR e.payload IS DISTINCT FROM i.payload""", (as_of,)).fetchone()['n']
                assert result['rank_payload_differences'] == 0
                t = time.perf_counter()
                home = c.execute("SELECT read_supply_home('OWNERCLAN',10) AS home").fetchone()['home']
                result['home_read_seconds'] = time.perf_counter()-t
                result['home_items'] = len(home['items'])
                result['evidence_rows'] = c.execute('SELECT (SELECT count(*) FROM scoring_coverage) AS coverage,(SELECT count(*) FROM scoring_supplier_identity) AS supplier_identity').fetchone()
                assert result['evidence_rows'] == {'coverage': 0, 'supplier_identity': 0}
                result['database_bytes'] = c.execute('SELECT pg_database_size(current_database()) AS n').fetchone()['n']
                c.commit()
                result['status'] = 'complete'
    except Exception as exc:
        result.update(status='stopped', stop_reason=str(exc) if isinstance(exc, StopProbe) else type(exc).__name__)
    finally:
        result['finished_at'] = datetime.now(timezone.utc).isoformat()
        result['pipeline_wall_seconds'] = time.perf_counter()-pipeline_start if pipeline_start else None
        result['counts'] = {'auth': sum(x['api']=='auth' for x in result['calls']),
                            'reads': sum(x['api']!='auth' for x in result['calls'])}
        save()
    print(json.dumps({k: v for k, v in result.items() if k not in ('calls','pages')}, ensure_ascii=True, default=str), flush=True)
    return 0 if result['status']=='complete' else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--bounded-recovery', action='store_true', help='Compatibility option; the real worker always owns recovery and continuation')
    args = parser.parse_args()
    if not args.execute:
        print(json.dumps({'execute': False, 'sample_products': [4500, LIMIT], 'read_cap': READ_CAP, 'auth_cap': 1,
                          'missing_key_supplemental_once': True,
                          'continue_other_batches_after_missing': True,
                          'max_seconds': 1200, 'production_writes': False, 'history_scope': 'bounded prefix, not whole day'}))
        return 0
    logging.disable(logging.CRITICAL)
    return run()


if __name__ == '__main__':
    raise SystemExit(main())
