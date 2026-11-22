"""Bounded scoring transactions. Dedicated connection, dict_row; no implicit DDL.

Only reviewed coverage rows enable source-stream counts. Their evidence must
certify completeness, scope and supplier membership, not just API traversal.
"""
from datetime import timedelta
import hashlib
import json

from psycopg.types.json import Jsonb

from product_data_collector.domain.scoring import VERSION, calculate, owner_reliability
from product_data_collector.domain.product_events import utc
from .postgres_storage import _json_safe

LOCK = 724639118507


def jb(value):
    return Jsonb(_json_safe(value), dumps=lambda obj: json.dumps(obj, default=lambda v: v.isoformat()))


def observed_prices(conn, product, start, as_of):
    """One authoritative observed-price stream, never added to source counts.

    Bounded cursor over changed snapshots plus the current re-observation. A
    discontinuous/unknown price scope breaks comparison, rather than fabricating
    an option/base-price delta. Coverage must explicitly certify this stream.
    """
    from .current_state import legacy_snapshot
    from product_data_collector.domain.current_state import project_current
    anchor = conn.execute('SELECT * FROM product_history WHERE product_id=%s AND observed_at<=%s '
                          'ORDER BY observed_at DESC,id DESC LIMIT 1',(product['id'],start)).fetchone()
    previous = None
    samples = changes = 0
    due = []
    last_time = None
    incomplete = anchor is None

    def consume(price, basis, at):
        nonlocal previous, samples, changes, last_time, incomplete
        if price is None or basis not in {'minimum_quantity_supply','uniform_option_final'}:
            previous = None
            incomplete = True
            return
        samples += 1
        if previous and previous[1]!=basis:
            incomplete = True
        if previous and previous[1]==basis and previous[0]!=price and at>start:
            changes += 1
            due.append(at+timedelta(days=90))
        previous = (price,basis)
        last_time = at

    if anchor:
        current=project_current(legacy_snapshot({**product,**anchor,'last_collected_at':anchor['observed_at']}))
        consume(current['current_supply_price'],current['supply_price_basis'],anchor['observed_at'])
    at, last_id = start, 0
    while True:
        rows=conn.execute('SELECT * FROM product_history WHERE product_id=%s AND observed_at>%s AND observed_at<=%s '
                          'AND (observed_at,id)>(%s,%s) ORDER BY observed_at,id LIMIT 500',
                          (product['id'],start,as_of,at,last_id)).fetchall()
        if not rows: break
        for row in rows:
            current=project_current(legacy_snapshot({**product,**row,'last_collected_at':row['observed_at']}))
            consume(current['current_supply_price'],current['supply_price_basis'],row['observed_at'])
        at,last_id=rows[-1]['observed_at'],rows[-1]['id']
    observed=product.get('current_observed_at')
    if observed and observed<=as_of and (last_time is None or observed>last_time):
        consume(product.get('current_supply_price'),product.get('supply_price_basis'),observed)
    return changes if samples>=2 and not incomplete else None, min(due) if due else None, samples==1 and not incomplete


def stockout_intervals(conn, product, start, as_of, certificate):
    """Certified source stream only; [soldout, restocked) overlaps (start, as_of].

    Keep only the last pre-window state and stream the window in bounded pages.
    Same-state notifications collapse; conflicting simultaneous states are unknown.
    A reviewed initial state at certificate.valid_from may replace a missing anchor.
    """
    args = (product['platform'], product['external_product_id'])
    predicate = "platform=%s AND external_product_id=%s AND event_origin='source' AND event_scope='product' AND semantic IN ('soldout','restocked')"

    def unknown(reason):
        # NULL can become calculable when a definite state becomes the boundary
        # anchor, after the last ambiguous timestamp has left the window.
        recovery = conn.execute('WITH states AS (SELECT occurred_at,count(DISTINCT semantic) AS n '
            'FROM product_events WHERE '+predicate+' AND occurred_at>%s AND occurred_at<=%s GROUP BY occurred_at) '
            'SELECT min(occurred_at)+interval \'90 days\' AS due FROM states WHERE n=1 AND occurred_at>'
            'COALESCE((SELECT max(occurred_at) FROM states WHERE n>1),%s)', args+(start,as_of,start)).fetchone()['due']
        return None, recovery, reason

    anchor = conn.execute('SELECT max(occurred_at) AS at FROM product_events WHERE '+predicate+' AND occurred_at<=%s', args+(start,)).fetchone()['at']
    state = certificate['evidence'].get('stockout_state_at_valid_from')
    state = {'available': 'restocked', 'soldout': 'soldout'}.get(state)
    if anchor is not None and (state is None or anchor>=certificate['valid_from']):
        states = conn.execute('SELECT DISTINCT semantic FROM product_events WHERE '+predicate+' AND occurred_at=%s', args+(anchor,)).fetchall()
        if len(states)!=1:
            return unknown('ambiguous_stockout_boundary')
        state = states[0]['semantic']
    if state is None:
        return unknown('unknown_stockout_boundary')
    count = int(state=='soldout')
    due = []
    at = start
    while True:
        rows = conn.execute('SELECT occurred_at,array_agg(DISTINCT semantic) AS states FROM product_events WHERE '+predicate+
            ' AND occurred_at>%s AND occurred_at<=%s GROUP BY occurred_at ORDER BY occurred_at LIMIT 500', args+(at,as_of)).fetchall()
        if not rows: break
        for row in rows:
            if len(row['states'])!=1:
                return unknown('ambiguous_stockout_sequence')
            new = row['states'][0]
            if new!=state:
                if new=='soldout': count += 1
                else: due.append(row['occurred_at']+timedelta(days=90))
                state = new
        at = rows[-1]['occurred_at']
    return count, min(due) if due else None, None


def event_counts(conn, product, start, as_of):
    return conn.execute("""SELECT
        count(DISTINCT occurred_at) FILTER (WHERE semantic='supply_price'
          AND value_status='source_parsed' AND verified_before IS NOT NULL
          AND verified_after IS NOT NULL AND verified_before<>verified_after) AS prices,
        count(*) FILTER (WHERE semantic='supply_price' AND value_status='source_parsed'
          AND verified_before IS NOT NULL AND verified_after IS NOT NULL) AS price_pairs,
        min(occurred_at + interval '90 days') FILTER (WHERE semantic='supply_price'
          AND value_status='source_parsed' AND verified_before IS NOT NULL AND verified_after IS NOT NULL) AS expiry
        FROM product_events WHERE platform=%s AND external_product_id=%s
        AND event_origin='source' AND event_scope='product' AND occurred_at>%s AND occurred_at<=%s""",
        (product['platform'], product['external_product_id'], start, as_of)).fetchone()


def history_metrics(conn, product, as_of, prepared=None):
    start = as_of - timedelta(days=90)
    certificates = prepared['coverage'].get(product['id'], []) if prepared is not None else conn.execute('SELECT * FROM scoring_coverage WHERE product_id=%s AND verified', (product['id'],)).fetchall()
    coverage = {r['metric']: r for r in certificates if r['valid_from']<=start and r['valid_until']>as_of}
    # Separate source supply-price semantics from base/option prices and later
    # snapshot verifications. GROUP BY occurrence time collapses replay aliases.
    counts = prepared['counts'][product['id']] if prepared is not None else event_counts(conn, product, start, as_of)
    reasons = {}
    stockouts, stockout_due, stockout_reason = (None, None, 'unverified_90d_coverage')
    if 'stockout' in coverage:
        stockouts, stockout_due, stockout_reason = stockout_intervals(conn, product, start, as_of, coverage['stockout'])
    prices = counts['prices'] if 'price' in coverage and counts['price_pairs'] >= 1 else None
    price_stream = coverage.get('price',{}).get('evidence',{}).get('stream','source_supply_v1')
    observed_due = None
    waiting_for_price_observation = False
    if 'price' in coverage and price_stream=='observed_supply_v1':
        prices,observed_due,waiting_for_price_observation = observed_prices(conn,product,start,as_of)
    elif price_stream!='source_supply_v1':
        prices = None
    if stockouts is None:
        reasons['stockout'] = stockout_reason
    if prices is None:
        reasons['price'] = 'unverified_90d_coverage' if 'price' not in coverage else 'fewer_than_two_verified_prices'
    due = [t for r in certificates for t in (r['valid_from']+timedelta(days=90),r['valid_until']) if t>as_of]
    if counts['expiry']:
        due.append(counts['expiry'])
    if observed_due:
        due.append(observed_due)
    if stockout_due:
        due.append(stockout_due)
    return {'stockout_count': stockouts, 'price_count': prices, 'reasons': reasons,
            'window_start': start, 'window_end': as_of, 'time_basis': 'occurred_at',
            'price_scope': 'supply_price', 'price_stream':price_stream, 'coverage': coverage,
            'waiting_for_price_observation': waiting_for_price_observation,
            'next_due': min(due) if due else None}


def supplier_metrics(conn, product, as_of, memo=None):
    vendor = (product['source_specific'] or {}).get('vendorKey')
    if not isinstance(vendor, str) or not vendor.strip():
        return {'score': None, 'reason': 'missing_supplier_identity'}
    remembered = memo.get(vendor) if memo is not None else None
    identity = remembered[0] if remembered else conn.execute('SELECT * FROM scoring_supplier_identity WHERE vendor_key=%s AND verified', (vendor,)).fetchone()
    if not identity or identity['valid_from']>as_of-timedelta(days=90) or identity['valid_until']<=as_of:
        due = [t for t in (identity['valid_from']+timedelta(days=90),identity['valid_until']) if t>as_of] if identity else []
        return {'score': None, 'reason': 'unverified_supplier_identity_or_catalog_coverage', 'next_due':min(due) if due else None}
    cached = {'sample': remembered[1]} if remembered else conn.execute('SELECT sample FROM scoring_supplier_samples WHERE vendor_key=%s AND as_of=%s AND valid', (vendor, as_of)).fetchone()
    if cached:
        sample = cached['sample']
    else:
        # Deterministic seeded pseudorandom sample, persisted for replay. 1001
        # allows exclusion of the target while retaining at most 1000 peers.
        peers = conn.execute("""SELECT p.id,p.platform,p.external_product_id,p.source_specific
            FROM products p JOIN scoring_coverage c ON c.product_id=p.id AND c.metric='stockout'
            AND c.verified AND c.valid_from<=%s AND c.valid_until>%s
            WHERE p.platform='ownerclan' AND p.source_specific->>'vendorKey'=%s
            ORDER BY md5(p.id::text || %s),p.id LIMIT 1001""",
            (as_of-timedelta(days=90), as_of, vendor, vendor+VERSION)).fetchall()
        sample = []
        prepared = prepare_chunk(conn, peers, as_of, fingerprints=False) if peers else None
        for peer in peers:
            metrics = history_metrics(conn, peer, as_of, prepared)
            # Experience includes an already-sold-out initial observation. It
            # is evidence for the peer ratio, never a new product occurrence.
            experience = conn.execute("""SELECT EXISTS(SELECT 1 FROM product_history
                WHERE product_id=%s AND status='soldout' AND observed_at>%s AND observed_at<=%s)
                OR COALESCE((SELECT semantic='soldout' FROM product_events
                WHERE platform='ownerclan' AND external_product_id=%s AND event_origin='source'
                AND event_scope='product' AND semantic IN ('soldout','restocked') AND occurred_at<=%s
                ORDER BY occurred_at DESC,id DESC LIMIT 1),false) AS experienced,
                (SELECT min(observed_at+interval '90 days') FROM product_history
                WHERE product_id=%s AND status='soldout' AND observed_at>%s AND observed_at<=%s) AS expiry""",
                (peer['id'],as_of-timedelta(days=90),as_of,peer['external_product_id'],as_of-timedelta(days=90),
                 peer['id'],as_of-timedelta(days=90),as_of)).fetchone()
            if metrics['stockout_count'] is None:
                continue
            next_due=min(t for t in (metrics['next_due'],experience['expiry']) if t is not None)
            sample.append({'id': peer['id'], 'soldout': metrics['stockout_count'] > 0 or experience['experienced'],
                           'next_due': next_due.isoformat()})
        conn.execute('INSERT INTO scoring_supplier_samples(vendor_key,as_of,sample) VALUES(%s,%s,%s) '
                     'ON CONFLICT(vendor_key) DO UPDATE SET as_of=EXCLUDED.as_of,sample=EXCLUDED.sample,valid=true', (vendor, as_of, jb(sample)))
    if memo is not None:
        if len(memo)>=8 and vendor not in memo:
            del memo[next(iter(memo))]
        memo[vendor] = (identity, sample)
    peers = [p for p in sample if p['id'] != product['id']][:1000]
    n, bad = len(peers), sum(p['soldout'] for p in peers)
    due = [identity['valid_until']] + [utc(p['next_due']) for p in peers if p['next_due']]
    return {'score': owner_reliability(n, bad), 'reason': None if n else 'no_verified_other_products',
            'vendor_key': vendor, 'denominator': n, 'stockout_products': bad,
            'rate_percent': 100*bad/n if n else None, 'sample_ids': [p['id'] for p in peers],
            'sample_policy': 'seeded_md5_vendor_current_membership_1000_v1',
            'identity_evidence': identity, 'next_due': min(due)}


def score_one(conn, product, as_of, prepared=None, pending=None, supplier_memo=None):
    # A later collection must never be backdated to a historical scoring run.
    if product['last_collected_at'] > as_of:
        raise ValueError('as_of precedes current product observation; start a new run')
    history = history_metrics(conn, product, as_of, prepared)
    supplier = supplier_metrics(conn, product, as_of, supplier_memo) if product['platform'] == 'ownerclan' else None
    result = calculate(product, as_of, history, supplier)
    fingerprint = prepared['inputs'][product['id']] if prepared is not None else conn.execute('SELECT scoring_product_input(p) AS input FROM products p WHERE id=%s', (product['id'],)).fetchone()['input']
    fingerprint = hashlib.sha256(json.dumps(fingerprint, sort_keys=True).encode()).hexdigest()
    payload = {'productId': product['id'], 'externalProductId': product['external_product_id'],
               'productName': product['product_name'], 'productUrl': product['product_url'],
               'imageUrl': product['image_url'], 'source': 'DOMEME' if product['platform']=='domeggook' else 'OWNERCLAN',
               'supplyPrice': product['current_supply_price'], 'supplyStabilityScore': result['stability'],
               'supplierReliabilityScore': result['reliability'], 'stockoutCount90d': history['stockout_count'],
               'supplierStockoutRate90d': (supplier or {}).get('rate_percent'), 'shippingDays': result['shipping_days'],
               'score': result['home'], 'asOf': as_of, 'observedAt': product['last_collected_at'],
               'formulaVersion': VERSION, 'missingReasons': result['reasons'],
               'eligible': product['sellable'] is True and product['current_supply_price'] is not None
                           and product['supply_price_status']=='source_contract' and not result['on_vacation']}
    if pending is not None:
        pending.append(dict(product_id=product['id'], platform=product['platform'],
            vendor_key=(product['source_specific'] or {}).get('vendorKey'), as_of=as_of,
            formula_version=VERSION, observed_at=product['last_collected_at'],
            stability=result['stability'], reliability=result['reliability'], home=result['home'],
            next_due=result['next_due'], result=result, input_fingerprint=fingerprint, payload=payload))
        return
    conn.execute("""INSERT INTO product_supply_scores(product_id,platform,vendor_key,as_of,formula_version,
        observed_at,stability,reliability,home,next_due,result,input_fingerprint,payload)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(product_id) DO UPDATE SET as_of=EXCLUDED.as_of,formula_version=EXCLUDED.formula_version,
        vendor_key=EXCLUDED.vendor_key,observed_at=EXCLUDED.observed_at,stability=EXCLUDED.stability,
        reliability=EXCLUDED.reliability,home=EXCLUDED.home,next_due=EXCLUDED.next_due,
        result=EXCLUDED.result,input_fingerprint=EXCLUDED.input_fingerprint,payload=EXCLUDED.payload
        WHERE product_supply_scores.as_of<=EXCLUDED.as_of""",
        (product['id'], product['platform'], (product['source_specific'] or {}).get('vendorKey'), as_of, VERSION,
         product['last_collected_at'], result['stability'], result['reliability'], result['home'], result['next_due'], jb(result), fingerprint, jb(payload)))


def prepare_chunk(conn, rows, as_of, *, fingerprints=True):
    """Only bounded current chunk inputs; history/event streams remain keyset paged."""
    ids = [r['id'] for r in rows]
    coverage = {}
    for r in conn.execute('SELECT * FROM scoring_coverage WHERE product_id=ANY(%s) AND verified', (ids,)).fetchall():
        coverage.setdefault(r['product_id'], []).append(r)
    counts = conn.execute("""SELECT p.id,
        count(DISTINCT e.occurred_at) FILTER (WHERE e.verified_before<>e.verified_after) AS prices,
        count(e.id) AS price_pairs, min(e.occurred_at+interval '90 days') AS expiry
        FROM products p LEFT JOIN product_events e ON e.platform=p.platform
        AND e.external_product_id=p.external_product_id AND e.event_origin='source'
        AND e.event_scope='product' AND e.semantic='supply_price' AND e.value_status='source_parsed'
        AND e.verified_before IS NOT NULL AND e.verified_after IS NOT NULL
        AND e.occurred_at>%s AND e.occurred_at<=%s WHERE p.id=ANY(%s) GROUP BY p.id""",
        (as_of-timedelta(days=90), as_of, ids)).fetchall()
    inputs = conn.execute('SELECT id,scoring_product_input(p) AS input FROM products p WHERE id=ANY(%s)', (ids,)).fetchall() if fingerprints else []
    return {'coverage': coverage, 'counts': {r['id']:r for r in counts}, 'inputs':{r['id']:r['input'] for r in inputs}}


def store_chunk(conn, pending, rows):
    conn.execute("""INSERT INTO product_supply_scores(product_id,platform,vendor_key,as_of,formula_version,
        observed_at,stability,reliability,home,next_due,result,input_fingerprint,payload)
        SELECT * FROM jsonb_to_recordset(%s) AS x(product_id bigint,platform text,vendor_key text,
        as_of timestamptz,formula_version text,observed_at timestamptz,stability numeric,
        reliability numeric,home numeric,next_due timestamptz,result jsonb,input_fingerprint text,payload jsonb)
        ON CONFLICT(product_id) DO UPDATE SET as_of=EXCLUDED.as_of,formula_version=EXCLUDED.formula_version,
        vendor_key=EXCLUDED.vendor_key,observed_at=EXCLUDED.observed_at,stability=EXCLUDED.stability,
        reliability=EXCLUDED.reliability,home=EXCLUDED.home,next_due=EXCLUDED.next_due,
        result=EXCLUDED.result,input_fingerprint=EXCLUDED.input_fingerprint,payload=EXCLUDED.payload
        WHERE product_supply_scores.as_of<=EXCLUDED.as_of""", (jb(pending),))
    conn.execute('DELETE FROM scoring_queue q USING unnest(%s::bigint[],%s::bigint[]) x(id,generation) '
                 'WHERE q.product_id=x.id AND q.generation=x.generation',
                 ([r['id'] for r in rows],[r['generation'] for r in rows]))


def expand_supplier(conn, limit):
    job = conn.execute('SELECT * FROM scoring_supplier_jobs ORDER BY vendor_key LIMIT 1 FOR UPDATE').fetchone()
    if not job:
        return False
    rows = conn.execute("SELECT id FROM products WHERE platform='ownerclan' AND source_specific->>'vendorKey'=%s AND id>%s ORDER BY id LIMIT %s",
                        (job['vendor_key'], job['after_id'], limit)).fetchall()
    if rows:
        conn.execute("SELECT scoring_enqueue(id,'supplier_impact') FROM unnest(%s::bigint[]) id", ([r['id'] for r in rows],))
    if rows:
        conn.execute('UPDATE scoring_supplier_jobs SET after_id=%s WHERE vendor_key=%s', (rows[-1]['id'], job['vendor_key']))
    else:
        conn.execute('DELETE FROM scoring_supplier_jobs WHERE vendor_key=%s', (job['vendor_key'],))
    return True


def publish(conn, run):
    """Single SQL transaction: rank snapshot + completion; no mutable master joins on reads."""
    # Serialize direct callers as well as the normal run_scoring session lock.
    state=conn.execute('SELECT * FROM scoring_runs WHERE run_id=%s FOR UPDATE',(run['run_id'],)).fetchone()
    existing = conn.execute('SELECT completed_at FROM supply_home_batches WHERE batch_id=%s', (run['run_id'],)).fetchone()
    if existing and existing['completed_at']:
        return
    if state is None or (state['mode']=='initial' and not state['scanned']):
        raise RuntimeError('Initial scan is incomplete')
    if existing and conn.execute('SELECT 1 FROM supply_home_items WHERE batch_id=%s LIMIT 1', (run['run_id'],)).fetchone():
        raise RuntimeError('Incomplete publication contains rows; review batch integrity')
    pending=conn.execute('SELECT 1 FROM scoring_queue WHERE platform=%s AND (%s OR product_id<=%s) LIMIT 1',
                         (run['platform'],state['mode']=='incremental',state['high_id'])).fetchone()
    if pending:
        raise RuntimeError('Scoring inputs changed before publication; resume the run')
    if conn.execute('SELECT 1 FROM product_discovery_targets WHERE platform=%s AND requested_version>completed_version LIMIT 1',
                    (run['platform'],)).fetchone():
        raise RuntimeError('Detail refresh incomplete')
    conn.execute('INSERT INTO supply_home_batches(batch_id,platform,as_of,formula_version) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                 (run['run_id'], run['platform'], run['as_of'], VERSION))
    conn.execute("""INSERT INTO supply_home_items(batch_id,rank,product_id,payload)
      SELECT %s,row_number() OVER(ORDER BY home DESC,product_id),product_id,payload
      FROM product_supply_scores WHERE platform=%s AND home IS NOT NULL
      AND (payload->>'eligible')::boolean AND as_of<=%s AND formula_version IN ('supply_v1',%s)""",
                 (run['run_id'], run['platform'], run['as_of'], VERSION))
    conn.execute('UPDATE supply_home_batches SET completed_at=clock_timestamp(),item_count=(SELECT count(*) FROM supply_home_items WHERE batch_id=%s) WHERE batch_id=%s', (run['run_id'], run['run_id']))


def _run_scoring(conn, *, run_id, mode, platform, as_of, chunk_size=500, max_chunks=None, stop_requested=lambda: False):
    """Resume by run_id; chunk results and checkpoint commit together.

    Use current UTC time for new runs. max_chunks supports controlled shutdown.
    A session lock serializes scoring only; collection continues transactionally.
    """
    as_of = utc(as_of)
    if mode not in ('initial','incremental') or platform not in ('domeggook','ownerclan') or not 1<=chunk_size<=10000:
        raise ValueError('Invalid scoring scope')
    if not conn.execute('SELECT pg_try_advisory_lock(%s) AS locked', (LOCK,)).fetchone()['locked']:
        conn.rollback()
        raise RuntimeError('Scoring already running')
    owns_scope = False
    attempted_product_id = None
    try:
        conn.execute('INSERT INTO scoring_runs(run_id,mode,platform,as_of,high_id,formula_version) '
                     'SELECT %s,%s,%s,%s,COALESCE(max(id),0),%s FROM products WHERE platform=%s ON CONFLICT DO NOTHING',
                     (run_id, mode, platform, as_of, VERSION, platform))
        run = conn.execute('SELECT * FROM scoring_runs WHERE run_id=%s', (run_id,)).fetchone()
        if (run['mode'],run['platform'],run['as_of'],run['formula_version']) != (mode,platform,as_of,VERSION):
            raise ValueError('Resume scope differs from persisted run')
        owns_scope = True
        conn.commit()
        if run['status']=='completed':
            return run
        chunks = 0
        while (max_chunks is None or chunks < max_chunks) and not stop_requested():
            due = conn.execute('SELECT product_id FROM product_supply_scores WHERE platform=%s AND next_due<=%s ORDER BY next_due,product_id LIMIT %s',
                               (platform,as_of,chunk_size)).fetchall()
            if due:
                for item in due:
                    conn.execute("SELECT scoring_enqueue(%s,'time_boundary')",(item['product_id'],))
                    if platform=='ownerclan':
                        conn.execute('SELECT scoring_vendor(source_specific->>\'vendorKey\') FROM products WHERE id=%s',(item['product_id'],))
                    conn.execute('UPDATE product_supply_scores SET next_due=NULL WHERE product_id=%s',(item['product_id'],))
                conn.commit()
                chunks += 1
                continue
            # Expand peer invalidations before computing; bounded DB/memory work.
            if platform=='ownerclan' and expand_supplier(conn, chunk_size):
                conn.commit()
                chunks += 1
                continue
            if mode=='initial' and not run['scanned']:
                rows = conn.execute('SELECT p.*,q.generation FROM products p LEFT JOIN scoring_queue q ON q.product_id=p.id '
                    'WHERE p.platform=%s AND p.id>%s AND p.id<=%s ORDER BY p.id LIMIT %s',
                    (platform,run['after_id'],run['high_id'],chunk_size)).fetchall()
                if not rows:
                    conn.execute('UPDATE scoring_runs SET scanned=true WHERE run_id=%s',(run_id,))
                    conn.commit()
                    run['scanned']=True
                    continue
            else:
                # Due index avoids an all-products scan. Do not enqueue the whole due set.
                rows = conn.execute("""SELECT p.*,q.generation FROM scoring_queue q
                    JOIN products p ON p.id=q.product_id WHERE q.platform=%s AND (%s OR q.product_id<=%s)
                    ORDER BY q.product_id LIMIT %s""", (platform,mode=='incremental',run['high_id'],chunk_size)).fetchall()
            if not rows:
                publish(conn, run)
                conn.execute("UPDATE scoring_runs SET status='completed',completed_at=clock_timestamp(),last_error=NULL,last_error_code=NULL,failed_product_id=NULL WHERE run_id=%s", (run_id,))
                conn.commit()
                return conn.execute('SELECT * FROM scoring_runs WHERE run_id=%s',(run_id,)).fetchone()
            prepared = prepare_chunk(conn, rows, as_of)
            pending = []
            supplier_memo = {}
            for row in rows:
                attempted_product_id = row['id']
                score_one(conn,row,as_of,prepared,pending,supplier_memo)
            store_chunk(conn, pending, rows)
            run['after_id'] = rows[-1]['id']
            conn.execute("UPDATE scoring_runs SET after_id=%s,processed=processed+%s,status='running',last_error=NULL WHERE run_id=%s", (run['after_id'],len(rows),run_id))
            conn.commit()
            chunks += 1
        return conn.execute('SELECT * FROM scoring_runs WHERE run_id=%s',(run_id,)).fetchone()
    except Exception as exc:
        conn.rollback()
        # Error class only: DSNs and raw input must not leak into durable errors.
        try:
            if owns_scope:
                code = 'future_observation' if isinstance(exc,ValueError) and str(exc).startswith('as_of precedes') else 'chunk_or_publication_failure'
                conn.execute("UPDATE scoring_runs SET status='failed',last_error=%s,last_error_code=%s,failed_product_id=%s WHERE run_id=%s AND status<>'completed'",
                             (type(exc).__name__,code,attempted_product_id,run_id))
            conn.commit()
        except Exception:
            conn.rollback()
        raise
    finally:
        conn.execute('SELECT pg_advisory_unlock(%s)', (LOCK,))
        conn.commit()


def read_published(conn, source, size=10):
    platform = {'DOMEME':'domeggook','OWNERCLAN':'ownerclan'}.get(source)
    if platform is None or not 1<=size<=1000:
        raise ValueError('Invalid source or size')
    row = conn.execute("""SELECT b.as_of,b.completed_at,b.batch_id,b.formula_version,
        COALESCE((SELECT jsonb_agg(i.payload || jsonb_build_object('rank',i.rank) ORDER BY i.rank)
        FROM supply_home_items i WHERE i.batch_id=b.batch_id AND i.rank<=%s),'[]'::jsonb) AS items
        FROM supply_home_published b WHERE platform=%s""", (size,platform)).fetchone()
    return None if row is None else {'updatedAt':row['completed_at'],'asOf':row['as_of'],
        'batchId':row['batch_id'],'formulaVersion':row['formula_version'],'items':row['items']}


def run_scoring(conn, **kwargs):
    # Old installations retain the original invalidation behavior until explicit migration.
    from product_data_collector.operations.fence import scoring_boundary
    with scoring_boundary(conn, kwargs['run_id'], kwargs['platform']):
        migrated = conn.execute("SELECT to_regclass('ops_score_inputs') AS t").fetchone()['t']
        completed = (conn.execute("SELECT 1 FROM scoring_runs WHERE run_id=%s AND status='completed'",
                                 (kwargs['run_id'],)).fetchone() if migrated else None)
        if migrated and not completed:
            future = conn.execute('SELECT 1 FROM products WHERE platform=%s AND last_collected_at>%s LIMIT 1',
                                  (kwargs['platform'], utc(kwargs['as_of']))).fetchone()
            if future:
                raise ValueError('as_of precedes current product observation; start a new run')
        return _run_scoring(conn, **kwargs)
