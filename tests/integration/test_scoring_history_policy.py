"""Policy repair regressions; isolated PostgreSQL, no external HTTP."""
from datetime import timedelta

import pytest

from tests.integration.test_scoring import (
    AT, scoring, product, certify, execute, history_metrics, supplier_metrics,
    ownerclan_event, save_events,
)


@pytest.mark.parametrize('events,verified,expected', [
    ([(-30, 'soldout'), (-1, 'restocked')], True, 1),
    ([(-100, 'soldout')], True, 1),
    ([(-100, 'soldout'), (-95, 'restocked')], True, 0),
    ([(-100, 'soldout'), (-2, 'restocked')], True, 1),
    ([(-100, 'soldout'), (-90, 'restocked')], True, 0),
    ([(-30, 'soldout'), (-20, 'soldout'), (-1, 'soldout')], True, 1),
    ([(-30, 'soldout'), (-20, 'restocked'), (-10, 'soldout'), (-1, 'restocked')], True, 2),
    ([(-30, 'soldout')], False, None),
    ([(-90, 'soldout')], True, 1),
])
def test_stockout_overlap_policy(scoring, events, verified, expected):
    p = product(scoring, platform='ownerclan')
    if verified:
        certify(scoring, p)
    save_events(scoring, [ownerclan_event({
        'itemKey': 'A', 'kind': kind,
        'createdAt': int((AT + timedelta(days=days)).timestamp() * 1000),
    }, AT) for days, kind in events], request_refresh=False)
    assert history_metrics(scoring, p, AT)['stockout_count'] == expected


def test_supplier_counts_old_ongoing_source_stockout(scoring):
    db = scoring
    p = product(db, 'A', 'ownerclan', 'V')
    peer = product(db, 'B', 'ownerclan', 'V')
    certify(db, p)
    certify(db, peer)
    db.execute("INSERT INTO scoring_supplier_identity VALUES('V',%s,%s,true,'{\"fixture_only\":true}')",
               (AT - timedelta(days=120), AT + timedelta(days=30)))
    save_events(db, [ownerclan_event({'itemKey': 'B', 'kind': 'soldout',
        'createdAt': int((AT - timedelta(days=100)).timestamp() * 1000)}, AT)], request_refresh=False)
    assert history_metrics(db, peer, AT)['stockout_count'] == 1
    assert supplier_metrics(db, p, AT)['stockout_products'] == 1


def test_source_price_boundary_only_recalculates_affected_product(scoring):
    from product_data_collector.domain.product_events import ProductEvent
    db = scoring
    p = product(db)
    product(db, 'unaffected')
    certify(db, p, 'price')
    save_events(db, [ProductEvent('domeggook', 'A', 'fixture', 'price',
        AT - timedelta(days=89), AT, 'supply_price', {}, verified_before=100,
        verified_after=120, value_status='source_parsed')], request_refresh=False)
    execute(db)
    assert history_metrics(db, p, AT)['price_count'] == 1
    assert execute(db, 'expiry', 'incremental', at=AT + timedelta(days=1))['processed'] == 1
    # No pair remains in the source window: insufficient evidence, not zero.
    assert history_metrics(db, p, AT + timedelta(days=1))['price_count'] is None


@pytest.mark.parametrize('second_price,expected', [(100,0),(120,1)])
def test_observed_price_readiness_repair(scoring, second_price, expected):
    from domeggook_API.services.parsing import parse_detail_product
    from product_data_collector.persistence import postgres_storage as storage
    db = scoring
    def observe(at, price=100):
        raw = {'no': 'A', 'basis': {'status': '판매중'}, 'channel': {'supply': '1'},
               'price': {'supply': price}}
        normalized = parse_detail_product(raw, at.isoformat())
        storage._save_product_batch_with_retry(db, [storage._snapshot_row('domeggook', at.isoformat(), normalized)])
        return db.execute("SELECT * FROM products WHERE external_product_id='A'").fetchone()
    p = observe(AT - timedelta(days=100))
    certify(db, p, 'price')
    db.execute("UPDATE scoring_coverage SET evidence='{\"stream\":\"observed_supply_v1\",\"fixture_only\":true}' WHERE product_id=%s", (p['id'],))
    execute(db)
    assert history_metrics(db, p, AT)['price_count'] is None
    p = observe(AT + timedelta(hours=1), second_price)
    assert history_metrics(db, p, AT + timedelta(days=1))['price_count'] == expected
    paused = execute(db, 'second', 'incremental', at=AT + timedelta(days=1), max_chunks=1, chunk_size=1)
    assert paused['processed'] == 1 and paused['status'] == 'running'
    assert execute(db, 'second', 'incremental', at=AT + timedelta(days=1))['status'] == 'completed'
    stored = db.execute('SELECT result FROM product_supply_scores WHERE product_id=%s', (p['id'],)).fetchone()['result']
    assert stored['history']['price_count'] == expected

    observe(AT + timedelta(days=2), second_price)
    assert execute(db, 'third', 'incremental', at=AT + timedelta(days=3))['processed'] == 0


def test_unknown_and_ambiguous_stockout_state(scoring):
    db=scoring; p=product(db,platform='ownerclan'); certify(db,p)
    db.execute("UPDATE scoring_coverage SET evidence='{\"fixture_only\":true}' WHERE product_id=%s",(p['id'],))
    m=history_metrics(db,p,AT)
    assert m['stockout_count'] is None and m['reasons']['stockout']=='unknown_stockout_boundary'
    save_events(db,[ownerclan_event({'itemKey':'A','kind':kind,
        'createdAt':int((AT-timedelta(days=100)).timestamp()*1000)},AT)
        for kind in ['soldout','restocked']],request_refresh=False)
    assert history_metrics(db,p,AT)['reasons']['stockout']=='ambiguous_stockout_boundary'


def test_only_closed_interval_boundary_is_due(scoring):
    db=scoring
    for key,events in [('closed',[(-100,'soldout'),(-89,'restocked')]),
                       ('ongoing',[(-100,'soldout')]),('clear',[])]:
        p=product(db,key,platform='ownerclan'); certify(db,p)
        save_events(db,[ownerclan_event({'itemKey':key,'kind':kind,
            'createdAt':int((AT+timedelta(days=days)).timestamp()*1000)},AT)
            for days,kind in events],request_refresh=False)
    execute(db,platform='ownerclan')
    assert execute(db,'boundary','incremental','ownerclan',AT+timedelta(days=1))['processed']==1
    rows=db.execute('SELECT p.external_product_id,s.payload FROM product_supply_scores s JOIN products p ON p.id=s.product_id').fetchall()
    assert {r['external_product_id']:r['payload']['stockoutCount90d'] for r in rows}=={'closed':0,'ongoing':1,'clear':0}


def test_policy_migration_repairs_only_affected_and_is_idempotent(db):
    from product_data_collector.persistence.migrations import apply_migration
    apply_migration(db,'stage7_supply_scoring')
    p=product(db); product(db,'unaffected'); certify(db,p)
    execute(db)
    db.execute("UPDATE product_supply_scores SET formula_version='supply_v1'")
    db.commit()
    apply_migration(db,'stage8_operations')
    assert apply_migration(db,'stage7_history_policy_v2')
    assert [r['product_id'] for r in db.execute('SELECT product_id FROM scoring_queue').fetchall()]==[p['id']]
    assert not apply_migration(db,'stage7_history_policy_v2')
    from product_data_collector.operations.fence import certify_collection
    certify_collection(db,'domeggook','fixture-only-policy-migration')
    assert execute(db,'repair','incremental')['processed']==1
    from product_data_collector.persistence.scoring import read_published
    assert len(read_published(db,'DOMEME')['items'])==2


@pytest.mark.parametrize('first_days,verified', [(-1,True),(-100,False)])
def test_observed_price_still_insufficient_not_enqueued(scoring,first_days,verified):
    from domeggook_API.services.parsing import parse_detail_product
    from product_data_collector.persistence import postgres_storage as storage
    db=scoring
    def observe(at):
        raw={'no':'A','basis':{'status':'판매중'},'channel':{'supply':'1'},'price':{'supply':100}}
        value=parse_detail_product(raw,at.isoformat())
        storage._save_product_batch_with_retry(db,[storage._snapshot_row('domeggook',at.isoformat(),value)])
        return db.execute("SELECT * FROM products WHERE external_product_id='A'").fetchone()
    p=observe(AT+timedelta(days=first_days))
    if verified:
        certify(db,p,'price')
        db.execute("UPDATE scoring_coverage SET evidence='{\"stream\":\"observed_supply_v1\"}' WHERE product_id=%s",(p['id'],))
    execute(db)
    p=observe(AT+timedelta(hours=1))
    assert history_metrics(db,p,AT+timedelta(days=1))['price_count'] is None
    assert execute(db,'insufficient','incremental',at=AT+timedelta(days=1))['processed']==0


def test_source_and_observation_never_add_stockout_intervals(scoring):
    from product_data_collector.domain.product_events import observation_event
    db=scoring; p=product(db,platform='ownerclan'); certify(db,p)
    source=ownerclan_event({'itemKey':'A','kind':'soldout',
        'createdAt':int((AT-timedelta(days=100)).timestamp()*1000)},AT)
    option=ownerclan_event({'itemKey':'A','kind':'optionSoldout',
        'createdAt':int((AT-timedelta(days=1)).timestamp()*1000)},AT)
    observed=observation_event(platform='ownerclan',external_product_id='A',kind='soldout',
        observed_at=AT,before='available',after='soldout',comparison_key='fixture:1:2',semantic='soldout')
    save_events(db,[source,source,option,observed],request_refresh=False)
    assert history_metrics(db,p,AT)['stockout_count']==1


@pytest.mark.parametrize('ambiguous',[False,True])
def test_unknown_stockout_recovers_only_when_definite_anchor_enters_boundary(scoring,ambiguous):
    db=scoring; p=product(db,platform='ownerclan'); certify(db,p)
    db.execute("UPDATE scoring_coverage SET evidence='{\"fixture_only\":true}' WHERE product_id=%s",(p['id'],))
    events=[(-89,'soldout')]
    if ambiguous: events += [(-89.5,'soldout'),(-89.5,'restocked')]
    save_events(db,[ownerclan_event({'itemKey':'A','kind':kind,
        'createdAt':int((AT+timedelta(days=days)).timestamp()*1000)},AT)
        for days,kind in events],request_refresh=False)
    m=history_metrics(db,p,AT)
    assert m['stockout_count'] is None and m['next_due']==AT+timedelta(days=1)
    execute(db,platform='ownerclan')
    assert execute(db,'recovered','incremental','ownerclan',AT+timedelta(days=1))['processed']==1
    assert db.execute('SELECT payload FROM product_supply_scores').fetchone()['payload']['stockoutCount90d']==1


def test_reviewed_initial_state_supersedes_older_source_anchor(scoring):
    db=scoring; p=product(db,platform='ownerclan'); certify(db,p)
    save_events(db,[ownerclan_event({'itemKey':'A','kind':'soldout',
        'createdAt':int((AT-timedelta(days=130)).timestamp()*1000)},AT)],request_refresh=False)
    assert history_metrics(db,p,AT)['stockout_count']==0
