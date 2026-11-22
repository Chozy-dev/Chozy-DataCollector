from datetime import datetime, timedelta, timezone

import pytest

from product_data_collector.persistence.migrations import apply_migration
from product_data_collector.persistence.scoring import run_scoring, read_published, history_metrics, supplier_metrics, LOCK
from product_data_collector.domain.product_events import ownerclan_event, ProductEvent
from product_data_collector.persistence.product_events import save_events

AT=datetime(2026,11,19,tzinfo=timezone.utc)


@pytest.fixture
def scoring(db):
    apply_migration(db,'stage7_supply_scoring')
    apply_migration(db,'stage7_history_policy_v2')
    apply_migration(db,'stage10_scoring_performance')
    return db


def product(db, key='A', platform='domeggook', vendor=None, **changes):
    from psycopg.types.json import Jsonb
    fields={'platform':platform,'external_product_id':key,'product_name':key,
            'first_seen_at':AT-timedelta(days=100),'last_collected_at':AT-timedelta(days=1),
            'current_observed_at':AT-timedelta(days=1),'source_specific':Jsonb({'vendorKey':vendor,'guaranteedShippingPeriod':1}),
            'stock_quantity':9999,'stock_is_infinite':True,'current_supply_price':100,
            'supply_price_status':'source_contract','supply_price_basis':'uniform_option_final',
            'sellable':True,'average_shipping_days':1,'seller_average_satisfaction':'96%',
            'seller_review_count':100,**changes}
    return db.execute(f"INSERT INTO products({','.join(fields)}) VALUES({','.join(['%s']*len(fields))}) RETURNING *",tuple(fields.values())).fetchone()


def certify(db,p,metric='stockout',until=None):
    db.execute("INSERT INTO scoring_coverage(product_id,metric,valid_from,valid_until,verified,evidence) VALUES(%s,%s,%s,%s,true,'{\"fixture_only\":true,\"stockout_state_at_valid_from\":\"available\"}') "
               'ON CONFLICT(product_id,metric) DO UPDATE SET verified=true,valid_until=EXCLUDED.valid_until',
               (p['id'],metric,AT-timedelta(days=120),until or AT+timedelta(days=30)))


def execute(db,run='first',mode='initial',platform='domeggook',at=AT,**kw):
    db.commit()
    return run_scoring(db,run_id=run,mode=mode,platform=platform,as_of=at,**kw)


def test_initial_resume_and_read_snapshot(scoring):
    db=scoring
    for key in ['A','B','C']: product(db,key)
    assert execute(db,chunk_size=1,max_chunks=1)['processed']==1
    assert read_published(db,'DOMEME') is None
    assert execute(db,chunk_size=1)['status']=='completed'
    result=read_published(db,'DOMEME')
    assert [p['rank'] for p in result['items']]==[1,2,3]
    assert result['items'][0]['stockoutCount90d'] is None
    assert result['items'][0]['score']==94
    assert execute(db)['processed']==3
    db.execute("UPDATE products SET product_name='changed' WHERE external_product_id='A'")
    assert read_published(db,'DOMEME')['items'][0]['productName']=='A'


def test_unchanged_input_not_enqueued_and_incremental(scoring):
    db=scoring
    p=product(db)
    execute(db)
    db.execute('UPDATE products SET last_collected_at=last_collected_at+interval \'1 hour\' WHERE id=%s',(p['id'],))
    assert db.execute('SELECT count(*) n FROM scoring_queue').fetchone()['n']==0
    assert execute(db,'daily','incremental',at=AT+timedelta(days=1))['processed']==0
    db.execute("UPDATE products SET seller_average_satisfaction=%s WHERE id=%s",('0%',p['id']))
    assert execute(db,'daily2','incremental',at=AT+timedelta(days=1))['processed']==1
    assert read_published(db,'DOMEME')['items'][0]['supplierReliabilityScore']==0


def test_source_option_replay_initial_and_coverage(scoring):
    db=scoring
    p=product(db,platform='ownerclan')
    event=ownerclan_event({'itemKey':'A','kind':'soldout','createdAt':int((AT-timedelta(days=2)).timestamp()*1000)},AT)
    option=ownerclan_event({'itemKey':'A','kind':'optionSoldout','createdAt':int((AT-timedelta(days=1)).timestamp()*1000)},AT)
    save_events(db,[event,event,option],request_refresh=False)
    assert history_metrics(db,p,AT)['stockout_count'] is None
    certify(db,p)
    assert history_metrics(db,p,AT)['stockout_count']==1
    assert history_metrics(db,p,AT)['price_count'] is None


def test_expiry_without_new_api_event(scoring):
    db=scoring
    p=product(db,platform='ownerclan')
    certify(db,p)
    event=ownerclan_event({'itemKey':'A','kind':'soldout','createdAt':int((AT-timedelta(days=89)).timestamp()*1000)},AT)
    restock=ownerclan_event({'itemKey':'A','kind':'restocked','createdAt':int((AT-timedelta(days=89)).timestamp()*1000)+1000},AT)
    save_events(db,[event,restock],request_refresh=False)
    execute(db,platform='ownerclan')
    assert read_published(db,'OWNERCLAN')['items'][0]['stockoutCount90d']==1
    assert execute(db,'daily','incremental','ownerclan',AT+timedelta(days=1,seconds=1))['processed']==1
    assert read_published(db,'OWNERCLAN')['items'][0]['stockoutCount90d']==0


def test_verified_supply_prices_only(scoring):
    db=scoring
    p=product(db)
    certify(db,p,'price')
    assert history_metrics(db,p,AT)['price_count'] is None
    for semantic in ['base_price','supply_price']:
        e=ProductEvent('domeggook','A','fixture','price',AT-timedelta(days=1),AT,semantic,{},
                       raw_before=100,raw_after=200,verified_before=100,verified_after=200,value_status='source_parsed')
        save_events(db,[e,e],request_refresh=False)
    assert history_metrics(db,p,AT)['price_count']==1


def test_supplier_distinct_other_products_and_invalidation(scoring):
    db=scoring
    db.execute("INSERT INTO scoring_supplier_identity VALUES('V',%s,%s,true,'{\"fixture_only\":true,\"stockout_state_at_valid_from\":\"available\"}')",(AT-timedelta(days=100),AT+timedelta(days=30)))
    p=product(db,'A','ownerclan','V'); other=product(db,'B','ownerclan','V')
    certify(db,p); certify(db,other)
    save_events(db,[ownerclan_event({'itemKey':'B','kind':'soldout','createdAt':int((AT-timedelta(days=d)).timestamp()*1000)},AT) for d in [1,2]],request_refresh=False)
    result=supplier_metrics(db,p,AT)
    assert result['denominator']==1 and result['stockout_products']==1 and result['score']==67
    assert supplier_metrics(db,other,AT)['stockout_products']==0
    execute(db,platform='ownerclan',chunk_size=1)
    db.execute('UPDATE scoring_coverage SET verified=false WHERE product_id=%s',(other['id'],))
    assert execute(db,'daily','incremental','ownerclan',AT)['processed']==2
    assert db.execute('SELECT reliability FROM product_supply_scores WHERE product_id=%s',(p['id'],)).fetchone()['reliability'] is None


@pytest.mark.parametrize('vendor', [None,'','V'])
def test_supplier_identity_never_invented(scoring,vendor):
    p=product(scoring,platform='ownerclan',vendor=vendor)
    assert supplier_metrics(scoring,p,AT)['score'] is None


def test_source_separation_old_completion_and_exclusions(scoring):
    db=scoring
    product(db,'A'); product(db,'B','ownerclan'); product(db,'C',sellable=False)
    product(db,'D',current_supply_price=None)
    execute(db,'new',at=AT+timedelta(days=1))
    execute(db,'old',at=AT)
    assert read_published(db,'DOMEME')['batchId']=='new'
    assert len(read_published(db,'DOMEME')['items'])==1
    execute(db,'owner',platform='ownerclan')
    assert read_published(db,'OWNERCLAN')['items'][0]['rank']==1


def test_rollback_and_previous_publication(scoring,monkeypatch):
    import product_data_collector.persistence.scoring as module
    db=scoring; product(db)
    execute(db)
    db.execute("UPDATE products SET stock_quantity=0")
    original=module.score_one
    def fail(*args):
        original(*args)
        raise RuntimeError('rollback')
    monkeypatch.setattr(module,'score_one',fail)
    with pytest.raises(RuntimeError): execute(db,'failed','incremental',at=AT+timedelta(days=1))
    assert read_published(db,'DOMEME')['batchId']=='first'
    assert db.execute("SELECT processed FROM scoring_runs WHERE run_id='failed'").fetchone()['processed']==0
    monkeypatch.setattr(module,'score_one',original)
    assert execute(db,'failed','incremental',at=AT+timedelta(days=1))['processed']==1


def test_concurrent_worker_lock(scoring,db_factory):
    other=db_factory()
    scoring.execute('SELECT pg_advisory_lock(%s)',(LOCK,)); scoring.commit()
    with pytest.raises(RuntimeError,match='already running'):
        execute(other)
    scoring.execute('SELECT pg_advisory_unlock(%s)',(LOCK,)); scoring.commit()


def test_coverage_expiry_and_recovery(scoring):
    db=scoring; p=product(db)
    certify(db,p,until=AT+timedelta(days=1))
    execute(db)
    execute(db,'expired','incremental',at=AT+timedelta(days=1))
    assert read_published(db,'DOMEME')['items'][0]['stockoutCount90d'] is None
    certify(db,p,until=AT+timedelta(days=10))
    assert execute(db,'recovered','incremental',at=AT+timedelta(days=2))['processed']==1
    assert read_published(db,'DOMEME')['items'][0]['stockoutCount90d']==0


def test_sql_spring_function(scoring):
    product(scoring)
    execute(scoring)
    result=scoring.execute("SELECT read_supply_home('DOMEME',10) payload").fetchone()['payload']
    assert result['items']==read_published(scoring,'DOMEME')['items']
    assert result['formulaVersion']=='supply_v2'
    assert scoring.execute("SELECT read_supply_home('OWNERCLAN',10) payload").fetchone()['payload'] is None


def test_partial_coverage_matures_without_input_change(scoring):
    db=scoring; p=product(db)
    certify(db,p)
    db.execute("UPDATE scoring_coverage SET valid_from=%s WHERE product_id=%s",(AT-timedelta(days=89),p['id']))
    execute(db)
    assert read_published(db,'DOMEME')['items'][0]['stockoutCount90d'] is None
    assert execute(db,'mature','incremental',at=AT+timedelta(days=1))['processed']==1
    assert read_published(db,'DOMEME')['items'][0]['stockoutCount90d']==0


def test_observations_not_double_counted(scoring):
    from product_data_collector.domain.product_events import observation_event
    db=scoring; p=product(db); certify(db,p)
    observed=observation_event(platform='domeggook',external_product_id='A',kind='soldout',observed_at=AT,
        before='available',after='soldout',comparison_key='history:1:2',semantic='soldout')
    save_events(db,[observed],request_refresh=False)
    assert history_metrics(db,p,AT)['stockout_count']==0


@pytest.mark.parametrize('phase',['checkpoint','publication'])
def test_commit_response_loss_is_replayable(scoring,phase):
    db=scoring; product(db); db.commit()
    class LostCommit:
        fired=False
        def __getattr__(self,name): return getattr(db,name)
        def commit(self):
            if self.fired:
                return db.commit()
            count=db.execute('SELECT count(*) n FROM '+('product_supply_scores' if phase=='checkpoint' else 'supply_home_published')).fetchone()['n']
            db.commit()
            if count:
                self.fired=True
                raise RuntimeError('response lost after actual commit')
    proxy=LostCommit()
    with pytest.raises(RuntimeError): execute(proxy)
    assert proxy.fired
    assert execute(db)['status']=='completed'
    assert db.execute('SELECT count(*) n FROM product_supply_scores').fetchone()['n']==1
    assert db.execute('SELECT count(*) n FROM supply_home_items').fetchone()['n']==1


def test_publish_rollback_preserves_previous_and_other_source(scoring,monkeypatch):
    import product_data_collector.persistence.scoring as module
    db=scoring; product(db); product(db,'B','ownerclan')
    execute(db)
    execute(db,'owner',platform='ownerclan')
    original=module.publish
    def fail(*args):
        original(*args)
        raise RuntimeError('rollback publication')
    monkeypatch.setattr(module,'publish',fail)
    with pytest.raises(RuntimeError): execute(db,'bad','incremental',at=AT+timedelta(days=1))
    assert read_published(db,'DOMEME')['batchId']=='first'
    assert read_published(db,'OWNERCLAN')['batchId']=='owner'
    assert db.execute("SELECT count(*) n FROM supply_home_batches WHERE batch_id='bad'").fetchone()['n']==0


def test_initial_drains_changes_behind_cursor(scoring):
    db=scoring; product(db); product(db,'B')
    execute(db,chunk_size=1,max_chunks=1)
    db.execute("UPDATE products SET stock_quantity=0 WHERE external_product_id='A'")
    assert execute(db,chunk_size=1)['processed']==3
    assert next(i for i in read_published(db,'DOMEME')['items'] if i['externalProductId']=='A')['supplyStabilityScore']==47


def test_vacation_boundary_without_api(scoring):
    from psycopg.types.json import Jsonb
    db=scoring
    product(db,source_specific=Jsonb({'seller':{'vacation':{'start':'2026-11-19','end':'2026-11-19'}}}))
    execute(db)
    assert read_published(db,'DOMEME')['items']==[]
    assert execute(db,'end','incremental',at=AT+timedelta(days=1))['processed']==1
    assert len(read_published(db,'DOMEME')['items'])==1


def test_sample_bounded_and_repeatable(scoring):
    db=scoring
    db.execute("INSERT INTO scoring_supplier_identity VALUES('V',%s,%s,true,'{\"fixture_only\":true,\"stockout_state_at_valid_from\":\"available\"}')",(AT-timedelta(days=100),AT+timedelta(days=30)))
    for n in range(1003):
        p=product(db,str(n),'ownerclan','V'); certify(db,p)
    a=supplier_metrics(db,p,AT)
    assert a['denominator']==1000 and p['id'] not in a['sample_ids']
    assert supplier_metrics(db,p,AT)['sample_ids']==a['sample_ids']


def test_certified_observed_prices_initial_unchanged_and_dedup(scoring):
    from domeggook_API.services.parsing import parse_detail_product
    from product_data_collector.persistence import postgres_storage as storage
    db=scoring
    for offset,price in [(-100,100),(-2,120),(-1,120)]:
        at=AT+timedelta(days=offset)
        raw={'no':'A','basis':{'status':'판매중'},'channel':{'supply':'1'},'price':{'supply':price}}
        normalized=parse_detail_product(raw,at.isoformat())
        storage._save_product_batch_with_retry(db,[storage._snapshot_row('domeggook',at.isoformat(),normalized)])
    p=db.execute("SELECT * FROM products WHERE external_product_id='A'").fetchone()
    certify(db,p,'price')
    db.execute("UPDATE scoring_coverage SET evidence='{\"stream\":\"observed_supply_v1\",\"fixture_only\":true,\"stockout_state_at_valid_from\":\"available\"}' WHERE product_id=%s",(p['id'],))
    event=ProductEvent('domeggook','A','fixture','price',AT-timedelta(days=2),AT,'supply_price',{},
                       raw_before=100,raw_after=120,verified_before=100,verified_after=120,value_status='source_parsed')
    save_events(db,[event],request_refresh=False)
    metrics=history_metrics(db,p,AT)
    assert metrics['price_count']==1 and metrics['price_stream']=='observed_supply_v1'


def test_wrong_resume_scope_preserves_completed(scoring):
    product(scoring); execute(scoring)
    with pytest.raises(ValueError,match='scope'):
        execute(scoring,at=AT+timedelta(days=1))
    assert scoring.execute('SELECT status FROM scoring_runs').fetchone()['status']=='completed'


def test_future_observation_records_failure_without_publication(scoring):
    p=product(scoring,last_collected_at=AT+timedelta(seconds=1))
    with pytest.raises(ValueError,match='precedes'): execute(scoring)
    row=scoring.execute('SELECT * FROM scoring_runs').fetchone()
    assert row['failed_product_id']==p['id'] and row['last_error_code']=='future_observation'
    assert read_published(scoring,'DOMEME') is None


def test_direct_partial_publication_is_rejected(scoring):
    from product_data_collector.persistence.scoring import publish
    product(scoring); product(scoring,'B')
    run=execute(scoring,max_chunks=1,chunk_size=1)
    with pytest.raises(RuntimeError,match='incomplete'): publish(scoring,run)
    scoring.rollback()
    assert read_published(scoring,'DOMEME') is None


def test_queue_generation_keeps_change_during_scoring(scoring,monkeypatch):
    import product_data_collector.persistence.scoring as module
    db=scoring; product(db)
    original=module.score_one
    changed=[]
    def change_after_read(conn,row,as_of,*args):
        if not changed:
            conn.execute('UPDATE products SET seller_average_satisfaction=%s WHERE id=%s',('0%',row['id']))
            changed.append(True)
        original(conn,row,as_of,*args)
    monkeypatch.setattr(module,'score_one',change_after_read)
    assert execute(db)['processed']==2
    assert read_published(db,'DOMEME')['items'][0]['supplierReliabilityScore']==0
