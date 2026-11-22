from dataclasses import replace
from datetime import timedelta, datetime, timezone
from unittest.mock import Mock
import psycopg
import pytest
from naver_API.collector import collect_season
from naver_API.store import SeasonStore
from naver_API.queries import read_published, read_compare
from naver_API.services import trend_body, select_keywords
from naver_API.client import NaverRateLimited
from tests.unit.naver.test_contracts import DAY, CONFIG, ads, trend, client
from product_data_collector.persistence.migrations import apply_migration
from coupang_API.workflows.best import collect_best
from coupang_API.config.best import BestConfig
from tests.unit.coupang.test_best import product, payload


@pytest.fixture
def season_db(db):
    apply_migration(db,'stage5_coupang_best')
    apply_migration(db,'stage6_naver_season')
    return db


def rows(db,sql,args=()):
    with db.transaction(): return db.execute(sql,args).fetchall()


def test_missing_migration(db):
    c=client()
    with pytest.raises(psycopg.errors.UndefinedTable): collect_season(db,c,CONFIG,DAY)
    c.keywords.assert_not_called()


def test_snapshot_idempotent_and_null_growth(season_db):
    c=client()
    assert collect_season(season_db,c,CONFIG,DAY)=='completed'
    result=read_published(season_db,6)
    assert len(result['items'])==10 and len(result['themes'])==3
    assert result['items'][0]['monthlySearchCount']==360 and result['items'][0]['growthRate']==100
    assert result['periodStart']==DAY-timedelta(days=56)
    assert c.keywords.call_count==3 and c.trend.call_count==6
    assert collect_season(season_db,c,CONFIG,DAY)=='completed'
    assert c.keywords.call_count==3
    assert len(rows(season_db,'SELECT * FROM naver_season_requests'))==9
    assert apply_migration(season_db,'stage6_naver_season') is False
    assert read_published(season_db,999)['items']==[]


@pytest.mark.parametrize('failure',['api','429','timeout','malformed'])
def test_request_resume_and_last_good(season_db,failure):
    collect_season(season_db,client(),CONFIG,DAY-timedelta(days=1))
    c=client()
    error={'results':[]} if failure=='malformed' else (NaverRateLimited('limit') if failure=='429' else RuntimeError('transport'))
    c.trend.side_effect=[trend(trend_body([r['keyword'] for r in select_keywords(ads())[:5]],DAY-timedelta(days=56),DAY-timedelta(days=1))),error]
    if failure=='429': assert collect_season(season_db,c,CONFIG,DAY)=='paused'
    else:
        with pytest.raises(Exception): collect_season(season_db,c,CONFIG,DAY)
    assert read_published(season_db,6)['snapshotDate']==DAY-timedelta(days=1)
    c=client()
    assert collect_season(season_db,c,CONFIG,DAY)=='completed'
    assert c.keywords.call_count==2 and c.trend.call_count==5


def test_budget_shared_across_dates_resume(season_db):
    small=replace(CONFIG,trend_daily_calls=1)
    c=client()
    assert collect_season(season_db,c,small,DAY)=='paused'
    assert collect_season(season_db,c,small,DAY)=='paused'
    assert c.trend.call_count==1
    assert read_published(season_db,6) is None
    assert collect_season(season_db,c,CONFIG,DAY)=='completed'
    assert c.keywords.call_count==3 and c.trend.call_count==6
    tiny=replace(CONFIG,ad_daily_calls=3)
    assert collect_season(season_db,client(),tiny,DAY-timedelta(days=1))=='paused'


def test_month_budget(season_db):
    c=client(); small=replace(CONFIG,trend_monthly_calls=1)
    assert collect_season(season_db,c,small,DAY)=='paused'
    assert c.trend.call_count==1


def test_scope_and_concurrent_lock(season_db,db_factory):
    s=SeasonStore(season_db); s.begin(DAY,CONFIG)
    with pytest.raises(ValueError): collect_season(season_db,client(),replace(CONFIG,themes=((1,'a',1),(2,'b',2),(3,'c',3))),DAY)
    other=db_factory()
    with other.transaction(): other.execute('SELECT pg_advisory_lock(724639118607)')
    c=client()
    with pytest.raises(RuntimeError): collect_season(season_db,c,CONFIG,DAY)
    c.keywords.assert_not_called()


def test_save_rollback_and_incomplete_publish(season_db):
    s=SeasonStore(season_db); s.begin(DAY,CONFIG)
    p=ads(); p['keywordList'][0]['monthlyPcQcCnt']='bad'
    with pytest.raises(ValueError): s.save(DAY,6,0,{},p,datetime.now(timezone.utc))
    assert rows(season_db,'SELECT * FROM naver_season_items')==[]
    assert not s.done(DAY,6,0)
    with pytest.raises(ValueError): s.publish(DAY)
    assert collect_season(season_db,client(),CONFIG,DAY)=='completed'


def test_publication_db_failure_pending_only_recovery(season_db):
    collect_season(season_db,client(),CONFIG,DAY-timedelta(days=1))
    with season_db.transaction():
        season_db.execute("ALTER TABLE naver_season_batches ADD CONSTRAINT test_publish CHECK(snapshot_date<>DATE '2026-11-19' OR status<>'completed')")
    with pytest.raises(psycopg.errors.CheckViolation): collect_season(season_db,client(),CONFIG,DAY)
    assert read_published(season_db,6)['snapshotDate']==DAY-timedelta(days=1)
    with season_db.transaction(): season_db.execute('ALTER TABLE naver_season_batches DROP CONSTRAINT test_publish')
    c=client(); collect_season(season_db,c,CONFIG,DAY)
    c.keywords.assert_not_called(); c.trend.assert_not_called()


@pytest.mark.parametrize('coupang_state',['absent','old','failed','completed'])
def test_compare_dates_scope_spaces_and_distinct_products(season_db,coupang_state):
    collect_season(season_db,client(),CONFIG,DAY)
    if coupang_state!='absent':
        p=product(1,1); p['productName']='good 상 품12'
        cfg=BestConfig(((1010,'one'),(2020,'two')),10,10)
        c=Mock(best_products=Mock(return_value=payload(p)))
        if coupang_state=='failed':
            c.best_products.side_effect=[payload(p),RuntimeError('fail')]
            with pytest.raises(RuntimeError): collect_best(season_db,c,cfg,DAY)
        else: collect_best(season_db,c,cfg,DAY if coupang_state=='completed' else DAY-timedelta(days=1))
    result=read_compare(season_db,6)
    if coupang_state=='completed':
        assert result['comparisonStatus']=='available'
        assert result['matchedKeywords'][0]['keyword']=='상품 12'
        assert result['matchedKeywords'][0]['matchedProductCount']==1
        assert len(result['unmatchedKeywords'])==9 and len(result['comparedCategories'])==2
    else:
        assert result['comparisonStatus']=='coupang_unavailable'
        assert result['matchedKeywords']==result['unmatchedKeywords']==[]
        assert len(result['unavailableKeywords'])==10


def test_null_missing_trends_can_publish(season_db):
    c=client()
    c.trend.side_effect=lambda body: dict(body,results=[])
    collect_season(season_db,c,CONFIG,DAY)
    assert all(i['growthRate'] is None and i['growthStatus']=='missing_group' for i in read_published(season_db,6)['items'])


def test_out_of_order_and_legacy_preserved(season_db):
    with season_db.transaction():
        season_db.execute("INSERT INTO products(platform,external_product_id,product_name,first_seen_at,last_collected_at) VALUES ('coupang','legacy','legacy',now(),now())")
    before=rows(season_db,'SELECT * FROM products')
    collect_season(season_db,client(),CONFIG,DAY)
    collect_season(season_db,client(),CONFIG,DAY-timedelta(days=1))
    assert read_published(season_db,6)['snapshotDate']==DAY
    assert rows(season_db,'SELECT * FROM products')==before


def test_commit_ack_loss_request_and_publish(season_db, monkeypatch):
    from contextlib import contextmanager
    class LostAck:
        def __init__(self, conn): self.conn=conn; self.depth=0; self.lose=False
        def execute(self,*args,**kwargs): return self.conn.execute(*args,**kwargs)
        @contextmanager
        def transaction(self):
            self.depth+=1
            try:
                with self.conn.transaction(): yield
            finally:
                self.depth-=1
            if self.lose and self.depth==0:
                self.lose=False
                raise RuntimeError('commit acknowledgement lost')
    s=SeasonStore(season_db); s.begin(DAY,CONFIG)
    ack=LostAck(season_db); s=SeasonStore(ack); ack.lose=True
    with pytest.raises(RuntimeError): s.save(DAY,6,0,{},ads(),datetime.now(timezone.utc))
    assert s.done(DAY,6,0)
    s.save(DAY,6,0,{},ads(),datetime.now(timezone.utc))
    original_publish=SeasonStore.publish
    def lost_publish(self, day):
        ack.lose=True
        return original_publish(s,day)
    with monkeypatch.context() as patch:
        patch.setattr(SeasonStore,'publish',lost_publish)
        with pytest.raises(RuntimeError): collect_season(season_db,client(),CONFIG,DAY)
    before=read_published(season_db,6)
    assert before is not None
    assert collect_season(season_db,client(),CONFIG,DAY)=='completed'
    assert read_published(season_db,6)==before


def test_real_constraint_rollback_checkpoint(season_db):
    s=SeasonStore(season_db); s.begin(DAY,CONFIG)
    with season_db.transaction():
        season_db.execute("ALTER TABLE naver_season_items ADD CONSTRAINT test_block CHECK(rank<>5)")
    with pytest.raises(psycopg.errors.CheckViolation): s.save(DAY,6,0,{},ads(),datetime.now(timezone.utc))
    assert rows(season_db,'SELECT * FROM naver_season_items')==[] and not s.done(DAY,6,0)
    with season_db.transaction(): season_db.execute('ALTER TABLE naver_season_items DROP CONSTRAINT test_block')
    collect_season(season_db,client(),CONFIG,DAY)


def test_publication_revalidates_actual_rows(season_db):
    s=SeasonStore(season_db); s.begin(DAY,CONFIG)
    c=client()
    for code,_,month in CONFIG.themes:
        s.save(DAY,code,0,{'event':code,'month':month},c.keywords(code,month),datetime.now(timezone.utc))
        for part in (1,2):
            body=trend_body(s.words(DAY,code,part),DAY-timedelta(days=56),DAY-timedelta(days=1))
            s.save(DAY,code,part,body,c.trend(body),datetime.now(timezone.utc))
    with season_db.transaction():
        season_db.execute('UPDATE naver_season_items SET growth_status=NULL,growth_rate=NULL WHERE theme_code=6 AND rank=1')
    with pytest.raises(ValueError): s.publish(DAY)
    assert read_published(season_db,6) is None
