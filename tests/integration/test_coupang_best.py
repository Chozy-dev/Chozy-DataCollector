from datetime import date, datetime, timezone
from unittest.mock import Mock

import psycopg
import pytest

from coupang_API.config.best import BestConfig
from coupang_API.persistence.best import BestStore, read_published
from coupang_API.services.best import parse_best_records
from tests.unit.coupang.test_best import product, payload
from coupang_API.workflows.best import collect_best
from product_data_collector.persistence.migrations import apply_migration
from scripts.export_product_lookup import _db_rows

DAY=date(2026,11,19)
PREV=date(2026,11,18)
CONFIG=BestConfig(((1010,'fixture'),(2020,'other')),10,10)


def test_missing_migration_fails_before_api_without_automatic_ddl(db):
    c=client()
    with pytest.raises(psycopg.errors.UndefinedTable):
        collect_best(db,c,CONFIG,DAY)
    c.best_products.assert_not_called()
    assert query(db,"SELECT to_regclass('coupang_best_batches') AS table_name")[0]['table_name'] is None


def test_lookup_export_works_without_best_migration(db):
    with db.transaction():
        db.execute("""INSERT INTO products(platform,external_product_id,product_name,first_seen_at,last_collected_at)
            VALUES ('ownerclan','W1','supplier name',now(),now()),
                   ('coupang','legacy-product','legacy name',now(),now())""")
        with db.cursor() as cursor:
            rows = list(_db_rows(cursor, None, None))
            assert list(_db_rows(cursor, "coupang", None)) == []

    assert [(row["external_product_id"], row["source"]) for row in rows] == [("W1", "postgres.products")]


@pytest.fixture
def bestdb(db):
    apply_migration(db,'stage5_coupang_best')
    return db


def client(p=None):
    return Mock(best_products=Mock(return_value=p or payload()))


def query(db,sql,args=()):
    with db.transaction():
        return db.execute(sql,args).fetchall()


def test_daily_snapshot_short_categories_and_idempotent(bestdb):
    c=client(payload(*[product(i+1,i+1) for i in range(81)]))
    assert collect_best(bestdb,c,CONFIG,DAY)=='completed'
    assert len(query(bestdb,'SELECT * FROM coupang_best_items'))==162
    assert collect_best(bestdb,c,CONFIG,DAY)=='completed'
    assert c.best_products.call_count==2
    result=read_published(bestdb,1010)
    assert len(result['categories'])==2 and len(result['items'])==81
    assert result['updatedAt'] is not None
    assert result['items'][0]['rankStatus'] is None
    assert result['items'][0]['comparisonStatus']=='previous_unavailable'


def test_all_movements_from_exact_previous_completed_day(bestdb):
    collect_best(bestdb,client(payload(product(1,3),product(2,1),product(3,2))),CONFIG,PREV)
    collect_best(bestdb,client(payload(product(1,1),product(2,4),product(3,2),product(4,3))),CONFIG,DAY)
    items=read_published(bestdb,1010)['items']
    assert {i['productId']:(i['previousRank'],i['rankChange'],i['rankStatus']) for i in items}=={
        1:(3,2,'UP'),2:(1,-3,'DOWN'),3:(2,0,'SAME'),4:(None,None,'NEW')}


@pytest.mark.parametrize('kind',['absent','failed','paused','old'])
def test_unavailable_previous_never_new(bestdb,kind):
    if kind=='old':
        collect_best(bestdb,client(),CONFIG,date(2026,11,17))
    elif kind!='absent':
        s=BestStore(bestdb)
        s.begin(PREV,CONFIG)
        s.save_category(PREV,1010,parse_best_records(payload(),1010),payload(),datetime.now(timezone.utc))
        s.stop(PREV,kind)
    collect_best(bestdb,client(),CONFIG,DAY)
    item=read_published(bestdb,1010)['items'][0]
    assert item['rankStatus'] is None and item['rankChange'] is None and item['previousRank'] is None


def test_budget_pause_durable_resume_and_last_publication(bestdb):
    collect_best(bestdb,client(),CONFIG,PREV)
    small=BestConfig(CONFIG.categories,1,10)
    c=client()
    assert collect_best(bestdb,c,small,DAY)=='paused'
    assert read_published(bestdb,1010)['snapshotDate']==PREV
    assert collect_best(bestdb,c,small,DAY)=='paused'
    assert c.best_products.call_count==1
    assert collect_best(bestdb,c,CONFIG,DAY)=='completed'
    assert c.best_products.call_count==2
    assert read_published(bestdb,1010)['snapshotDate']==DAY


def test_lookup_export_uses_latest_completed_batch_across_categories(bestdb):
    collect_best(bestdb, client(payload(product(99, 1))), CONFIG, date(2026, 11, 17))
    published = client()
    published.best_products.side_effect = [
        payload(dict(product(1, 2), productName="lower rank"), product(2, 1)),
        payload(dict(product(1, 1), productName="higher rank"), product(3, 2)),
    ]
    assert collect_best(bestdb, published, CONFIG, PREV) == "completed"
    assert collect_best(bestdb, client(payload(product(98, 1))), BestConfig(CONFIG.categories, 1, 10), DAY) == "paused"

    with bestdb.cursor() as cursor:
        rows = list(_db_rows(cursor, "coupang", 2))
        all_rows = list(_db_rows(cursor, None, None))

    assert [(row["external_product_id"], row["source"]) for row in all_rows] == [
        ("1", "postgres.coupang_best_published"),
        ("2", "postgres.coupang_best_published"),
        ("3", "postgres.coupang_best_published"),
    ]
    assert [row["external_product_id"] for row in rows] == ["1", "2"]
    assert rows[0]["product_name"] == "higher rank"


@pytest.mark.parametrize('failure',['api','rcode','malformed'])
def test_failure_resume_only_pending_category(bestdb,failure):
    c=client()
    error=RuntimeError('API failure') if failure=='api' else ({'rCode':'error'} if failure=='rcode' else {'rCode':'0','data':[]})
    c.best_products.side_effect=[payload(),error]
    with pytest.raises(Exception):
        collect_best(bestdb,c,CONFIG,DAY)
    assert read_published(bestdb,1010) is None
    assert query(bestdb,'SELECT status FROM coupang_best_batches')[0]['status']=='failed'
    c=client()
    assert collect_best(bestdb,c,CONFIG,DAY)=='completed'
    assert c.best_products.call_count==1


def test_db_constraint_rollback_category_checkpoint(bestdb):
    s=BestStore(bestdb)
    s.begin(DAY,CONFIG)
    records=parse_best_records(payload(product(1,1),product(2,2)),1010)
    records[1]['rank']=1  # real PostgreSQL unique constraint failure after first insert
    with pytest.raises(psycopg.errors.UniqueViolation):
        s.save_category(DAY,1010,records,payload(),datetime.now(timezone.utc))
    assert query(bestdb,'SELECT * FROM coupang_best_items')==[]
    assert query(bestdb,'SELECT status FROM coupang_best_categories WHERE category_id=1010')[0]['status']=='pending'
    assert collect_best(bestdb,client(),CONFIG,DAY)=='completed'


def test_publication_rechecks_row_count_and_rolls_back(bestdb):
    s=BestStore(bestdb)
    s.begin(DAY,CONFIG)
    s.save_category(DAY,1010,parse_best_records(payload(),1010),payload(),datetime.now(timezone.utc))
    with pytest.raises(ValueError):
        s.publish(DAY)
    assert read_published(bestdb,1010) is None
    s.save_category(DAY,2020,parse_best_records(payload(),2020),payload(),datetime.now(timezone.utc))
    with bestdb.transaction():
        bestdb.execute('UPDATE coupang_best_categories SET product_count=2 WHERE category_id=2020')
    with pytest.raises(ValueError):
        s.publish(DAY)
    assert query(bestdb,'SELECT rank_status FROM coupang_best_items')[0]['rank_status'] is None
    assert read_published(bestdb,1010) is None


def test_scope_change_refused_and_legacy_preserved(bestdb):
    with bestdb.transaction():
        row=bestdb.execute("""INSERT INTO products(platform,external_product_id,product_name,first_seen_at,last_collected_at)
            VALUES ('coupang','legacy-product','legacy name',now(),now()) RETURNING id""").fetchone()
        bestdb.execute("""INSERT INTO product_history(product_id,observed_at,change_type,prices)
            VALUES (%s,now(),'initial','{"legacy":123}'::jsonb)""",(row['id'],))
        bestdb.execute("""INSERT INTO product_search_ranks(platform,collected_at,keyword,external_product_id,rank)
            VALUES ('coupang',now(),'legacy-keyword','legacy-product',1)""")
    before={t:query(bestdb,f'SELECT * FROM {t}') for t in ('products','product_history','product_search_ranks')}
    s=BestStore(bestdb)
    s.begin(DAY,CONFIG)
    with pytest.raises(ValueError):
        collect_best(bestdb,client(),BestConfig(((3030,'changed'),),10,10),DAY)
    collect_best(bestdb,client(),CONFIG,DAY)
    for t,rows in before.items():
        assert query(bestdb,f'SELECT * FROM {t}')==rows
    assert apply_migration(bestdb,'stage5_coupang_best') is False


def test_concurrent_run_rejected(bestdb,db_factory):
    other=db_factory()
    with other.transaction():
        other.execute("SELECT pg_advisory_lock(hashtextextended(%s,0))",('coupang_best:'+DAY.isoformat(),))
    c=client()
    with pytest.raises(RuntimeError,match='already running'):
        collect_best(bestdb,c,CONFIG,DAY)
    c.best_products.assert_not_called()


def test_commit_ack_loss_category_and_publication_resume(bestdb):
    class LostAck:
        def __init__(self,conn):
            self.conn=conn
            self.lose=False
        def execute(self,*args,**kwargs):
            return self.conn.execute(*args,**kwargs)
        def transaction(self):
            from contextlib import contextmanager
            @contextmanager
            def tx():
                with self.conn.transaction():
                    yield
                if self.lose:
                    self.lose=False
                    raise RuntimeError('commit acknowledgement lost')
            return tx()
    s=BestStore(bestdb)
    s.begin(DAY,CONFIG)
    ack=LostAck(bestdb)
    s=BestStore(ack)
    records=parse_best_records(payload(),1010)
    ack.lose=True
    with pytest.raises(RuntimeError):
        s.save_category(DAY,1010,records,payload(),datetime.now(timezone.utc))
    s.save_category(DAY,1010,records,payload(),datetime.now(timezone.utc))
    s.save_category(DAY,2020,records,payload(),datetime.now(timezone.utc))
    ack.lose=True
    with pytest.raises(RuntimeError):
        s.publish(DAY)
    before=read_published(bestdb,1010)
    assert collect_best(bestdb,client(),CONFIG,DAY)=='completed'
    assert read_published(bestdb,1010)==before
    assert len(query(bestdb,'SELECT * FROM coupang_best_items'))==2


def test_latest_batch_does_not_mix_old_category(bestdb):
    collect_best(bestdb,client(),CONFIG,PREV)
    collect_best(bestdb,client(),BestConfig(((3030,'new scope'),),10,10),DAY)
    result=read_published(bestdb,1010)
    assert result['snapshotDate']==DAY and result['items']==[]
    assert result['categories']==[{'categoryId':3030,'categoryName':'new scope'}]


def test_publication_db_failure_rolls_back_comparisons_then_retries(bestdb):
    collect_best(bestdb,client(payload(product(1,2))),CONFIG,PREV)
    with bestdb.transaction():
        bestdb.execute("""ALTER TABLE coupang_best_batches ADD CONSTRAINT test_block_publish
            CHECK (snapshot_date <> DATE '2026-11-19' OR status <> 'completed')""")
    with pytest.raises(psycopg.errors.CheckViolation):
        collect_best(bestdb,client(payload(product(1,1))),CONFIG,DAY)
    assert read_published(bestdb,1010)['snapshotDate']==PREV
    assert all(r['rank_status'] is None for r in query(bestdb,
        'SELECT rank_status FROM coupang_best_items WHERE snapshot_date=%s',(DAY,)))
    assert query(bestdb,'SELECT status FROM coupang_best_batches WHERE snapshot_date=%s',(DAY,))[0]['status']=='failed'
    with bestdb.transaction():
        bestdb.execute('ALTER TABLE coupang_best_batches DROP CONSTRAINT test_block_publish')
    c=client()
    assert collect_best(bestdb,c,CONFIG,DAY)=='completed'
    c.best_products.assert_not_called()
    assert read_published(bestdb,1010)['items'][0]['rankStatus']=='UP'


def test_out_of_order_publication_does_not_replace_newer_day(bestdb):
    collect_best(bestdb,client(),CONFIG,DAY)
    first=read_published(bestdb,1010)
    collect_best(bestdb,client(),CONFIG,PREV)
    assert read_published(bestdb,1010)==first
    # Already published comparisons remain immutable even when yesterday arrives late.
    assert first['items'][0]['rankStatus'] is None
