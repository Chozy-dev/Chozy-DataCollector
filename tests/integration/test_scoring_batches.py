"""Supplier influence boundaries and chunk/single-row equivalence on real SQL."""
from datetime import timedelta
import pytest
from tests.integration.test_scoring import scoring, product, certify, execute, AT
from product_data_collector.persistence.migrations import apply_migration
from product_data_collector.persistence import scoring as m

@pytest.fixture
def optimized(scoring):
    apply_migration(scoring, 'stage10_scoring_performance')
    return scoring

def clear(db):
    db.execute('DELETE FROM scoring_queue'); db.execute('DELETE FROM scoring_supplier_jobs')

@pytest.mark.parametrize('field,value', [('product_name','new'),('image_url','https://fixture.invalid/new'),('current_supply_price',120),('stock_quantity',0),('status','soldout')])
def test_product_only_inputs(optimized,field,value):
    db=optimized;p=product(db,platform='ownerclan',vendor='V');product(db,'B','ownerclan','V');clear(db)
    db.execute(f'UPDATE products SET {field}=%s WHERE id=%s',(value,p['id']))
    assert db.execute('SELECT product_id FROM scoring_queue').fetchall()==[{'product_id':p['id']}]
    assert not db.execute('SELECT * FROM scoring_supplier_jobs').fetchall()
    assert execute(db,platform='ownerclan')['processed']==2
    payload=db.execute('SELECT payload FROM product_supply_scores WHERE product_id=%s',(p['id'],)).fetchone()['payload']
    if field=='current_supply_price': assert payload['supplyPrice']==120
    if field=='product_name': assert payload['productName']=='new'
    if field=='image_url': assert payload['imageUrl']==value

@pytest.mark.parametrize('metric,expected',[('price',[]),('stockout',['V'])])
def test_coverage_scope(optimized,metric,expected):
    db=optimized;p=product(db,platform='ownerclan',vendor='V');clear(db);certify(db,p,metric)
    assert [r['vendor_key'] for r in db.execute('SELECT vendor_key FROM scoring_supplier_jobs').fetchall()]==expected


def test_membership_both_vendors_and_migration_replay(optimized):
    db=optimized;p=product(db,platform='ownerclan',vendor='OLD');clear(db)
    db.execute("UPDATE products SET source_specific=jsonb_set(source_specific,'{vendorKey}','\"NEW\"') WHERE id=%s",(p['id'],))
    assert {r['vendor_key'] for r in db.execute('SELECT vendor_key FROM scoring_supplier_jobs').fetchall()}=={'OLD','NEW'}
    db.commit();assert apply_migration(db,'stage10_scoring_performance') is False

@pytest.mark.parametrize('status,expected',[('available',False),('soldout',True)])
def test_history_experience_propagates(optimized,status,expected):
    db=optimized;p=product(db,platform='ownerclan',vendor='V');clear(db)
    db.execute("INSERT INTO product_history(product_id,observed_at,status,change_type) VALUES(%s,%s,%s,'fixture')",(p['id'],AT,status))
    assert bool(db.execute('SELECT * FROM scoring_supplier_jobs').fetchall())==expected

@pytest.mark.parametrize('semantic,scope,expected',[('supply_price','product',False),('soldout','option',False),('soldout','product',True),('restocked','product',True)])
def test_event_scope(optimized,semantic,scope,expected):
    from product_data_collector.domain.product_events import ProductEvent
    from product_data_collector.persistence.product_events import save_events
    db=optimized;product(db,platform='ownerclan',vendor='V');clear(db)
    e=ProductEvent('ownerclan','A','fixture',semantic,AT,AT,semantic,{},event_scope=scope)
    save_events(db,[e],request_refresh=False)
    assert bool(db.execute('SELECT * FROM scoring_supplier_jobs').fetchall())==expected


def test_chunk_exact_result_and_stale_write(optimized):
    db=optimized
    for i in range(15):
        p=product(db,str(i),'ownerclan','V');certify(db,p);certify(db,p,'price')
    db.execute("INSERT INTO scoring_supplier_identity VALUES('V',%s,%s,true,'{\"fixture_only\":true}')",(AT-timedelta(days=120),AT+timedelta(days=30)))
    rows=db.execute('SELECT p.*,q.generation FROM products p JOIN scoring_queue q ON p.id=q.product_id ORDER BY p.id').fetchall()
    for r in rows:m.score_one(db,r,AT)
    original=db.execute('SELECT * FROM product_supply_scores ORDER BY product_id').fetchall()
    prepared=m.prepare_chunk(db,rows,AT);pending=[]
    for r in rows:m.score_one(db,r,AT,prepared,pending,{})
    m.store_chunk(db,pending,rows)
    assert db.execute('SELECT * FROM product_supply_scores ORDER BY product_id').fetchall()==original
    for r in pending:r['as_of']=AT-timedelta(seconds=1);r['home']=0
    m.store_chunk(db,pending,rows)
    assert db.execute('SELECT * FROM product_supply_scores ORDER BY product_id').fetchall()==original


def test_chunk_preserves_newer_generation(optimized):
    db=optimized;p=product(db)
    rows=db.execute('SELECT p.*,q.generation FROM products p JOIN scoring_queue q ON p.id=q.product_id').fetchall()
    prepared=m.prepare_chunk(db,rows,AT);pending=[];m.score_one(db,rows[0],AT,prepared,pending)
    db.execute("SELECT scoring_enqueue(%s,'newer')",(p['id'],));m.store_chunk(db,pending,rows)
    assert db.execute('SELECT generation FROM scoring_queue').fetchone()['generation']==2
