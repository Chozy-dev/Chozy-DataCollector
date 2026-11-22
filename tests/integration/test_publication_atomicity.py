from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from datetime import timedelta

import pytest
from product_data_collector.persistence.scoring import publish, read_published
from tests.integration.test_scoring import scoring, product, execute, AT


def new_run(db):
    execute(db)
    previous = read_published(db,'DOMEME')
    db.execute("""INSERT INTO scoring_runs(run_id,mode,platform,as_of,high_id,formula_version,scanned)
        SELECT 'next','initial','domeggook',%s,max(id),'supply_v2',true FROM products""", (AT+timedelta(days=1),))
    db.commit()
    run = db.execute("SELECT * FROM scoring_runs WHERE run_id='next'").fetchone()
    db.commit()
    return run, previous


def test_concurrent_direct_publication_and_completed_retry(scoring, db_factory):
    product(scoring)
    run, _ = new_run(scoring)
    barrier = Barrier(2)
    def call(_):
        with db_factory() as c:
            barrier.wait(timeout=10)
            publish(c,run)
            c.commit()
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(call,range(2)))
    publish(scoring,run)
    scoring.commit()
    assert scoring.execute("SELECT count(*) AS n FROM supply_home_items WHERE batch_id='next'").fetchone()['n'] == 1
    assert read_published(scoring,'DOMEME')['batchId'] == 'next'


def test_persisted_incomplete_rows_are_not_silently_mixed(scoring):
    product(scoring)
    run, previous = new_run(scoring)
    scoring.execute("""INSERT INTO supply_home_batches(batch_id,platform,as_of,formula_version)
        VALUES('next','domeggook',%s,'supply_v2')""", (run['as_of'],))
    scoring.execute("""INSERT INTO supply_home_items(batch_id,rank,product_id,payload)
        SELECT 'next',rank,product_id,payload || '{"corrupt_fixture":true}'::jsonb
        FROM supply_home_items WHERE batch_id='first'""")
    scoring.commit()
    with pytest.raises(RuntimeError,match='review batch integrity'):
        publish(scoring,run)
    scoring.rollback()
    assert read_published(scoring,'DOMEME') == previous
    assert scoring.execute("SELECT completed_at FROM supply_home_batches WHERE batch_id='next'").fetchone()['completed_at'] is None
    assert scoring.execute("SELECT payload->>'corrupt_fixture' AS flag FROM supply_home_items WHERE batch_id='next'").fetchone()['flag'] == 'true'
