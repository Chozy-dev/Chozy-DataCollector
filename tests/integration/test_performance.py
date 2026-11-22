"""Synthetic isolated DB measurements. No external API or production load."""
import json
import time
import tracemalloc
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest
from product_data_collector.persistence.migrations import apply_migration
from product_data_collector.persistence.scoring import run_scoring
from product_data_collector.operations.fence import certify_collection


@pytest.mark.parametrize('size',[100,1000,5000])
def test_scaled_initial_incremental(db,size):
    for name in ('stage7_supply_scoring','stage8_operations'): apply_migration(db,name)
    at=datetime.now(timezone.utc)
    before=db.execute('SELECT pg_current_wal_insert_lsn() AS lsn').fetchone()['lsn']
    started=time.perf_counter()
    db.execute("""INSERT INTO products(platform,external_product_id,product_name,first_seen_at,last_collected_at,
        current_supply_price,supply_price_status,supply_price_basis,sellable,average_shipping_days,
        seller_average_satisfaction,seller_review_count)
        SELECT 'domeggook',i::text,'fixture '||i,%s,%s,100,'source_contract','uniform_option_final',true,1,'96%%',100
        FROM generate_series(1,%s) i""",(at,at,size))
    db.commit()
    insert_seconds=time.perf_counter()-started
    certify_collection(db,'domeggook','fixture-only')
    tracemalloc.start()
    started=time.perf_counter()
    result=run_scoring(db,run_id='initial',mode='initial',platform='domeggook',as_of=at+timedelta(seconds=1),chunk_size=100)
    initial_seconds=time.perf_counter()-started
    _,peak=tracemalloc.get_traced_memory();tracemalloc.stop()
    assert result['processed']==size
    db.execute("UPDATE products SET product_name='changed' WHERE platform='domeggook' AND external_product_id='1'");db.commit()
    certify_collection(db,'domeggook','fixture-only')
    started=time.perf_counter()
    updated=run_scoring(db,run_id='incremental',mode='incremental',platform='domeggook',as_of=at+timedelta(seconds=2),chunk_size=100)
    assert updated['processed']==1
    incremental_seconds=time.perf_counter()-started
    row=db.execute("""SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),%s) AS wal_bytes,
        (SELECT sum(pg_total_relation_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
         WHERE n.nspname=current_schema() AND c.relkind='r') AS table_index_bytes""",(before,)).fetchone()
    measurement=dict(size=size,insert_seconds=insert_seconds,initial_seconds=initial_seconds,
        incremental_seconds=incremental_seconds,python_peak_bytes=peak,**row)
    path=Path('artifacts/experiments/scoring-smoke');path.mkdir(parents=True,exist_ok=True)
    (path/f'{size}.json').write_text(json.dumps(measurement,default=str,indent=2),encoding='utf8')
