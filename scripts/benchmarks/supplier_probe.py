"""Measure a genuine first stockout of a previously available certified peer."""
import json
from datetime import timedelta
from scripts.benchmarks.pipeline import AT,OUT,connect,guard,measure_run,ProductEvent,save_events,fixture,_save_product_batch_with_retry
for size in (1000,10000):
    guard();path=OUT/f'{size}.json';x=json.loads(path.read_text(encoding='utf8'));schema=f'collection_pipeline_{size}_v3'
    with connect(schema) as c:
        peer=c.execute("SELECT p.* FROM products p JOIN scoring_supplier_identity i ON i.vendor_key=p.source_specific->>'vendorKey' AND i.verified WHERE p.platform='ownerclan' AND substring(p.external_product_id from 4)::int % 10=0 AND i.vendor_key<>'SYNTHETIC_ONLY_V0' AND NOT EXISTS(SELECT 1 FROM product_events e WHERE e.product_id=p.id AND e.semantic='soldout') ORDER BY p.id LIMIT 1").fetchone()
        vendor=peer['source_specific']['vendorKey']
        from product_data_collector.persistence.scoring import history_metrics
        assert history_metrics(c,peer,AT+timedelta(days=6))['stockout_count']==0
        sample=c.execute('SELECT sample FROM scoring_supplier_samples WHERE vendor_key=%s',(vendor,)).fetchone()['sample']
        assert any(z['id']==peer['id'] and not z['soldout'] for z in sample)
        before_bad=sum(z['soldout'] for z in sample)
        c.commit()
        # A new source event can precede detail refresh; it independently invalidates supplier evidence.
        # Update status/quantity atomically to the matching collected current state.
        from argparse import Namespace
        row=fixture(int(peer['external_product_id'][3:]),'ownerclan',AT+timedelta(days=6),Namespace(**x['config']),0)
        row['status']='soldout';row['stock_quantity']=0;row['inventory_payload']['stockQuantity']=0
        for option in row['options_payload']:
            option['quantity']=0;option['status']='soldout'
        _save_product_batch_with_retry(c,[row])
        save_events(c,[ProductEvent('ownerclan',peer['external_product_id'],'SYNTHETIC_FIXTURE_ONLY','soldout',AT+timedelta(days=6),AT+timedelta(days=6),'soldout',{'fixture_only':True,'first_stockout_peer':True})],request_refresh=False)
        rec=measure_run(c,schema,size,'ownerclan','incremental','supplier-first-stockout-v2',AT+timedelta(days=6,hours=1))
        after=c.execute('SELECT sample FROM scoring_supplier_samples WHERE vendor_key=%s',(vendor,)).fetchone()['sample'];after_bad=sum(z['soldout'] for z in after)
        assert after_bad==before_bad+1
        x['supplier_rate_change']={'peer_id':peer['id'],'vendor':vendor,'before_bad':before_bad,'after_bad':after_bad,'sample_count':len(after),'run':rec}
        x['runs'].append(rec);c.commit()
    path.write_text(json.dumps(x,default=str,indent=2),encoding='utf8')
