"""Offline report for daily_change_sample.py. Never calls APIs or databases."""
import argparse
import csv
import json
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime
import math
from decimal import Decimal, InvalidOperation

ROOT=Path(__file__).resolve().parents[2]
OUT=ROOT/'artifacts/experiments/daily-change-2026-11-20'

def classify(source, events):
    kinds={e.get('kind') if source=='ownerclan' else e.get('status') for e in events}
    if source=='ownerclan':
        price=bool(kinds & {'priceIncreased','priceDecreased'})
        stock=bool(kinds & {'soldout','restocked','optionSoldout','optionRestocked'})
        product_stock=bool(kinds & {'soldout','restocked'})
        option=any(k and k.startswith('option') for k in kinds)
        shipping=any(k and ('shipping' in k.lower()) for k in kinds)
        other=bool(kinds-{'priceIncreased','priceDecreased','soldout','restocked','optionSoldout','optionRestocked'})
        score=price or stock or option or bool(kinds & {'stockChanged','optionStockChanged','prohibited','discontinued','boxQuantityChanged'})
    else:
        price='AMT' in kinds; stock=bool(kinds & {'SOLDOUT','RESTART','OPTSOLDOUT','OPTRESTART'})
        product_stock=bool(kinds & {'SOLDOUT','RESTART'})
        option=bool(kinds & {'OPTAMT','OPTSOLDOUT','OPTRESTART'})
        shipping=False; other=bool(kinds & {'OPEN','CLOSE','DEL'})
        score=price or stock or option or bool(kinds & {'OPEN','CLOSE','DEL'})
    verified_price=False
    if source=='ownerclan':
        for e in events:
            if e.get('kind') in {'priceIncreased','priceDecreased'}:
                try:
                    before,after=Decimal(e['valueBefore']),Decimal(e['valueAfter'])
                    verified_price |= before.is_finite() and after.is_finite() and before!=after
                except (InvalidOperation,TypeError):pass
    return dict(changed=bool(kinds),initial_score_candidate=source=='domeme' and 'OPEN' in kinds,
        base_price_signal=price,base_price_verified=verified_price,stock_signal=stock,
        product_stock_signal=product_stock,option_signal=option,shipping_signal=shipping,
        other_signal=other,score_possible=score,kinds=sorted(kinds))

def main():
    global OUT
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir',type=Path,help='Restored full survey directory containing result.json')
    args=parser.parse_args()
    if args.input_dir:
        OUT=args.input_dir.resolve()
    if not (OUT/'result.json').is_file():
        parser.error('Full survey inputs were archived; restore them and pass --input-dir')
    data=json.loads((OUT/'result.json').read_text(encoding='utf-8'))
    summaries={}; products=data['products']; categories=data['categories']
    for source in ['domeme','ownerclan']:
        feed=data.get('events',{}).get(source,{})
        mapping=defaultdict(list)
        for e in feed.get('rows',[]):
            mapping[str(e['itemKey'] if source=='ownerclan' else e['no'])].append(e)
        rows=[p for p in products if p['source']==source]
        for p in rows:
            p.update(classify(source,mapping.get(p['id'],[])))
            p['history_complete']=p['history_complete'] or bool(feed.get('complete'))
            p['change_status']='changed' if p['changed'] else 'no_captured_event' if p['history_complete'] else 'unknown'
        known=[p for p in rows if p['history_complete'] or p['changed']]
        n=len(rows); unknown=n-len(known)
        crows=[c for c in categories if c['source']==source]
        counts=Counter(str(c['stratum']) for c in crows if c.get('status')=='complete')
        catmap={c['category']:c for c in crows}
        selected_counts=Counter(p['category'] for p in rows)
        for p in rows:
            h=str(catmap[p['category']]['stratum'])
            p['weight']=data['population'][source]['strata'][h]/counts[h]*p['total']/selected_counts[p['category']]
        metrics={}
        for metric in ['changed','initial_score_candidate','base_price_signal','base_price_verified','stock_signal','product_stock_signal','option_signal','shipping_signal','other_signal','score_possible']:
            yes=sum(p[metric] for p in rows)
            mk=[p for p in rows if p['history_complete'] or p[metric]]
            weighted=sum(p['weight']*p[metric] for p in mk)/sum(p['weight'] for p in mk) if mk else None
            metrics[metric]={'observed_products':yes,'known_products':len(mk),'unknown_products':n-len(mk),
                'rate_known':yes/len(mk) if mk else None,
                'lower_bound_sample':yes/n if n else None,'upper_bound_sample':(yes+n-len(mk))/n if n else None,
                'weighted_known':weighted}
            metrics[metric]['available_in_api_scope']=not(source=='domeme' and metric in ('base_price_verified','shipping_signal'))
            if not metrics[metric]['available_in_api_scope']:
                metrics[metric].update(known_products=0,unknown_products=n,rate_known=None,
                    lower_bound_sample=None,upper_bound_sample=None,weighted_known=None)
        calls=[c for c in data['calls'] if c['source']==source and not c.get('in_flight')]
        api={}
        for name in sorted({c['api'] for c in calls}):
            cc=[c for c in calls if c['api']==name]
            duration=max(c['offset']+c['seconds'] for c in cc)-min(c['offset'] for c in cc)
            # Completion of N requests includes the first latency. Also preserve peak rolling 60s starts.
            peak=max(sum(c['offset']<=x['offset']<c['offset']+60 for x in calls) for c in calls)
            api[name]={'calls':len(cc),'average_seconds':sum(c['seconds'] for c in cc)/len(cc),
                'active_span_seconds':duration,'span_rpm':len(cc)/duration*60 if duration else 0,
                'source_peak_rolling_60_calls':peak,'timeouts':sum(c.get('error')=='ReadTimeout' for c in cc)}
        summaries[source]={'categories_attempted':len(crows),'categories_complete':sum(c.get('status')=='complete' for c in crows),
            'population_daily_change_rate':None,
            'scope':'nine getAllSupplyChk signals only' if source=='domeme' else 'itemHistories API events',
            'full_modification_unknown_rate':1.0 if source=='domeme' else unknown/n if n else None,
            'unavailable_metrics':['all_modifications','actual_price_change','shipping_changes'] if source=='domeme' else ['actual_score_recalculation_rate','supplier_peer_count'],
            'categories_nonempty':sum(bool(c.get('selected')) for c in crows),'sample_products':n,'known':len(known),
            'unknown':unknown,'unknown_rate':unknown/n if n else None,'history_feed_complete':feed.get('complete',False),
            'event_rows':len(feed.get('rows',[])),'event_unique_products':len(mapping),
            'metrics':metrics,'api':api,'category_strata':dict(Counter(str(c['stratum']) for c in crows))}
        global_classes=[classify(source,es) for es in mapping.values()]
        summaries[source]['feed_unique_product_types']={k:sum(c[k] for c in global_classes) for k in metrics}
        summaries[source]['feed_kind_events']=dict(Counter(e.get('kind',e.get('status')) for e in feed.get('rows',[])))
    (OUT/'analysis.json').write_text(json.dumps(summaries,ensure_ascii=False,indent=2),encoding='utf-8')
    for name,rows in [('products',products),('categories',categories),('calls',data['calls'])]:
        keys=list(dict.fromkeys(k for r in rows for k in r))
        with (OUT/f'{name}.csv').open('w',encoding='utf-8-sig',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=keys);writer.writeheader()
            writer.writerows({k:json.dumps(v,ensure_ascii=False) if isinstance(v,(dict,list)) else v for k,v in r.items()} for r in rows)
    print(json.dumps(summaries,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
