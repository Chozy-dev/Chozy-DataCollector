"""Offline checks of survey bounds and random sampling; no credentials or database."""
import random
import time
import pytest

from scripts.probes import daily_change_sample as module

def test_stratification_preserves_ratio_and_seed():
    rows=[{'id':i,'depth':3 if i<1409 else 4} for i in range(4363)]
    a=module.stratified(rows,lambda c:c['depth'],200,random.Random(20261120))
    b=module.stratified(rows,lambda c:c['depth'],200,random.Random(20261120))
    assert a==b and len({c['id'] for c in a})==200
    assert sum(c['depth']==3 for c in a)==65

@pytest.mark.parametrize('elapsed,calls,blocked',[(1586,0,False),(0,800,False),(0,0,True)])
def test_guards_stop_before_new_calls(elapsed,calls,blocked):
    s=module.Survey.__new__(module.Survey)
    s.started=time.monotonic()-elapsed;s.counts={'ownerclan':calls};s.blocked={'ownerclan'} if blocked else set()
    with pytest.raises(module.StopSurvey):s.guard('ownerclan')

def test_cursor_prefix_never_becomes_product_sample():
    class Client:
        def __init__(self):self.page=0
        def graphql(self,q):
            self.page+=1
            return {'allItems':{'edges':[{'node':{'key':str(self.page)}}],
                'pageInfo':{'hasNextPage':True,'endCursor':str(self.page)}}}
    s=module.Survey.__new__(module.Survey)
    s.result={'categories':[],'products':[]};s.clients={'ownerclan':Client()}
    s.owner_category({'key':'test','fullName':'root>leaf'})
    assert not s.result['products']
    assert s.result['categories'][0]['status']=='incomplete_no_sample'

def test_event_scope_and_verified_supply_price():
    from scripts.probes import daily_change_report as report
    dome=report.classify('domeme',[{'status':'AMT'}])
    assert dome['base_price_signal'] and not dome['base_price_verified']
    owner=report.classify('ownerclan',[{'kind':'priceIncreased','valueBefore':'100','valueAfter':'101'},
        {'kind':'optionSoldout'},{'kind':'optionSoldout'}])
    assert owner['base_price_verified'] and owner['stock_signal'] and not owner['product_stock_signal']
    assert report.classify('ownerclan',[{'kind':'nameChanged'}])['score_possible'] is False
