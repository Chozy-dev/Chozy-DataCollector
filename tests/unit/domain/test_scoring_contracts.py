from datetime import datetime, timezone
from decimal import Decimal

import pytest

from product_data_collector.domain.scoring import (
    stock_score, shipping_score, dome_reliability, owner_reliability,
    weighted, rounded, calculate, vacation_bounds,
)


@pytest.mark.parametrize('qty,expected', [(None,None),(-1,None),(0,0),(1,20),(29,20),(30,40),(99,40),(100,60),(299,60),(300,80),(999,80),(1000,100),(9999,100)])
def test_stock_boundaries(qty, expected):
    assert stock_score(qty)==expected


@pytest.mark.parametrize('platform,days,fast,expected', [
    ('domeggook',None,True,None),('domeggook',0,True,100),('domeggook','0.5',False,100),
    ('domeggook','0.51',False,85),('domeggook',1,True,90),('domeggook','1.01',False,60),
    ('domeggook',2,False,60),('domeggook','2.01',True,35),
    ('ownerclan',0,False,None),('ownerclan',1,False,100),('ownerclan',2,False,75),
    ('ownerclan',3,False,50),('ownerclan',4,False,25)])
def test_shipping_boundaries(platform,days,fast,expected):
    assert shipping_score(platform,days,fast)==expected


@pytest.mark.parametrize('s,n,excellent,t,expected', [
    (None,0,False,None,None),('0%',0,False,None,70),('0%',50,False,None,0),
    ('100%',None,False,None,None),('100%',25,False,None,85),('96%',128,True,None,100),
    ('96%',128,True,'간이과세자',91),('90%',50,False,'개인판매자',80),('101%',10,False,None,None)])
def test_dome_reliability(s,n,excellent,t,expected):
    assert dome_reliability(s,n,excellent,t)==expected


@pytest.mark.parametrize('n,bad,expected', [(0,0,None),(100,5,100),(100,15,80),(100,30,55),(100,50,30),(100,51,10),(10,0,85),(1,1,67)])
def test_owner_reliability(n,bad,expected):
    assert owner_reliability(n,bad)==expected


def test_weights_and_half_up():
    assert rounded(weighted([(94,30),(100,25)]))==97
    assert weighted([(None,30),(0,25)])==0
    assert weighted([(None,30),(None,25)]) is None
    assert rounded(Decimal('74.5'))==75


@pytest.mark.parametrize('count,stock,price', [(0,100,100),(1,70,80),(2,40,50),(3,10,20),(5,10,20)])
def test_history_score_boundaries(count,stock,price):
    result = calculate({'platform':'ownerclan'},datetime.now(timezone.utc),
                       {'stockout_count':count,'price_count':count,'reasons':{}}, {})
    assert result['components']['stockout']==stock
    assert result['components']['price']==price


def test_vacation_inclusive_kst_and_missing():
    p={'platform':'domeggook','stock_quantity':9999,'average_shipping_days':0,
       'source_specific':{'seller':{'vacation':{'start':'2026-11-18','end':'2026-11-19'}}}}
    h={'stockout_count':None,'price_count':None,'reasons':{'stockout':'coverage'}}
    start,end=vacation_bounds(p)
    assert calculate(p,start,h)['stability']==50
    assert calculate(p,end,h)['stability']==100
    assert calculate(p,start,h)['history']['stockout_count'] is None


def test_offline_cli(capsys):
    from product_data_collector.scoring_cli import main
    assert main(['initial','--source','ownerclan','--run-id','x','--as-of','2026-11-19T00:00:00+00:00'])==0
    assert '"execute": false' in capsys.readouterr().out
