from datetime import date, timedelta
from unittest.mock import Mock
import pytest
import requests
from naver_API.config import SeasonConfig, load_config
from naver_API.services import count, select_keywords, growth_results, trend_body
from naver_API.client import NaverClient, NaverAPIError, NaverRateLimited
from naver_API.__main__ import main

DAY = date(2026,11,19)
CONFIG = SeasonConfig(((6,'winter',11),(5,'fixture5',12),(8,'fixture8',1)),100,100,1000,0.001,0.001)


def ads():
    return {'keywordList':[{'relKeyword':f'상품 {i}','monthlyPcQcCnt':i*10,'monthlyMobileQcCnt':i*20} for i in range(1,13)]}


def trend(body):
    start = date.fromisoformat(body['startDate'])
    return dict(body, results=[{'title':g['groupName'],'keywords':g['keywords'],
        'data':[{'period':str(start+timedelta(days=i)),'ratio':10 if i<28 else 20} for i in range(56)]}
        for g in body['keywordGroups']])


def client():
    return Mock(keywords=Mock(return_value=ads()),trend=Mock(side_effect=trend))


@pytest.mark.parametrize('value',[True,-1,None,1.5,'bad','< 11'])
def test_invalid_count(value):
    with pytest.raises(ValueError):
        count(value)


@pytest.mark.parametrize('value',['<10','< 10',' <  10 ', '<\t10'])
def test_small(value):
    assert count(value) is None


def test_top_exact_counts():
    p=ads()
    p['keywordList'] += [{'relKeyword':'소량','monthlyPcQcCnt':'< 10','monthlyMobileQcCnt':999999}]
    result=select_keywords(p)
    assert len(result)==10 and result[0]['total']==360 and result[0]['keyword']=='상품 12'
    assert count(10)==10 and count('10')==10
    p['keywordList'][0]['relKeyword']='상품 2'
    with pytest.raises(ValueError):
        select_keywords(p)


@pytest.mark.parametrize('kind',['valid','zero','missing','duplicate','outside','nan','missing_group'])
def test_growth(kind):
    start=DAY-timedelta(days=56); end=DAY-timedelta(days=1)
    body=trend_body(['word'],start,end); p=trend(body)
    points=p['results'][0]['data']
    if kind=='zero':
        for item in points[:28]: item['ratio']=0
    if kind=='missing': points.pop()
    if kind=='duplicate': points[-1]=points[0]
    if kind=='outside': points[-1]['period']=str(DAY)
    if kind=='nan': points[-1]['ratio']=float('nan')
    if kind=='missing_group': p['results']=[]
    rate,status=growth_results(p,['word'],start,end)['word']
    if kind=='valid': assert rate==100 and status=='available'
    else: assert rate is None and status!='available'


def test_relative_scale_and_shuffled_group():
    start=DAY-timedelta(days=56); end=DAY-timedelta(days=1)
    body=trend_body(['a','b'],start,end); p=trend(body)
    p['results'].reverse()
    for point in p['results'][0]['data']: point['ratio']*=2
    assert all(v==(100,'available') for v in growth_results(p,['a','b'],start,end).values())
    p['endDate']=str(DAY)
    with pytest.raises(ValueError): growth_results(p,['a','b'],start,end)


@pytest.mark.parametrize('status',[401,403,429,500,302])
def test_http_status_and_no_retry(status):
    session=Mock(); session.request.return_value.status_code=status
    c=NaverClient('ad','secret','customer','hub','hubsecret',CONFIG,session)
    with pytest.raises(NaverRateLimited if status==429 else NaverAPIError): c.keywords(6,11)
    assert session.request.call_count==1


def test_auth_separation_timeout_and_sanitization():
    session=Mock(); session.request.return_value.status_code=200
    session.request.return_value.json.return_value=ads()
    c=NaverClient('ad','secret','customer','hub','hubsecret',CONFIG,session)
    c.keywords(6,11)
    ad=session.request.call_args.kwargs
    assert ad['params']=={'event':6,'month':11,'showDetail':1}
    assert 'X-Signature' in ad['headers'] and 'X-NCP-APIGW-API-KEY' not in ad['headers']
    c.trend({})
    hub=session.request.call_args.kwargs
    assert hub['headers']['X-NCP-APIGW-API-KEY']=='hubsecret' and 'X-API-KEY' not in hub['headers']
    assert hub['timeout']==(10,60) and hub['allow_redirects'] is False
    session.request.side_effect=requests.Timeout('secret')
    with pytest.raises(NaverAPIError) as err: c.trend({})
    assert 'secret' not in str(err.value)


@pytest.mark.parametrize('body',[[],{'error':{}},{'errId':'x'},{'errorCode':'x'}])
def test_provider_errors(body):
    session=Mock(); session.request.return_value.status_code=200; session.request.return_value.json.return_value=body
    with pytest.raises(NaverAPIError): NaverClient('a','b','c','d','e',CONFIG,session).trend({})


def test_config_and_offline_cli(monkeypatch):
    config=load_config('naver_API/season.example.yaml',DAY)
    assert len(config.themes)==3
    monkeypatch.setattr('psycopg.connect',Mock(side_effect=AssertionError('DB forbidden')))
    monkeypatch.setattr('requests.Session.request',Mock(side_effect=AssertionError('HTTP forbidden')))
    assert main(['--config','naver_API/season.example.yaml','--dsn-env','MISSING'])==0
    with pytest.raises(KeyError): load_config('naver_API/season.example.yaml',date(2027,1,1))
    with pytest.raises(ValueError): SeasonConfig(CONFIG.themes[:2],1,1,1,1,1)
