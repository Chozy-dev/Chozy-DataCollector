from decimal import Decimal
from unittest.mock import Mock

import pytest

from coupang_API.api.client import (BestCategoryRequest, CoupangPartnersClient,
    CoupangApiError, CoupangHttpError, CoupangResponseError, build_best_uri)
from coupang_API.config.best import BestConfig, load_best_config
from coupang_API.services.best import parse_best_records, rank_movement


def product(pid=5000000001, rank=7):
    return dict(productId=pid, rank=rank, productName='test', productPrice=4440.0,
                productUrl='https://example.test/p', productImage='https://example.test/i',
                isRocket=False, categoryName='fixture', keyword='fixture')


def payload(*products):
    return dict(rCode='0',rMessage='OK',data=list(products) or [product()])


def test_best_parser_rank_price_raw_and_short_response():
    p = payload(*[product(i+1,81-i) for i in range(81)])
    records = parse_best_records(p,1010)
    assert len(records) == 81
    assert records[0]['rank'] == 81
    assert records[0]['price'] == Decimal('4440.0')
    assert records[0]['is_rocket'] is False
    assert records[0]['raw'] == p['data'][0]
    assert parse_best_records(payload(),1010)[0]['product_id'] == 5000000001


@pytest.mark.parametrize('data',[None,{},[],[None],{'productData':[product()]}])
def test_best_invalid_array(data):
    with pytest.raises(ValueError):
        parse_best_records(dict(rCode='0',data=data),1)


@pytest.mark.parametrize('field,value', [('rank',None),('rank',True),('rank',1.5),('rank',0),
    ('productId',None),('productId',False),('productPrice',None),('productPrice','NaN'),
    ('productPrice',-1),('productPrice','1.123'),('isRocket','false'),('productName',''),
    ('productUrl',None),('productImage',None)])
def test_best_malformed_product(field,value):
    p=product()
    p[field]=value
    with pytest.raises((ValueError,TypeError)):
        parse_best_records(payload(p),1)


def test_best_duplicate_rank_and_over_limit():
    for p in (payload(product(),product()),payload(product(1,1),product(2,1))):
        with pytest.raises(ValueError):
            parse_best_records(p,1)
    with pytest.raises(ValueError):
        parse_best_records(payload(product(1,1),product(2,2)),1,1)


def test_best_repeated_product_keeps_original_highest_rank():
    highest = {**product(1, 2), "productPrice": 5000}
    rows = parse_best_records(payload(product(1, 7), product(2, 3), highest, product(1, 10)), 1013)
    assert [(row["product_id"], row["rank"]) for row in rows] == [(1, 2), (2, 3)]
    assert rows[0]["raw"] == highest
    assert rows[0]["price"] == Decimal(5000)


@pytest.mark.parametrize('rank,previous,available,expected',[
    (1,3,True,(2,'UP')),(4,1,True,(-3,'DOWN')),(2,2,True,(0,'SAME')),
    (1,None,True,(None,'NEW')),(1,None,False,(None,None))])
def test_rank_comparison(rank,previous,available,expected):
    assert rank_movement(rank,previous,available)==expected


@pytest.mark.parametrize('status,body,error',[(200,{'rCode':'1'},CoupangResponseError),
    (200,[],CoupangApiError),(403,{},CoupangHttpError),(429,{},CoupangHttpError),
    (500,{},CoupangHttpError)])
def test_best_http_one_attempt(status,body,error):
    response=Mock(status_code=status,reason='error',json=Mock(return_value=body))
    session=Mock(get=Mock(return_value=response))
    client=CoupangPartnersClient('a','b',session=session,rate_limiter=Mock())
    with pytest.raises(error):
        client.best_products(BestCategoryRequest(1010))
    assert session.get.call_count == 1


def test_best_invalid_json_and_network():
    response=Mock(status_code=200,json=Mock(side_effect=ValueError()))
    client=CoupangPartnersClient('a','b',session=Mock(get=Mock(return_value=response)),rate_limiter=Mock())
    with pytest.raises(CoupangApiError):
        client.best_products(BestCategoryRequest(1010))
    import requests
    client._session.get.side_effect=requests.Timeout()
    with pytest.raises(CoupangApiError):
        client.best_products(BestCategoryRequest(1010))


def test_best_uri_and_configuration(tmp_path):
    assert build_best_uri(BestCategoryRequest(1010)).endswith('/1010?limit=100')
    for req in (BestCategoryRequest(0),BestCategoryRequest(1,101)):
        with pytest.raises(ValueError):
            build_best_uri(req)
    for categories in ((),((1,'one'),(1,'two'))):
        with pytest.raises(ValueError):
            BestConfig(categories,2,2)
    p=tmp_path/'best.yaml'
    p.write_text('categories: [{id: 1010, name: fixture}]\nmax_calls: 2\nrequests_per_minute: 1',encoding='utf8')
    assert load_best_config(p).categories == ((1010,'fixture'),)
    # Default is an offline plan and never loads credentials or opens PostgreSQL.
    from coupang_API.__main__ import main as entry

    assert entry(['--config',str(p),'--dsn-env','NONEXISTENT_DSN']) == 0
    with pytest.raises(SystemExit) as error:
        entry(['--mode', 'legacy-search', '--config', str(p), '--dsn-env', 'NONEXISTENT_DSN'])
    assert error.value.code == 2


def test_best_success_uses_existing_auth_and_explicit_budget_limiter():
    session=Mock(get=Mock(return_value=Mock(status_code=200,json=Mock(return_value=payload()))))
    limiter=Mock()
    c=CoupangPartnersClient('access','secret',rate_limiter=limiter,session=session)
    assert c.best_products(BestCategoryRequest(1010))==payload()
    args,kwargs=session.get.call_args
    assert args[0].endswith('/bestcategories/1010?limit=100')
    assert kwargs['headers']['Authorization'].startswith('CEA ')
    limiter.wait.assert_called_once()
    with pytest.raises(ValueError,match='explicitly configured'):
        CoupangPartnersClient('a','b',session=session).best_products(BestCategoryRequest(1010))
