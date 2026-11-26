import json
import requests
import pytest

from scripts.probes import ownerclan_missing_items_probe as probe


def response(raw):
    value=requests.Response()
    value._content=raw
    return value


def test_exact_null_and_empty_array_envelopes_are_distinguished():
    for raw,kind in [(b'{"data":{"itemsByKeys":null}}','NoneType'),
                     (b'{"data":{"itemsByKeys":[]}}','list')]:
        result=probe.describe_response(response(raw))
        assert result['data_field_types']['itemsByKeys']==kind
        assert result['exact_empty_envelope_utf8'].encode()==raw


def test_product_body_is_not_retained_but_keys_and_types_are():
    raw=json.dumps({'data':{'itemsByKeys':[{'key':'A','name':'private-name','price':3,'status':'available'}]}}).encode()
    result=probe.describe_response(response(raw))
    assert 'exact_empty_envelope_utf8' not in result
    assert 'private-name' not in json.dumps(result)
    assert result['operations']['itemsByKeys']['returned_keys']==['A']
    assert result['operations']['itemsByKeys']['products'][0]['field_types']['price']=='int'


def test_invalid_json_and_error_body_do_not_leak_raw_text():
    for raw in (b'private-invalid-body',b'{"errors":[{"message":"private-error"}],"data":{"item":null}}'):
        result=probe.describe_response(response(raw))
        assert 'private-' not in json.dumps(result)
        assert 'exact_empty_envelope_utf8' not in result


def test_offline_plan_never_opens_network_or_loads_credentials(monkeypatch, ownerclan_comparison_plan):
    def forbidden(*args,**kwargs):
        pytest.fail('network/credentials accessed')
    monkeypatch.setattr(probe,'load_credentials',forbidden)
    monkeypatch.setattr(probe,'MeasuredSession',forbidden)
    assert probe.main([])==0
