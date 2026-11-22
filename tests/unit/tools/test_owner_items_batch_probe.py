"""Exercise the archived benchmark transport with real offline request preparation."""
import json

import pytest
import requests

from ownerclan_API.api.client import API_ENDPOINTS, OwnerclanClient
from ownerclan_API.api.queries import item_query, items_by_keys_query
from ownerclan_API.api.rate_limiter import RateLimiter
from scripts.probes.owner_items_batch_probe import GraphqlPostSession


@pytest.mark.parametrize("bulk", [False, True])
def test_legacy_get_and_native_post_reach_the_same_json_transport(bulk):
    sent = []

    class Adapter(requests.adapters.BaseAdapter):
        def send(self, request, **kwargs):
            sent.append(request)
            response = requests.Response()
            response.status_code = 200
            response._content = b'{"data":{"itemsByKeys":[]}}'
            return response

        def close(self):
            pass

    class Provider:
        def token(self):
            return "fixture-token"

    query = items_by_keys_query(["A"]) if bulk else item_query("A")
    with GraphqlPostSession() as session:
        session.mount("https://", Adapter())
        client = OwnerclanClient(Provider(), "production", RateLimiter(0), 45, 0, 90, session=session)
        assert client.graphql(query) == {"itemsByKeys": []}
    assert len(sent) == 1
    assert sent[0].method == "POST"
    assert sent[0].url == API_ENDPOINTS["production"]
    assert json.loads(sent[0].body) == {"query": query}
