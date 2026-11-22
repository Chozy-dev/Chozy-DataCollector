"""Instrumentation must preserve the real worker's missing-key continuation."""
from types import SimpleNamespace

import pytest

from ownerclan_API.api.queries import items_by_keys_query
from scripts.probes import ownerclan_live_e2e as probe


def test_partial_response_is_returned_without_poisoning_transport():
    data = {'itemsByKeys': [{'key': 'A', 'price': 1, 'status': 'available'}]}
    session = SimpleNamespace(stopped=False)
    result = {'calls': [{}]}
    client = probe.SampleClient(SimpleNamespace(graphql=lambda _: data), session, result, lambda: None)
    assert client.graphql(items_by_keys_query(['A', 'B'])) is data
    assert not session.stopped
    assert result['calls'][0]['assessment']['missing'] == 1


def test_complete_response_allows_next_batch():
    data = {'itemsByKeys': [{'key': 'A', 'price': 1, 'status': 'available'}]}
    session = SimpleNamespace(stopped=False)
    client = probe.SampleClient(SimpleNamespace(graphql=lambda _: data), session, {'calls': [{}]}, lambda: None)
    client.graphql(items_by_keys_query(['A']))
    assert not session.stopped


def test_stopped_session_cannot_enter_transport(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('stopped probe entered HTTP transport')
    monkeypatch.setattr(probe.MeasuredSession, 'request', forbidden)
    with probe.ProbeSession({'calls': []}, lambda: None) as session:
        session.stopped = True
        with pytest.raises(probe.StopProbe, match='previous_transport_error'):
            session.post('https://example.invalid')


def test_default_plan_never_runs_live_probe(monkeypatch, capsys):
    monkeypatch.setattr('sys.argv', ['probe'])
    monkeypatch.setattr(probe, 'run', lambda: pytest.fail('default opened live probe'))
    assert probe.main() == 0
    assert '"execute": false' in capsys.readouterr().out


def test_supplemental_response_is_observed_without_changing_worker_policy():
    results = iter([{'itemsByKeys': [{'key': 'A', 'price': 1, 'status': 'available'}]},
                    {'itemsByKeys': [{'key': 'B', 'price': 1, 'status': 'available'}]}])
    session = SimpleNamespace(stopped=False)
    client = probe.SampleClient(SimpleNamespace(graphql=lambda _: next(results)), session, {'calls': [{}]}, lambda: None)
    client.graphql(items_by_keys_query(['A', 'B']))
    assert not session.stopped
    client.graphql(items_by_keys_query(['B']))
    assert not session.stopped


def test_persistent_missing_does_not_block_a_different_batch():
    session = SimpleNamespace(stopped=False)
    client = probe.SampleClient(SimpleNamespace(graphql=lambda _: {'itemsByKeys': []}), session, {'calls': [{}]}, lambda: None)
    client.graphql(items_by_keys_query(['A']))
    assert not session.stopped
    assert client.graphql(items_by_keys_query(['A'])) == {'itemsByKeys': []}
    assert not session.stopped
    assert client.graphql(items_by_keys_query(['B'])) == {'itemsByKeys': []}
    assert not session.stopped
