import sys

import pytest

from scripts import rehearse_migrations as rehearsal


@pytest.mark.parametrize('dsn', [
    'host=127.0.0.1 port=5432 dbname=collector_operations_copy',
    'host=production port=55432 dbname=collector_operations_copy',
    'host=127.0.0.1 port=55432 dbname=product_data_collector',
    'host=127.0.0.1 hostaddr=192.0.2.1 port=55432 dbname=collector_operations_copy',
])
def test_rehearsal_rejects_nonisolated_targets_before_connection(monkeypatch, dsn):
    monkeypatch.setenv('REHEARSAL_TEST_DSN', dsn)
    monkeypatch.setattr(sys, 'argv', ['rehearsal', '--dsn-env', 'REHEARSAL_TEST_DSN', '--execute'])
    monkeypatch.setattr(rehearsal.psycopg, 'connect', lambda *_a, **_k: pytest.fail('Must not connect'))
    with pytest.raises(SystemExit) as exc:
        rehearsal.main()
    assert exc.value.code == 2


def test_rehearsal_plan_does_not_resolve_credentials_or_connect(monkeypatch, capsys):
    monkeypatch.delenv('UNDEFINED_REHEARSAL_DSN', raising=False)
    monkeypatch.setattr(sys, 'argv', ['rehearsal', '--dsn-env', 'UNDEFINED_REHEARSAL_DSN'])
    monkeypatch.setattr(rehearsal.psycopg, 'connect', lambda *_a, **_k: pytest.fail('Must not connect'))
    rehearsal.main()
    assert '"execute": false' in capsys.readouterr().out
