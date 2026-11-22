import pytest

from product_data_collector.persistence.access import access_plan
from scripts.rehearse_access import guard


class NoConnection:
    def execute(self, *_):
        pytest.fail('Invalid input must be rejected before DB access')


@pytest.mark.parametrize('kwargs', [
    {'collector':'same','spring':'same'},
    {'collector':''},
    {'collector':'가'*22},
])
def test_access_role_identity_validation(kwargs):
    with pytest.raises(ValueError):
        access_plan(NoConnection(),**kwargs)


@pytest.mark.parametrize('dsn', [
    'host=127.0.0.1 port=5432 dbname=collector_operations_copy',
    'host=127.0.0.1 port=55450 dbname=production',
    'host=remote port=55450 dbname=collector_operations_copy',
    'host=127.0.0.1 hostaddr=192.0.2.1 port=55450 dbname=collector_operations_copy',
])
def test_access_rehearsal_rejects_other_targets(dsn):
    with pytest.raises(ValueError,match='isolated'):
        guard(dsn)


def test_existing_role_is_not_repurposed():
    class Existing:
        def execute(self,*_):
            return self
        def fetchone(self):
            return {'exists':1}
    with pytest.raises(ValueError,match='fresh roles'):
        access_plan(Existing())
