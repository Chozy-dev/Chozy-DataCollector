from datetime import datetime, timezone
import pytest
from product_data_collector.operations.runner import slots
from product_data_collector.operations.budget import BudgetSession, BudgetStopped


def test_kst_midnight_and_catchup():
    cfg={'catchup_days':1,'schedules':{'naver':{'at':'00:30'}}}
    result=list(slots(cfg,datetime(2026,11,19,16,tzinfo=timezone.utc)))
    assert [t.isoformat() for _,t in result]==['2026-11-18T15:30:00+00:00','2026-11-19T15:30:00+00:00']


@pytest.mark.parametrize('task',['domeggook','ownerclan','coupang','naver','comparison'])
def test_configurable_daily_tasks(task):
    cfg={'catchup_days':0,'schedules':{task:{'at':'13:00'}}}
    assert len(list(slots(cfg,datetime(2026,11,19,6,tzinfo=timezone.utc))))==1


def test_discovery_interval():
    cfg={'catchup_days':10,'schedules':{'ownerclan-discovery':{'at':'01:00','every_days':7}}}
    assert 1<=len(list(slots(cfg,datetime(2026,11,19,tzinfo=timezone.utc))))<=2


@pytest.mark.parametrize('spec',[{'at':'00:00','every_days':0},{'at':'00:00+09:00'}])
def test_invalid_schedules(spec):
    with pytest.raises(ValueError): list(slots({'schedules':{'naver':spec}},datetime.now(timezone.utc)))


def test_offline_default(capsys):
    from product_data_collector.operations.__main__ import main
    assert main(['plan','--config','product_data_collector/operations/example.yaml'])==0
    assert '"execute": false' in capsys.readouterr().out
