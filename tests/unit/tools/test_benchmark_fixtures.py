"""Pipeline fixtures use real normalization but never authenticate real suppliers."""
from argparse import Namespace
from datetime import timedelta
import pytest
from scripts.benchmarks.pipeline import AT, fixture, validate_config
from product_data_collector.domain.current_state import project_current, option_states


def config(**kw):
    return Namespace(size=1000,seed=917,max_options=5,vendors=30,skew=.2,change_rate=.01,history_changes=4,**kw)


@pytest.mark.parametrize('size',[0,100001,1000000])
def test_refuses_unstaged_or_excess_scale(size):
    cfg=config();cfg.size=size
    with pytest.raises(ValueError):validate_config(cfg)


@pytest.mark.parametrize('platform',['domeggook','ownerclan'])
def test_fixture_identity_prices_options_and_missing_fields(platform,monkeypatch):
    import socket
    monkeypatch.setattr(socket,'create_connection',lambda *a,**k:pytest.fail('Offline fixture attempted network'))
    cfg=config()
    eligible=0
    for i in range(1,101):
        row=fixture(i,platform,AT,cfg,5)
        assert row==fixture(i,platform,AT,cfg,5)
        assert row['source_specific']['fixture_only'] is True
        options=option_states(platform,row['options_payload'],row['prices_payload'])
        assert len(options)<=cfg.max_options
        assert len({o['option_identity'] for o in options})==len(options)
        current=project_current(row)
        if platform=='ownerclan' and i%10 in (0,8): assert current['sellable'] is True
        eligible+=int(current['sellable'] is True and current['current_supply_price'] is not None)
        if i%10==6:assert current['current_supply_price'] is None
        if platform=='ownerclan' and i%10!=9:
            assert row['source_specific']['vendorKey'].startswith('SYNTHETIC_ONLY_')

    assert eligible>0, 'Fixture must exercise nonempty publication'


def test_price_change_and_long_stockout_are_scenarios():
    cfg=config()
    for platform in ['domeggook','ownerclan']:
        old=project_current(fixture(4,platform,AT-timedelta(days=1),cfg,5))
        new=project_current(fixture(4,platform,AT,cfg,6))
        assert fixture(4,platform,AT,cfg,5)['prices_payload']!=fixture(4,platform,AT,cfg,6)['prices_payload']
        long=project_current(fixture(2,platform,AT,cfg,5))
        assert long['sellable'] is False


def test_events_agree_with_snapshot_stock():
    from scripts.benchmarks.pipeline import stock_semantic
    cfg=config()
    for scenario in (2,3,4,5):
        for phase in range(6):
            if scenario!=4 and phase not in (0,1):continue
            row=fixture(scenario,'ownerclan',AT,cfg,phase)
            assert (row['status']=='soldout')==(stock_semantic(scenario,phase)=='soldout')
