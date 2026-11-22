import pytest

from scripts.probes import owner_history_resume as resume


def page(times=(10, 9), cursors=('a', 'b'), more=True):
    return {'edges': [{'cursor': c, 'node': {'createdAt': t}} for t, c in zip(times, cursors)],
            'pageInfo': {'hasNextPage': more, 'startCursor': cursors[0] if cursors else None,
                         'endCursor': cursors[-1] if cursors else None}}


def test_boundary_ties_and_terminal_empty_are_valid():
    resume.validate_page(page((10, 10)), None, set(), 11)
    resume.validate_page(page((), (), False), 'b', {'a','b'}, 10)


@pytest.mark.parametrize('data,after,seen,previous', [
    (page((9,10)), None, set(), 11),
    (page(), None, set(), 9),
    (page(), 'a', set(), 11),
    (page(), None, {'b'}, 11),
    (page((), ()), None, set(), 11),
])
def test_discontinuities_stop_before_checkpoint(data, after, seen, previous):
    with pytest.raises(ValueError):
        resume.validate_page(data, after, seen, previous)


@pytest.mark.parametrize('overlap,terminal,expected', [(True,True,True),(False,True,False),(True,False,False)])
def test_completion_requires_boundary_and_terminal(monkeypatch, tmp_path, overlap, terminal, expected):
    node = dict(itemKey='A',kind='soldout',title='',valueBefore='',valueAfter='',createdAt=10)
    older = {**node, 'itemKey':'B', 'createdAt':9}
    tail = ([node] if overlap else []) + [older]
    pages = [{'data': {'edges':[{'node': n} for n in tail]}}]
    monkeypatch.setattr(resume,'OUT',tmp_path)
    monkeypatch.setattr(resume,'load_chain',lambda: ({'window':['lo','hi']},[node,node],0,20,10,
        pages,'last',set(),9,terminal))
    result = resume.report()
    # The expected boundary multiset has two indistinguishable occurrences.
    assert result['complete'] is False
    if overlap:
        pages[0]['data']['edges'].insert(0,{'node':node})
        result = resume.report()
        assert result['complete'] is expected
        assert result['max_observed_multiplicity_event_rows'] == 3
        assert result['unique_events'] == 2
        assert result['unique_changed_products'] == 2


def test_timestamp_recovery_starts_a_new_cursor_scope(monkeypatch, tmp_path):
    import json
    monkeypatch.setattr(resume,'OUT',tmp_path)
    old = {'window':['1970-01-01T00:00:00+00:00','1970-01-01T00:00:01+00:00'],
           'events':{'ownerclan':{'rows':[{'createdAt':10}]*59000}}}
    original_read = resume.read
    monkeypatch.setattr(resume,'read',lambda p: old if p == resume.BASE/'result.json' else original_read(p))
    for number, upper, data in [(1,11,page((10,9))),(2,10,page((9,8),more=False))]:
        (tmp_path/f'page-{number:05d}.json').write_text(json.dumps(
            dict(after=None,dateFrom=-1,dateTo=upper,data=data)),encoding='utf-8')
    (tmp_path/'recoveries.json').write_text(json.dumps([dict(start_page=2,boundary=9,dateTo=10)]),encoding='utf-8')
    *_, pages, after, seen, previous, terminal = resume.load_chain()
    assert len(pages) == 2 and terminal and previous == 8
