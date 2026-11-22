"""Request schema-confirmed option status without changing the minimal fallback."""
import pytest

from ownerclan_API.api.queries import all_items_query, item_query


@pytest.mark.parametrize("minimal", [False, True])
@pytest.mark.parametrize("query", [lambda minimal: item_query("A", minimal=minimal),
                                  lambda minimal: all_items_query(first=1, minimal=minimal)])
def test_option_status_requested_in_full_query_only(query, minimal):
    options = query(minimal).split("options {", 1)[1].split("optionAttributes", 1)[0]
    assert ("status" in options) is (not minimal)
