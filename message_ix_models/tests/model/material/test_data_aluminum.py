from typing import TYPE_CHECKING

import pytest

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.data_aluminum import (
    gen_alumina_trade_tecs,
    gen_data_aluminum,
    gen_refining_hist_act,
    load_bgs_data,
)
from message_ix_models.testing import bare_res

if TYPE_CHECKING:
    from message_ix import Scenario
    from pytest import FixtureRequest

    from message_ix_models import Context


@pytest.fixture
def scenario(request: "FixtureRequest", test_context: "Context") -> "Scenario":
    """Same fixture as in :mod:`.test_tools`."""
    test_context.model.regions = "R12"
    scen = bare_res(request, test_context, solved=False)
    # add R12_GLB region required for aluminum trade model
    with scen.transact():
        scen.add_set("node", "R12_GLB")
    return scen


#: Expected parameters and number of values.
EXP_LEN = {
    "var_cost": 700,
    "inv_cost": 756,
    "technical_lifetime": 756,
    "input": 7444,
    "output": 5666,
    "fix_cost": 1560,
    "capacity_factor": 2064,
    "growth_activity_lo": 392,
    "growth_activity_up": 194,
    "bound_activity_up": 100,
    "bound_activity_lo": 70,
    "historical_new_capacity": 196,
    "fixed_new_capacity": 143,
    "historical_activity": 245,
    "relation_lower": 156,
    "relation_activity": 2640,
    "relation_upper": 168,
    "demand": 168,
}


@pytest.mark.usefixtures("ssp_user_data")
def test_gen_data_aluminum(scenario: "Scenario") -> None:
    result = gen_data_aluminum(scenario, dry_run=False)

    # Data is generated for expected parameters
    assert set(EXP_LEN) == set(result)

    for name, df in result.items():
        # Data have the expected length
        assert EXP_LEN[name] == len(df), f"Wrong length for {name!r}"

        # Valid parameter data: no NaNs anywhere
        assert not df.isna().any(axis=None), f"NaN entries for {name!r}"

    # TODO Extend assertions


@pytest.mark.parametrize("commodity", ["aluminum", "alumina"])
def test_load_bgs_data(commodity):
    out = load_bgs_data(commodity)

    # assert that there is an ISO 3166-1 alpha-3 code and R12 region
    # assigned to every timeseries row
    assert not out[["R12", "ISO"]].isna().any(axis=None)


def test_gen_refining_hist_act():
    out = gen_refining_hist_act()
    for v in out.values():
        assert not v.isna().any(axis=None)
    print()


def test_gen_alumina_trade_tecs():
    info = ScenarioInfo()
    info.set["node"] = ["node0", "node1", "R12_GLB"]
    info.set["year"] = [2020, 2025]
    out = gen_alumina_trade_tecs(info)
    for v in out.values():
        assert not v.isna().any(axis=None)
