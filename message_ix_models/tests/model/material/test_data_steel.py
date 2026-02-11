from typing import TYPE_CHECKING

import pytest

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.data_steel import (
    gen_data_steel,
    gen_dri_act_bound,
    gen_dri_coal_model,
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

    # add R12_GLB region required for steel trade model
    with scen.transact():
        scen.add_set("node", "R12_GLB")
    return scen


#: Expected parameters and number of values.
EXP_LEN = {
    "input": 133684,
    "soft_activity_up": 156,
    "historical_activity": 200,
    "bound_new_capacity_lo": 40,
    "growth_activity_lo": 14,
    "addon_conversion": 4068,
    "growth_activity_up": 1142,
    "fix_cost": 1404,
    "demand": 168,
    "initial_new_capacity_up": 336,
    "historical_new_capacity": 209,
    "bound_activity_up": 201,
    "level_cost_activity_soft_up": 312,
    "var_cost": 23499,
    "relation_lower": 193,
    "capacity_factor": 14688,
    "soft_activity_lo": 156,
    "abs_cost_activity_soft_up": 312,
    "bound_activity_lo": 39,
    "emission_factor": 33804,
    "relation_activity": 2750,
    "initial_activity_up": 822,
    "growth_new_capacity_up": 912,
    "output": 56980,
    "relation_upper": 193,
    "bound_new_capacity_up": 93,
    "inv_cost": 576,
    "technical_lifetime": 4608,
}


@pytest.mark.usefixtures("ssp_user_data")
def test_gen_data_steel(scenario: "Scenario") -> None:
    result = gen_data_steel(scenario, dry_run=False)

    # Data is generated for expected parameters
    assert set(EXP_LEN) == set(result)

    for name, df in result.items():
        # Data have the expected length
        assert EXP_LEN[name] == len(df), f"Wrong length for {name!r}"

        # Valid parameter data: no NaNs anywhere
        assert not df.isna().any(axis=None), f"NaN entries for {name!r}"

    # TODO Extend assertions


def test_gen_dri_coal_model() -> None:
    info = ScenarioInfo()
    info.set["node"] = ["node0", "node1"]
    info.set["year"] = [2020, 2025]
    par_dict = gen_dri_coal_model(info)
    for k, v in par_dict.items():
        assert not v.isna().any(axis=None)  # Completely full


def test_gen_dri_act_bound() -> None:
    par_dict = gen_dri_act_bound()
    for k, v in par_dict.items():
        assert not v.isna().any(axis=None)  # Completely full
