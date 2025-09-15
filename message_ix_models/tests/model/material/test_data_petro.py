from typing import TYPE_CHECKING

import pytest

from message_ix_models.model.material.data_petro import gen_data_petro_chemicals
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
    "input": 84784,
    "output": 108976,
    "emission_factor": 29568,
    "inv_cost": 2904,
    "fix_cost": 12096,
    "var_cost": 12208,
    "capacity_factor": 13440,
    "technical_lifetime": 2904,
    "growth_activity_lo": 336,
    "growth_activity_up": 333,
    "share_mode_lo": 2,
    "demand": 168,
    "historical_new_capacity": 736,
    "historical_activity": 796,
    "initial_activity_up": 169,
    "bound_activity_lo": 24,
    "bound_activity_up": 38,
    "relation_activity": 1176,
}


@pytest.mark.usefixtures("ssp_user_data")
def test_gen_data_petro_chemicals(scenario: "Scenario") -> None:
    result = gen_data_petro_chemicals(scenario, dry_run=False)

    # Data is generated for expected parameters
    assert set(EXP_LEN) == set(result)

    for name, df in result.items():
        # Data have the expected length
        assert EXP_LEN[name] == len(df), f"Wrong length for {name!r}"

        # Valid parameter data: no NaNs anywhere
        assert not df.isna().any(axis=None), f"NaN entries for {name!r}"

    # TODO Extend assertions
