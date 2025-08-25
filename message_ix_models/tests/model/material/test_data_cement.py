from typing import TYPE_CHECKING

import pytest

from message_ix_models.model.material.data_cement import gen_data_cement
from message_ix_models.testing import bare_res

if TYPE_CHECKING:
    from message_ix import Scenario
    from pytest import FixtureRequest

    from message_ix_models import Context


@pytest.fixture
def scenario(request: "FixtureRequest", test_context: "Context") -> "Scenario":
    """Same fixture as in :mod:`.test_tools`."""
    test_context.model.regions = "R12"
    return bare_res(request, test_context, solved=False)


#: Expected parameters and number of values.
EXP_LEN = {
    "addon_conversion": 1368,
    "bound_activity_lo": 96,
    "bound_activity_up": 144,
    "capacity_factor": 6048,
    "demand": 168,
    "emission_factor": 6912,
    "fix_cost": 6048,
    "growth_new_capacity_up": 336,
    "historical_activity": 60,
    "historical_new_capacity": 60,
    "initial_new_capacity_up": 336,
    "input": 16416,
    "inv_cost": 1320,
    "output": 9504,
    "technical_lifetime": 1848,
}


@pytest.mark.usefixtures("ssp_user_data")
def test_gen_data_cement(scenario: "Scenario") -> None:
    result = gen_data_cement(scenario, dry_run=False)

    # Data is generated for expected parameters
    assert set(EXP_LEN) == set(result)

    for name, df in result.items():
        # Data have the expected length
        assert EXP_LEN[name] == len(df), f"Wrong length for {name!r}"

        # Valid parameter data: no NaNs anywhere
        assert not df.isna().any(axis=None), f"NaN entries for {name!r}"

    # TODO Extend assertions
