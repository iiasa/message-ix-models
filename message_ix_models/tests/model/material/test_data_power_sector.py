from typing import TYPE_CHECKING

import pytest

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.data_power_sector import read_material_intensities
from message_ix_models.testing import bare_res

if TYPE_CHECKING:
    from message_ix import Scenario
    from pytest import FixtureRequest

    from message_ix_models import Context


@pytest.fixture
def scenario(request: "FixtureRequest", test_context: "Context") -> "Scenario":
    """Same fixture as in :mod:`.test_tools`."""
    test_context.model.regions = "R11"
    return bare_res(request, test_context, solved=False)


EXP_LEN = {1: 52416, 2: 145152}


@pytest.mark.parametrize("version", (1, 2))
def test_read_material_intensities(scenario: "Scenario", version) -> None:
    if version == 1:
        # simulate old MESSAGEix-GLOBIOM version by removing proxy technology
        with scenario.transact():
            scenario.remove_set("technology", "solar_res_hist_2000")
    result = read_material_intensities(ScenarioInfo(scenario))

    # Data is generated for expected parameters
    assert EXP_LEN[version] == result.index.size

    # TODO Extend assertions
