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


def test_read_material_intensities(scenario: "Scenario") -> None:
    result = read_material_intensities(ScenarioInfo(scenario))

    # Data is generated for expected parameters
    assert 52416 == result.index.size

    # TODO Extend assertions
