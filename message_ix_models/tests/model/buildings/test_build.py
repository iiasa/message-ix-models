import pytest

from message_ix_models import Context
from message_ix_models.model.buildings.build import get_prices_B
from message_ix_models.testing import bare_res


def test_get_prices_B(
    request: pytest.FixtureRequest, mock_buildings_context: Context
) -> None:
    """:func:`get_prices_B` runs on a scenario with no solution."""
    scenario = bare_res(request, mock_buildings_context)
    get_prices_B(scenario, mock_buildings_context.buildings.sturm_input_dir)
