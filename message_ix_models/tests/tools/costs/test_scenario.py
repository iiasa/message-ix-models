from typing import TYPE_CHECKING

import pytest

from message_ix_models import testing
from message_ix_models.tools.costs import Config
from message_ix_models.tools.costs.scenario import (
    filter_fix_cost_by_lifetime,
    replace_pre_base_year_cost,
    update_scenario_costs,
)

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models import Context


@pytest.fixture(scope="module")
def config() -> "Config":
    return Config()


@pytest.fixture
def scenario(request: "pytest.FixtureRequest", test_context: "Context") -> "Scenario":
    # Code only functions with R12
    test_context.model.regions = "R12"
    return testing.bare_res(request, test_context)


def test_filter_fix_cost_by_lifetime(scenario: "Scenario") -> None:
    # Function runs without error
    # TODO Expand by adding actual data to `scenario`
    filter_fix_cost_by_lifetime(scenario)


@pytest.mark.parametrize(
    "par",
    (
        "fix_cost",
        "inv_cost",
        pytest.param("var_cost", marks=pytest.mark.xfail(raises=ValueError)),
    ),
)
def test_replace_pre_base_year_cost(
    scenario: "Scenario", config: "Config", par: str
) -> None:
    # Function runs without error
    replace_pre_base_year_cost(scenario, config, par)


@pytest.mark.usefixtures("ssp_user_data")
def test_update_scenario_costs(scenario: "Scenario", config: "Config") -> None:
    # Function runs without error
    update_scenario_costs(scenario, config)
