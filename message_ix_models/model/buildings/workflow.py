"""Tools for integrating MESSAGEix-Buildings in :mod:`message_ix_models` workflows."""

import logging
from dataclasses import replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models import Context
    from message_ix_models.model.buildings.config import Config

log = logging.getLogger(__name__)


def navigate(
    context: "Context",
    scenario: "Scenario",
    navigate_scenario: str,
    config: dict | None = None,
) -> "Scenario":
    """NAVIGATE-style workflow step for invoking MESSAGEix-Buildings.

    This is identical to :func:`.project.navigate.build_solve_buildings`. It works by
    invoking :func:`.model.buildings.build_and_solve`; see the documentation of that
    function.
    """
    from message_ix_models.model.buildings import build_and_solve, sturm
    from message_ix_models.project.navigate.workflow import BUILDINGS_CONFIG, _strip

    _strip(scenario)

    # Configure
    context.buildings = replace(
        BUILDINGS_CONFIG,
        sturm_scenario=sturm.scenario_name(navigate_scenario),
        **(config or {}),
    )

    return build_and_solve(context)


def ngfs(context: "Context", scenario: "Scenario") -> "Scenario":
    """NGFS Phase 6-style workflow step for invoking MESSAGEix-Buildings.

    This differs from :func:`.navigate` in the following ways:

    - :func:`.model.buildings.build_and_solve` is not used.
    - Instead of :func:`.buildings.get_prices`, call :func:`.get_prices_B`.
    - :func:`.sturm.run` is called directly, instead of through :func:`.build_and_solve`
      → :func:`.pre_solve`. :attr:`.buildings.Config.sturm_method` is set to
      :data:`.sturm.METHOD.RSCRIPT_B`.
    - :func:`.buildings.build.main` is *not* called.
    """
    from . import build, sturm

    config: "Config" = context.buildings
    config.sturm_method = sturm.METHOD.RSCRIPT_B

    price_dir = config.sturm_code_dir.joinpath("data")
    assert price_dir.exists()

    df = build.get_prices_B(scenario, price_dir)

    sturm.run(context, df, first_iteration=False)

    return scenario
