"""ENGAGE workflow pieces for reuse with :class:`message_ix_models.Workflow`.

These functions emulate the collective behaviour of :class:`.engage.runscript_main`,
:class:`.engage.scenario_runner` and the associated configuration, but are adapted to be
reusable, particularly in the Workflow pattern used in e.g. :mod:`.projects.navigate`.
"""

from dataclasses import dataclass, field
from typing import List, Literal, Optional, Union

from message_ix import Scenario
from message_ix_models import Context
from message_ix_models.util.config import ConfigHelper
from message_ix_models.workflow import Workflow

from message_data.tools.utilities import (
    add_CO2_emission_constraint,
    add_emission_trajectory,
    remove_emission_bounds,
)

from .runscript_main import glb_co2_relation as RELATION_GLOBAL_CO2
from .scenario_runner import ScenarioRunner


@dataclass
class PolicyConfig(ConfigHelper):
    """Configuration for the 3-step ENGAGE workflow for climate policy scenarios."""

    #: Label of the climate policy scenario, often related to a global carbon budget in
    #: Gt CO₂ for the 21st century (varies).
    label: int

    #: Actual quantity of the carbon budget to be imposed, or the value "calc", in which
    #: case the value is calculated from :attr:`label` by
    #: :meth:`.ScenarioRunner.calc_budget`.
    budget: Union[int, Literal["calc"]] = "calc"

    #: Name of a reference scenario for copying demands.
    #:
    #: TODO choose a more informative name.
    low_dem_scen: Optional[str] = None

    #: Which steps of the ENGAGE workflow to run. Empty list = don't run any steps.
    steps: List[int] = field(default_factory=lambda: [1, 2, 3])


def step_1(
    context: Context, scenario: Scenario, policy_config: PolicyConfig
) -> Scenario:
    """Step 1 of the ENGAGE climate policy workflow."""

    remove_emission_bounds(scenario)

    add_CO2_emission_constraint(
        scenario,
        relation_name=RELATION_GLOBAL_CO2,
        constraint_value=0.0,
        type_rel="lower",
        reg=f"{context.regions}_GLB",
    )

    # FIXME calc_budget(…) in turn calls calc_hist_cum_CO2(…) which relies on:
    #
    # scen.timeseries(region="GLB region (R11)", variable="Emissions|CO2")
    #
    # …i.e. it is necessary to run the legacy reporting for this variable name to be
    # produced prior to this step. The method also hard-codes the regional aggregation.
    ScenarioRunner.calc_budget(
        bdgt=policy_config.budget_label,
        method=policy_config.budget_value,
        type_emission="TCE_CO2",
    )

    # TODO add ENGAGE solve steps

    return scenario


def step_2(context: Context, scenario: Scenario) -> Scenario:
    """Step 2 of the ENGAGE climate policy workflow."""
    # Retrieve a pandas.DataFrame with the CO2 emissions trajectory
    #
    # NB this method:
    # - does not use any class or context attributes, so it can be called on any
    #   instance of ScenarioRunner.
    #   TODO separate the method from the class as a stand-alone function
    # - does not require legacy reporting output; only the variable "EMISS", i.e.
    #   `scenario` must have solution data.
    sr = ScenarioRunner(context)
    df = sr.retr_CO2_trajectory(scenario)

    # Add this trajectory as bound_emission values
    add_emission_trajectory(
        scenario,
        data=df,
        type_emission="TCE_CO2",
        unit="Mt C/yr",
        remove_bounds_emission=True,
    )

    with scenario.transact(message="Remove lower bound on global total CO₂ emissions"):
        name = "relation_lower"
        scenario.remove_par(
            name, scenario.par(name, filters={"relation": [RELATION_GLOBAL_CO2]})
        )

    # TODO add ENGAGE solve steps

    return scenario


def step_3(context: Context, scenario: Scenario) -> Scenario:
    """Step 3 of the ENGAGE climate policy workflow."""
    # Retrieve a data frame with CO₂ prices
    #
    # NB this method:
    # - does not use any class or context attributes, so it can be called on any
    #   instance of ScenarioRunner.
    #   TODO separate the method from the class as a stand-alone function
    # - does not require legacy reporting output; only the variable "PRICE_EMISSION",
    #   i.e. `scenario` must have solution data.
    sr = ScenarioRunner(context)
    df = sr.retr_CO2_price(scenario, new_type_emission="TCE_non-CO2")

    with scenario.transact(message="Add price for TCE_non-CO2"):
        scenario.add_par("tax_emission", df)

    # TODO add ENGAGE solve steps

    return scenario


def add_steps(
    workflow: Workflow, base: str, config: PolicyConfig, name: Optional[str] = None
) -> str:
    """Add steps to `workflow` for running ENGAGE scenarios on `base`.

    Parameters
    ----------
    workflow
    base
       Prior workflow step/scenario to start from.
    config
       Depending on the :attr:`~.PolicyConfig.steps` attribute, from 0 to 3 workflow
       steps are added.
    name : str, optional
       Name template for the added steps.

    Returns
    -------
    str
        name of the last workflow step added, or `base` if none are added.
    """
    # Base name for the added steps
    name_root = f"{name or base} + ENGAGE {config.label} step"

    _base = base
    for step in config.steps:
        # Name for the output of this step
        new_name = f"{name_root} {step}"

        # Arguments: only step_1 responds to `config`
        args = dict(policy_config=config) if step == 1 else {}

        # Add step
        workflow.add_step(new_name, _base, globals()[f"step_{step}"], **args)

        # Update the base step/scenario for the next iteration
        _base = new_name

    return _base
