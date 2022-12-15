"""ENGAGE workflow pieces for reuse with :class:`message_ix_models.Workflow`.

These functions emulate the collective behaviour of :class:`.engage.runscript_main`,
:class:`.engage.scenario_runner` and the associated configuration, but are adapted to be
reusable, particularly in the Workflow pattern used in e.g. :mod:`.projects.navigate`.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from iam_units import convert_gwp
from message_ix import Scenario
from message_ix_models import Context, ScenarioInfo
from message_ix_models.util import identify_nodes
from message_ix_models.util.config import ConfigHelper
from message_ix_models.workflow import Workflow

from message_data.scenario_generation.reserve_margin.res_marg import main as res_marg
from message_data.tools.utilities import (
    add_budget,
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
    label: Union[int, str]

    #: Actual quantity of the carbon budget to be imposed, or the value "calc", in which
    #: case the value is calculated from :attr:`label` by
    #: :meth:`.ScenarioRunner.calc_budget`.
    budget: Union[int, Literal["calc"]] = "calc"

    #: Scenario name of a reference scenario for copying demands.
    #:
    #: TODO choose a more informative name.
    low_dem_scen: Optional[str] = None

    #: :obj:`True` to call :func:`.reserve_margin.res_marg.main`.
    reserve_margin: bool = True

    #: Which steps of the ENGAGE workflow to run. Empty list = don't run any steps.
    steps: List[int] = field(default_factory=lambda: [1, 2, 3])

    #: Keyword arguments for :meth:`.message_ix.Scenario.solve`. To replicate the
    #: behaviour of the `macro_params` argument to :meth:`.ScenarioRunner.run`, which in
    #: turn sets the `convergence_issues` argument to :meth:`.ScenarioRunner.solve`,
    #: set max_adjustment to 0.1.
    solve: Dict[str, Any] = field(
        default_factory=lambda: dict(model="MESSAGE-MACRO", max_adjustment=0.2)
    )


def calc_hist_cum_CO2(
    context: Context, scen: Scenario, info: ScenarioInfo
) -> Tuple[float, float]:
    """Calculate historic CO2 emissions.

    Adapted from :meth:`.engage.scenario_runner.ScenarioRunner.calc_hist_cum_CO2`, with
    the following differences:

    - Reported emissions are retrieved for a region with the code "GLB region (R##)",
      based on `context`. This allows the method to work with either R11 or R12 models.

    Returns
    -------
    float
        Cumulative emissions [megatonne CO₂].
    float
        Total duration of historical periods [years].
    """
    # TODO carefully check whether self.scen and scen being different scenarios would
    #      impact behaviour

    # Original comment: Filters years between 2010 and first_model_year
    # TODO clarify the difference between the comment and the code
    years = list(filter(lambda y: 2020 <= y < info.y0, info.set["year"]))

    df = scen.timeseries(
        region=f"GLB region ({context.model.regions})", variable="Emissions|CO2"
    )
    df = df[df.year.isin(years)]

    df_emis = df[["year", "value"]].set_index("year")

    df_duration = scen.par("duration_period", filters={"year": years})[
        ["year", "value"]
    ].set_index("year")

    # Manually set 2018 and 2020 multiplication factor
    df_duration.at[2018, "value"] = 0.5
    df_duration.at[2020, "value"] = 3.5
    # manually set 2018 Value
    df_emis.at[2018, "value"] = 41000
    value = (df_emis * df_duration).value.sum()
    hist_years = df_duration.value.sum()
    return (value, hist_years)


def calc_budget(
    context: Context,
    scenario: Scenario,
    bdgt: int,
    method: Union[int, Literal["calc"]],
    type_emission="TCE",
) -> None:
    """Calculate **and apply** budget.

    Adapted from :meth:`.engage.scenario_runner.ScenarioRunner.calc_budget`, with the
    following differences:

    - :func:`.calc_hist_cum_CO2` above is called, instead of the method of the same name
      on ScenarioRunner.
    - `scenario` is an argument, not retrieved from the `scen` attribute of
      ScenarioRunner.
    - :mod:`iam_units` is used for unit conversion.

    Parameters
    ----------
    bdgt
        Budget in gigatonnes of CO₂ for the period 2010–2100, or the value "calc".
    """
    if method == "calc":
        info = ScenarioInfo(scenario)

        # Target is provided in cumulative Gt 2010-2100
        value = bdgt
        # Convert Gt CO2 to Mt CO2
        value *= 1000

        # NB the original passed ScenarioRunner.base, rather than ScenarioRunner.scen
        # TODO carefully check whether this distinction meant anything
        hist_cum, hist_year = calc_hist_cum_CO2(context, scenario, info)

        value -= hist_cum
        # Conversion to MtC
        value *= convert_gwp(None, (value, "Mt CO2"), "C").magnitude

        # Divide by the number of years covered. "-2" is because in the calculation of
        # the budget we account for 2 more years than the model sees.
        value /= (info.Y[-1] - info.Y[0] + 10) - 2

        # The amount of years over which the budget is distributed should be 82 years
        # (from 2018 to 2100). The number of historic years are subtracted.
        # (do the extra 10 years need to be added)?
        # value /= 82 - hist_year + 10
    else:
        value = method

    add_budget(scenario, value, type_emission=type_emission)


def solve(context: Context, scenario: Scenario, config: PolicyConfig):
    if config.reserve_margin:
        res_marg(scenario)

    var_list = ["I", "C"]
    if config.solve["model"] == "MESSAGE-MACRO":
        var_list.append("GDP")

    scenario.solve(var_list=var_list, **config.solve)

    # Solve was successful; set default version
    scenario.set_as_default()

    return scenario


def step_1(context: Context, scenario: Scenario, config: PolicyConfig) -> Scenario:
    """Step 1 of the ENGAGE climate policy workflow.

    If the :attr:`~.PolicyConfig.method` attribute of `policy_config` is "calc", then
    `scenario` must contain time series data for variable="Emissions|CO2" and
    region="GLB region (R##)"; see :func:`calc_hist_cum_CO2`. This is typically provided
    by the legacy reporting. If the attribute has a float value specifying a budget
    directly, this condition does not apply.
    """
    remove_emission_bounds(scenario)

    context.model.regions = identify_nodes(scenario)
    add_CO2_emission_constraint(
        scenario,
        relation_name=RELATION_GLOBAL_CO2,
        constraint_value=0.0,
        type_rel="lower",
        reg=f"{context.model.regions}_GLB",
    )

    # Calculate **and apply** budget
    calc_budget(
        context,
        scenario,
        bdgt=config.label,
        method=config.budget,
        type_emission="TCE_CO2",
    )

    # commented: need reference scenario for NAVIGATE application
    # transfer_demands(self.load_scenario(config.low_dem_scen), scenario)

    return scenario


def step_2(context: Context, scenario: Scenario, config: PolicyConfig) -> Scenario:
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

    try:
        scenario.remove_solution()
    except ValueError:
        pass  # Solution did not exist

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

    # commented: need reference scenario for NAVIGATE application
    # transfer_demands(self.load_scenario(config.low_dem_scen), scenario)

    return scenario


def step_3(context: Context, scenario: Scenario, config: PolicyConfig) -> Scenario:
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

    try:
        scenario.remove_solution()
    except ValueError:
        pass  # Solution did not exist

    with scenario.transact(message="Add price for TCE_non-CO2"):
        scenario.add_par("tax_emission", df)

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
    workflow.graph["context"].setdefault("run_reporting_only", False)

    # Base name for the added steps
    name_root = f"{name or base} EN"

    # Model and scenario name for the scenario produced by the base step
    # TODO this may not work if the previous step is a passthrough; make more robust
    info = workflow.graph[base][0].scenario_info.copy()

    _base = base
    for step in config.steps:
        # Name for this step
        new_name = f"{name_root}{step}"

        # New scenario name
        s = f"{info['scenario']}_ENGAGE_{config.label.replace(' ', '_')}_step-{step}"

        # Add step; always clone to a new model/scenario name
        workflow.add_step(
            new_name,
            _base,
            globals()[f"step_{step}"],
            clone=dict(shift_first_model_year=2025) if step == 1 else True,
            target=f"{info['model']}/{s}",
            config=config,
        )

        workflow.add_step(f"{new_name} solved", new_name, solve, config=config)

        # Update the base step/scenario for the next iteration
        _base = f"{new_name} solved"

    return _base
