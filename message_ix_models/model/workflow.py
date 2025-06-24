"""Common steps for workflows."""

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from message_ix import Scenario

from message_ix_models.util import identify_nodes
from message_ix_models.util.config import ConfigHelper

if TYPE_CHECKING:
    from typing import TypedDict

    from message_ix_models import Context

    class CommonArgs(TypedDict):
        relation_name: str
        reg: str


log = logging.getLogger(__name__)


@dataclass
class Config(ConfigHelper):
    """Common configuration for model workflows across projects.

    Currently, the three settings are understood by :func:`solve`, which is used in
    ENGAGE, NAVIGATE, :mod:`.transport.workflow`, and possibly other workflows.
    """

    #: Information on an optional, other scenario from which to copy demand data in
    #: :func:`solve` using :func:`transfer_demands`. Default: empty, do nothing.
    demand_scenario: dict = field(default_factory=dict)

    #: :obj:`True` to call :func:`.reserve_margin.res_marg.main` in :func:`solve`.
    reserve_margin: bool = True

    #: Keyword arguments for :meth:`.message_ix.Scenario.solve` via :func:`solve`.
    #:
    #: To replicate the behaviour of the `macro_params` argument to
    #: :meth:`.engage.ScenarioRunner.run`, which in turn sets the `convergence_issues`
    #: argument to :meth:`.engage.ScenarioRunner.solve`, set max_adjustment to 0.1.
    solve: dict[str, Any] = field(
        default_factory=lambda: dict(model="MESSAGE-MACRO", max_adjustment=0.2)
    )


def solve(
    context: "Context",
    scenario: Scenario,
    *,
    config: Optional[Config] = None,
    set_as_default: bool = False,
):
    """Common model solve step for ENGAGE, NAVIGATE, and other workflows.

    The steps respond settings from an optional instance of :class:`Config` passed as a
    keyword argument.

    *Before* `scenario` is solved:

    1. If :attr:`Config.reserve_margin` is :obj:`True`, :func:`.res_marg.main` is
       called.
    2. If :attr:`Config.demand_scenario` is non-empty and ``config.solve["model"]`` is
       ``MESSAGE-MACRO``, then :func:`.transfer_demands` is called to transfer data from
       the indicated scenario to `scenario`. This scenario **must** exist on the same
       :class:`.Platform` as `scenario`; the default version is loaded.

    Then:

    3. `scenario` is solved passing :attr:`Config.solve` as keyword arguments to
       :meth:`.message_ix.Scenario.solve`. The keyword argument `var_list` has ``I``,
       ``C``, and (if ``config.solve["model"]`` is ``MESSAGE-MACRO``) ``GDP`` appended.

    *After* `scenario` is solved without exception:

    4. :meth:`.set_as_default` is called if the keyword argument `set_as_default` is
       :obj:`True`.
    """

    from message_data.scenario_generation.reserve_margin import res_marg
    from message_data.tools.utilities import transfer_demands

    config = config or Config()

    # Set reserve margin values
    if config.reserve_margin:
        res_marg.main(scenario)

    # Explicit list of model variables for which to read data
    var_list = ["I", "C"]

    if config.solve["model"] == "MESSAGE-MACRO":
        var_list.append("GDP")

    if config.demand_scenario:
        # Retrieve DEMAND variable data from a different scenario and set as values
        # for the demand parameter
        source = Scenario(scenario.platform, **config.demand_scenario)
        transfer_demands(source, scenario)

    scenario.solve(var_list=var_list, **config.solve)

    if set_as_default:
        # Solve was successful; set default version
        scenario.set_as_default()

    return scenario


def step_0(context: "Context", scenario: "Scenario", **kwargs) -> "Scenario":
    """Preparation step for climate policy workflows.

    This is similar to (and shares the name of) :func:`.project.engage.workflow.step_0`,
    but uses settings specific to the model structure used in |ssp-scenariomip| at (5)
    and (6).

    1. Remove the model solution.
    2. Call :mod:`.remove_emission_bounds`.
    3. Update :attr:`.Config.regions` to match `scenario`.
    4. Call :mod:`.add_FFI_CO2_accounting`.
    5. Call :func:`.add_AFOLU_CO2_accounting` with the default `method`, currently
       :attr:`METHOD.B <.add_AFOLU_CO2_accounting.METHOD.B>`.
    6. Call :mod:`.add_alternative_TCE_accounting` with the default `method`,
       currently :attr:`METHOD.B <.add_alternative_TCE_accounting.METHOD.B>`.
    7. Call :mod:`.add_CO2_emission_constraint` with :py:`constraint_value=0,
       type_rel="lower"`, effectively preventing negative emissions.

    .. todo:: Merge :func:`.project.engage.workflow.step_0` into this function and
       generalize with appropriate options/parameters.
    """
    from message_ix_models.tools import (
        add_AFOLU_CO2_accounting,
        add_alternative_TCE_accounting,
        add_CO2_emission_constraint,
        add_FFI_CO2_accounting,
        remove_emission_bounds,
    )

    try:
        scenario.remove_solution()
    except ValueError:
        pass  # Solution did not exist

    remove_emission_bounds.main(scenario)

    # Identify the node codelist used by `scenario` (in case it is not set on `context`)
    context.model.regions = identify_nodes(scenario)

    kw: "CommonArgs" = dict(
        relation_name=context.model.relation_global_co2,
        reg=f"{context.model.regions}_GLB",
    )

    # “Step1.3 Make changes required to run the ENGAGE setup” (per .runscript_main)
    log.info("Add separate FFI and AFOLU CO2 accounting")
    add_FFI_CO2_accounting.main(scenario, **kw, constraint_value=None)
    add_AFOLU_CO2_accounting.add_AFOLU_CO2_accounting(scenario, **kw)

    log.info("Add alternative TCE accounting")
    add_alternative_TCE_accounting.main(scenario)

    add_CO2_emission_constraint.main(
        scenario, **kw, constraint_value=0.0, type_rel="lower"
    )

    return scenario
