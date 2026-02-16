"""Common steps for workflows."""

import logging
from collections.abc import Collection
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

from message_ix import Scenario

from message_ix_models import Workflow
from message_ix_models.util import identify_nodes, short_hash
from message_ix_models.util.config import ConfigHelper

if TYPE_CHECKING:
    from typing import TypedDict

    from message_ix_models import Context, Workflow
    from message_ix_models.util.sdmx import StructureFactory

    class CommonArgs(TypedDict):
        relation_name: str
        reg: str


log = logging.getLogger(__name__)


class STAGE(Enum):
    """Enumeration of common workflow stages."""

    #: Build stage of a workflow.
    BUILD = auto()

    #: Solve stage of a workflow.
    SOLVE = auto()

    #: Report stage of a workflow.
    REPORT = auto()


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


def from_codelist(context: "Context", cl: type["StructureFactory"]) -> "Workflow":
    """Generate a workflow using a code list.

    - IDs of codes in `cl` are used to label steps.
    - The following are invoked automatically:

      - :func:`.model.transport.workflow.add_steps`

    .. todo:: Add and also invoke functions like:

       - :py:`model.buildings.workflow.add_steps`
       - :py:`model.material.workflow.add_steps`

       …in a standard or configurable order.

    See also
    --------
    .project.circeular.workflow.generate
    .model.transport.workflow.generate
    """
    from genno import KeyExistsError

    from message_ix_models.model.transport import workflow as transport
    from message_ix_models.report import report

    # Create the workflow
    wf = Workflow(context)

    # Collection of step names
    reported = []

    # Iterate over all scenarios in IIASA_ECE:CL_CIRCEULAR_TRANSPORT_SCENARIO
    for scenario_code in cl.get(force=True):
        # Identify the URL of the base scenario
        base_url = scenario_code.eval_annotation(id="base-scenario-URL")

        # Short label for subsequent steps
        label = scenario_code.id

        # Name of the base step
        name = f"base {short_hash(base_url)}"
        try:
            # Load the base model scenario
            wf.add_step(name, None, target=base_url)
        except KeyExistsError:
            # Base scenario URL is identical to another (ssp, policy) combination; use
            # the scenario returned by that step
            pass

        name = transport.add_steps(wf, name, scenario_code)

        # Solve
        wf.add_step(f"{label} solved", name, solve)

        # Report
        reported.append(f"{label} reported")
        wf.add_step(reported[-1], f"{label} solved", report)

    # Report all the scenarios
    wf.add("all reported", reported)
    wf.default_key = "all reported"

    return wf


def solve(
    context: "Context",
    scenario: Scenario,
    *,
    config: Config | None = None,
    set_as_default: bool = False,
):
    """Common model solve step for ENGAGE, NAVIGATE, and other workflows.

    The steps respond to settings from a :class:`Config` instance. In order of
    precedence:

    1. The keyword argument `config`.
    2. The "solve" key on `context`.
    3. A default instance of :class:`Config`

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

    # Identify configuration
    try:
        config = config or context.solve
    except AttributeError:
        config = Config()

    # Set reserve margin values
    if config.reserve_margin:
        # FIXME Use an analogous function in message-ix-models, with tests
        from message_data.scenario_generation.reserve_margin import res_marg

        res_marg.main(scenario)

    # Explicit list of model variables for which to read data
    var_list = ["I", "C"]

    if config.solve["model"] == "MESSAGE-MACRO":
        var_list.append("GDP")

    if config.demand_scenario:
        # Retrieve DEMAND variable data from a different scenario and set as values
        # for the demand parameter
        # FIXME Use an analogous function in message-ix-models, with tests
        from message_data.tools.utilities import transfer_demands

        source = Scenario(scenario.platform, **config.demand_scenario)
        transfer_demands(source, scenario)

    scenario.solve(var_list=var_list, **config.solve)

    if set_as_default:
        # Solve was successful; set default version
        scenario.set_as_default()

    return scenario


def step_0(
    context: "Context",
    scenario: "Scenario",
    *,
    remove_emission_parameters: Collection[str] = ("bound_emission", "tax_emission"),
    **kwargs,
) -> "Scenario":
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

    if len(kwargs):  # pragma: no cover
        raise TypeError("Unhandled keyword arguments: {kwargs}")

    try:
        scenario.remove_solution()
    except ValueError:
        pass  # Solution did not exist

    remove_emission_bounds.main(scenario, parameters=remove_emission_parameters)

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
