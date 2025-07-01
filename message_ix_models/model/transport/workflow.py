import logging
from copy import deepcopy
from hashlib import blake2s
from typing import TYPE_CHECKING, Literal, Optional

from genno import KeyExistsError

from message_ix_models.model.workflow import Config as WorkflowConfig
from message_ix_models.tools.policy import single_policy_of_type
from message_ix_models.util import minimum_version

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models.util.context import Context
    from message_ix_models.workflow import Workflow

    from .config import Config

log = logging.getLogger(__name__)


#: Default :class:`.workflow.Config` for solving MESSAGEix-Transport.
#:
#: - :py:`lpmethod=4, scaind=1` to overcome LP status 5 (optimal with unscaled
#:   infeasibilities) when running on SSP(2024) base scenarios.
#: - :py:`iis=1` to display verbose conflict information on infeasibility.
#: - :py:`tilim=45 * 60` to limit runtime to 45 minutes on IIASA-hosted GitHub Actions
#:   runners.
SOLVE_CONFIG = WorkflowConfig(
    reserve_margin=False,
    solve=dict(
        model="MESSAGE",
        solve_options=dict(
            iis=1,
            lpmethod=4,
            scaind=1,
            tilim=45 * 60,
        ),
    ),
)


def base_scenario_url(
    context: "Context", config: "Config", method: Literal["auto", "bare"] = "bare"
) -> str:
    """Identify the base MESSAGEix-GLOBIOM scenario.

    If :attr:`.scenario_info` is set on `context` (for instance, provided via the
    :program:`--url` CLI option), nothing is done, and the URL corresponding to that
    scenario is returned.

    If not, then the behaviour depends on `method`:

    :py:`method = "auto"`
       Automatically identify the base scenario URL from the contents of
       ``CL_TRANSPORT_SCENARIO``. The settings :attr:`.Config.ssp
       <.transport.config.Config.ssp>` and :attr:`.Config.policy` are used to match an
       entry in the file.
    :py:`method = "bare"`
       Construct bare RES scenario using :mod:`.model.bare.create_res` and the settings
       on `context` such as :attr:`.Config.regions`. Return the URL to this scenario.
    """
    if context.scenario_info:
        assert context.core.url
        return context.core.url

    if method == "auto":
        return config.base_scenario_url
    elif method == "bare":
        # Use a 'bare' RES or empty scenario
        if context.platform_info["name"] in (__name__, "message-ix-models"):
            # Temporary platform or testing → use the bare RES
            from message_ix_models.model import bare

            log.info("No --model/--scenario/--url; use the bare RES as base")
            # Build a bare RES scenario given .model.Config settings
            s = bare.create_res(context)

            return f"ixmp://{context.platform_info['name']}/{s.url}"
        else:
            log.warning("No --model/--scenario/--url; some workflow steps may not work")
            return f"ixmp://{context.platform_info.get('name', 'NONE')}/NONE/NONE"


def maybe_use_temporary_platform(context: "Context") -> None:
    """Set up a temporary, in-memory platform.

    .. todo:: Move upstream, to :mod:`message_ix_models`.
    """
    if context.platform_info:
        return

    from ixmp import config as ixmp_config

    ixmp_config.add_platform(
        __name__, "jdbc", "hsqldb", url=f"jdbc:hsqldb:mem:{__name__}"
    )
    context.platform_info.update(name=__name__)
    log.info("No --platform/--url; using temporary, in-memory database")


def scenario_url(context: "Context", label: Optional[str] = None) -> str:
    """Construct a target URL for a built MESSAGEix-Transport scenario.

    If the :attr:`.dest` URL is set on `context` (for instance, provided via the
    :program:`--dest` CLI option), this URL returned with `label` appended to the
    scenario name.

    If not, a form is used like:

    - :py:`model = "MESSAGEix-GLOBIOM 1.1-T-{regions}"`. Any value of the "model" key
      from :attr:`.core.Config.dest_scenario` is appended.
    - :py:`scenario = "{label}"`. Any value of the "scenario" key from
      :attr:`.core.Config.dest_scenario` is appended; if this is not set, then either
      "policy" (if :attr:`.transport.Config.policy` is set) or "baseline".
    """
    # Construct a URL template for MESSAGEix-Transport scenarios
    if context.core.dest:
        # Value from --dest CLI option
        # TODO Check that this works if a version # is specified
        return f"{context.dest} {label or ''}".strip()
    else:
        # Values from --model-extra, --scenario-extra CLI options
        m_extra = context.core.dest_scenario.get("model", "")
        s_extra = context.core.dest_scenario.get("scenario") or (
            "policy" if context.transport.policy else "baseline"
        )

        return "/".join(
            (
                f"MESSAGEix-GLOBIOM 1.1-T-{context.model.regions} {m_extra}".rstrip(),
                f"{label or ''} {s_extra}".strip(),
            )
        )


def short_hash(value: str) -> str:
    """Return a short (length 3) hash of `value`."""
    return blake2s(value.encode()).hexdigest()[:3]


def tax_emission(context: "Context", scenario: "Scenario", price: float) -> "Scenario":
    """Add emission tax.

    See also
    --------
    message_ix_models.project.engage.workflow.step_0
    message_ix_models.project.navigate.workflow.tax_emission
    """
    from message_ix import make_df

    from message_ix_models.model.workflow import step_0
    from message_ix_models.project.navigate import workflow as navigate_workflow
    from message_ix_models.util import broadcast

    # Prepare emissions accounting for carbon pricing
    scenario = step_0(context, scenario)

    # Add values for the MACRO 'drate' parameter.
    # message_data.tools.utilities.add_tax_emission() refers to this parameter, rather
    # than the MESSAGE 'interestrate' parameter, to compute nominal future values of the
    # tax. The parameter is not present if MACRO has not been set up on the scenario.
    name = "drate"
    df = make_df(name, value=0.05, unit="-").pipe(broadcast, node=scenario.set("node"))
    with scenario.transact(f"Add values for {name}"):
        scenario.add_par(name, df)

    return navigate_workflow.tax_emission(context, scenario, price)


@minimum_version("message_ix 3.11")
def generate(
    context: "Context",
    *,
    report_key: str = "transport all",
    dry_run: bool = False,
    **options,
) -> "Workflow":
    from message_ix.tools.migrate import initial_new_capacity_up_v311

    from message_ix_models import Workflow
    from message_ix_models.model.workflow import solve
    from message_ix_models.report import report

    from . import build
    from .config import Config, get_cl_scenario
    from .policy import ExogenousEmissionPrice, TaxEmission
    from .report import multi

    # Handle CLI options
    # TODO respect quiet
    options.pop("target_model_name", None)  # Stored on context.core.dest_scenario
    options.pop("target_scenario_name", None)  # Stored on context.core.dest_scenario
    base_scenario_method = options.pop("base_scenario")

    maybe_use_temporary_platform(context)

    # Prepare base/common transport configuration, passing the remaining `options`
    Config.from_context(context, options=options)

    # Set the default .report.Config key for ".* reported" steps
    context.report.key = report_key
    context.report.register("model.transport")

    # Create the workflow
    wf = Workflow(context)

    # Collections of step names
    debug, reported, targets = [], [], []

    # Iterate over all scenarios in IIASA_ECE:CL_TRANSPORT_SCENARIO
    for scenario_code in get_cl_scenario():
        # Make a copy of the base .transport.Config for this particular workflow branch
        config = deepcopy(context.transport)

        # Update the .transport.Config from the `scenario_code` and `policy`
        label, label_full = config.use_scenario_code(scenario_code)

        # Identify the base scenario
        base_url = base_scenario_url(context, config, base_scenario_method)
        # log.debug(f"Base scenario for scenario={label_full!r}: {base_url}")
        # log.debug(f"{config.policy = }")

        # Name of the base step
        base = f"base {short_hash(base_url)}"

        try:
            # Load the base model scenario
            wf.add_step(base, None, target=base_url)
        except KeyExistsError:
            # Base scenario URL is identical to another (ssp, policy) combination; use
            # the scenario returned by that step
            pass

        # Identify the target of the build step
        target_url = scenario_url(context, label_full)
        targets.append(target_url)

        # Build MESSAGEix-Transport on the scenario
        name = wf.add_step(
            f"{label} built",
            base,
            build.main,
            target=target_url,
            clone=True,
            config=config,
        )

        # Adjust initial_new_capacity_up values for message_ix#924
        name = wf.add_step(
            f"{label} incu adjusted",
            name,
            lambda _, s: initial_new_capacity_up_v311(s, safety_factor=1.05),
        )

        # Add step(s) to implement policies
        if p0 := single_policy_of_type(config.policy, TaxEmission):
            name = wf.add_step(f"{label} added", name, tax_emission, price=p0.value)
        elif p1 := single_policy_of_type(config.policy, ExogenousEmissionPrice):
            log.info(f"Not implemented: {p1}")

        # 'Simulate' build and produce debug outputs
        debug.append(f"{label} debug build")
        wf.add_step(debug[-1], base, build.main, config=config, dry_run=True)

        # Solve
        wf.add_step(f"{label} solved", name, solve, config=SOLVE_CONFIG)

        # Report
        reported.append(f"{label} reported")
        wf.add_step(reported[-1], f"{label} solved", report)

    # NB the following use genno.Computer.add(), not .Workflow.add_step(). This is
    #    because the operations are not WorkflowSteps that receive, modify, and return
    #    Scenario objects—only ordinary Python functions.

    # Compare debug outputs from multiple simulated builds
    wf.add("debug build", build.debug_multi, "context", *debug)

    # Report (including plot) using data from multiple, solved scenarios
    wf.add("report multi", multi, "context", targets=targets)

    # Report all the scenarios
    wf.add("all reported", reported)
    wf.default_key = "all reported"

    return wf
