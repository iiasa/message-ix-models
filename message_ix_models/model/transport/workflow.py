import logging
from typing import TYPE_CHECKING

from message_ix_models.project.ssp import SSP_2024

if TYPE_CHECKING:
    import message_ix_models

log = logging.getLogger(__name__)


def generate(
    context: "message_ix_models.Context",
    *,
    report_key="transport all",
    dry_run: bool = False,
    **options,
):
    from message_ix_models import Workflow
    from message_ix_models.report import register, report

    from message_data.model.workflow import Config as SolveConfig
    from message_data.projects import navigate
    from message_data.projects.navigate.workflow import solve

    from . import build
    from .config import Config
    from .report import multi

    # TODO respect quiet

    options.pop("target_model_name")
    options.pop("target_scenario_name")

    # Prepare transport configuration
    Config.from_context(context, options=options)

    # Identify the base MESSAGEix-GLOBIOM scenario
    if not (context.platform_info or context.scenario_info):
        # TODO Move this functionality upstream to message-ix-models

        # Set up a temporary, in-memory platform
        from ixmp import config as ixmp_config
        from message_ix_models.model import bare

        ixmp_config.add_platform(
            __name__, "jdbc", "hsqldb", url=f"jdbc:hsqldb:mem:{__name__}"
        )
        context.platform_info.update(name=__name__)

        # Build a bare RES scenario given .model.Config settings
        s = bare.create_res(context)

        # Update Context.core.scenario_info to match `s`
        context.set_scenario(s)
        # Also update context.core.url  FIXME Do this in Context.set_scenario()
        context.core.url = f"ixmp://{__name__}/{s.url}"
    elif not context.core.url:
        log.warning("No --url given; some workflow steps may not work")
        platform = context.platform_info.get("name", "NONE")
        context.core.url = f"ixmp://{platform}/NONE/NONE"

    base_url = context.core.url

    # Construct a URL template for MESSAGEix-Transport scenarios
    if context.core.dest:
        # Value from --dest CLI option
        url_template = context.dest + " {{}}"
    else:
        # Values from --model-extra, --scenario-extra CLI options
        m_extra = context.core.dest_scenario.pop("model", "")
        s_extra = context.core.dest_scenario.pop("scenario", "baseline")

        url_template = "/".join(
            (
                f"MESSAGEix-GLOBIOM 1.1-T-{context.model.regions} {m_extra}".rstrip(),
                f"{{}} {s_extra}",
            )
        )

    # Set values expected by workflow steps re-used from .projects.navigate
    context.navigate = navigate.Config(
        scenario="baseline", buildings=False, material=False
    )
    solve_config = SolveConfig(reserve_margin=False, solve=dict(model="MESSAGE"))

    # Set the default key for ".* reported" steps
    register("model.transport")
    context.report.key = report_key

    # Create the workflow
    wf = Workflow(context)

    # Load the base model scenario
    wf.add_step("base", None, target=base_url)

    reported = []
    targets = []
    debug = []
    for ssp in SSP_2024:
        # Construct a label including the SSP
        # TODO split to a separate function
        label = f"SSP{ssp.name}"
        label_full = f"SSP_2024.{ssp.name}"

        # Identify the target of this step
        target = url_template.format(label_full)
        targets.append(target)

        # Build Transport on the scenario
        # TODO Add functionality like gen-activity
        # TODO Add functionality like build_cmd() with report_build
        wf.add_step(
            f"{label} built",
            "base",
            build.main,
            target=target,
            clone=True,
            ssp=ssp,
        )

        # Simulate build
        debug.append(f"{label} debug build")
        wf.add_step(debug[-1], "base", build.main, ssp=ssp, dry_run=True)

        # Solve
        wf.add_step(f"{label} solved", f"{label} built", solve, config=solve_config)

        # Report
        reported.append(f"{label} reported")
        wf.add_step(reported[-1], f"{label} solved", report)

    # Compare simulated builds
    # NB the following use genno.Computer.add(), not
    #    message_ix_models.Workflow.add_step(). This is because the operations are not
    #    modifying and handling scenarios, just calling ordinary functions.
    wf.add("debug build", build.debug_multi, "context", *debug)

    # Report across multiple scenarios
    wf.add("report multi", multi, "context", targets=targets)

    wf.add("all reported", reported)
    wf.default_key = "all reported"

    return wf
