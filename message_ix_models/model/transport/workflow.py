import logging
from typing import TYPE_CHECKING

from message_ix_models.project.ssp import SSP_2024

if TYPE_CHECKING:
    import message_ix_models

log = logging.getLogger(__name__)


def generate(
    context: "message_ix_models.Context", *, report_key="transport all", **options
):
    from message_ix_models import Workflow
    from message_ix_models.report import register, report

    from message_data.model.workflow import Config as SolveConfig
    from message_data.projects import navigate
    from message_data.projects.navigate.workflow import solve

    from . import build
    from .config import Config
    from .report import multi

    # TODO respect dry-run
    # TODO respect quiet

    options.pop("target_model_name")
    options.pop("target_scenario_name")

    # Prepare transport configuration
    Config.from_context(context, options=options)

    # Identify the base MESSAGEix-GLOBIOM scenario
    base_url = context.url
    if not base_url:
        log.warning("No --url given; some workflow steps may not work")
        base_url = f"ixmp://{context.platform_info['name']}/NONE/NONE"

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

    all_keys = []
    all_targets = []
    for ssp in SSP_2024:
        # Construct a label including the SSP
        # TODO split to a separate function
        label = f"SSP{ssp.name}"
        label_full = f"SSP_2024.{ssp.name}"

        # Identify the target of this step
        target = url_template.format(label_full)
        all_targets.append(target)

        # Build Transport on the scenario
        # TODO Add functionality like gen-activity
        # TODO Add functionality like build_cmd() with report_build
        wf.add_step(
            f"{label} built", "base", build.main, target=target, clone=True, ssp=ssp
        )

        # Solve
        wf.add_step(f"{label} solved", f"{label} built", solve, config=solve_config)

        # Report
        all_keys.append(wf.add_step(f"{label} reported", f"{label} solved", report))

    # Report across multiple scenarios
    wf.add("report multi", multi, "context", targets=all_targets)

    wf.add("all reported", all_keys)
    wf.default_key = "all reported"

    return wf
