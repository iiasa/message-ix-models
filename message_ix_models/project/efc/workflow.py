# The workflow mainly contains the steps to build EFC scenarios,
# as well as the steps to apply climate target settings. See efc-workflow.svg.
# Example cli command:
# mix-models efc run --from="base" "cpol reported" --dry-run

import logging

import message_ix  # type: ignore

from message_ix_models import Context
from message_ix_models.workflow import Workflow

log = logging.getLogger(__name__)

# EFC ixmp model name (single source of truth for cloned scenario targets).
EFC_MODEL_NAME = "MESSAGEix-GLOBIOM-GAINS 2.1-BMT-R12 EFC"

# Functions for individual workflow steps


def _run_transport_report(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Run MESSAGEix-Transport reporting on the scenario."""
    from message_ix_models.model.transport.key import report as k_report
    from message_ix_models.model.transport.report import callback as transport_callback
    from message_ix_models.report import prepare_reporter

    if transport_callback not in context.report.callback:
        context.report.register(transport_callback)
    rep, _ = prepare_reporter(context, scenario=scenario)
    rep.get(k_report.all)
    return scenario


def report(context: Context, scenario: message_ix.Scenario) -> message_ix.Scenario:
    """Report the scenario.

    Runs transport, materials, hydrogen, and legacy reporting.
    """
    # EFC legacy IAMC tables
    from message_data.tools.post_processing import iamc_report_hackathon  # type: ignore

    from message_ix_models.model.material.report.run_reporting import (
        run as _materials_report,
    )

    report_config_check = scenario.par(
        "demand", filters={"commodity": "transport pax UREAM"}
    )
    run_config = (
        "materials_daccs_bmt_run_config.yaml"
        if report_config_check is not None and len(report_config_check) > 0
        else "materials_daccs_run_config.yaml"
    )
    log.info("Legacy report will use run_config=%s", run_config)

    def _legacy_report(scen):
        iamc_report_hackathon.report(
            mp=scen.platform,
            scen=scen,
            merge_hist=True,
            run_config=run_config,
        )

    # 1. Transport reporting (only if transport is built)
    if report_config_check is not None and len(report_config_check) > 0:
        try:
            _run_transport_report(context, scenario)
        except Exception as e:
            log.warning("Transport reporting skipped: %s", e)
    else:
        log.info("Transport reporting skipped (no transport pax demand).")

    # message_data/tools/post_processing/iamc_report_hackathon.py#L320-L342
    # legacy report merges scenario ts into each table by root
    # (3 main tables: Final Energy, Emissions, Energy Service)
    # TODO: so one needs to make sure that the transport report is mergable to
    # legacy report, which is basically already covered in the transport
    # test_report.py and transport parts in the 3 main tables of legacy report
    # are deactivated so that no double counting happens. In the next report PR,
    # ideally the B and T reporting can be handled in a way similar to
    # message_data/blob/navigate5.3/.../navigate/report.py#L290-L298

    # 2. Materials reporting
    try:
        scenario.check_out(timeseries_only=True)
    except ValueError:
        log.debug(f"Scenario {scenario.model}/{scenario.scenario} already checked out")

    _materials_report(scenario, region="R12_GLB", upload_ts=True)
    scenario.commit("Add materials reporting")

    # 3. Hydrogen reporting
    # _placeholder

    # 4. Legacy reporting
    _legacy_report(scenario)

    return scenario


def placeholder(context: Context, scenario: message_ix.Scenario) -> message_ix.Scenario:
    """Placeholder function that does nothing, just for building workflow."""
    return scenario


def solve(
    context: Context, scenario: message_ix.Scenario, model="MESSAGE"
) -> message_ix.Scenario:
    """Plain solve."""
    solve_options = {
        "advind": 0,
        "lpmethod": 4,
        "threads": 4,
        "epopt": 1e-6,
        "scaind": -1,
        # "predual": 1,
        "barcrossalg": 0,
    }

    # scenario.solve(model, gams_args=["--cap_comm=0"])
    scenario.solve(model, solve_options=solve_options)
    scenario.set_as_default()

    return scenario


# EFC scenarios:
_scen_all = [
    "cpol",
    "chn_full_2060_1p5c",
    # "chn_full_2060_2c",
    # "chn_partial_2060_2c",
]


# main function to generate the workflow
def generate(context: Context) -> Workflow:
    wf = Workflow(context)
    context.ssp = "SSP2"
    context.model.regions = "R12"

    # EFC workflow: clone from parent BMT-R12 baseline on ixmp-dev into EFC model name.
    model_name = "ixmp://ixmp-dev/" + EFC_MODEL_NAME
    url = model_name + "/"
    base_url = "ixmp://ixmp-dev/MESSAGEix-GLOBIOM-GAINS 2.1-BMT-R12/baseline_BMT#3"

    # Common keyword argument for cloning (without solution; smaller DB writes)
    c = dict(keep_solution=False)

    name = wf.add_step("base", None, target=base_url)
    name = wf.add_step(
        "base cloned",
        name,
        target=f"{url}base",
        clone=dict(keep_solution=True),
    )
    name = wf.add_step("base reported", name, report)

    name = wf.add_step(
        "hydrogen added",
        name,
        placeholder,
        target=f"{url}baseline",
        clone=c,
    )
    name = wf.add_step("baseline solved", name, solve)
    name = wf.add_step("baseline reported", name, report)

    name = wf.add_step(
        "cpol added",
        "baseline reported",
        placeholder,
        target=f"{url}cpol",
        clone=c,
    )
    name = wf.add_step("cpol solved", name, solve)
    name = wf.add_step("cpol reported", name, report)

    name = wf.add_step(
        "1p5c added",
        "baseline reported",
        placeholder,
        target=f"{url}chn_full_2060_1p5c",
        clone=c,
    )
    name = wf.add_step("1p5c solved", name, solve)
    name = wf.add_step("1p5c reported", name, report)

    return wf
