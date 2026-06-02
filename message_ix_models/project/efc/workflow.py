# The workflow mainly contains the steps to build EFC scenarios,
# as well as the steps to apply climate target settings. See efc-workflow.svg.
# Example cli command:
# mix-models efc run --from="base" "cpol reported" --dry-run

import logging

import message_ix  # type: ignore

from message_ix_models import Context
from message_ix_models.model.hydrogen.data_hydrogen import add_hydrogen_techs
from message_ix_models.model.hydrogen.yoga_modes import apply_meth_h2_mode_parity
from message_ix_models.workflow import Workflow

# Hyway electrolyser techs that take over h2_elec's role as
# methanol_synthesis_addon parents after the hydrogen build removes h2_elec.
# h2_pyro_elec and h2_ct are excluded — pyro is a separate turquoise framing;
# h2_ct consumes H2.
METHANOL_ADDON_PARENTS = ["h2_elec_alk", "h2_elec_pem", "h2_elec_soe"]

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
            log.warning("Transport reporting skipped: %s", e, exc_info=True)
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

    # 3. Legacy reporting (runs first; uploads zero rows for any hydrogen
    # variables that still query the now-absent legacy h2_elec technology —
    # see docs/reporting/coexistence.md in the EFC_2026 project repo).
    _legacy_report(scenario)

    # 4. Genno hydrogen reporting — overwrites the hydrogen IAMC rows the
    # legacy step just wrote with values derived from the five hyway techs
    # (h2_elec_alk/pem/soe, h2_pyro_elec, h2_ct). ixmp.add_timeseries
    # overwrites by design; that is the coexistence mechanism. Skipped on
    # scenarios where none of the hyway techs are present (e.g. the "base
    # reported" step that runs before "hydrogen added").
    hyway_techs = {
        "h2_elec_alk",
        "h2_elec_pem",
        "h2_elec_soe",
        "h2_pyro_elec",
        "h2_ct",
    }
    scenario_techs = set(scenario.set("technology").tolist())
    if not (hyway_techs & scenario_techs):
        log.info(
            "Genno hydrogen reporting skipped: no hyway H2 techs in scenario."
        )
        return scenario

    from message_ix.report import Reporter

    from message_ix_models.report.hydrogen.h2_reporting import run_h2_reporting

    rep = Reporter.from_scenario(scenario)
    iam_df = run_h2_reporting(rep, scenario.model, scenario.scenario)
    try:
        scenario.check_out(timeseries_only=True)
    except ValueError:
        log.debug(f"Scenario {scenario.model}/{scenario.scenario} already checked out")
    scenario.add_timeseries(iam_df.timeseries().reset_index())
    scenario.commit("Add Genno hydrogen reporting")

    return scenario


def placeholder(context: Context, scenario: message_ix.Scenario) -> message_ix.Scenario:
    """Placeholder function that does nothing, just for building workflow."""
    return scenario


def build_hydrogen(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Add the hyway hydrogen techs and apply Yoga's meth_h2 mode-parity port.

    Two-step composition:

    1. ``add_hydrogen_techs`` populates the new techs (h2_elec_alk/pem/soe,
       h2_pyro_elec, h2_ct, carbon_black_*) from per-tech CSVs and removes
       h2_elec from the technology set. Each electrolyser receives addon_*
       parameters in pre-Yoga feedstock/fuel modes and input/output/var_cost
       in M1.
    2. ``apply_meth_h2_mode_parity`` ports Yoga's mode broadcast onto the
       chosen parent techs so meth_h2 (which already has the 6 split modes
       in the BMT base) can bind via methanol_synthesis_addon. Without this
       step ADDON_ACTIVITY_UP collapses against the missing h2_elec and
       meth_h2 silently zeros.
    """
    # add_hydrogen_techs assumes the global region exists.
    if "R12_GLB" not in list(scenario.platform.regions()["region"]):
        log.info("Adding global region R12_GLB")
        scenario.platform.add_region("R12_GLB", "region", "World")

    add_hydrogen_techs(scenario)

    with scenario.transact(
        message="Yoga meth_h2 mode-parity port for hyway electrolysers"
    ):
        apply_meth_h2_mode_parity(scenario, METHANOL_ADDON_PARENTS)

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

    # Build context.transport (and the rest of the BMT config) before any report
    # step runs. Without this the CLI path never configures transport, so
    # _run_transport_report -> prepare_reporter builds the transport graph against
    # an unconfigured context and raises — surfacing as an empty
    # "Transport reporting skipped:" message. Mirrors the call in
    # scripts/verify/report_efc_workflow.py.
    from message_ix_models.model.bmt.config import apply_bmt_config

    apply_bmt_config(context)

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
        build_hydrogen,
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
