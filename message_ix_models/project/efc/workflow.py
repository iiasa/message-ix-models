# The workflow mainly contains the steps to build EFC scenarios,
# as well as the steps to apply climate target settings. See efc-workflow.svg.
# Example cli command:
# mix-models efc run --from="base" "cpol reported" --dry-run

import logging
from typing import TYPE_CHECKING

import message_ix  # type: ignore

if TYPE_CHECKING:
    import pandas as pd

from message_ix_models import Context
from message_ix_models.model.hydrogen.data_hydrogen import add_hydrogen_techs
from message_ix_models.model.hydrogen.yoga_modes import apply_meth_h2_mode_parity
from message_ix_models.project.efc import (
    aas_co2_storage_growth,
    aas_co2_storage_share_mode,
    aas_coal_growth_near_term,
    freeze_truck_history,
)
from message_ix_models.tools.policy import add_anchor
from message_ix_models.workflow import Workflow

# Hyway electrolyser techs that take over h2_elec's role as
# methanol_synthesis_addon parents after the hydrogen build removes h2_elec.
# h2_pyro_elec and h2_ct are excluded — pyro is a separate turquoise framing;
# h2_ct consumes H2.
METHANOL_ADDON_PARENTS = ["h2_elec_alk", "h2_elec_pem", "h2_elec_soe"]
METH_H2_CO2_RELATIONS = ("CO2_Emission", "CO2_Emission_Global_Total")
METH_H2_CO2_COEFFICIENT = 0.549

# CPOL anchor policies to try first.
SF_POLICY_IDS = [
    # ammonia
    "10e-CHN-ENE-MOD-25_02",
    "10e-CHN-ENE-MOD-30_01",
    "10e-CHN-ENE-MOD-30_04",
    "10e-CHN-ENE-MOD-30_05",
    # methanol
    "10f-CHN-ENE-MOD-25_02",
    "10f-CHN-ENE-MOD-30_01",
    "10f-CHN-ENE-MOD-30_03",
    "10f-CHN-ENE-MOD-30_04",
    "10f-CHN-ENE-MOD-30_05",
    "10f-CHN-ENE-MOD-30_06",
]
SF_ACC_POLICY_IDS = [
    # ammonia
    "10e-CHN-ENE-MOD-25_02",
    "10e-CHN-ENE-MOD-30_03",  # accelerated
    "10e-CHN-ENE-MOD-30_04",
    "10e-CHN-ENE-MOD-30_05",
    # methanol
    "10f-CHN-ENE-MOD-25_02",
    "10f-CHN-ENE-MOD-30_02",  # accelerated
    "10f-CHN-ENE-MOD-30_03",
    "10f-CHN-ENE-MOD-30_04",
    "10f-CHN-ENE-MOD-30_05",
    "10f-CHN-ENE-MOD-30_06",
]
log = logging.getLogger(__name__)

# EFC ixmp model name (single source of truth for cloned scenario targets).
EFC_MODEL_NAME = "MESSAGEix-GLOBIOM-GAINS 2.1-MT-R12 EFC"


def configure_context(context: Context) -> None:
    """Configure *context* for every EFC workflow and reporting entry point."""
    context.ssp = "SSP2"
    context.model.regions = "R12"

    from message_ix_models.model.bmt.config import apply_bmt_config

    apply_bmt_config(context)


# Donor scenario for 1p5c ``bound_emission`` / ``tax_emission`` policy data.
_1P5C_SOURCE_MODEL = "MESSAGEix-GLOBIOM-GAINS 2.1-BMT-R12 NGFS C2"
_1P5C_SOURCE_SCENARIO = "o_1p5c_locdr_t3"
_EMISSION_CONSTRAINT_PARAMETERS = ("bound_emission", "tax_emission")

# Functions for individual workflow steps


def _write_report_xlsx(scenario: message_ix.Scenario) -> None:
    """Write IAMC Excel from the scenario's full timeseries.

    Legacy reporting (step 3) writes ``reporting_output/{model}_{scenario}.xlsx``
    before genno sectoral reporting runs. Re-export here so in|/out| sectoral
    variables (hydrogen, power, chemicals) are included in the local file.
    """
    from message_ix_models.report.legacy import pp_utils
    from message_ix_models.util import package_data_path
    from message_ix_models.util.compat.message_data.utilities import (
        retrieve_region_mapping,
    )

    _, reg_ts = retrieve_region_mapping(
        scenario, scenario.platform, include_region_id=False
    )

    df = scenario.timeseries(iamc=True)
    df = df.rename(
        columns={
            "model": "Model",
            "scenario": "Scenario",
            "region": "Region",
            "variable": "Variable",
            "unit": "Unit",
        }
    )
    df["Model"] = scenario.model
    df["Scenario"] = scenario.scenario
    df["Region"] = df["Region"].map(reg_ts)

    if "subannual" in df.columns:
        df = df.drop(columns=["subannual"])

    out_dir = package_data_path("report", "legacy", "reporting_output")
    out_dir.mkdir(parents=True, exist_ok=True)
    pp_utils.model_nm = scenario.model  # type: ignore[assignment]
    pp_utils.scen_nm = scenario.scenario  # type: ignore[assignment]
    pp_utils.write_xlsx(df, out_dir)
    log.info(
        "Wrote post-sectoral IAMC Excel to %s",
        out_dir / f"{scenario.model}_{scenario.scenario}.xlsx",
    )


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
        "materials_daccs_mt_run_config.yaml"
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

    # Transport output is required whenever the scenario contains transport
    # demand. Propagate failures: continuing would publish a partial workbook.
    if report_config_check is not None and len(report_config_check) > 0:
        _run_transport_report(context, scenario)
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

    # 3. Legacy reporting; writes reporting_output/{model}_{scenario}.xlsx (step 5
    # overwrites that file from full timeseries).
    _legacy_report(scenario)

    # 4. Genno sectoral reporting (hydrogen + power in|/out| flows) — overwrites
    # the hydrogen IAMC rows the legacy step just wrote with values derived from
    # the five hyway techs (h2_elec_alk/pem/soe, h2_pyro_elec, h2_ct) and adds the
    # power-sector in|/out| flows (e.g. in|Power|h2_ct|hydrogen). ixmp.add_timeseries
    # overwrites by design; that is the coexistence mechanism. Skipped on scenarios
    # where none of the hyway techs are present (e.g. the "base reported" step that
    # runs before "hydrogen added"); the power flows ride the same gate since their
    # only current tech, h2_ct, is in this set.
    hyway_techs = {
        "h2_elec_alk",
        "h2_elec_pem",
        "h2_elec_soe",
        "h2_pyro_elec",
        "h2_ct",
    }
    if hyway_techs <= set(scenario.set("technology").tolist()):
        from message_ix.report import Reporter

        from message_ix_models.report.hydrogen.h2_reporting import (
            run_sectoral_reporting,
        )

        rep = Reporter.from_scenario(scenario)
        iam_df = run_sectoral_reporting(
            rep,
            scenario.model,
            scenario.scenario,
            domains=["hydrogen", "power", "chemicals", "transport", "industry"],
            add_global_aggregates=True,
            add_net_trade=True,
        )
        try:
            scenario.check_out(timeseries_only=True)
        except ValueError:
            log.debug(
                "Scenario %s/%s already checked out",
                scenario.model,
                scenario.scenario,
            )
        scenario.add_timeseries(iam_df.timeseries().reset_index())
        scenario.commit("Add Genno sectoral reporting")

        # Re-write xlsx so in|/out| sectoral variables are included (step 3 wrote
        # before genno ran).
        _write_report_xlsx(scenario)
    else:
        log.info("Genno sectoral reporting skipped: no new hydrogen techs build.")

    return scenario


def generic_flow(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Write generic reporting to a local Excel file.

    in and out flows for all technologies are reported.
    No unit column.
    """
    from message_ix.report import Reporter

    from message_ix_models.project.efc.generic_report import genno_generic

    rep = Reporter.from_scenario(scenario)
    df = genno_generic(
        rep,
        scenario.model,
        scenario.scenario,
        firstmodelyear=scenario.firstmodelyear,
    )

    out_path = context.get_local_path(
        "efc",
        f"{scenario.model}_{scenario.scenario}_generic_flows.xlsx",
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(out_path, index=False)
    log.info("Wrote generic in|/out| flow report to %s (%d rows)", out_path, len(df))

    return scenario


def calibrate_base(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    scenario = freeze_truck_history(context, scenario)
    scenario = aas_coal_growth_near_term(context, scenario)
    return scenario


def placeholder(context: Context, scenario: message_ix.Scenario) -> message_ix.Scenario:
    """Placeholder function that does nothing, just for building workflow."""
    return scenario


def add_anchors(
    context: Context,
    scenario: message_ix.Scenario,
    policy_ids: list[str] | None = None,
) -> message_ix.Scenario:
    """Add current-policy (CPOL) anchors to the scenario."""
    context.anchor_data_file = "20260828efc_current_policy.csv"
    add_anchor(context, scenario, policy_ids=policy_ids)
    return scenario


def _meth_h2_co2_rows(rows: "pd.DataFrame") -> "pd.DataFrame":
    """Select and validate the meth_h2 relation rows removed upstream."""
    selected = rows[
        (rows["technology"] == "meth_h2")
        & (rows["relation"].isin(METH_H2_CO2_RELATIONS))
    ]
    bad = selected[(selected["value"] - METH_H2_CO2_COEFFICIENT).abs() > 1e-9]
    if not bad.empty:
        raise RuntimeError(
            "Unexpected meth_h2 CO2 relation coefficients: "
            f"{sorted(bad['value'].unique())}"
        )
    return selected


def _remove_meth_h2_co2_relations(scenario: message_ix.Scenario) -> None:
    """Match upstream PR #537 by removing both meth_h2 CO2 relation charges."""
    rows = scenario.par(
        "relation_activity",
        filters={
            "technology": "meth_h2",
            "relation": list(METH_H2_CO2_RELATIONS),
        },
    )
    selected = _meth_h2_co2_rows(rows)
    if selected.empty:
        log.info("meth_h2 CO2 relation charges are already absent")
        return

    scenario.remove_par("relation_activity", selected)
    remaining = scenario.par(
        "relation_activity",
        filters={
            "technology": "meth_h2",
            "relation": list(METH_H2_CO2_RELATIONS),
        },
    )
    if not remaining.empty:
        raise RuntimeError(
            f"Failed to remove {len(remaining)} meth_h2 CO2 relation rows"
        )
    log.info("Removed %d meth_h2 CO2 relation rows", len(selected))


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

    with scenario.transact(message="Remove erroneous meth_h2 CO2 relation charges"):
        _remove_meth_h2_co2_relations(scenario)

    return scenario


def add_1p5c(context: Context, scenario: message_ix.Scenario) -> message_ix.Scenario:
    """Copy 1p5c emission constraints and add CO2 storage AAS parameters.

    Adds ``bound_emission`` and ``tax_emission`` data from the donor scenario, then
    applies ``growth_activity_up`` and ``share_mode_up`` for CO2 storage.
    """
    source = message_ix.Scenario(
        scenario.platform,
        _1P5C_SOURCE_MODEL,
        _1P5C_SOURCE_SCENARIO,
    )

    from message_ix_models.tools.remove_emission_bounds import (
        main as remove_emission_bounds,
    )

    remove_emission_bounds(scenario, remove_all=True)

    with scenario.transact("Copy bound_emission and tax_emission from donor scenario"):
        for par_name in _EMISSION_CONSTRAINT_PARAMETERS:
            df = source.par(par_name)
            if df.empty:
                log.warning(
                    "No %s in %s/%s; skipping",
                    par_name,
                    _1P5C_SOURCE_MODEL,
                    _1P5C_SOURCE_SCENARIO,
                )
                continue
            scenario.add_par(par_name, df)

    log.info(
        "add_1p5c: copied %s from %s/%s to %s/%s",
        ", ".join(_EMISSION_CONSTRAINT_PARAMETERS),
        _1P5C_SOURCE_MODEL,
        _1P5C_SOURCE_SCENARIO,
        scenario.model,
        scenario.scenario,
    )

    scenario = aas_co2_storage_growth(context, scenario)
    scenario = aas_co2_storage_share_mode(context, scenario)
    log.info(
        "add_1p5c: additional assumptions added to %s/%s",
        scenario.model,
        scenario.scenario,
    )

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
    "chn_base",
    "chn_base_sf_policy",
    "chn_nz2060",
    "chn_nz2060_sf_policy",
    "chn_acc_nz2060_sf_policy",
]


# main function to generate the workflow
def generate(context: Context) -> Workflow:
    wf = Workflow(context)
    configure_context(context)

    # EFC workflow: clone the parent scenario on ixmp-dev into the EFC model name.
    #
    # The parent is the MT chain rebuilt on 2026-08-18 from the step_14b shipping
    # patch, which restores loil_bunker's input/output (deleted upstream in model
    # v6.5; a hollow addon-parent with degenerate activity inflated shipping CO2).
    # It is cre_user juyiyi and lives under the EFC model name, but it is NOT ours
    # to overwrite: pin the version and clone, never build in place.
    #
    # The chain is MT, never BMT, even though the previous parent URL named a
    # "2.1-BMT-R12" model: that was an upstream naming legacy and the scenario
    # inside it never ran the Buildings step. The discriminator is the Buildings
    # rename of residential/commercial _rc -> _afofio in model/buildings/build.py;
    # the parent has ZERO *afofio* technologies and the full _rc set (eth_rc,
    # h2_rc, elec_rc, gas_rc, foil_rc), so reporting filters must use _rc names.
    # Re-check with EFC_2026 scripts/verify/check_mt_vs_bmt_lineage.py.
    #
    # apply_bmt_config() above is not a Buildings build step either — it is a
    # Context configurator required for transport reporting.
    model_name = "ixmp://ixmp-dev/" + EFC_MODEL_NAME
    url = model_name + "/"
    base_url = (
        "ixmp://ixmp-dev/MESSAGEix-GLOBIOM-GAINS 2.1-MT-R12 EFC"
        "/baseline_MT_calibrated#1"
    )

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
        "base calibrated",
        name,
        calibrate_base,
        target=f"{url}base_calibrated",
        clone=c,
    )
    name = wf.add_step(
        "hydrogen added",
        name,
        build_hydrogen,
        # target=f"{url}baseline_20260827",
        target=f"{url}chn_base",
        clone=c,
    )
    name = wf.add_step("baseline solved", name, solve)
    name = wf.add_step("baseline reported", name, report)
    # name = wf.add_step(
    #     "baseline generic reported",
    #     "baseline solved",
    #     generic_flow,
    # )

    name = wf.add_step(
        "cpol added",
        "baseline reported",
        add_anchors,
        policy_ids=SF_POLICY_IDS,
        target=f"{url}chn_base_sf_policy",
        clone=c,
    )
    name = wf.add_step("cpol solved", name, solve)
    name = wf.add_step("cpol reported", name, report)

    name = wf.add_step(
        "1p5c added",
        "cpol reported",
        add_1p5c,
        target=f"{url}chn_nz2060",
        clone=dict(keep_solution=False, shift_first_model_year=2030),
    )
    name = wf.add_step("1p5c solved", name, solve)
    name = wf.add_step("1p5c reported", "1p5c solved", report)
    # name = wf.add_step(
    #     "1p5c generic reported",
    #     "1p5c solved",
    #     generic_flow,
    # )

    name = wf.add_step(
        "1p5c_sf added",
        "1p5c solved",
        add_anchors,
        target=f"{url}chn_nz2060_sf_policy",
        policy_ids=SF_POLICY_IDS,
        clone=dict(keep_solution=False),
    )
    name = wf.add_step("1p5c_sf solved", name, solve)
    name = wf.add_step("1p5c_sf reported", name, report)

    name = wf.add_step(
        "1p5c_sf_acc added",
        "1p5c solved",
        add_anchors,
        target=f"{url}chn_acc_nz2060_sf_policy",
        policy_ids=SF_ACC_POLICY_IDS,
        clone=dict(keep_solution=False),
    )
    name = wf.add_step("1p5c_sf_acc solved", name, solve)
    name = wf.add_step("1p5c_sf_acc reported", name, report)

    return wf
