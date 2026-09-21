# The workflow mainly contains the steps to build ngfs scenarios,
# as well as the steps to apply policy scenario settings. See ngfs-workflow.svg.
# Example cli command:
# mix-models ngfs run --from="base" "glasgow+" --dry-run

import logging
import re

import message_ix  # type: ignore
import pandas as pd

# TODO: think about integrating `interpolate_c_price` into the
# scenario runner.
from genno import Key  # type: ignore
from message_ix import make_df

from message_ix_models import Context, ScenarioInfo
from message_ix_models.model.buildings.sturm import call_buildings_demand, call_sturm
from message_ix_models.model.material.data_util import add_macro_materials
from message_ix_models.model.material.util import update_macro_calib_file
from message_ix_models.project.engage.workflow import (
    PolicyConfig,
    step_1,
    step_2,
    step_3,
    step_4,
)
from message_ix_models.project.ngfs import (
    interpolate_c_price,
    qf_remove_meth_h2_co2_relations,
)
from message_ix_models.tools import add_CO2_emission_constraint
from message_ix_models.workflow import Workflow

log = logging.getLogger(__name__)

# Single source of truth for the NGFS model name (config key and scenario target prefix)
NGFS_MODEL_NAME = "MESSAGEix-GLOBIOM-GAINS 2.1-BMT-R12 NGFS C3"

# Functions for individual workflow steps


def _get_ngfs_config(context):
    """Load and cache NGFS config from config.yaml.

    The config is stored in context.ngfs_config to avoid reloading.

    Parameters
    ----------
    context : Context
        Context object where config will be cached

    Returns
    -------
    dict
        The model config dictionary from config.yaml
    """
    if not hasattr(context, "ngfs_config") or context.ngfs_config is None:
        import yaml

        from message_ix_models.util import private_data_path

        config_file = private_data_path("projects", "ngfs", "config.yaml")
        with open(config_file) as f:
            context.ngfs_config = yaml.safe_load(f)[NGFS_MODEL_NAME]

    return context.ngfs_config


def _ngfs_base_scenario_name(name: str) -> str:
    """Strip trailing ``_EN1`` / ``_EN2`` / … suffix for :file:`config.yaml` keys."""
    return re.sub(r"_EN\d+$", "", name)


def report(context: Context, scenario: message_ix.Scenario) -> message_ix.Scenario:
    """Report the scenario.

    NGFS delegates to :func:`message_ix_models.model.bmt.workflow.report`, but
    transport reporting requires ``context.transport.code`` to be set.
    """
    # Ensure MESSAGEix-Transport reporting can run: the reporter expects
    # context.transport.code (e.g. "SSP2") to be set on the transport.Config instance.
    from message_ix_models.model.transport.config import Config

    if "transport" not in context:
        context.transport = Config.from_context(context)

    if getattr(context.transport, "_code", None) is None:
        context.transport.code = "SSP2"  # M SSP2 for the one with materials
        log.info("Transport reporting enabled with manually set codes.")

    from message_ix_models.model.bmt.workflow import report as bmt_report

    return bmt_report(context, scenario)
    # TODO: it seems transport report cannot work alone without following after build


# YJ: just for quick vetting, not really needed, can remove later
def report_transport(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Run only MESSAGEix-Transport reporting."""
    from message_ix_models.model.bmt.workflow import _run_transport_report
    from message_ix_models.model.transport.config import Config

    if "transport" not in context:
        context.transport = Config.from_context(context)
    if getattr(context.transport, "_code", None) is None:
        context.transport.code = "SSP2"
        log.info("Transport reporting enabled with manually set codes.")

    return _run_transport_report(context, scenario)


def placeholder(context: Context, scenario: message_ix.Scenario) -> message_ix.Scenario:
    """Placeholder function that does nothing, just for building workflow."""
    return scenario


def temp_borrow_par(
    context: Context,
    scenario: message_ix.Scenario,
    target_model_name: str,
    target_scen_name: str,
    par_names: list[str],
) -> message_ix.Scenario:
    """Borrow parameter data from another scenario and copy to `scenario`."""
    source = message_ix.Scenario(
        mp=scenario.platform,
        model=target_model_name,
        scenario=target_scen_name,
    )

    with scenario.transact(
        f"Borrow parameters from {target_model_name}/{target_scen_name}"
    ):
        for par_name in par_names:
            df = source.par(par_name)
            if df.empty:
                log.info(
                    "Skip borrowing '%s': no data in %s/%s",
                    par_name,
                    target_model_name,
                    target_scen_name,
                )
                continue

            existing = scenario.par(par_name)
            if not existing.empty:
                scenario.remove_par(par_name, existing)
                log.info(f"Removed existing {par_name} from the original scenario")

            scenario.add_par(par_name, df)
            log.info(
                "Borrowed '%s' from %s/%s (%d rows)",
                par_name,
                target_model_name,
                target_scen_name,
                len(df),
            )

    solve(context, scenario)
    scenario.set_as_default()

    return scenario


def make_scenario_runner(context):
    """Create and configure a ScenarioRunner instance for NGFS workflows.

    This function sets up the necessary context attributes and creates a ScenarioRunner
    instance that is used to build and run NGFS policy scenarios. It also pre-populates
    the policy_baseline scenario if it doesn't already exist.
    """
    from message_data.model.scenario_runner import ScenarioRunner

    # Get biomass_trade setting from context, default to False if not set
    try:
        biomass_trade = context.biomass_trade
    except AttributeError:
        biomass_trade = False

    context.ssp = "SSP2"
    context.run_reporting_only = False
    context.policy_data_file = "ngfs_p6_policy_data.xlsx"
    context.policy_config = "ngfs_p6_config.yaml"
    context.region_id = "R12"

    # Load config (cached in context for reuse)
    model_config = _get_ngfs_config(context)

    # Slack data is selected based on the slack scenario from config
    sr = ScenarioRunner(
        context,
        slack_data=model_config["policy_slacks"][model_config["slack_scn"]][
            context.ssp
        ],
        biomass_trade=biomass_trade,
    )

    # Pre-populate baseline scenario(s) if they do not exist.
    # Use baseline_DEFAULT to match the workflow target
    # (base cloned -> baseline_DEFAULT).
    # policy_baseline is used by the runner's internal logic; baseline_DEFAULT is the
    # prerequisite name passed to sr.add(..., start_scen="baseline_DEFAULT")
    # by add_glasgow, etc.
    if "policy_baseline" not in sr.scen:
        base_scenario = message_ix.Scenario(
            mp=sr.mp,
            model=sr.model_name,
            scenario="baseline_DEFAULT",
            cache=False,
        )
        sr.scen["policy_baseline"] = base_scenario
        sr.scen["baseline_DEFAULT"] = base_scenario

    return sr


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


def add_NPi2030(context: Context, scenario: message_ix.Scenario) -> message_ix.Scenario:
    """Add NPi2030 to the scenario."""

    sr = make_scenario_runner(context)
    sr.add(
        "NPi2030",
        "baseline_DEFAULT",
        # must start with this scenario name (hard-coded in the general scenario runner)
        mk_INDC=True,
        slice_year=2025,
        policy_year=2030,
        target_kind="Target",
        run_reporting=False,
        # TODO: set to MESSAGE-MACRO when workflow test finished.
        solve_typ="MESSAGE-MACRO",
    )

    # sr.add(
    #     "npi_low_dem_scen",
    #     "NPi2030",
    #     slice_year=2025,
    #     tax_emission=150,
    #     run_reporting = False,
    #     # TODO: set to MESSAGE-MACRO when workflow test finished.
    #     solve_typ="MESSAGE-MACRO",
    # )

    sr.run_all()

    return sr.scen["NPi2030"]


def add_NPi_low_dem(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Add NPi2030 to the scenario."""

    sr = make_scenario_runner(context)

    sr.add(
        "npi_low_dem_scen",
        "NPi2030",
        slice_year=2025,
        tax_emission=150,
        run_reporting=False,
        # TODO: set to MESSAGE-MACRO when workflow test finished.
        solve_typ="MESSAGE-MACRO",
    )

    sr.run_all()

    return sr.scen["npi_low_dem_scen"]


def add_NDC2035(context, scenario):
    """Add NDC policies to the scenario."""
    sr = make_scenario_runner(context)

    sr.add(
        "INDC2035",
        "INDC2030i_weak",
        mk_INDC=True,
        slice_year=2030,
        policy_year=2035,
        target_kind="Target",
        # Replaced with npi_low_dem_scen after it is solved.
        copy_demands="baseline_low_dem_scen",
        run_reporting=False,
        # TODO: set to MESSAGE-MACRO when workflow test finished.
        solve_typ="MESSAGE-MACRO",
    )

    sr.run_all()

    return sr.scen["INDC2035"]


def add_NDC2030(context, scenario):
    """Add NDC policies to the scenario."""
    sr = make_scenario_runner(context)

    sr.add(
        "INDC2030i",
        "baseline_DEFAULT",
        mk_INDC=True,
        slice_year=2025,
        policy_year=2030,
        target_kind="Target",
        copy_demands=False,  # Turned off for bmt version
        run_reporting=False,
        solve_typ="MESSAGE-MACRO",
    )

    sr.run_all()

    return sr.scen["INDC2030i"]


# Hardcoded EEU/WEU split of combined Europe ``target_mtc``.
EEU_WEU_SHARE = {"R12_EEU": 0.22, "R12_WEU": 0.78}

# Hardcoded regional shares of global bunker ``EMISS`` (``R12_GLB`` / ``TCE``).
BUNKER_SHARE_R12 = {
    "R12_AFR": 0.01,
    "R12_CHN": 0.34,
    "R12_EEU": 0.005,
    "R12_FSU": 0.19,
    "R12_LAM": 0.04,
    "R12_MEA": 0.04,
    "R12_NAM": 0.01,
    "R12_PAO": 0.005,
    "R12_PAS": 0.01,
    "R12_RCPA": 0.005,
    "R12_SAS": 0.34,
    "R12_WEU": 0.005,
}
BUNKER_SCALE = 2.8  # aviation bunker + international CO2 trade
BUNKER_GLB_DEFAULT = 237.539  # baseline MtC if R12_GLB EMISS missing


def _adjust_ndc_2030(
    region_df: pd.DataFrame,
    source: message_ix.Scenario,
    year_act: int,
) -> pd.DataFrame:
    """2030 adjustments: EEU/WEU split + subtract scaled R12_GLB bunker shares."""
    df = region_df.copy()
    n = df["region"].astype(str)

    eu = n.isin(EEU_WEU_SHARE)
    if eu.any():
        total = float(df.loc[eu, "target_mtc"].sum())
        for node, share in EEU_WEU_SHARE.items():
            df.loc[n.eq(node), "target_mtc"] = total * share

    bunker = source.var(
        "EMISS",
        filters={
            "emission": ["TCE"],
            "year": [year_act],
            "node": ["R12_GLB"],
            "type_tec": ["all"],
        },
    )
    if bunker.empty:
        bunker = source.var(
            "EMISS",
            filters={"emission": ["TCE"], "year": [year_act], "node": ["R12_GLB"]},
        )
    glb = float(bunker["lvl"].sum()) if not bunker.empty else BUNKER_GLB_DEFAULT
    if bunker.empty:
        log.warning(
            "No EMISS/TCE R12_GLB year=%s; using default %.4g MtC",
            year_act,
            BUNKER_GLB_DEFAULT,
        )
    glb *= BUNKER_SCALE

    for node, share in BUNKER_SHARE_R12.items():
        df.loc[n.eq(node), "target_mtc"] -= glb * share
    return df


def add_NDC2030_anchor(
    context: Context,
    scenario: message_ix.Scenario,
    *,
    policy_file: str = "20260917pbl_ndc_2030.csv",
    year_act: int = 2030,
) -> message_ix.Scenario:
    """Add PBL NDC 2030 TCE bounds from national targets, then solve.

    Steps: map → adjust → add_par → add_anchor → solve.
    """
    from message_ix_models.tools.anchor import map_ndc_targets
    from message_ix_models.tools.policy import add_anchor

    # 1. Map national targets → regional ``target_mtc``
    source = message_ix.Scenario(
        mp=scenario.platform,
        model=scenario.model,
        scenario="baseline_BMT",
    )  # YJ: move to args later, maybe
    region_df = map_ndc_targets(
        source,
        policy_file=policy_file,
        year_act=year_act,
        region_id=context.model.regions,
    )["region"]

    # 2. Addtional adjustments to ``target_mtc``
    region_df = _adjust_ndc_2030(region_df, source, year_act)

    # 3. Add ``bound_emission``
    ok = region_df.dropna(subset=["target_mtc"])
    bound_emission = make_df(
        "bound_emission",
        node=ok["region"].astype(str),
        type_emission="TCE",
        type_tec="all",
        type_year=year_act,
        value=ok["target_mtc"].astype(float),
        unit="Mt C/yr",
    )
    with scenario.transact("add NDC bound_emission from PBL mapper"):
        scenario.add_par("bound_emission", bound_emission)
    log.info(
        "Added %d bound_emission rows from %s (year_act=%s)",
        len(bound_emission),
        policy_file,
        year_act,
    )

    # 4. Apply remaining anchors
    add_anchor(context, scenario, stage="INDC2030i")

    # 5. Solve
    solve(context, scenario, model="MESSAGE-MACRO")
    scenario.set_as_default()
    return scenario


def add_NDC2035_anchor(
    context: Context,
    scenario: message_ix.Scenario,
    *,
    policy_file: str = "20260917pbl_ndc_2035.csv",
    year_act: int = 2035,
) -> message_ix.Scenario:
    """Add PBL NDC 2035 TCE bounds from national targets, then solve.

    Steps: map → adjust → add_par → add_anchor → solve.
    """
    from message_ix_models.tools.anchor import map_ndc_targets
    from message_ix_models.tools.policy import add_anchor

    # 1. Map national targets → regional ``target_mtc``
    source = message_ix.Scenario(
        mp=scenario.platform,
        model=scenario.model,
        scenario="baseline_BMT",
    )  # YJ: move to args later, maybe
    region_df = map_ndc_targets(
        source,
        policy_file=policy_file,
        year_act=year_act,
        region_id=context.model.regions,
    )["region"]
    print(region_df.to_string())

    # 2. No additional adjustments to ``target_mtc`` (to anchor)

    # 3. Add ``bound_emission``
    ok = region_df.dropna(subset=["target_mtc"])
    bound_emission = make_df(
        "bound_emission",
        node=ok["region"].astype(str),
        type_emission="TCE",
        type_tec="all",
        type_year=year_act,
        value=ok["target_mtc"].astype(float),
        unit="Mt C/yr",
    )
    with scenario.transact("add NDC bound_emission from PBL mapper"):
        scenario.add_par("bound_emission", bound_emission)
    log.info(
        "Added %d bound_emission rows from %s (year_act=%s)",
        len(bound_emission),
        policy_file,
        year_act,
    )

    # 4. Apply remaining anchors
    add_anchor(context, scenario, stage="INDC2035i")

    # 5. Solve
    solve(context, scenario, model="MESSAGE")
    scenario.set_as_default()
    return scenario


def add_glasgow(context, scenario, level, start_scen, target_scen, slice_yr):
    """Add Glasgow policies to the scenario."""
    sr = make_scenario_runner(context)

    # Prepare add() arguments
    add_kwargs = {
        "mk_INDC": True,
        "slice_year": slice_yr,
        "run_reporting": False,
        "solve_typ": "MESSAGE-MACRO",  # TODO: args go to config too?
    }
    if level.lower() == "full":
        add_kwargs["copy_demands"] = "baseline_low_dem_scen"

    sr.add(target_scen, start_scen, **add_kwargs)

    sr.run_all()

    # Return the target scenario that was created
    return sr.scen[target_scen]


def remove_2030_tce_tax_emission(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Remove 2030 ``tax_emission`` rows with ``type_emission`` TCE."""
    df = scenario.par(
        "tax_emission", filters={"type_emission": ["TCE"], "type_year": [2030, "2030"]}
    )
    if df.empty:
        log.info("No 2030 TCE tax_emission rows to remove from %s", scenario.url)
        return scenario

    with scenario.transact("Remove 2030 TCE tax_emission"):
        scenario.remove_par("tax_emission", df)
    log.info("Removed %d 2030 TCE tax_emission rows from %s", len(df), scenario.url)

    solve(context, scenario, model="MESSAGE")
    scenario.set_as_default()
    return scenario


def add_NPiREF(context, scenario):
    """Add NPi forever."""
    # TODO:not using _post_target for now
    # sr = make_scenario_runner(context)
    # sr.add("NPiREF",
    #        "NPi2030",
    #        pst=False,
    #        slice_year=2030,
    # )

    # sr.run_all()
    # return sr.scen["NPiREF"]

    # Get scenario name from the scenario object
    # (after clone, this is the target scenario name).
    scen = scenario.scenario

    # Load prc_terminal and prc_start_scen from config
    model_config = _get_ngfs_config(context)
    scen_config = model_config.get(scen, {})

    prc_terminal = scen_config.get("prc_terminal")
    prc_start_scen = scen_config.get("prc_start_scen")

    log.info(
        "add_NPiREF: Using prc_terminal=%s, prc_start_scen=%s for scenario '%s'",
        prc_terminal,
        prc_start_scen,
        scen,
    )
    sc_ref = message_ix.Scenario(scenario.platform, scenario.model, prc_start_scen)

    df_price = interpolate_c_price(sc_ref, price_2100=prc_terminal)

    with scenario.transact("Interpolate C-price"):
        scenario.add_par("tax_emission", df_price)
        log.info(
            "Added interpolated carbon prices to terminal year %s USD/tC",
            prc_terminal,
        )

    solve(context, scenario)
    scenario.set_as_default()

    return scenario


# Constant carbon price (USD/tC) per region for h_cpol
# YJ: only EEU/WEU/USA are solved benchmark regions, other regions proxies
NPiREF_C0_PRICE = {
    "R12_AFR": 5.12,
    "R12_CHN": 22.34,
    "R12_EEU": 67.99,
    "R12_FSU": 6.57,
    "R12_LAM": 15.13,
    "R12_MEA": 6.57,
    "R12_NAM": 34.57,
    "R12_PAO": 68.54,
    "R12_PAS": 15.13,
    "R12_RCPA": 6.57,
    "R12_SAS": 6.57,
    "R12_WEU": 82.43,
}


def add_NPiREF_c0(
    context: Context,
    scenario: message_ix.Scenario,
    *,
    start_year: int = 2030,
    end_year: int = 2110,
) -> message_ix.Scenario:
    """Apply constant regional carbon prices (``tax_emission``) for a year window."""
    info = ScenarioInfo(scenario)
    model_years = [y for y in info.Y if start_year <= y <= end_year]
    regions = [n for n in info.N if n in NPiREF_C0_PRICE]

    if not model_years:
        raise ValueError(
            f"No model years in [{start_year}, {end_year}] for {scenario.url}"
        )

    df = pd.concat(
        [
            make_df(
                "tax_emission",
                node=node,
                type_emission="TCE",
                type_tec="all",
                type_year=model_years,
                unit="USD/tC",
                value=NPiREF_C0_PRICE[node],
            )
            for node in regions
        ],
        ignore_index=True,
    )

    with scenario.transact(
        f"Apply constant NPiREF c0 carbon prices ({start_year}–{end_year})"
    ):
        bound_df = scenario.par("bound_emission")
        if not bound_df.empty:
            scenario.remove_par("bound_emission", bound_df)
        scenario.add_par("tax_emission", df)

    log.info(
        "Added constant carbon prices (%s–%s) to %s/%s (%d regions, %d years)",
        start_year,
        end_year,
        scenario.model,
        scenario.scenario,
        len(regions),
        len(model_years),
    )
    solve(context, scenario, model="MESSAGE-MACRO")
    scenario.set_as_default()
    return scenario


def add_NDC_forever(context, scenario):
    """Add NDC forever."""
    # TODO:not using _post_target for now

    # Get scenario name from context or scenario object
    scen = context.scenario_info.get("scenario", scenario.scenario)
    log.info(f"add_NDC_forever: Using scenario name '{scen}' for config lookup")

    # Load prc_terminal and prc_start_scen from config
    model_config = _get_ngfs_config(context)
    scen_config = model_config.get(scen, {})

    prc_terminal = scen_config.get("prc_terminal")
    prc_start_scen = scen_config.get("prc_start_scen")

    log.info(
        "add_NDC_forever: Using prc_terminal=%s, prc_start_scen=%s",
        prc_terminal,
        prc_start_scen,
    )
    sc_ref = message_ix.Scenario(scenario.platform, scenario.model, prc_start_scen)

    df_price = interpolate_c_price(sc_ref, price_2100=prc_terminal)

    with scenario.transact("Interpolate C-price"):
        scenario.add_par("tax_emission", df_price)
        log.info(
            "Added interpolated carbon prices to terminal year %s USD/tC",
            prc_terminal,
        )

    solve(context, scenario)
    scenario.set_as_default()

    return scenario


def step_0(context: Context, scenario: message_ix.Scenario) -> message_ix.Scenario:
    """Prepare step 0 of the EN 3 steps."""

    # A step to get a scenario ready to enter the EN 3 steps
    # For now only add the lower bound of global CO2 emissions
    # to limit high penetration of negative emissions.
    from message_ix_models.util import identify_nodes

    context.model.regions = identify_nodes(scenario)

    # Get scenario name
    scen = context.scenario_info.get("scenario", scenario.scenario)
    if scen.endswith("_base"):
        scen = scen[:-5]  # Remove "_base" suffix to get base scenario name

    # Get global_co2_bnd from config
    global_co2_bnd = _get_ngfs_config(context).get(scen, {}).get("global_co2_bnd")

    # If not found in config, default to 0.0
    if global_co2_bnd is None:
        constraint_value = 0.0
        log.info(
            "No global_co2_bnd found in config for scenario '%s', using default: 0.0",
            scen,
        )
    else:
        # If it is a string (like "500.0 / (44.0 / 12.0)"), evaluate it
        if isinstance(global_co2_bnd, str):
            try:
                constraint_value = eval(global_co2_bnd)
            except Exception as e:
                raise ValueError(
                    f"Could not evaluate global_co2_bnd expression '{global_co2_bnd}' "
                    f"for scenario '{scen}': {e}"
                )
        else:
            # If it is already a number, use it directly
            constraint_value = float(global_co2_bnd)

        log.info(
            f"Using global_co2_bnd={constraint_value:.2f} for scenario '{scen}' "
            f"(from config: {global_co2_bnd})"
        )

    add_CO2_emission_constraint.main(
        scenario,
        reg=f"{context.model.regions}_GLB",
        relation_name="CO2_Emission_Global_Total",
        constraint_value=constraint_value,
        type_rel="lower",
    )

    scenario.set_as_default()
    return scenario


def call_low_macro_demand(
    context: Context,
    scenario: message_ix.Scenario,
) -> message_ix.Scenario:
    target_model_name = "MESSAGEix-GLOBIOM 2.2-NGFS-R12"
    target_scen_name = "npi_low_dem_scen"
    commodities = ("i_spec", "i_therm")

    source = message_ix.Scenario(
        mp=scenario.platform,
        model=target_model_name,
        scenario=target_scen_name,
    )
    src = source.par("demand")

    borrowed = src[src["commodity"].isin(commodities)].copy()

    with scenario.transact("Merge low macro demand"):
        scenario.add_par("demand", borrowed)

    log.info(
        "Merged %d low-macro demand rows from %s/%s (commodities=%s)",
        len(borrowed),
        target_model_name,
        target_scen_name,
        commodities,
    )
    return scenario


def iterate_mixB(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Load MIXB buildings demand from STURM outputs, then solve."""

    scenario = call_buildings_demand(context, scenario)
    solve(context, scenario, model="MESSAGE-MACRO")

    return scenario


def step_1_and_solve(
    context: Context,
    scenario: message_ix.Scenario,
) -> message_ix.Scenario:
    """Apply budget constraint (step_1 of the EN 3 steps) and solve the scenario.

    This function gets the scenario name from context, loads the budget from config,
    validates it, applies the budget constraint using step_1,
    and then solves the scenario.
    """
    scen = _ngfs_base_scenario_name(
        context.scenario_info.get("scenario", scenario.scenario)
    )

    # Get budget from config
    budget_value = _get_ngfs_config(context).get(scen, {}).get("budget")

    # Use "non-calc" mode:
    # - `label` is ignored
    # - `budget` is passed directly to add_budget() as the bound value
    policy_config = PolicyConfig(label=str(budget_value), budget=float(budget_value))

    step_1(context, scenario, policy_config)
    # call_low_macro_demand(context, scenario)
    call_buildings_demand(context, scenario)
    solve(context, scenario)

    return scenario


def step_2_and_solve(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Apply emission trajectory (step_2 of the EN 3 steps) and solve the scenario.

    This function applies the emission trajectory constraint using step_2, which
    retrieves the CO2 emission trajectory from the solved scenario and applies it
    as bound_emission. Then it solves the scenario.

    Note: step_2 requires the scenario to have a solution (from step_1) to retrieve
    the emission trajectory from.
    """

    if not hasattr(context, "run_reporting_only"):
        # step_2 uses its own scenario_runner under engage,
        # but this flag does not matter here.
        context.run_reporting_only = False

    # TODO: discuss with Paul, step_2 does not actually use any PolicyConfig attributes?
    # Create an empty PolicyConfig to satisfy the function signature
    policy_config = PolicyConfig()

    step_2(context, scenario, policy_config)
    solve(context, scenario, model="MESSAGE")

    return scenario


def step_3_and_solve(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Apply tax_emission prices (step_3 of the EN 3 steps) and solve the scenario.

    This function loads step_3_type_emission from config, applies tax_emission prices
    using the specified type_emission (TCE_non-CO2 by default),
    and then solves the scenario.
    """
    scen = _ngfs_base_scenario_name(
        context.scenario_info.get("scenario", scenario.scenario)
    )

    # Get step_3_type_emission from config
    step_3_type_emission = (
        _get_ngfs_config(context).get(scen, {}).get("step_3_type_emission")
    )
    if step_3_type_emission is None:
        step_3_type_emission = ["TCE_non-CO2"]
    elif isinstance(step_3_type_emission, str):
        step_3_type_emission = [step_3_type_emission]
    elif isinstance(step_3_type_emission, list):
        step_3_type_emission = step_3_type_emission

    policy_config = PolicyConfig(step_3_type_emission=step_3_type_emission)

    step_3(context, scenario, policy_config)
    solve(context, scenario)

    return scenario


def step_4_and_solve(
    context: Context, scenario: message_ix.Scenario
) -> message_ix.Scenario:
    """Lock in regional TCE emission path to deliver regional carbon prices."""

    step_4(context, scenario)
    solve(context, scenario)

    return scenario


def calibrate_macro(context, scenario):
    """Runs macro calibration process on a scenario."""

    log.info("Calibrating macro to scenario.")
    # update cost_ref and price_ref with new solution
    model_config = _get_ngfs_config(context)
    update_macro_calib_file(scenario, eval(model_config["macro_data_file"]))

    # After solving, add macro calibration
    scenario = add_macro_materials(scenario, eval(model_config["macro_data_file"]))

    scenario.solve(model="MESSAGE-MACRO")
    scenario.set_as_default()

    return scenario


# NGFS P6 scenarios:
_scen_all = [
    "h_ndc",  # c0 decision:use the "h_ndc_2035" as h_ndc
    "d_delfrag",  # c0 decision:use the "d_delfrag_2035" as d_delfrag
    "h_cpol",
    "o_1p5c",
    "o_2c",
    "d_strain",
]

_scen_en_steps = [
    "o_1p5c",
    "o_2c",
    "d_delfrag",
]


def generate(context: Context) -> Workflow:
    """Create the NGFS workflow.

    Both BMT setps and NGFS steps are included in this workflow.
    """
    from message_ix_models.model.bmt.config import apply_bmt_config
    from message_ix_models.model.bmt.workflow import (
        _set_as_default,
        add_macro,
        prep_for_macro,
    )
    from message_ix_models.model.buildings.build import main as build_B
    from message_ix_models.model.transport import workflow as transport
    from message_ix_models.tools.policy import (
        add_anchor,
        add_forever_constant,
        add_forever_interpolate,
    )

    wf = Workflow(context)
    context.ssp = "SSP2"
    context.model.regions = "R12"
    context.anchor_data_file = "20260918ngfs.csv"
    apply_bmt_config(context)
    # YJ: model name is defined there in the bmt config but fine
    # YJ: nothing in this file reads context.bmt["model_name"] so all good

    # Full scenario target prefix (platform + model name)
    model_name = f"ixmp://ixmp-dev/{NGFS_MODEL_NAME}"
    base_url = (
        "ixmp://ixmp-dev/MESSAGEix-GLOBIOM-GAINS 2.1-MT-R12 EFC/"
        "baseline_DEFAULT_step_14b"
    )
    # which is the Oliver v6.6 step14 with the loil bunker fix by LC

    c = dict(keep_solution=False)

    # BMT steps
    name = wf.add_step("M", None, target=base_url)
    name = wf.add_step(
        "M cloned",
        name,
        target=f"{model_name}/baseline_M",
        clone=dict(keep_solution=True),
    )
    name = wf.add_step(
        "M fix1",
        name,
        qf_remove_meth_h2_co2_relations,
        target=f"{model_name}/baseline_M_fix1",
        clone=True,
    )  # YJ: to be integrated into anchor sheet too

    # YJ: not needed after applying anchor
    # name = wf.add_step(
    #     "M fix2",
    #     name,
    #     aas_coal_growth_near_term,
    #     target=f"{model_name}/baseline_M_fix2",
    #     clone=True,
    # )
    # name = wf.add_step(
    #     "M fix3",
    #     name,
    #     aas_dri_coal_steel_growth_near_term,
    #     target=f"{model_name}/baseline_M_fix3",
    #     clone=True,
    # )
    # name = wf.add_step("M reported", name, report)

    name = transport.add_steps(wf, name, context.transport.code)

    name = wf.add_step(
        "MT built",
        name,
        _set_as_default,
        target=f"{model_name}/baseline_MT",
        clone=True,
    )

    # YJ: not needed anymore after w34 rebase
    # name = wf.add_step(
    #     "MT fix1",
    #     name,
    #     qf_remove_ELC100_near_term_infeasibility,
    #     target=f"{model_name}/baseline_MT_fix1",
    #     clone=True,
    # )
    # name = wf.add_step(
    #     "MT fix2",
    #     name,
    #     qf_freeze_truck_history,
    #     target=f"{model_name}/baseline_MT_fix2",
    #     clone=True,
    # )
    name = wf.add_step("MT solved", name, solve)
    # name = wf.add_step("MT reported", "MT solved", report_transport)

    name = wf.add_step(
        "BMT built",
        "MT solved",
        build_B,
        target=f"{model_name}/baseline_BMT_raw",
        clone=c,
    )
    name = wf.add_step("BMT solved", name, solve)
    name = wf.add_step(
        "BMT anchored",
        name,
        add_anchor,
        target=f"{model_name}/baseline_BMT",
        clone=c,
        stage="baseline",
    )
    name = wf.add_step("BMT anchor solved", name, solve)
    name = wf.add_step("BMT reported", "BMT anchor solved", report)

    # NGFS steps

    # --- h_cpol scenario ---
    # Approach 1: add NPi2030 through ScenarioRunner
    # disabled as our coverage of demand sector current policy is thin,
    # the run did not generate meaningful carbon prices
    # wf.add_step(
    #     "NPi2030 solved",
    #     "base reported",
    #     add_NPi2030,
    #     target=f"{model_name}/NPi2030",
    # )
    # wf.add_step(
    #     "NPi_low_dem solved",
    #     "NPi2030 solved",
    #     add_NPi_low_dem,
    #     target=f"{model_name}/npi_low_dem_scen",
    #     clone=dict(keep_solution=False),
    # )

    # Approach 2: borrow the constraints from other project workflow runs
    # disabled as SMIP version can have zeros in specific regions,
    # while NGFS context interprets carbon prices as mitigation efforts.
    # The inconsistency makes this approach not suitable.
    # wf.add_step(
    #     "NPi2030 solved",
    #     "base reported",
    #     temp_borrow_par,
    #     target=f"{model_name}/NPi2030",
    #     clone=dict(keep_solution=False),
    #     target_model_name="SSP_SSP2_v6.6",
    #     target_scen_name="NPi2030",
    #     par_names=["bound_emission", "tax_emission"],
    # )

    # Approach 3: use benchmark region prices
    wf.add_step(
        "NPi2030 anchored",
        "BMT reported",
        add_anchor,
        target=f"{model_name}/NPi2030",
        clone=dict(keep_solution=False),
        stage="NPi2030",
    )  # solve EEU/WEU/USA current policy targets
    wf.add_step("NPi2030 solved", "NPi2030 anchored", solve)
    wf.add_step("NPi2030 reported", "NPi2030 solved", report)
    wf.add_step(
        "NPi2030 prep macro",
        "NPi2030 reported",
        prep_for_macro,
        target=f"{model_name}/NPi2030_message",
        clone=dict(shift_first_model_year=2030),
    )
    wf.add_step(
        "NPi2030 macro",
        "NPi2030 prep macro",
        add_macro,
        target=f"{model_name}/NPi2030_message_macro",
        # YJ: cannot rename
    )
    wf.add_step(
        "baseline built",
        "NPi2030 macro",
        target=f"{model_name}/baseline_DEFAULT",
        clone=dict(keep_solution=True),
        # YJ: cannot rename in the last step but have to
        # start with this scen name for the old SR policy scenarios
        # YJ: the baseline_DEFAULT in this wf is actually
        # solved NPi2030 with FMY=2030 and MACRO added,
        # as this is what NiGEM takes as a baseline to compare other scens with
    )
    wf.add_step("baseline reported", "baseline built", report)
    wf.add_step(
        "h_cpol solved",
        "baseline reported",
        add_forever_constant,
        target=f"{model_name}/h_cpol",
        clone=dict(keep_solution=True),
        price_year=2030,
        solve_type="MESSAGE-MACRO",
    )

    # --- d_strain scenario ---
    wf.add_step(
        "glasgow_partial_2030 solved",
        "baseline reported",
        add_glasgow,
        target=f"{model_name}/glasgow_partial_2030",
        target_scen="glasgow_partial_2030",
        slice_yr=2025,
        start_scen="baseline_DEFAULT",
        level="Partial",
    )
    wf.add_step(
        "d_strain calibrated",
        "glasgow_partial_2030 solved",
        remove_2030_tce_tax_emission,
        target=f"{model_name}/d_strain_calibrated",
        clone=dict(keep_solution=False),
    )

    wf.add_step(
        "d_strain mixb called",
        "d_strain calibrated",
        call_sturm,
    )

    wf.add_step(
        "d_strain solved",
        "d_strain mixb called",
        iterate_mixB,
        target=f"{model_name}/d_strain",
        clone=dict(keep_solution=False),
    )

    # --- h_ndc scenario ---
    # Approach 1: add NDC2030 through ScenarioRunner
    # disabled to be consistent with other model implementations,
    # also the SR way can give higher emission bounds under NDC
    # than baseline in some regions (see red flagged regions in mapper),
    # which generates zero carbon prices and is not desirable for NGFS context.
    # wf.add_step(
    #     "NDC2030 solved",
    #     "base reported",
    #     add_NDC2030,
    #     target=f"{model_name}/INDC2030i",
    # )

    # Approach 2: use anchor and mapper to add PBL ndc levels
    wf.add_step(
        "NDC2030 solved",
        "baseline reported",
        add_NDC2030_anchor,
        target=f"{model_name}/INDC2030i",
        clone=dict(keep_solution=False),
    )
    wf.add_step(
        "NDC2030 reported",
        "NDC2030 solved",
        report,
    )
    wf.add_step(
        "NDC2035 solved",
        "NDC2030 reported",
        add_NDC2035_anchor,
        target=f"{model_name}/INDC2035i",
        clone=dict(keep_solution=False, shift_first_model_year=2035),
    )

    wf.add_step(
        "NDC_forever solved",
        "NDC2035 solved",
        add_forever_constant,
        target=f"{model_name}/NDC_forever",
        clone=dict(keep_solution=True),
        price_year=2035,
        solve_type="MESSAGE",
    )
    wf.add_step(
        "NDC_forever mixb called",
        "NDC_forever solved",
        call_sturm,
    )
    wf.add_step(
        "h_ndc solved",
        "NDC_forever mixb called",
        iterate_mixB,
        target=f"{model_name}/h_ndc",
        clone=dict(keep_solution=False),
    )

    # --- o_2c scenario ---
    wf.add_step(
        "o_2c base built",
        "glasgow_partial_2030 solved",
        step_0,
        target=f"{model_name}/o_2c_base",
        clone=dict(keep_solution=False),
    )

    # --- o_1p5c scenario ---
    # Prepare: high carbon price test runs
    wf.add_step(
        "o_1p5c high price test",
        "baseline reported",
        add_forever_interpolate,
        target=f"{model_name}/o_1p5c_high_price_test",
        clone=dict(keep_solution=True),
        price_2110=1400,
        solve_type="MESSAGE",
    )

    # Approach 1: add through ScenarioRunner
    wf.add_step(
        "glasgow_full_2030 solved",
        "baseline reported",
        add_glasgow,
        target=f"{model_name}/glasgow_full_2030",
        start_scen="baseline_DEFAULT",
        target_scen="glasgow_full_2030",
        slice_yr=2025,
        level="Full",
    )

    wf.add_step(
        "glasgow_full_2030 reported",
        "glasgow_full_2030 solved",
        report,
    )

    wf.add_step(
        "o_1p5c base built",
        "baseline reported",
        step_0,
        target=f"{model_name}/o_1p5c_base",
        clone=dict(keep_solution=False),
    )

    # --- d_delfrag scenario ---
    # Approach 1: add through ScenarioRunner and with delay by 2035
    # wf.add_step(
    #     "d_delfrag_2030_2035 solved",
    #     "h_cpol solved",
    #     add_glasgow,
    #     target=f"{model_name}/d_delfrag_2030_glasgow_partial",
    #     target_scen="d_delfrag_2030_glasgow_partial",
    #     slice_yr=2030,
    #     start_scen="h_cpol",
    #     level="Partial",
    #     clone=dict(keep_solution=True, shift_first_model_year=2035),
    # )

    # wf.add_step(
    #     "d_delfrag_2030_2035 reported",
    #     "d_delfrag_2030_2035 solved",
    #     report,
    # )

    # wf.add_step(
    #     "d_delfrag base built",
    #     "d_delfrag_2030_2035 reported",
    #     step_0,
    #     target=f"{model_name}/d_delfrag_base",
    #     clone=dict(keep_solution=False),
    # )

    # Approach 2: add through ScenarioRunner and with delay by 2030
    wf.add_step(
        "d_delfrag_2035 solved",
        "h_cpol solved",
        add_glasgow,
        target=f"{model_name}/d_delfrag_2035_glasgow_partial",
        target_scen="d_delfrag_2035_glasgow_partial",
        slice_yr=2035,
        start_scen="h_cpol",
        level="Partial",
        clone=dict(keep_solution=True, shift_first_model_year=2040),
    )

    wf.add_step(
        "d_delfrag_2035 reported",
        "d_delfrag_2035 solved",
        report,
    )

    wf.add_step(
        "d_delfrag base built",
        "d_delfrag_2035 reported",
        step_0,
        target=f"{model_name}/d_delfrag_base",
        clone=dict(keep_solution=False),
    )

    # --- Fill in EN steps ---
    for scen in _scen_en_steps:
        wf.add_step(
            f"{scen} EN1",
            f"{scen} base built",
            step_1_and_solve,
            target=f"{model_name}/{scen}_EN1",
            clone=dict(keep_solution=False),
        )

        wf.add_step(
            f"{scen} EN2",
            f"{scen} EN1",
            step_2_and_solve,
            target=f"{model_name}/{scen}_EN2",
            # Must have solution to retrieve emission trajectories.
            clone=dict(keep_solution=True),
        )

        wf.add_step(
            f"{scen} EN2 reported",
            f"{scen} EN2",
            report,
        )

        wf.add_step(
            f"{scen} EN3",
            f"{scen} EN2",
            step_3_and_solve,
            target=f"{model_name}/{scen}_EN3",
            # Must have solution to retrieve prices.
            clone=dict(keep_solution=True),
        )

        wf.add_step(
            f"{scen} EN3 reported",
            f"{scen} EN3",
            report,
        )

        wf.add_step(
            f"{scen} solved",
            f"{scen} EN3",
            step_4_and_solve,
            target=f"{model_name}/{scen}",
            # Must have solution to retrieve prices.
            clone=dict(keep_solution=True),
        )

    for scen in _scen_all:
        wf.add_step(
            f"{scen} reported",
            f"{scen} solved",
            report,
        )

    reported_keys = [Key(f"{scen} reported") for scen in _scen_all]
    wf.add("prep all", *reported_keys, placeholder, "context")

    return wf
