"""Prepare data for water use for cooling & energy technologies."""

import logging
from typing import TYPE_CHECKING, Optional

import numpy as np
import pandas as pd
import yaml
from message_ix import make_df

from message_ix_models import Context
from message_ix_models.model.water.data.water_supply import map_basin_region_wat
from message_ix_models.model.water.utils import get_vintage_and_active_years
from message_ix_models.util import broadcast, make_matched_dfs, package_data_path, same_node

if TYPE_CHECKING:
    from message_ix import Scenario

log = logging.getLogger(__name__)

# Load configuration
_CONFIG_PATH = package_data_path("water", "ppl_cooling_config.yaml")
with open(_CONFIG_PATH) as f:
    CONFIG = yaml.safe_load(f)


def _load_scenario_and_cooling_data(
    context: "Context", scenario: Optional["Scenario"] = None
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list, "Scenario", pd.DataFrame]:
    """Load cooling tech specs and parent technology data from scenario.

    Returns
    -------
    cooling_df : DataFrame with cooling technology specifications
    ref_input : DataFrame with parent technology input/output parameters
    df_node : DataFrame with basin delineation
    node_region : List of unique region nodes
    scen : Scenario object
    cost_share_df : DataFrame with cost and share data
    """
    # File paths
    tech_perf_path = package_data_path(
        "water", "ppl_cooling_tech", "tech_water_performance_ssp_msg.csv"
    )
    cost_share_file = (
        f"cooltech_cost_and_shares_"
        f"{'ssp_msg_' + context.regions if context.type_reg == 'global' else 'country'}.csv"
    )
    cost_share_path = package_data_path("water", "ppl_cooling_tech", cost_share_file)
    basin_path = package_data_path(
        "water", "delineation", f"basins_by_region_simpl_{context.regions}.csv"
    )

    # Load basin delineation
    df_node = pd.read_csv(basin_path)
    df_node["node"] = "B" + df_node["BCU_name"].astype(str)
    df_node["mode"] = "M" + df_node["BCU_name"].astype(str)
    df_node["region"] = (
        context.map_ISO_c[context.regions]
        if context.type_reg == "country"
        else f"{context.regions}_" + df_node["REGION"].astype(str)
    )
    node_region = df_node["region"].unique()

    # Load cooling technology specs
    cooling_df = pd.read_csv(tech_perf_path)
    cooling_df = cooling_df[cooling_df["technology_group"] == "cooling"].copy()
    cooling_df["parent_tech"] = cooling_df["technology_name"].str.split("__").str[0]

    # Get scenario and parent tech parameters
    scen = scenario or context.get_scenario()
    ref_input = scen.par("input", {"technology": cooling_df["parent_tech"]})

    # Handle technologies with only output (e.g., CSP)
    missing = cooling_df["parent_tech"][~cooling_df["parent_tech"].isin(ref_input["technology"])]
    if not missing.empty:
        ref_output = scen.par("output", {"technology": missing})
        if not ref_output.empty:
            # Map output columns to input column semantics
            ref_output = ref_output.rename(columns={
                "node_dest": "node_origin",
                "time_dest": "time_origin"
            })
            ref_input = pd.concat([ref_input, ref_output], ignore_index=True)

    # Load cost/share data for later use
    cost_share_df = pd.read_csv(cost_share_path)

    return cooling_df, ref_input, df_node, list(node_region), scen, cost_share_df


def _fill_missing_tech_values(ref_input: pd.DataFrame) -> pd.DataFrame:
    """Fill missing technology efficiency values from config."""
    efficiencies = CONFIG["missing_tech_efficiencies"]

    def apply_fill(row):
        if pd.isna(row["technology"]):
            return row["value"], row["level"]

        for tech_key, eff_value in efficiencies.items():
            if tech_key in row["technology"]:
                value = eff_value if row["value"] >= 1 else max(row["value"], eff_value)
                level = "dummy_supply" if row["level"] == "cooling" else row["level"]
                return value, level

        return row["value"], row["level"]

    ref_input[["value", "level"]] = ref_input.apply(
        lambda r: pd.Series(apply_fill(r)), axis=1
    )
    return ref_input


def _compute_cooling_rates(input_cool: pd.DataFrame) -> pd.DataFrame:
    """Compute water withdrawal, return, and consumption rates."""
    flue_loss = CONFIG["flue_gas_loss_fraction"]
    m3_to_mcm = CONFIG["m3_gj_to_mcm_gwa"]

    # Cooling fraction: heat to be rejected
    input_cool["cooling_fraction"] = input_cool.apply(
        lambda r: r["value"] - 1
        if "hpl" in str(r.get("parent_tech", ""))
        else r["value"] * (1 - flue_loss) - 1,
        axis=1,
    )

    # Water withdrawal rate (MCM/GWa)
    input_cool["value_cool"] = (
        input_cool["water_withdrawal_mid_m3_per_output"]
        * m3_to_mcm
        / input_cool["cooling_fraction"]
    )
    input_cool["value_cool"] = np.where(input_cool["value_cool"] < 0, 1e-6, input_cool["value_cool"])

    # Return and consumption rates
    withdrawal = input_cool["water_withdrawal_mid_m3_per_output"]
    consumption = input_cool["water_consumption_mid_m3_per_output"]
    input_cool["return_rate"] = 1 - consumption / withdrawal
    input_cool["consumption_rate"] = consumption / withdrawal
    input_cool["value_return"] = input_cool["return_rate"] * input_cool["value_cool"]
    input_cool["value_consumption"] = input_cool["consumption_rate"] * input_cool["value_cool"]

    return input_cool


def _make_input_params(
    input_cool: pd.DataFrame, context: "Context"
) -> pd.DataFrame:
    """Generate input parameter DataFrame."""
    commodity = "surfacewater" if context.nexus_set == "nexus" else "freshwater"

    # Electricity input for parasitic demand
    electr = input_cool[input_cool["parasitic_electricity_demand_fraction"] > 0].copy()
    electr["value_cool"] = (
        electr["parasitic_electricity_demand_fraction"] / electr["cooling_fraction"]
    )
    electr["value_cool"] = np.where(electr["value_cool"] < 0, 1e-6, electr["value_cool"])

    inp_elec = make_df(
        "input",
        node_loc=electr["node_loc"],
        technology=electr["technology_name"],
        year_vtg=electr["year_vtg"],
        year_act=electr["year_act"],
        mode=electr["mode"],
        node_origin=electr["node_origin"],
        commodity="electr",
        level="secondary",
        time="year",
        time_origin="year",
        value=electr["value_cool"],
        unit="GWa",
    )

    # Freshwater input (exclude saline and air)
    fresh = input_cool[
        ~input_cool["technology_name"].str.endswith(("ot_saline", "air"), na=False)
    ]
    inp_fresh = make_df(
        "input",
        node_loc=fresh["node_loc"],
        technology=fresh["technology_name"],
        year_vtg=fresh["year_vtg"],
        year_act=fresh["year_act"],
        mode=fresh["mode"],
        node_origin=fresh["node_origin"],
        commodity=commodity,
        level="water_supply",
        time="year",
        time_origin="year",
        value=fresh["value_cool"],
        unit="MCM/GWa",
    )

    # Saline water input
    saline = input_cool[input_cool["technology_name"].str.endswith("ot_saline", na=False)]
    inp_saline = make_df(
        "input",
        node_loc=saline["node_loc"],
        technology=saline["technology_name"],
        year_vtg=saline["year_vtg"],
        year_act=saline["year_act"],
        mode=saline["mode"],
        node_origin=saline["node_origin"],
        commodity="saline_ppl",
        level="saline_supply",
        time="year",
        time_origin="year",
        value=saline["value_cool"],
        unit="MCM/GWa",
    )

    return pd.concat([inp_elec, inp_fresh, inp_saline]).dropna(subset=["value"])


def _make_output_params(input_cool: pd.DataFrame) -> pd.DataFrame:
    """Generate output parameter DataFrame."""
    # Share commodity output
    out_share = make_df(
        "output",
        node_loc=input_cool["node_loc"],
        technology=input_cool["technology_name"],
        year_vtg=input_cool["year_vtg"],
        year_act=input_cool["year_act"],
        mode=input_cool["mode"],
        node_dest=input_cool["node_origin"],
        commodity=input_cool["technology_name"].str.split("__").str[1],
        level="share",
        time="year",
        time_dest="year",
        value=1,
        unit="-",
    )

    # Water return output (freshwater only, not air)
    fresh = input_cool[
        ~input_cool["technology_name"].str.endswith(("ot_saline", "air"), na=False)
    ]
    out_return = make_df(
        "output",
        node_loc=fresh["node_loc"],
        technology=fresh["technology_name"],
        year_vtg=fresh["year_vtg"],
        year_act=fresh["year_act"],
        mode=fresh["mode"],
        node_dest=fresh["node_origin"],
        commodity="water_return",
        level="water_supply",
        time="year",
        time_dest="year",
        value=fresh["value_return"],
        unit="MCM/GWa",
    )

    return pd.concat([out_share, out_return])


def _make_emission_factor(input_cool: pd.DataFrame) -> pd.DataFrame:
    """Generate emission_factor for water return tracking."""
    # Exclude air cooling
    df = input_cool[~input_cool["technology_name"].str.endswith("air", na=False)]
    return make_df(
        "emission_factor",
        node_loc=df["node_loc"],
        technology=df["technology_name"],
        year_vtg=df["year_vtg"],
        year_act=df["year_act"],
        mode=df["mode"],
        emission="fresh_return",
        value=df["value_return"],
        unit="MCM/GWa",
    )


def _make_addon_params(input_cool: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate addon_conversion and addon_lo parameters."""
    addon = make_df(
        "addon_conversion",
        node=input_cool["node_loc"],
        technology=input_cool["parent_tech"],
        year_vtg=input_cool["year_vtg"],
        year_act=input_cool["year_act"],
        mode=input_cool["mode"],
        time="year",
        type_addon="cooling__" + input_cool["parent_tech"].astype(str),
        value=input_cool["cooling_fraction"],
        unit="-",
    )
    addon_lo = make_matched_dfs(addon, addon_lo=1)["addon_lo"]
    return addon, addon_lo


def _make_technical_lifetime(
    inp: pd.DataFrame, node_region: list, info
) -> pd.DataFrame:
    """Generate technical_lifetime parameter."""
    min_year = CONFIG["min_vintage_year"]
    lifetime = CONFIG["technical_lifetime_years"]

    year = info.yv_ya.year_vtg.drop_duplicates()
    year = year[year >= min_year]

    return (
        make_df(
            "technical_lifetime",
            technology=inp["technology"].drop_duplicates(),
            value=lifetime,
            unit="year",
        )
        .pipe(broadcast, year_vtg=year, node_loc=node_region)
        .pipe(same_node)
    )


def _make_capacity_factor(inp: pd.DataFrame, context: "Context") -> pd.DataFrame:
    """Generate capacity_factor parameter with optional climate impacts."""
    cap_fact = make_matched_dfs(inp, capacity_factor=1)["capacity_factor"]

    if context.RCP == "no_climate":
        return cap_fact

    # Apply climate impacts on freshwater cooling
    impact_path = package_data_path(
        "water",
        "ppl_cooling_tech",
        f"power_plant_cooling_impact_MESSAGE_{context.regions}_{context.RCP}.csv",
    )
    df_impact = pd.read_csv(impact_path)

    for node in df_impact["node"]:
        is_fresh = cap_fact["technology"].str.contains("fresh")
        node_match = cap_fact["node_loc"] == node

        conditions = [
            is_fresh & (cap_fact["year_act"] >= 2025) & (cap_fact["year_act"] < 2050) & node_match,
            is_fresh & (cap_fact["year_act"] >= 2050) & (cap_fact["year_act"] < 2070) & node_match,
            is_fresh & (cap_fact["year_act"] >= 2070) & node_match,
        ]
        choices = [
            df_impact[df_impact["node"] == node]["2025s"].values[0],
            df_impact[df_impact["node"] == node]["2050s"].values[0],
            df_impact[df_impact["node"] == node]["2070s"].values[0],
        ]
        cap_fact["value"] = np.select(conditions, choices, default=cap_fact["value"])

    return cap_fact


def _make_investment_cost(context: "Context") -> pd.DataFrame:
    """Generate inv_cost from cost projection tool."""
    from message_ix_models.tools.costs.config import MODULE, Config
    from message_ix_models.tools.costs.projections import create_cost_projections

    excluded = CONFIG["excluded_cooling_techs"]
    techs_to_remove = [
        f"{t}{suffix}"
        for t in excluded
        for suffix in CONFIG["cooling_suffixes"]
    ]

    cfg = Config(
        module=MODULE.cooling,
        scenario=context.ssp,
        method="gdp",
        node=context.regions,
    )
    cost_proj = create_cost_projections(cfg)
    inv_cost = cost_proj["inv_cost"][["year_vtg", "node_loc", "technology", "value", "unit"]]
    inv_cost = inv_cost[~inv_cost["technology"].isin(techs_to_remove)]
    inv_cost = inv_cost[inv_cost["technology"].str.contains("__")]

    return inv_cost


def _expand_historical_params(
    scen: "Scenario", cooling_df: pd.DataFrame, cost_share_df: pd.DataFrame, input_cool_2015: pd.DataFrame, context: "Context"
) -> dict:
    """Expand historical_activity and historical_new_capacity to cooling variants.

    Uses regional average shares (not per-parent-tech) to avoid over-allocating
    to saline cooling. Per-tech shares gave nuclear plants 50-65% saline which
    inflated saline historical_activity by 2x, causing model to expand saline.
    """
    results = {}
    suffixes = CONFIG["cooling_suffixes"]
    cap_flex = CONFIG["capacity_flexibility_factor"]

    # Get parent capacity factors
    cap_fact_parent = scen.par("capacity_factor", {"technology": cooling_df["parent_tech"]})
    cap_fact_pre = cap_fact_parent[cap_fact_parent["year_vtg"] < scen.firstmodelyear]
    cap_fact_pre = cap_fact_pre.groupby(["node_loc", "technology", "year_vtg"], as_index=False)["value"].min()

    cap_fact_post = cap_fact_parent[cap_fact_parent["year_act"] >= scen.firstmodelyear]
    cap_fact_post = cap_fact_post.groupby(["node_loc", "technology", "year_act"], as_index=False)["value"].min()
    cap_fact_post.rename(columns={"year_act": "year_vtg"}, inplace=True)

    cap_fact_combined = pd.concat([cap_fact_pre, cap_fact_post])
    cap_fact_combined.rename(columns={"value": "cap_fact", "technology": "utype"}, inplace=True)

    # Compute regional average shares by cooling type (not per-parent-tech)
    # This prevents nuclear's high saline share from inflating saline allocation
    mix_cols = [c for c in cost_share_df.columns if c.startswith("mix_")]
    share_cols = [c.replace("mix_", "") for c in mix_cols]

    regional_avg_shares = cost_share_df.groupby("cooling")[mix_cols].mean()
    regional_avg_shares.columns = share_cols

    # Build share lookup: cooling_type -> region -> share
    regional_shares_long = regional_avg_shares.reset_index().melt(
        id_vars=["cooling"],
        var_name="node_loc",
        value_name="share"
    )

    # Get average cooling fraction per cooling type per region
    hold_df = input_cool_2015[["node_loc", "technology_name", "cooling_fraction"]].copy()
    hold_df["cooling"] = hold_df["technology_name"].str.split("__").str[1]
    avg_cf = hold_df.groupby(["node_loc", "cooling"])["cooling_fraction"].mean().reset_index()

    for param_name in ["historical_activity", "historical_new_capacity"]:
        parent_data = scen.par(param_name, {"technology": cooling_df["parent_tech"]})
        if parent_data.empty:
            continue

        # Expand to cooling variants
        expanded = pd.concat([
            parent_data.assign(technology=parent_data["technology"] + suffix)
            for suffix in suffixes
        ])
        expanded["cooling"] = expanded["technology"].str.split("__").str[1]

        # Merge regional average share
        expanded = expanded.merge(
            regional_shares_long,
            on=["cooling", "node_loc"],
            how="left"
        )

        # Merge average cooling fraction
        expanded = expanded.merge(
            avg_cf,
            on=["cooling", "node_loc"],
            how="left"
        )

        # Apply multiplier: share × cooling_fraction (zero if missing cf)
        expanded["multiplier"] = expanded["share"] * expanded["cooling_fraction"].fillna(0)
        expanded["value"] *= expanded["multiplier"]
        expanded = expanded[expanded["value"] > 0]

        # Apply capacity factor for capacity params
        expanded["utype"] = expanded["technology"].str.split("__").str[0]
        if "capacity" in param_name:
            expanded = expanded.merge(cap_fact_combined, how="left")
            expanded = expanded[expanded["cap_fact"].notna()]
            expanded["value"] *= expanded["cap_fact"] * cap_flex
            expanded.drop(columns=["cap_fact"], inplace=True, errors="ignore")

        expanded.drop(columns=["utype", "multiplier", "cooling", "share", "cooling_fraction"], inplace=True, errors="ignore")
        results[param_name] = expanded

    return results


def _make_ssp_share_constraints(context: "Context") -> pd.DataFrame:
    """Generate share_commodity_up from SSP YAML config."""
    yaml_path = package_data_path("water", "ssp.yaml")
    try:
        with open(yaml_path) as f:
            yaml_data = yaml.safe_load(f)
    except FileNotFoundError:
        log.warning(f"SSP YAML file not found at {yaml_path}")
        return pd.DataFrame()

    ssp = context.ssp
    scenarios = yaml_data.get("scenarios", {})
    if ssp not in scenarios:
        log.warning(f"SSP '{ssp}' not found in SSP YAML")
        return pd.DataFrame()

    macro_regions = yaml_data.get("macro-regions", {})
    ssp_data = scenarios[ssp]["cooling_tech"]
    info = context["water build info"]
    start_year = CONFIG["share_constraint_start_year"]
    year_constraint = [y for y in info.Y if y >= start_year]

    results = []
    for macro_region, region_data in ssp_data.items():
        shares = region_data.get("share_commodity_up", {})
        reg_nodes = [n for n in info.N if any(n.endswith(r) for r in macro_regions.get(macro_region, []))]

        for share_name, value in shares.items():
            df = make_df(
                "share_commodity_up",
                shares=[share_name],
                time=["year"],
                value=[value],
                unit=["-"],
            ).pipe(broadcast, year_act=year_constraint, node_share=reg_nodes)
            results.append(df)

    return pd.concat(results) if results else pd.DataFrame()


def _add_saline_bounds(results: dict, info) -> None:
    """Add bound_activity_up for saline water extraction."""
    if "historical_activity" not in results:
        return

    saline_hist = results["historical_activity"][
        results["historical_activity"]["technology"].str.endswith("__ot_saline", na=False)
    ]
    if saline_hist.empty:
        return

    last_year = saline_hist["year_act"].max()
    regional = saline_hist[saline_hist["year_act"] == last_year].groupby("node_loc")["value"].sum()

    default_bound = CONFIG["default_saline_bound"]
    bounds = [
        {"node_loc": region, "value": max(val, default_bound) if val > 0 else default_bound}
        for region, val in regional.items()
    ]

    if bounds:
        bound_df = make_df(
            "bound_activity_up",
            technology="extract_salinewater_cool",
            node_loc=[b["node_loc"] for b in bounds],
            mode="M1",
            time="year",
            value=[b["value"] for b in bounds],
            unit="MCM",
        ).pipe(broadcast, year_act=info.Y)

        results["bound_activity_up"] = pd.concat(
            [results.get("bound_activity_up", pd.DataFrame()), bound_df],
            ignore_index=True,
        )


def _add_nexus_params(results: dict, context: "Context", node_region: list, info) -> None:
    """Add basin-region distribution for nexus mode."""
    if context.nexus_set != "nexus":
        return

    df_sw = map_basin_region_wat(context)
    df_sw.drop(columns=["mode", "date", "MSGREG"], inplace=True, errors="ignore")
    df_sw.rename(
        columns={"region": "node_dest", "node": "node_dest", "time": "time_dest", "year": "year_act"},
        inplace=True,
    )
    df_sw["time_dest"] = df_sw["time_dest"].astype(str)

    year_comb = get_vintage_and_active_years(info, technical_lifetime=1, same_year_only=True)

    # Input: regional water_return
    reg_input = make_df(
        "input",
        technology="reg_to_basin",
        node_loc=node_region,
        commodity="water_return",
        level="water_supply",
        time="year",
        time_origin="year",
        node_origin=node_region,
        value=1,
        unit="MCM/GWa",
        mode="M1",
    ).pipe(broadcast, year_comb)

    # Output: distribute to basins by share
    outputs = []
    for region in node_region:
        region_code = region.split("_")[-1]
        basins = df_sw[df_sw["node_dest"].str.contains(region_code, na=False)]
        if basins.empty:
            continue

        out = (
            make_df(
                "output",
                technology="reg_to_basin",
                node_loc=region,
                commodity="surfacewater_basin",
                level="water_avail_basin",
                time="year",
                time_dest="year",
                value=1,
                unit="MCM/GWa",
                mode="M1",
            )
            .pipe(broadcast, year_comb)
            .pipe(broadcast, node_dest=basins["node_dest"].unique())
            .merge(basins.drop_duplicates("node_dest")[["node_dest", "share"]], on="node_dest", how="left")
        )
        out["value"] *= out["share"]
        outputs.append(out.drop(columns=["share"]).dropna(subset=["value"]))

    if outputs:
        results["input"] = pd.concat([results["input"], reg_input])
        results["output"] = pd.concat([results["output"], pd.concat(outputs)])


def cool_tech(context: "Context", scenario: Optional["Scenario"] = None) -> dict[str, pd.DataFrame]:
    """Process cooling technology data for a scenario instance.

    Parameters
    ----------
    context : .Context
    scenario : .Scenario, optional

    Returns
    -------
    dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names. Values are DataFrames for add_par().
    """
    info = context["water build info"]

    # Load data
    cooling_df, ref_input, df_node, node_region, scen, cost_share_df = _load_scenario_and_cooling_data(
        context, scenario
    )

    # Fill missing efficiency values
    ref_input = _fill_missing_tech_values(ref_input)

    # Merge cooling specs with parent tech data
    input_cool = (
        cooling_df.set_index("parent_tech")
        .combine_first(ref_input.set_index("technology"))
        .reset_index()
    )
    input_cool = input_cool.dropna(subset=["value"])
    input_cool["year_vtg"] = input_cool["year_vtg"].astype(int)
    input_cool["year_act"] = input_cool["year_act"].astype(int)

    # Filter valid year combinations
    year_comb = get_vintage_and_active_years(info, technical_lifetime=30, same_year_only=False)
    valid_years = set(zip(year_comb["year_vtg"], year_comb["year_act"]))
    input_cool = input_cool[
        input_cool.apply(lambda r: (int(r["year_vtg"]), int(r["year_act"])) in valid_years, axis=1)
    ]

    # Filter levels and technologies
    input_cool = input_cool[~input_cool["level"].isin(["water_supply", "cooling"])]
    input_cool = input_cool[~input_cool["technology_name"].str.contains("hpl", na=False)]

    # Handle global region swapping
    glb = f"{context.regions}_GLB"
    input_cool.loc[input_cool["node_loc"] == glb, "node_loc"] = input_cool["node_origin"]
    input_cool.loc[input_cool["node_origin"] == glb, "node_origin"] = input_cool["node_loc"]

    # Compute cooling rates
    input_cool = _compute_cooling_rates(input_cool)

    # Backfill 2015 data for historical params
    input_cool_2015 = input_cool[
        (input_cool["year_act"] == 2015) & (input_cool["year_vtg"] == 2015)
    ].copy()
    existing = set(zip(input_cool["parent_tech"], input_cool["node_loc"]))
    have_2015 = set(zip(input_cool_2015["parent_tech"], input_cool_2015["node_loc"]))
    missing = existing - have_2015

    for year in [2020, 2010, 2030, 2050, 2000, 2080, 1990]:
        if not missing:
            break
        fill = input_cool[
            (input_cool["year_act"] == year)
            & (input_cool["year_vtg"] == year)
            & input_cool.apply(lambda r: (r["parent_tech"], r["node_loc"]) in missing, axis=1)
        ].copy()
        if not fill.empty:
            fill[["year_act", "year_vtg"]] = 2015
            input_cool_2015 = pd.concat([input_cool_2015, fill])
            have_2015 = set(zip(input_cool_2015["parent_tech"], input_cool_2015["node_loc"]))
            missing = existing - have_2015

    if missing:
        log.warning(f"Missing 2015 data for: {missing}")

    # Build parameters
    inp = _make_input_params(input_cool, context)
    results = {
        "input": inp,
        "output": _make_output_params(input_cool),
        "emission_factor": _make_emission_factor(input_cool),
        "technical_lifetime": _make_technical_lifetime(inp, node_region, info),
        "capacity_factor": _make_capacity_factor(inp, context),
        "inv_cost": _make_investment_cost(context),
    }

    addon, addon_lo = _make_addon_params(input_cool)
    results["addon_conversion"] = addon
    results["addon_lo"] = addon_lo

    # Nexus basin distribution
    _add_nexus_params(results, context, node_region, info)

    # Historical parameters
    hist_params = _expand_historical_params(scen, cooling_df, cost_share_df, input_cool_2015, context)
    results.update(hist_params)

    # SSP share constraints
    share_constraints = _make_ssp_share_constraints(context)
    if not share_constraints.empty:
        results["share_commodity_up"] = share_constraints

    # Saline extraction bounds
    _add_saline_bounds(results, info)

    return results


def non_cooling_tec(context: "Context", scenario=None) -> dict[str, pd.DataFrame]:
    """Process water usage for non-cooling power plant technologies.

    Parameters
    ----------
    context : .Context
    scenario : .Scenario, optional

    Returns
    -------
    dict of (str -> pandas.DataFrame)
    """
    path = package_data_path("water", "ppl_cooling_tech", "tech_water_performance_ssp_msg.csv")
    df = pd.read_csv(path)

    non_cool = df[
        (df["technology_group"] != "cooling") & (df["water_supply_type"] == "freshwater_supply")
    ].copy()
    non_cool.rename(columns={"technology_name": "technology"}, inplace=True)

    scen = scenario or context.get_scenario()
    all_tech = scen.par("technical_lifetime")["technology"].unique()
    non_cool = non_cool[non_cool["technology"].isin(all_tech)]

    non_cool["value"] = non_cool["water_withdrawal_mid_m3_per_output"] * CONFIG["m3_gj_to_mcm_gwa"]

    output_data = scen.par("output", {"technology": non_cool["technology"].unique()})
    output_data = output_data[
        (output_data["node_loc"] != f"{context.regions}_GLB")
        & (output_data["node_dest"] != f"{context.regions}_GLB")
    ]

    merged = output_data.merge(non_cool, on="technology", how="right").dropna()

    commodity = "surfacewater" if context.nexus_set == "nexus" else "freshwater"

    inp = make_df(
        "input",
        technology=merged["technology"],
        value=merged["value_y"],
        unit="MCM/GWa",
        level="water_supply",
        commodity=commodity,
        time_origin="year",
        mode="M1",
        time="year",
        year_vtg=merged["year_vtg"].astype(int),
        year_act=merged["year_act"].astype(int),
        node_loc=merged["node_loc"],
        node_origin=merged["node_dest"],
    )

    return {"input": inp}
