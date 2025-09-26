"""Prepare data for water use for cooling & energy technologies."""

import logging
from typing import TYPE_CHECKING, Optional

import numpy as np
import pandas as pd
import yaml
from message_ix import make_df

from message_ix_models import Context
from message_ix_models.model.water.data.water_supply import map_basin_region_wat
from message_ix_models.model.water.utils import (
    get_vintage_and_active_years,
    m3_GJ_TO_MCM_GWa,
)
from message_ix_models.util import (
    broadcast,
    make_matched_dfs,
    package_data_path,
    same_node,
)

if TYPE_CHECKING:
    from message_ix import Scenario
log = logging.getLogger(__name__)


def missing_tech(x: pd.Series) -> pd.Series:
    """Assign values to missing data.
    It goes through the input data frame and extract the technologies which
    don't have input values and then assign manual  values to those technologies
    along with assigning them an arbitrary level i.e dummy supply
    """
    data_dic = {
        "geo_hpl": 1 / 0.850,
        "geo_ppl": 1 / 0.385,
        "gas_hpl": 1 / 0.3,
        "foil_hpl": 1 / 0.25,
        "nuc_hc": 1 / 0.326,
        "nuc_lc": 1 / 0.326,
        "solar_th_ppl": 1 / 0.385,
        "csp_sm1_res": 1 / 0.385,
        "csp_sm3_res": 1 / 0.385,
    }

    if pd.notna(x["technology"]):
        # Find a matching key in `data_dic` using substring matching
        matched_key = next((key for key in data_dic if key in x["technology"]), None)

        if matched_key:
            value = data_dic[matched_key]
            if x["value"] < 1:
                value = max(x["value"], value)
            # for backwards compatibility
            return (
                pd.Series({"value": value, "level": "dummy_supply"})
                if x["level"] == "cooling"
                else pd.Series({"value": value, "level": x["level"]})
            )

    # Return the original values if no match is found
    return pd.Series({"value": x["value"], "level": x["level"]})


def _load_cooling_data(
    context: "Context", scenario: Optional["Scenario"] = None
) -> dict:
    """Load all cooling technology data files and scenario parameters.

    Parameters
    ----------
    context : Context
        Model context containing configuration
    scenario : Scenario, optional
        Scenario to extract data from

    Returns
    -------
    dict
        Dictionary containing loaded data:
        - tech_performance_path: path to technology performance file
        - cost_share_path: path to cost and share data file
        - basin_delineation_path: path to basin delineation file
        - df_node: processed basin delineation dataframe
        - node_region: unique region nodes
        - cooling_df: cooling technology dataframe with parent_tech column
        - scenario: scenario object
        - ref_input: combined input/output parameters from scenario
    """
    # File paths with sensible names
    tech_performance_path = package_data_path(
        "water", "ppl_cooling_tech", "tech_water_performance_ssp_msg.csv"
    )

    cost_share_filename = (
        "cooltech_cost_and_shares_"
        + (f"ssp_msg_{context.regions}" if context.type_reg == "global" else "country")
        + ".csv"
    )
    cost_share_path = package_data_path(
        "water", "ppl_cooling_tech", cost_share_filename
    )

    basin_delineation_filename = f"basins_by_region_simpl_{context.regions}.csv"
    basin_delineation_path = package_data_path(
        "water", "delineation", basin_delineation_filename
    )

    # Load and process basin delineation
    df_node = pd.read_csv(basin_delineation_path)

    # Assign proper nomenclature to basin data
    df_node["node"] = "B" + df_node["BCU_name"].astype(str)
    df_node["mode"] = "M" + df_node["BCU_name"].astype(str)
    df_node["region"] = (
        context.map_ISO_c[context.regions]
        if context.type_reg == "country"
        else f"{context.regions}_" + df_node["REGION"].astype(str)
    )
    node_region = df_node["region"].unique()

    # Load cooling technology performance data
    cooling_tech_df = pd.read_csv(tech_performance_path)
    cooling_df = cooling_tech_df.loc[
        cooling_tech_df["technology_group"] == "cooling"
    ].copy()

    # Extract parent technology names
    cooling_df["parent_tech"] = (
        cooling_df["technology_name"]
        .apply(lambda x: pd.Series(str(x).split("__")))
        .drop(columns=1)
    )

    # Get scenario and extract parent technology parameters
    scen = scenario or context.get_scenario()

    # Extract input parameters for parent technologies
    ref_input = scen.par("input", {"technology": cooling_df["parent_tech"]})

    # Handle missing technologies (some only have outputs, like CSP)
    missing_tech_list = cooling_df["parent_tech"][
        ~cooling_df["parent_tech"].isin(ref_input["technology"])
    ]
    ref_output = scen.par("output", {"technology": missing_tech_list})
    ref_output.columns = ref_input.columns
    ref_input = pd.concat([ref_input, ref_output])

    return {
        "tech_performance_path": tech_performance_path,
        "cost_share_path": cost_share_path,
        "basin_delineation_path": basin_delineation_path,
        "df_node": df_node,
        "node_region": node_region,
        "cooling_df": cooling_df,
        "scenario": scen,
        "ref_input": ref_input,
    }


def _process_cooling_data(cooling_data: dict, context: "Context", info) -> dict:
    """Process and clean cooling technology data.

    Parameters
    ----------
    cooling_data : dict
        Data dictionary from _load_cooling_data
    context : Context
        Model context
    info :
        Water build info context

    Returns
    -------
    dict
        Dictionary containing processed data:
        - input_cool: main processed cooling dataframe
        - electr: electricity-requiring technologies
        - saline_df: saline water technologies
        - icmse_df: freshwater technologies (excluding saline and air)
    """
    cooling_df = cooling_data["cooling_df"]
    ref_input = cooling_data["ref_input"]

    # Apply missing technology fixes and merge data
    ref_input[["value", "level"]] = ref_input.apply(missing_tech, axis=1)

    # Combine cooling data with input parameters from parent technologies
    input_cool = (
        cooling_df.set_index("parent_tech")
        .combine_first(ref_input.set_index("technology"))
        .reset_index()
    )

    # Clean data
    input_cool = input_cool.dropna(subset=["value"])
    input_cool.year_vtg = input_cool.year_vtg.astype(int)
    input_cool.year_act = input_cool.year_act.astype(int)

    # Fix invalid year combinations using proper vintage-active combinations
    year_combinations = get_vintage_and_active_years(
        info, technical_lifetime=30, same_year_only=False
    )
    valid_years = set(zip(year_combinations["year_vtg"], year_combinations["year_act"]))

    input_cool_valid_mask = input_cool.apply(
        lambda row: (int(row["year_vtg"]), int(row["year_act"])) in valid_years, axis=1
    )
    input_cool = input_cool[input_cool_valid_mask]

    # Filter out unwanted technologies for backwards compatibility
    input_cool = input_cool[
        (input_cool["level"] != "water_supply") & (input_cool["level"] != "cooling")
    ]
    # Heat plants need no cooling
    input_cool = input_cool[
        ~input_cool["technology_name"].str.contains("hpl", na=False)
    ]

    # Handle global region swapping
    global_region = f"{context.regions}_GLB"
    input_cool.loc[input_cool["node_loc"] == global_region, "node_loc"] = input_cool[
        "node_origin"
    ]
    input_cool.loc[input_cool["node_origin"] == global_region, "node_origin"] = (
        input_cool["node_loc"]
    )

    # Calculate cooling fractions
    input_cool["cooling_fraction"] = input_cool.apply(cooling_fr, axis=1)

    # Convert water withdrawal units and calculate rates
    input_cool["value_cool"] = (
        input_cool["water_withdrawal_mid_m3_per_output"]
        * m3_GJ_TO_MCM_GWa
        / input_cool["cooling_fraction"]
    )
    input_cool["value_cool"] = np.where(
        input_cool["value_cool"] < 0, 1e-6, input_cool["value_cool"]
    )

    input_cool["return_rate"] = 1 - (
        input_cool["water_consumption_mid_m3_per_output"]
        / input_cool["water_withdrawal_mid_m3_per_output"]
    )
    input_cool["consumption_rate"] = (
        input_cool["water_consumption_mid_m3_per_output"]
        / input_cool["water_withdrawal_mid_m3_per_output"]
    )

    input_cool["value_return"] = input_cool["return_rate"] * input_cool["value_cool"]
    input_cool["value_consumption"] = (
        input_cool["consumption_rate"] * input_cool["value_cool"]
    )

    # Create technology-specific dataframes
    electr = input_cool[input_cool["parasitic_electricity_demand_fraction"] > 0.0]
    electr["value_cool"] = (
        electr["parasitic_electricity_demand_fraction"] / electr["cooling_fraction"]
    )
    electr["value_cool"] = np.where(
        electr["value_cool"] < 0, 1e-6, electr["value_cool"]
    )

    saline_df = input_cool[
        input_cool["technology_name"].str.endswith("ot_saline", na=False)
    ]

    # Fresh water technologies (excluding saline and air cooling)
    con1 = input_cool["technology_name"].str.endswith("ot_saline", na=False)
    con2 = input_cool["technology_name"].str.endswith("air", na=False)
    icmse_df = input_cool[(~con1) & (~con2)]

    return {
        "input_cool": input_cool,
        "electr": electr,
        "saline_df": saline_df,
        "icmse_df": icmse_df,
        "con2": con2,  # Needed for emission processing
    }


def _process_share_constraints(
    input_cool: pd.DataFrame, cost_share_path: str, context: "Context"
) -> dict:
    """Process share constraints and historical data backfilling.

    Parameters
    ----------
    input_cool : pd.DataFrame
        Processed cooling technology dataframe
    cost_share_path : str
        Path to cost and share data file
    context : Context
        Model context

    Returns
    -------
    dict
        Dictionary containing:
        - share_constraints: DataFrame with share constraint data
        - hold_cost: processed cost data with regional factors
        - input_cool_2015: historical data for 2015
    """
    # Load cost and share data
    cost = pd.read_csv(cost_share_path)
    cost["technology"] = cost["utype"] + "__" + cost["cooling"]
    cost["share"] = cost["utype"] + "_" + cost["cooling"]

    # Process share constraints
    share_filtered = cost.loc[
        :,
        ["utype", "share"] + [col for col in cost.columns if col.startswith("mix_")],
    ]

    share_long = share_filtered.melt(
        id_vars=["utype", "share"], var_name="node_share", value_name="value"
    )
    share_long = share_long[
        share_long["utype"].isin(input_cool["parent_tech"].unique())
    ].reset_index(drop=True)

    # Clean share data
    share_long["node_share"] = share_long["node_share"].str.replace(
        "mix_", "", regex=False
    )
    share_long["value"] = share_long["value"] * 1.05  # flexibility
    share_long["value"] = share_long["value"].replace(0, 0.0001)

    share_long["shares"] = "share_calib_" + share_long["share"]
    share_long.drop(columns={"utype", "share"}, inplace=True)
    cost.drop(columns="share", inplace=True)
    share_long["time"] = "year"
    share_long["unit"] = "-"

    # FIXME: Temporarily commenting out share calib constraints.
    # Causes 4X size explosion. Likely problematic.
    # share_calib = share_long.copy()
    # # Expand for years [2020, 2025]
    # share_calib = share_calib.loc[share_calib.index.repeat(2)].reset_index(drop=True)
    # years_calib = [2020, 2025]
    # share_calib["year_act"] = years_calib * (len(share_calib) // 2)
    # # take year in info.N but not years_calib
    # years_fut = [year for year in info.Y if year not in years_calib]
    # share_fut = share_long.copy()
    # share_fut = share_fut.loc[share_fut.index.repeat(len(years_fut))].reset_index(
    #     drop=True
    # )
    # share_fut["year_act"] = years_fut * (len(share_fut) // len(years_fut))
    # # filter only shares that contain "ot_saline"
    # share_fut = share_fut[share_fut["shares"].str.contains("ot_saline")]
    # # if value < 0.4 set to 0.4, not so allow too much saline where there is no
    # share_fut["value"] = np.where(share_fut["value"] < 0.45, 0.45, share_fut["value"])
    # # keep only after 2050
    # share_fut = share_fut[share_fut["year_act"] >= 2050]
    # # append share_calib and (share_fut only to add constraints on ot_saline)
    # results["share_commodity_up"] = pd.concat([share_calib])

    # Historical data backfilling logic
    input_cool_2015 = input_cool[
        (input_cool["year_act"] == 2015) & (input_cool["year_vtg"] == 2015)
    ]
    input_cool_set = set(zip(input_cool["parent_tech"], input_cool["node_loc"]))
    year_list = [2020, 2010, 2030, 2050, 2000, 2080, 1990]

    for year in year_list:
        input_cool_2015_set = set(
            # FIXME: This should have been a one off script for debugging.
            zip(input_cool_2015["parent_tech"], input_cool_2015["node_loc"])
        )
        missing_combinations = input_cool_set - input_cool_2015_set

        if not missing_combinations:
            break

        missing_rows = input_cool[
            (input_cool["year_act"] == year)
            & (input_cool["year_vtg"] == year)
            & input_cool.apply(
                lambda row: (row["parent_tech"], row["node_loc"])
                in missing_combinations,
                axis=1,
            )
        ]

        if not missing_rows.empty:
            missing_rows = missing_rows.copy()
            missing_rows["year_act"] = 2015
            missing_rows["year_vtg"] = 2015
            input_cool_2015 = pd.concat(
                [input_cool_2015, missing_rows], ignore_index=True
            )

    # Final check for missing combinations
    input_cool_2015_set = set(
        # FIXME: This should have been a one off script for debugging.
        zip(input_cool_2015["parent_tech"], input_cool_2015["node_loc"])
    )
    still_missing = input_cool_set - input_cool_2015_set

    if still_missing:
        log.warning(
            f"Warning: Some combinations are still missing even after trying all "
            f"years: {still_missing}"
        )

    # Process regional share factors
    cost.rename(columns=lambda name: name.replace("mix_", ""), inplace=True)
    search_cols = [
        col
        for col in cost.columns
        if context.regions in col or col in ["technology", "utype"]
    ]
    hold_df = input_cool_2015[
        ["node_loc", "technology_name", "cooling_fraction"]
    ].drop_duplicates()
    search_cols_cooling_fraction = [
        col for col in search_cols if col not in ["technology", "utype"]
    ]

    # Apply cooling factors to regional shares
    hold_cost = cost[search_cols].apply(
        shares,
        axis=1,
        context=context,
        search_cols_cooling_fraction=search_cols_cooling_fraction,
        hold_df=hold_df,
        search_cols=search_cols,
    )

    # SSP-based share constraints
    ssp_share_constraints = cooling_shares_SSP_from_yaml(context)

    return {
        "share_constraints": ssp_share_constraints,
        "hold_cost": hold_cost,
        "input_cool_2015": input_cool_2015,
    }


def _create_cooling_parameters(
    processed_data: dict, node_region: list, context: "Context", info
) -> dict:
    """Create basic MESSAGEix parameters for cooling technologies.

    Parameters
    ----------
    processed_data : dict
        Processed cooling data from _process_cooling_data
    node_region : list
        List of regional nodes
    context : Context
        Model context
    info :
        Water build info context

    Returns
    -------
    dict
        Dictionary of parameter DataFrames
    """
    input_cool = processed_data["input_cool"]
    electr = processed_data["electr"]
    saline_df = processed_data["saline_df"]
    icmse_df = processed_data["icmse_df"]
    con2 = processed_data["con2"]

    results = {}

    # Create input parameters
    # Electricity inputs for parasitic demand
    inp = make_df(
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

    # Fresh water inputs (once-through and closed-loop)
    inp = pd.concat(
        [
            inp,
            make_df(
                "input",
                node_loc=icmse_df["node_loc"],
                technology=icmse_df["technology_name"],
                year_vtg=icmse_df["year_vtg"],
                year_act=icmse_df["year_act"],
                mode=icmse_df["mode"],
                node_origin=icmse_df["node_origin"],
                commodity="surfacewater",
                level="water_supply",
                time="year",
                time_origin="year",
                value=icmse_df["value_cool"],
                unit="MCM/GWa",
            ),
        ]
    )

    # Saline water inputs
    inp = pd.concat(
        [
            inp,
            make_df(
                "input",
                node_loc=saline_df["node_loc"],
                technology=saline_df["technology_name"],
                year_vtg=saline_df["year_vtg"],
                year_act=saline_df["year_act"],
                mode=saline_df["mode"],
                node_origin=saline_df["node_origin"],
                commodity="saline_ppl",
                level="saline_supply",
                time="year",
                time_origin="year",
                value=saline_df["value_cool"],
                unit="MCM/GWa",
            ),
        ]
    )

    inp = inp.dropna(subset=["value"])
    results["input"] = inp

    # Create emission factors for water return
    emiss_df = input_cool[(~con2)]
    emi = make_df(
        "emission_factor",
        node_loc=emiss_df["node_loc"],
        technology=emiss_df["technology_name"],
        year_vtg=emiss_df["year_vtg"],
        year_act=emiss_df["year_act"],
        mode=emiss_df["mode"],
        emission="fresh_return",
        value=emiss_df["value_return"],
        unit="MCM/GWa",
    )
    results["emission_factor"] = emi

    # Create output parameters
    # Share constraints output
    out = make_df(
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

    # Water return flows output
    out_return = make_df(
        "output",
        node_loc=icmse_df["node_loc"],
        technology=icmse_df["technology_name"],
        year_vtg=icmse_df["year_vtg"],
        year_act=icmse_df["year_act"],
        mode=icmse_df["mode"],
        node_dest=icmse_df["node_origin"],
        commodity="water_return",
        level="water_supply",
        time="year",
        time_dest="year",
        value=icmse_df["value_return"],
        unit="MCM/GWa",
    )

    out = pd.concat([out, out_return])
    results["output"] = out

    # Create addon conversion parameters
    adon_df = input_cool.copy()
    adon_df["tech"] = "cooling__" + adon_df["parent_tech"].astype(str)
    addon_df = make_df(
        "addon_conversion",
        node=adon_df["node_loc"],
        technology=adon_df["parent_tech"],
        year_vtg=adon_df["year_vtg"],
        year_act=adon_df["year_act"],
        mode=adon_df["mode"],
        time="year",
        type_addon=adon_df["tech"],
        value=adon_df["cooling_fraction"],
        unit="-",
    )
    results["addon_conversion"] = addon_df

    # Addon lower bound (allows 100% activity of parent technologies)
    addon_lo = make_matched_dfs(addon_df, addon_lo=1)
    results["addon_lo"] = addon_lo["addon_lo"]

    # Technical lifetime
    year = info.yv_ya.year_vtg.drop_duplicates()
    year = year[year >= 1990]

    tl = (
        make_df(
            "technical_lifetime",
            technology=inp["technology"].drop_duplicates(),
            value=30,
            unit="year",
        )
        .pipe(broadcast, year_vtg=year, node_loc=node_region)
        .pipe(same_node)
    )
    results["technical_lifetime"] = tl

    # Capacity factors
    results["capacity_factor"] = _compose_capacity_factor(inp=inp, context=context)

    return results


def _expand_parent_parameters(
    scenario, cooling_df: pd.DataFrame, hold_cost: pd.DataFrame
) -> dict:
    """Expand parent technology parameters across cooling variants.

    Parameters
    ----------
    scenario : Scenario
        MESSAGEix scenario object
    cooling_df : pd.DataFrame
        Cooling technology dataframe with parent_tech column
    hold_cost : pd.DataFrame
        Processed cost data with regional factors

    Returns
    -------
    dict
        Dictionary of expanded parameter DataFrames
    """
    results = {}

    # Process capacity factors from parent technologies
    cap_fact_parent = scenario.par(
        "capacity_factor", {"technology": cooling_df["parent_tech"]}
    )
    # cap_fact_parent = cap_fact_parent[
    #     (cap_fact_parent["node_loc"] == "R12_NAM")
    #     & (cap_fact_parent["technology"] == "coal_ppl_u") # nuc_lc
    # ]

    # Split capacity factors by model periods
    cap_fact_parent1 = cap_fact_parent[
        ["node_loc", "technology", "year_vtg", "value"]
    ].drop_duplicates()
    cap_fact_parent1 = cap_fact_parent1[
        cap_fact_parent1["year_vtg"] < scenario.firstmodelyear
    ]
    cap_fact_parent1 = cap_fact_parent1.groupby(
        ["node_loc", "technology", "year_vtg"], as_index=False
    ).min()

    cap_fact_parent2 = cap_fact_parent[
        ["node_loc", "technology", "year_act", "value"]
    ].drop_duplicates()
    cap_fact_parent2 = cap_fact_parent2[
        cap_fact_parent2["year_act"] >= scenario.firstmodelyear
    ]
    cap_fact_parent2 = cap_fact_parent2.groupby(
        ["node_loc", "technology", "year_act"], as_index=False
    ).min()
    cap_fact_parent2.rename(columns={"year_act": "year_vtg"}, inplace=True)

    cap_fact_parent_combined = pd.concat([cap_fact_parent1, cap_fact_parent2])
    cap_fact_parent_combined.rename(
        columns={"value": "cap_fact", "technology": "utype"}, inplace=True
    )

    # Parameter names to expand
    param_names = [
        "historical_activity",
        "historical_new_capacity",
        "initial_activity_up",
        "initial_activity_lo",
        "initial_new_capacity_up",
        "soft_activity_up",
        # "soft_activity_lo", #causes infeasibilty.
        "soft_new_capacity_up",
        "level_cost_activity_soft_up",
        "level_cost_activity_soft_lo",
        # "growth_activity_lo", #cause infeasibility
        "growth_activity_up",
        "growth_new_capacity_up",
    ]

    # Parameters that need multiplier application
    multip_list = [
        "historical_activity",
        "historical_new_capacity",
        "initial_activity_up",
        "initial_activity_lo",
        "initial_new_capacity_up",
    ]

    # Extract parameters from parent technologies
    list_params = [
        (scenario.par(p, {"technology": cooling_df["parent_tech"]}), p)
        for p in param_names
    ]

    # Cooling technology suffixes
    suffixes = ["__ot_fresh", "__cl_fresh", "__air", "__ot_saline"]

    # Expand each parameter across cooling variants
    for df, param_name in list_params:
        df_param = pd.DataFrame()
        for suffix in suffixes:
            df_add = df.copy()
            df_add["technology"] = df_add["technology"] + suffix
            df_param = pd.concat([df_param, df_add])

        # Apply multipliers if needed
        df_param_share = (
            apply_act_cap_multiplier(
                df_param, hold_cost, cap_fact_parent_combined, param_name
            )
            if param_name in multip_list
            else df_param
        )

        # Special handling for growth parameters on saline technologies
        if param_name in ["growth_new_capacity_up", "growth_activity_up"]:
            df_param_share.loc[
                df_param_share["technology"].str.endswith("__ot_saline"), "value"
            ] = 1e-3

        results[param_name] = df_param_share

    return results


def cooling_fr(x: pd.Series) -> float:
    """Calculate cooling fraction

    Returns
    -------
    The calculated cooling fraction after for two categories;
    1. Technologies that produce heat as an output
        cooling_fraction(h_cool) = input value(hi) - 1
    Simply subtract 1 from the heating value since the rest of the part is already
    accounted in the heating value
    2. Rest of technologies
        h_cool  =  hi -Hi* h_fg - 1,
        where:
            h_fg (flue gasses losses) = 0.1 (10% assumed losses)
    """
    try:
        if "hpl" in x["parent_tech"]:
            return x["value"] - 1
        else:
            return x["value"] - (x["value"] * 0.1) - 1
    except KeyError:
        return x["value"] - (x["value"] * 0.1) - 1


def shares(
    x: pd.Series,
    context: "Context",
    search_cols_cooling_fraction: list,
    hold_df: pd.DataFrame,
    search_cols: list,
) -> pd.Series:
    """Process share and cooling fraction.

    Returns
    -------
    Product of value of shares of cooling technology types of regions with
    corresponding cooling fraction
    """
    for col in search_cols_cooling_fraction:
        col2 = context.map_ISO_c[col] if context.type_reg == "country" else col

        # Filter the cooling fraction
        cooling_fraction = hold_df[
            (hold_df["node_loc"] == col2)
            & (hold_df["technology_name"] == x["technology"])
        ]["cooling_fraction"]

        # Log unmatched rows
        if cooling_fraction.empty:
            log.info(
                f"No cooling_fraction found for node_loc: {col2}, "
                f"technology: {x['technology']}"
            )
            cooling_fraction = pd.Series([0])

        # Ensure the Series is not empty before accessing its first element
        # # Default to 0 if cooling_fraction is empty
        x[col] = (
            x[col] * cooling_fraction.iloc[0]
            if not cooling_fraction.empty
            else x[col] * 0
        )

    # Construct the output
    results = []
    for i in x:
        if isinstance(i, str):
            results.append(i)
        else:
            results.append(float(i) if not isinstance(i, pd.Series) else i.iloc[0])

    return pd.Series(results, index=search_cols)


def apply_act_cap_multiplier(
    df: pd.DataFrame,
    hold_cost: pd.DataFrame,
    cap_fact_parent: Optional[pd.DataFrame] = None,
    param_name: str = "",
) -> pd.DataFrame:
    """
    Generalized function to apply hold_cost factors and optionally divide by cap factor.
    hold cost contain the share per cooling technologies and their activity factors
    compared to parent technologies.

    Parameters
    ----------
    df : pd.DataFrame
        The input dataframe in long format, containing 'node_loc', 'technology', and
        'value'.
    hold_cost : pd.DataFrame
        DataFrame with 'utype', region-specific multipliers (wide format), and
        'technology'.
    cap_fact_parent : pd.DataFrame, optional
        DataFrame with capacity factors, used only if 'capacity' is in param_name.
    param_name : str, optional
        The name of the parameter being processed.

    Returns
    -------
    pd.DataFrame
        The modified dataframe.
    """

    # Melt hold_cost to long format
    hold_cost_long = hold_cost.melt(
        id_vars=["utype", "technology"], var_name="node_loc", value_name="multiplier"
    )

    # Merge and apply hold_cost multipliers: share * addon_factor
    # ACT,c = ACT,p * share * addon_factor
    df = df.merge(hold_cost_long, how="left")
    df["value"] *= df["multiplier"]

    # filter with value > 0
    df = df[df["value"] > 0]

    # If parameter is capacity-related, multiply by cap_fact
    # CAP,c * cf,c(=1) = CAP,p * share * addon_factor * cf,p
    if "capacity" in param_name and cap_fact_parent is not None:
        df = df.merge(cap_fact_parent, how="left")
        df["value"] *= df["cap_fact"] * 1.2  # flexibility
        df.drop(columns="cap_fact", inplace=True)
    # remove if there are Nan values, but write a log that inform on the parameter and
    # the head of the data
    # Identify missing or invalid values
    missing_values = (
        df["value"].isna()
        | (df["value"] == "")
        | (df["value"].astype(str).str.strip() == "")
    )

    if missing_values.any():
        print("diobo")
        log.warning(
            f"Missing or empty values found in {param_name}.head(1):\n"
            f"{df[missing_values].head(1)}"
        )
        df = df[~missing_values]  # Remove rows with missing/empty values

    df.drop(columns=["utype", "multiplier"], inplace=True)

    return df


def cooling_shares_SSP_from_yaml(
    context: "Context",  # Aligning with the style of the functions provided
) -> pd.DataFrame:
    """
    Populate a DataFrame for 'share_commodity_up' from a YAML configuration file.

    Parameters
    ----------
    context : Context
        Context object containing SSP information (e.g., context.SSP)

    Returns
    -------
    pd.DataFrame
        A DataFrame populated with values from the YAML configuration file.
    """
    # Load the YAML file
    FILE = "ssp.yaml"
    yaml_file_path = package_data_path("water", FILE)
    try:
        with open(yaml_file_path, "r") as file:
            yaml_data = yaml.safe_load(file)
    except FileNotFoundError:
        log.warning(f"YAML file '{FILE}' not found. Please, check your data.")

    # Read the SSP from the context
    ssp = context.ssp

    # Navigate to the scenarios section in the YAML file
    macro_regions_data = yaml_data.get("macro-regions", {})
    scenarios = yaml_data.get("scenarios", {})

    # Validate that the SSP exists in the YAML data
    if ssp not in scenarios:
        log.warning(
            f"SSP '{ssp}' not found in the 'scenarios' section of the YAML file."
        )
        return pd.DataFrame()

    # Extract data for the specified SSP
    ssp_data = scenarios[ssp]["cooling_tech"]

    # Initialize an empty list to hold DataFrames
    df_region = pd.DataFrame()
    info = context["water build info"]
    year_constraint = [year for year in info.Y if year >= 2050]

    # Loop through all regions and shares
    for macro_region, region_data in ssp_data.items():
        share_data = region_data.get("share_commodity_up", {})
        reg_shares = macro_regions_data[macro_region]
        # filter reg shares that are also in info.N
        reg_shares = [
            node
            for node in info.N
            if any(node.endswith(reg_share) for reg_share in reg_shares)
        ]
        for share, value in share_data.items():
            # Create a DataFrame for the current region and share
            df_region = pd.concat(
                [
                    df_region,
                    make_df(
                        "share_commodity_up",
                        shares=[share],
                        time=["year"],
                        value=[value],
                        unit=["-"],
                    ).pipe(broadcast, year_act=year_constraint, node_share=reg_shares),
                ]
            )

    return df_region


def _add_saline_extract_bounds(results: dict, info) -> None:
    """Add dynamic bound_activity_up for extract_salinewater_cool based on
    historical saline cooling demand.

    Parameters
    ----------
    results : dict
        Dictionary of parameter DataFrames being built
    info :
        Water build info context containing model years
    """
    if "historical_activity" not in results:
        return

    hist_activity = results["historical_activity"]

    # Filter to saline cooling technologies
    saline_hist = hist_activity[
        hist_activity["technology"].str.endswith("__ot_saline", na=False)
    ]

    if saline_hist.empty:
        return

    # Get last historical year
    last_hist_year = saline_hist["year_act"].max()

    # Filter to last historical year and sum by region
    last_year_saline = saline_hist[saline_hist["year_act"] == last_hist_year]
    regional_bounds = last_year_saline.groupby("node_loc")["value"].sum().reset_index()

    # Create bound values for each region (fallback to 1e4 if no activity)
    bound_values = []
    bound_regions = []

    for _, row in regional_bounds.iterrows():
        region = row["node_loc"]
        total_activity = row["value"]
        bound_value = (
            total_activity if total_activity > 0 else 1e4
        )  # FIXME: Aribitrary 1e4
        bound_values.append(bound_value)
        bound_regions.append(region)

    # Create bound_activity_up for extract_salinewater_cool
    if bound_values:
        saline_water_cool_cap = make_df(
            "bound_activity_up",
            technology="extract_salinewater_cool",
            node_loc=bound_regions,
            mode="M1",
            time="year",
            value=bound_values,
            unit="MCM",
        ).pipe(broadcast, year_act=info.Y)

        # Add to results (concatenate if bound_activity_up already exists)
        if "bound_activity_up" in results:
            results["bound_activity_up"] = pd.concat(
                [results["bound_activity_up"], saline_water_cool_cap], ignore_index=True
            )
        else:
            results["bound_activity_up"] = saline_water_cool_cap


def _compose_capacity_factor(inp: pd.DataFrame, context: "Context") -> pd.DataFrame:
    """Create the capacity_factor base on data in `inp` and `context.

    Parameters
    ----------
    inp : pd.DataFrame
        The DataFrame representing the "input" parameter.
    context : .Context

    Returns
    -------
    pd.DataFrame
        A DataFrame representing the "capacity_factor" parameter.
    """
    cap_fact = make_matched_dfs(inp, capacity_factor=1)
    # Climate Impacts on freshwater cooling capacity
    # Taken from
    # https://www.sciencedirect.com/science/article/pii/S0959378016301236?via%3Dihub#sec0080
    if context.RCP == "no_climate":
        df = cap_fact["capacity_factor"]
    else:
        df = cap_fact["capacity_factor"]
        # reading ppl cooling impact dataframe
        file_path = package_data_path(
            "water",
            "ppl_cooling_tech",
            f"power_plant_cooling_impact_MESSAGE_{context.regions}_{context.RCP}.csv",
        )
        df_impact = pd.read_csv(file_path)

        for n in df_impact["node"]:
            conditions = [
                df["technology"].str.contains("fresh")
                & (df["year_act"] >= 2025)
                & (df["year_act"] < 2050)
                & (df["node_loc"] == n),
                df["technology"].str.contains("fresh")
                & (df["year_act"] >= 2050)
                & (df["year_act"] < 2070)
                & (df["node_loc"] == n),
                df["technology"].str.contains("fresh")
                & (df["year_act"] >= 2070)
                & (df["node_loc"] == n),
            ]

            choices = [
                df_impact[(df_impact["node"] == n)]["2025s"],
                df_impact[(df_impact["node"] == n)]["2050s"],
                df_impact[(df_impact["node"] == n)]["2070s"],
            ]

            df["value"] = np.select(conditions, choices, default=df["value"])

    return df


# water & electricity for cooling technologies
def cool_tech(
    context: "Context", scenario: Optional["Scenario"] = None
) -> dict[str, pd.DataFrame]:
    """Process cooling technology data for a scenario instance.
    The input values of parent technologies are read in from a scenario instance and
    then cooling fractions are calculated by using the data from
    ``tech_water_performance_ssp_msg.csv``.
    It adds cooling technologies as addons to the parent technologies. The
    nomenclature for cooling technology is <parenttechnologyname>__<coolingtype>.
    E.g: `coal_ppl__ot_fresh`

    Parameters
    ----------
    context : .Context
    scenario : .Scenario, optional
        Scenario to use. If not provided, uses context.get_scenario().

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'.
        Values are data frames ready for :meth:`~.Scenario.add_par`.
        Years in the data include the model horizon indicated by
        ``context["water build info"]``, plus the additional year 2010.
    """
    # Get water build info
    info = context["water build info"]

    # Step 1: Load all cooling data files and scenario parameters
    cooling_data = _load_cooling_data(context, scenario)

    # Step 2: Process and clean cooling technology data
    processed_data = _process_cooling_data(cooling_data, context, info)

    # Step 3: Process share constraints and historical data backfilling
    share_data = _process_share_constraints(
        processed_data["input_cool"], cooling_data["cost_share_path"], context
    )

    # Step 4: Create basic MESSAGEix parameters
    results = _create_cooling_parameters(
        processed_data, cooling_data["node_region"], context, info
    )

    # Step 5: Handle nexus-specific logic (basin-region distribution)
    if context.nexus_set == "nexus":
        # Get basin-region mapping
        df_sw = map_basin_region_wat(context)
        df_sw.drop(columns={"mode", "date", "MSGREG"}, inplace=True)
        df_sw.rename(
            columns={"region": "node_dest", "time": "time_dest", "year": "year_act"},
            inplace=True,
        )
        df_sw["time_dest"] = df_sw["time_dest"].astype(str)

        year_combinations = get_vintage_and_active_years(
            info, technical_lifetime=1, same_year_only=True
        )

        # Create reg_to_basin technology for distributing return flows
        reg_to_basin_input = make_df(
            "input",
            technology="reg_to_basin",
            node_loc=cooling_data["node_region"],
            commodity="water_return",
            level="water_supply",
            time="year",
            time_origin="year",
            node_origin=cooling_data["node_region"],
            value=1,
            unit="MCM/GWa",
            mode="M1",
        ).pipe(broadcast, year_combinations)

        reg_to_basin_output_list = []
        for region in cooling_data["node_region"]:
            region_code = region.split("_")[-1]
            matching_basins = df_sw[
                df_sw["node_dest"].str.contains(region_code, na=False)
            ]

            if matching_basins.empty:
                continue

            region_output = (
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
                .pipe(broadcast, year_combinations)
                .pipe(broadcast, node_dest=matching_basins["node_dest"].unique())
                .merge(
                    matching_basins.drop_duplicates(["node_dest"])[
                        ["node_dest", "share"]
                    ],
                    on="node_dest",
                    how="left",
                )
            )
            reg_to_basin_output_list.append(region_output)

        reg_to_basin_output = (
            pd.concat(reg_to_basin_output_list, ignore_index=True)
            if reg_to_basin_output_list
            else pd.DataFrame()
        )

        reg_to_basin_output["value"] = (
            reg_to_basin_output["value"] * reg_to_basin_output["share"]
        )
        reg_to_basin_output = reg_to_basin_output.drop(columns=["share"]).dropna(
            subset=["value"]
        )

        # Add nexus parameters to results
        results["input"] = pd.concat([results["input"], reg_to_basin_input])
        results["output"] = pd.concat([results["output"], reg_to_basin_output])

    # Step 6: Add investment cost projections
    from message_ix_models.tools.costs.config import MODULE, Config
    from message_ix_models.tools.costs.projections import create_cost_projections

    techs_to_remove = [
        "mw_ppl__ot_fresh",
        "mw_ppl__ot_saline",
        "mw_ppl__cl_fresh",
        "mw_ppl__air",
        "nuc_fbr__ot_fresh",
        "nuc_fbr__ot_saline",
        "nuc_fbr__cl_fresh",
        "nuc_fbr__air",
        "nuc_htemp__ot_fresh",
        "nuc_htemp__ot_saline",
        "nuc_htemp__cl_fresh",
        "nuc_htemp__air",
    ]

    cfg = Config(
        module=MODULE.cooling, scenario=context.ssp, method="gdp", node=context.regions
    )
    cost_proj = create_cost_projections(cfg)
    inv_cost = cost_proj["inv_cost"][
        ["year_vtg", "node_loc", "technology", "value", "unit"]
    ]
    inv_cost = inv_cost[~inv_cost["technology"].isin(techs_to_remove)]
    inv_cost = inv_cost[inv_cost["technology"].str.contains("__")]
    results["inv_cost"] = inv_cost

    # Step 7: Expand parent technology parameters across cooling variants
    expanded_params = _expand_parent_parameters(
        cooling_data["scenario"], cooling_data["cooling_df"], share_data["hold_cost"]
    )

    # Merge expanded parameters into results
    for param_name, param_df in expanded_params.items():
        results[param_name] = pd.concat(
            [results.get(param_name, pd.DataFrame()), param_df], ignore_index=True
        )

    # Step 8: Add SSP share constraints
    if not share_data["share_constraints"].empty:
        results["share_commodity_up"] = pd.concat(
            [share_data["share_constraints"]], ignore_index=True
        )

    # Step 9: Add dynamic saline extraction bounds
    _add_saline_extract_bounds(results, info)

    return results


# Water use & electricity for non-cooling technologies
def non_cooling_tec(context: "Context", scenario=None) -> dict[str, pd.DataFrame]:
    """Process data for water usage of power plants (non-cooling technology related).
    Water withdrawal values for power plants are read in from
    ``tech_water_performance_ssp_msg.csv``

    Parameters
    ----------
    context : .Context
    scenario : .Scenario, optional
        Scenario to use. If not provided, uses context.get_scenario().

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'.
        Values are data frames ready for :meth:`~.Scenario.add_par`.
        Years in the data include the model horizon indicated by
        ``context["transport build info"]``, plus the additional year 2010.
    """
    results = {}

    FILE = "tech_water_performance_ssp_msg.csv"
    path = package_data_path("water", "ppl_cooling_tech", FILE)
    df = pd.read_csv(path)
    cooling_df = df.copy()
    cooling_df = cooling_df.loc[cooling_df["technology_group"] == "cooling"]
    # Separate a column for parent technologies of respective cooling
    # techs
    cooling_df["parent_tech"] = (
        cooling_df["technology_name"]
        .apply(lambda x: pd.Series(str(x).split("__")))
        .drop(columns=1)
    )
    non_cool_df = df[
        (df["technology_group"] != "cooling")
        & (df["water_supply_type"] == "freshwater_supply")
    ]

    scen = scenario if scenario is not None else context.get_scenario()
    tec_lt = scen.par("technical_lifetime")
    all_tech = list(tec_lt["technology"].unique())
    # all_tech = list(scen.set("technology"))
    tech_non_cool_csv = list(non_cool_df["technology_name"])
    techs_to_remove = [tec for tec in tech_non_cool_csv if tec not in all_tech]

    non_cool_df = non_cool_df[~non_cool_df["technology_name"].isin(techs_to_remove)]
    non_cool_df = non_cool_df.rename(columns={"technology_name": "technology"})

    non_cool_df["value"] = (
        non_cool_df["water_withdrawal_mid_m3_per_output"] * m3_GJ_TO_MCM_GWa
    )  # Conversion factor

    non_cool_tech = list(non_cool_df["technology"].unique())

    n_cool_df = scen.par("output", {"technology": non_cool_tech})
    n_cool_df = n_cool_df[
        (n_cool_df["node_loc"] != f"{context.regions}_GLB")
        & (n_cool_df["node_dest"] != f"{context.regions}_GLB")
    ]
    n_cool_df_merge = pd.merge(n_cool_df, non_cool_df, on="technology", how="right")
    n_cool_df_merge.dropna(inplace=True)

    # Input dataframe for non cooling technologies
    # only water withdrawals are being taken
    # Dedicated freshwater is assumed for simplicity
    inp_n_cool = make_df(
        "input",
        technology=n_cool_df_merge["technology"],
        value=n_cool_df_merge["value_y"],
        unit="MCM/GWa",
        level="water_supply",
        commodity="surfacewater",
        time_origin="year",
        mode="M1",
        time="year",
        year_vtg=n_cool_df_merge["year_vtg"].astype(int),
        year_act=n_cool_df_merge["year_act"].astype(int),
        node_loc=n_cool_df_merge["node_loc"],
        node_origin=n_cool_df_merge["node_dest"],
    )

    # append the input data to results
    results["input"] = inp_n_cool

    return results
