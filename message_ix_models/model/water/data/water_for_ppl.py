import numpy as np
import pandas as pd

from message_ix_models import Context
from message_ix_models.model.water.data.water_for_ppl_rules import (
    COOL_TECH_ADDON_RULES,
    COOL_TECH_EMISSION_RULES,
    COOL_TECH_INPUT_RULES,
    COOL_TECH_LIFETIME_RULES,
    COOL_TECH_OUTPUT_RULES,
    NON_COOL_INPUT_RULES,
)
from message_ix_models.model.water.data.water_for_ppl_support import (
    _compose_capacity_factor,
    apply_act_cap_multiplier,
    cooling_fr,
    cooling_shares_SSP_from_yaml,
    missing_tech,
    shares,
)
from message_ix_models.model.water.data.water_supply import map_basin_region_wat
from message_ix_models.model.water.dsl_engine import run_standard
from message_ix_models.model.water.utils import safe_concat
from message_ix_models.tools.costs.config import Config
from message_ix_models.tools.costs.projections import create_cost_projections
from message_ix_models.util import (
    make_matched_dfs,
    minimum_version,
    package_data_path,
)

# 60 * 60 * 24 * 365
COOL_CONST = {
    "SECINYEAR": 31536000,
    "M3toMCM": 1e-6,
    "SALINE_SHARE_FLEX": 0.45,
    "SHARE_LONG_FLEX": 1.05,
    "NUMERICAL_SMOOTHING": 1e-4,
}


def _prepare_share_constraints(
    input_cool: pd.DataFrame, FILE1: str, info: pd.DataFrame, results: dict
) -> tuple[pd.DataFrame, dict]:
    """Process calibration data and prepare share constraints for cooling technologies."""
    # costs and historical parameters
    PATH = package_data_path("water", "ppl_cooling_tech", FILE1)
    cost = pd.read_csv(PATH)
    # Combine technology name to get full cooling tech names
    cost["technology"] = cost["utype"] + "__" + cost["cooling"]
    cost["share"] = cost["utype"] + "_" + cost["cooling"]

    # add contraint with relation, based on shares
    # Keep only "utype", "technology", and columns starting with "mix_"
    share_filtered = cost.loc[
        :,
        ["utype", "share"] + [col for col in cost.columns if col.startswith("mix_")],
    ]

    # Melt to long format
    share_long = share_filtered.melt(
        id_vars=["utype", "share"], var_name="node_share", value_name="value"
    )
    # filter with uypt in cooling_df["parent_tech"]
    share_long = share_long[
        share_long["utype"].isin(input_cool["parent_tech"].unique())
    ].reset_index(drop=True)

    # Remove "mix_" prefix from region names
    share_long["node_share"] = share_long["node_share"].str.replace(
        "mix_", "", regex=False
    )
    # Replace 0 values with 0.0001
    share_long["value"] = (
        share_long["value"]
        .mul(COOL_CONST["SHARE_LONG_FLEX"])  # Apply flexibility factor of 1.05
        .replace(0, COOL_CONST["NUMERICAL_SMOOTHING"])
        # Replace zeros with small value 1e-4
    )

    share_long["shares"] = "share_calib_" + share_long["share"]
    share_long.drop(columns={"utype", "share"}, inplace=True)
    # restore cost as it was for future use
    cost.drop(columns="share", inplace=True)
    share_long["time"] = "year"
    share_long["unit"] = "-"
    share_calib = share_long.copy()
    # Expand for years [2020, 2025]
    share_calib = share_calib.loc[share_calib.index.repeat(2)].reset_index(drop=True)
    years_calib = [2020, 2025]
    share_calib["year_act"] = years_calib * (len(share_calib) // 2)
    # take year in info.N but not years_calib
    years_fut = [year for year in info.Y if year not in years_calib]
    share_fut = share_long.copy()
    share_fut = share_fut.loc[share_fut.index.repeat(len(years_fut))].reset_index(
        drop=True
    )
    share_fut["year_act"] = years_fut * (len(share_fut) // len(years_fut))
    # filter only shares that contain "ot_saline"
    share_fut = share_fut[share_fut["shares"].str.contains("ot_saline")]
    # if value < 0.4 set to 0.4, not so allow too much saline where there is no

    share_fut["value"] = np.where(
        share_fut["value"] < COOL_CONST["SALINE_SHARE_FLEX"],
        COOL_CONST["SALINE_SHARE_FLEX"],
        share_fut["value"],
    )
    # keep only after 2050
    share_fut = share_fut[share_fut["year_act"] >= 2050]
    # append share_calib and (share_fut only to add constraints on ot_saline)
    results["share_commodity_up"] = pd.concat([share_calib])

    return cost, results


def _load_auxiliary_data(context: Context) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """Load auxiliary data files needed for cooling technology analysis."""
    # Name of the input file with water withdrawals and emission heating fractions
    FILE = "tech_water_performance_ssp_msg.csv"

    # Investment costs & regional shares of hist. activities of cooling techs
    FILE1 = (
        "cooltech_cost_and_shares_"
        + (f"ssp_msg_{context.regions}" if context.type_reg == "global" else "country")
        + ".csv"
    )

    # Basin delineation file
    FILE2 = f"basins_by_region_simpl_{context.regions}.csv"
    PATH = package_data_path("water", "delineation", FILE2)

    # Load basin delineation data
    df_node = pd.read_csv(PATH)
    # Assigning proper nomenclature
    df_node["node"] = "B" + df_node["BCU_name"].astype(str)
    df_node["mode"] = "M" + df_node["BCU_name"].astype(str)
    df_node["region"] = (
        context.map_ISO_c[context.regions]
        if context.type_reg == "country"
        else f"{context.regions}_" + df_node["REGION"].astype(str)
    )

    # Load cooling technology data
    path = package_data_path("water", "ppl_cooling_tech", FILE)
    df = pd.read_csv(path)
    cooling_df = df.loc[df["technology_group"] == "cooling"].copy()
    # Separate parent technologies
    cooling_df["parent_tech"] = (
        cooling_df["technology_name"]
        .apply(lambda x: pd.Series(str(x).split("__")))
        .drop(columns=1)
    )

    return df_node, cooling_df, FILE1


def _prepare_initial_cooling_data(
    context: Context, cooling_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Prepare initial cooling technology data by merging with scenario data."""
    scen = context.get_scenario()

    # Extract input database from scenario for parent technologies
    ref_input = scen.par("input", {"technology": cooling_df["parent_tech"]})
    # List of tec in cooling_df["parent_tech"] that are not in ref_input
    missing_tec = cooling_df["parent_tech"][
        ~cooling_df["parent_tech"].isin(ref_input["technology"])
    ]
    # Some techs only have output, like csp
    ref_output = scen.par("output", {"technology": missing_tec})
    # Set columns names of ref_output to be the same as ref_input
    ref_output.columns = ref_input.columns
    # Merge ref_input and ref_output
    ref_input = pd.concat([ref_input, ref_output])

    ref_input[["value", "level"]] = ref_input.apply(missing_tech, axis=1)

    # Combines the input df of parent_tech with water withdrawal data
    input_cool = (
        cooling_df.set_index("parent_tech")
        .combine_first(ref_input.set_index("technology"))
        .reset_index()
    )

    # Drops NA values from the value column
    input_cool = input_cool.dropna(subset=["value"])

    # Convert year values into integers to be compatible for model
    input_cool.year_vtg = input_cool.year_vtg.astype(int)
    input_cool.year_act = input_cool.year_act.astype(int)

    # Drops extra technologies from the data. backwards compatibility
    input_cool = input_cool[
        (input_cool["level"] != "water_supply") & (input_cool["level"] != "cooling")
    ]
    # Heat plants need no cooling
    input_cool = input_cool[
        ~input_cool["technology_name"].str.contains("hpl", na=False)
    ]
    # Swap node_loc if node_loc equals "{context.regions}_GLB"
    input_cool.loc[input_cool["node_loc"] == f"{context.regions}_GLB", "node_loc"] = (
        input_cool["node_origin"]
    )
    # Swap node_origin if node_origin equals "{context.regions}_GLB"
    input_cool.loc[
        input_cool["node_origin"] == f"{context.regions}_GLB", "node_origin"
    ] = input_cool["node_loc"]

    # Calculate cooling fraction
    input_cool.loc[:, "cooling_fraction"] = input_cool.apply(cooling_fr, axis=1)

    # Converting water withdrawal units to MCM/GWa
    # This refers to activity per cooling requirement (heat)
    input_cool.loc[:, "value_cool"] = (
        input_cool["water_withdrawal_mid_m3_per_output"]
        * COOL_CONST["SECINYEAR"]
        * COOL_CONST["M3toMCM"]
        / input_cool["cooling_fraction"]
    )
    # Set to 1e-6 if value_cool is negative
    input_cool.loc[:, "value_cool"] = np.where(
        input_cool["value_cool"] < 0, COOL_CONST["M3toMCM"], input_cool["value_cool"]
    )
    input_cool.loc[:, "return_rate"] = 1 - (
        input_cool["water_consumption_mid_m3_per_output"]
        / input_cool["water_withdrawal_mid_m3_per_output"]
    )
    # Consumption to be saved in emissions rates for reporting purposes
    input_cool.loc[:, "consumption_rate"] = (
        input_cool["water_consumption_mid_m3_per_output"]
        / input_cool["water_withdrawal_mid_m3_per_output"]
    )

    input_cool.loc[:, "value_return"] = (
        input_cool["return_rate"] * input_cool["value_cool"]
    )

    # Only for reporting purposes
    input_cool.loc[:, "value_consumption"] = (
        input_cool["consumption_rate"] * input_cool["value_cool"]
    )

    # Filter out technologies that requires parasitic electricity
    electr = input_cool[input_cool["parasitic_electricity_demand_fraction"] > 0.0]

    # Make a new column 'value_cool' for calculating values against technologies
    electr.loc[:, "value_cool"] = (
        electr["parasitic_electricity_demand_fraction"] / electr["cooling_fraction"]
    )
    # Set to 1e-6 if value_cool is negative
    electr.loc[:, "value_cool"] = np.where(
        electr["value_cool"] < 0, COOL_CONST["M3toMCM"], electr["value_cool"]
    )
    # Filters out technologies requiring saline water supply
    saline_df = input_cool[
        input_cool["technology_name"].str.endswith("ot_saline", na=False)
    ]

    # input_cool_minus_saline_elec_df
    con1 = input_cool["technology_name"].str.endswith("ot_saline", na=False)
    con2 = input_cool["technology_name"].str.endswith("air", na=False)
    icmse_df = input_cool[(~con1) & (~con2)]

    return input_cool, electr, saline_df, icmse_df


def _generate_input_parameter(
    electr: pd.DataFrame, icmse_df: pd.DataFrame, saline_df: pd.DataFrame
) -> pd.DataFrame:
    """Generate the input parameter DataFrame for cooling technologies."""
    inp_list = []
    # Electricity inputs
    for rule in COOL_TECH_INPUT_RULES.get_rule():
        rule_dfs = {
            "electr": electr,
            "icmse_df": icmse_df,
            "saline_df": saline_df,
        }

        base_args = {
            "rule_dfs": rule_dfs,
        }
        inp_list.append(run_standard(rule, base_args))
    inp = safe_concat(inp_list)

    # Drops NA values from the value column
    inp = inp.dropna(subset=["value"])

    return inp


def _generate_emission_factor(input_cool: pd.DataFrame) -> pd.DataFrame:
    """Generate emission factor parameter for cooling technologies."""
    # Skip technologies with air cooling (no water consumption)
    con2 = input_cool["technology_name"].str.endswith("air", na=False)
    emiss_df = input_cool[(~con2)]
    base_args = {
        "rule_dfs": emiss_df,
    }
    emi_list = []
    for rule in COOL_TECH_EMISSION_RULES.get_rule():
        rule_dfs = emiss_df
        base_args = {
            "rule_dfs": rule_dfs,
        }
        emi_list.append(run_standard(rule, base_args))
    emi = safe_concat(emi_list)

    return emi


@minimum_version("python 3.10")
def _generate_output_parameter(
    context: Context,
    input_cool: pd.DataFrame,
    icmse_df: pd.DataFrame,
    df_node: pd.DataFrame,
    sub_time: list,
) -> pd.DataFrame:
    """Generate output parameter for cooling technologies."""
    for rule in COOL_TECH_OUTPUT_RULES.get_rule():
        match context.nexus_set, rule["condition"]:
            # Basic output for share constraints
            case _, "default":
                args = {
                    "rule_dfs": input_cool,
                }
                out = run_standard(rule, args)
            # Add water return flows for cooling technologies
            # Use share of basin availability to distribute the return flow
            case "nexus", "nexus":
                df_sw = map_basin_region_wat(context)
                df_sw.drop(columns={"mode", "date", "MSGREG"}, inplace=True)
                df_sw.rename(
                    columns={
                        "region": "node_dest",
                        "time": "time_dest",
                        "year": "year_act",
                    },
                    inplace=True,
                )
                df_sw["time_dest"] = df_sw["time_dest"].astype(str)
                for nn in icmse_df.node_loc.unique():
                    # Input cooling fresh basin
                    icfb_df = icmse_df[icmse_df["node_loc"] == nn]
                    bs = list(df_node[df_node["region"] == nn]["node"])
                    out_t_args = {
                        "rule_dfs": icfb_df,
                    }
                    extra_args = {
                        "node_dest": bs,
                        "time_dest": pd.Series(sub_time),
                    }
                    out_t = run_standard(rule, out_t_args, extra_args)
                    out_t = out_t.merge(df_sw, how="left")
                    # Multiply by basin water availability share
                    out_t["value"] = out_t["value"] * out_t["share"]
                    out_t.drop(columns={"share"}, inplace=True)
                    out = pd.concat([out, out_t])

                out = out.dropna(subset=["value"])
                out.reset_index(drop=True, inplace=True)

    return out


def _prepare_historical_data(input_cool: pd.DataFrame) -> pd.DataFrame:
    """Prepare historical data by aggregating from various years."""
    # Set of parent_tech and node_loc for input_cool
    input_cool_set = set(zip(input_cool["parent_tech"], input_cool["node_loc"]))

    # Filtering out 2015 data to use for historical values
    input_cool_2015 = input_cool[
        (input_cool["year_act"] == 2015) & (input_cool["year_vtg"] == 2015)
    ]

    year_list = [2020, 2010, 2030, 2050, 2000, 2080, 1990]

    for year in year_list:
        # Identify missing combinations in the current aggregate
        input_cool_2015_set = set(
            zip(input_cool_2015["parent_tech"], input_cool_2015["node_loc"])
        )
        missing_combinations = input_cool_set - input_cool_2015_set

        if not missing_combinations:
            break  # Stop if no missing combinations remain

        # Extract missing rows from input_cool with the current year
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
            # Modify year columns to 2015
            missing_rows = missing_rows.copy()
            missing_rows["year_act"] = 2015
            missing_rows["year_vtg"] = 2015

            # Append to the aggregated dataset
            input_cool_2015 = pd.concat(
                [input_cool_2015, missing_rows], ignore_index=True
            )

    return input_cool_2015


def _calculate_historical_shares(
    context: Context,
    cost: pd.DataFrame,
    input_cool_2015: pd.DataFrame,
    search_cols_cooling_fraction: list,
) -> pd.DataFrame:
    """Calculate historical shares based on cooling fractions."""
    # Prepare necessary data for shares calculation
    hold_df = input_cool_2015[
        ["node_loc", "technology_name", "cooling_fraction"]
    ].drop_duplicates()

    # Calculation of multiplication factor with cooling factor and shares
    search_cols = [
        col
        for col in cost.columns
        if context.regions in col or col in ["technology", "utype"]
    ]
    search_cols_cooling_fraction = [
        col for col in search_cols if col not in ["technology", "utype"]
    ]

    hold_cost = cost[search_cols].apply(
        shares,
        axis=1,
        context=context,
        search_cols_cooling_fraction=search_cols_cooling_fraction,
        hold_df=hold_df,
        search_cols=search_cols,
    )

    return hold_cost


def _get_parent_capacity_factors(
    scen: object, cooling_df: pd.DataFrame
) -> pd.DataFrame:
    """Retrieve and process capacity factors for parent technologies."""
    # Historical capacity to be divided by cap_factor of the parent tec
    cap_fact_parent = scen.par(
        "capacity_factor", {"technology": cooling_df["parent_tech"]}
    )

    # Keep node_loc, technology, year_vtg and value
    cap_fact_parent1 = cap_fact_parent[
        ["node_loc", "technology", "year_vtg", "value"]
    ].drop_duplicates()
    cap_fact_parent1 = cap_fact_parent1[
        cap_fact_parent1["year_vtg"] < scen.firstmodelyear
    ]
    # Group by "node_loc", "technology", "year_vtg" and get the minimum value
    cap_fact_parent1 = cap_fact_parent1.groupby(
        ["node_loc", "technology", "year_vtg"], as_index=False
    ).min()

    # In some cases the capacity parameters are used with year_all
    # (e.g. initial_new_capacity_up). Need year_act for this
    cap_fact_parent2 = cap_fact_parent[
        ["node_loc", "technology", "year_act", "value"]
    ].drop_duplicates()
    cap_fact_parent2 = cap_fact_parent2[
        cap_fact_parent2["year_act"] >= scen.firstmodelyear
    ]
    # Group by "node_loc", "technology", "year_vtg" and get the minimum value
    cap_fact_parent2 = cap_fact_parent2.groupby(
        ["node_loc", "technology", "year_act"], as_index=False
    ).min()
    cap_fact_parent2.rename(columns={"year_act": "year_vtg"}, inplace=True)

    cap_fact_parent = pd.concat([cap_fact_parent1, cap_fact_parent2])

    # Rename value to cap_fact
    cap_fact_parent.rename(
        columns={"value": "cap_fact", "technology": "utype"}, inplace=True
    )

    return cap_fact_parent


def _project_future_costs(context: Context) -> pd.DataFrame:
    """Project future investment costs for cooling technologies."""
    # Technologies to remove from cost projections
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

    # Set config for cost projections
    # Using GDP method for cost projections
    cfg = Config(
        module="cooling", scenario=context.ssp, method="gdp", node=context.regions
    )

    # Get projected investment and fixed o&m costs
    cost_proj = create_cost_projections(cfg)

    # Get only the investment costs for cooling technologies
    inv_cost = cost_proj["inv_cost"][
        ["year_vtg", "node_loc", "technology", "value", "unit"]
    ]

    # Remove technologies that are not required
    inv_cost = inv_cost[~inv_cost["technology"].isin(techs_to_remove)]

    # Only keep cooling module technologies by filtering for technologies with "__"
    inv_cost = inv_cost[inv_cost["technology"].str.contains("__")]

    return inv_cost


def _generate_addon_parameters(
    input_cool: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate addon conversion and addon_lo parameters."""
    # Addon conversion
    adon_df = input_cool.copy()
    # Add 'cooling_' before name of parent technologies that are type_addon
    # nomenclature
    adon_df["tech"] = "cooling__" + adon_df["parent_tech"].astype(str)
    # technology : 'parent technology' and type_addon is type of addons such
    # as 'cooling__bio_hpl'
    rule = COOL_TECH_ADDON_RULES.get_rule()[0]
    args = {
        "rule_dfs": adon_df,
    }
    addon_df = run_standard(rule, args)
    # Addon_lo will remain 1 for all cooling techs so it allows 100% activity of
    # parent technologies
    addon_matched = make_matched_dfs(addon_df, addon_lo=1)
    addon_lo = addon_matched["addon_lo"]

    return addon_df, addon_lo


def _generate_lifetime_parameter(
    inp: pd.DataFrame, node_region: list, info: pd.DataFrame
) -> pd.DataFrame:
    """Generate technical lifetime parameter."""
    year = info.yv_ya.year_vtg.drop_duplicates()
    year = year[year >= 1990]

    rule = COOL_TECH_LIFETIME_RULES.get_rule()[0]
    args = {
        "rule_dfs": inp,
    }
    extra_args = {
        "year_vtg": year,
        "node_loc": node_region,
    }
    tl = run_standard(rule, args, extra_args)

    return tl


def _expand_parent_parameters(
    scen: object,
    cooling_df: pd.DataFrame,
    hold_cost: pd.DataFrame,
    cap_fact_parent: pd.DataFrame,
) -> dict:
    """Expand parameters from parent technologies to cooling technologies."""
    # Define parameter names to be extracted
    param_names = [
        "historical_activity",
        "historical_new_capacity",
        "initial_activity_up",
        "initial_activity_lo",
        "initial_new_capacity_up",
        "soft_activity_up",
        "soft_activity_lo",
        "soft_new_capacity_up",
        "level_cost_activity_soft_up",
        "level_cost_activity_soft_lo",
        "growth_activity_lo",
        "growth_activity_up",
        "growth_new_capacity_up",
    ]

    multip_list = [
        "historical_activity",
        "historical_new_capacity",
        "initial_activity_up",
        "initial_activity_lo",
        "initial_new_capacity_up",
    ]

    # Fetch parameters
    param_data_list = [
        scen.par(p, {"technology": cooling_df["parent_tech"]}) for p in param_names
    ]
    # Combine results with parameter names
    list_params = list(zip(param_data_list, param_names))

    # Cooling technology suffixes
    suffixes = ["__ot_fresh", "__cl_fresh", "__air", "__ot_saline"]

    # Dictionary to store expanded parameters
    expanded_params = {}

    # Expand parameters for cooling technologies
    for df, param_name in list_params:
        df_param = pd.DataFrame()
        for suffix in suffixes:
            df_add = df.copy()
            df_add["technology"] = df_add["technology"] + suffix
            df_param = pd.concat([df_param, df_add])

        df_param_share = (
            apply_act_cap_multiplier(df_param, hold_cost, cap_fact_parent, param_name)
            if param_name in multip_list
            else df_param
        )

        expanded_params[param_name] = df_param_share

    return expanded_params


def _calculate_share_constraints(
    context: Context, input_cool: pd.DataFrame, FILE1: str, info: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Calculate share constraints for cooling technologies."""
    results = {}

    # Prepare basic share constraints
    cost, results = _prepare_share_constraints(input_cool, FILE1, info, results)

    # Add SSP-specific share constraints if available
    df_share = cooling_shares_SSP_from_yaml(context)

    if not df_share.empty and "share_commodity_up" in results:
        results["share_commodity_up"] = pd.concat(
            [results["share_commodity_up"], df_share], ignore_index=True
        )

    return cost, results.get("share_commodity_up", pd.DataFrame())


# water & electricity for cooling technologies
@minimum_version("message_ix 3.7")
def cool_tech(context: "Context") -> dict[str, pd.DataFrame]:
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

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'.
        Values are data frames ready for :meth:`~.Scenario.add_par`.
        Years in the data include the model horizon indicated by
        ``context["water build info"]``, plus the additional year 2010.
    """
    # Initialize result dictionary
    results = {}

    # Get reference to water configuration and time periods
    info = context["water build info"]
    sub_time = context.time

    # Step 1: Load auxiliary data (basin delineation, cooling tech specs)
    df_node, cooling_df, FILE1 = _load_auxiliary_data(context)
    node_region = df_node["region"].unique()

    # Step 2: Prepare initial cooling technology data
    input_cool, electr, saline_df, icmse_df = _prepare_initial_cooling_data(
        context, cooling_df
    )

    # Step 3: Generate input parameter
    inp = _generate_input_parameter(electr, icmse_df, saline_df)
    results["input"] = inp

    # Step 4: Generate emission factor parameter
    emi = _generate_emission_factor(input_cool)
    results["emission_factor"] = emi

    # Step 5: Generate output parameter
    out = _generate_output_parameter(context, input_cool, icmse_df, df_node, sub_time)
    results["output"] = out

    # Step 6: Prepare historical data
    input_cool_2015 = _prepare_historical_data(input_cool)

    # Step 7: Calculate share constraints
    cost, share_commodity_up = _calculate_share_constraints(
        context, input_cool, FILE1, info
    )
    if not share_commodity_up.empty:
        results["share_commodity_up"] = share_commodity_up

    # Step 8: Calculate historical shares
    search_cols = [
        col
        for col in cost.columns
        if context.regions in col or col in ["technology", "utype"]
    ]
    search_cols_cooling_fraction = [
        col for col in search_cols if col not in ["technology", "utype"]
    ]
    hold_cost = _calculate_historical_shares(
        context, cost, input_cool_2015, search_cols_cooling_fraction
    )

    # Step 9: Get parent capacity factors
    scen = context.get_scenario()
    cap_fact_parent = _get_parent_capacity_factors(scen, cooling_df)

    # Step 10: Project future costs
    inv_cost = _project_future_costs(context)
    results["inv_cost"] = inv_cost

    # Step 11: Generate addon parameters
    addon_df, addon_lo = _generate_addon_parameters(input_cool)
    results["addon_conversion"] = addon_df
    results["addon_lo"] = addon_lo

    # Step 12: Generate technical lifetime parameter
    tl = _generate_lifetime_parameter(inp, node_region, info)
    results["technical_lifetime"] = tl

    # Step 13: Generate capacity factor parameter
    results["capacity_factor"] = _compose_capacity_factor(inp=inp, context=context)

    # Step 14: Expand parent parameters
    expanded_params = _expand_parent_parameters(
        scen, cooling_df, hold_cost, cap_fact_parent
    )
    results.update(expanded_params)

    return results


# Water use & electricity for non-cooling technologies
def non_cooling_tec(context: "Context") -> dict[str, pd.DataFrame]:
    """Process data for water usage of power plants (non-cooling technology related).
    Water withdrawal values for power plants are read in from
    ``tech_water_performance_ssp_msg.csv``

    Parameters
    ----------
    context : .Context

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

    scen = context.get_scenario()
    tec_lt = scen.par("technical_lifetime")
    all_tech = list(tec_lt["technology"].unique())
    # all_tech = list(scen.set("technology"))
    tech_non_cool_csv = list(non_cool_df["technology_name"])
    techs_to_remove = [tec for tec in tech_non_cool_csv if tec not in all_tech]

    non_cool_df = non_cool_df[~non_cool_df["technology_name"].isin(techs_to_remove)]
    non_cool_df = non_cool_df.rename(columns={"technology_name": "technology"})

    non_cool_df["value"] = (
        non_cool_df["water_withdrawal_mid_m3_per_output"]
        * COOL_CONST["SECINYEAR"]
        * COOL_CONST["M3toMCM"]
    )

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
    # Only freshwater supply is assumed for simplicity
    dfs_rule = n_cool_df_merge
    args = {
        "rule_dfs": dfs_rule,
    }
    inp_n_cool_list = []
    for rule in NON_COOL_INPUT_RULES.get_rule():
        inp_n_cool_list.append(run_standard(rule, args))
    # append the input data to results
    results["input"] = safe_concat(inp_n_cool_list)

    return results
