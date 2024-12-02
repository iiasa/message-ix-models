"""Prepare data for water use for cooling & energy technologies."""

import logging
from typing import Literal, Union

import numpy as np
import pandas as pd
import yaml
from message_ix import make_df

from message_ix_models import Context
from message_ix_models.model.water.data.water_supply import map_basin_region_wat
from message_ix_models.util import (
    broadcast,
    make_matched_dfs,
    minimum_version,
    package_data_path,
    same_node,
)

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


def hist_act(x: pd.Series, context: "Context", hold_cost: pd.DataFrame) -> list:
    """Calculate historical activity of cooling technology.
    The data for shares is read from ``cooltech_cost_and_shares_ssp_msg.csv``

    Returns
    -------
    hist_activity(cooling_tech) = hist_activitiy(parent_technology) * share
    *cooling_fraction
    """
    tech_df = hold_cost[
        hold_cost["technology"].str.startswith(x.technology)
    ]  # [x.node_loc]

    node_search = context.regions if context.type_reg == "country" else x["node_loc"]

    node_loc = x["node_loc"]
    technology = x["technology"]
    cooling_technologies = list(tech_df["technology"])
    new_values = tech_df[node_search] * x.value

    return [
        [
            node_loc,
            technology,
            cooling_technology,
            x.year_act,
            x.value,
            new_value,
            x.unit,
        ]
        for new_value, cooling_technology in zip(new_values, cooling_technologies)
    ]


def hist_cap(x: pd.Series, context: "Context", hold_cost: pd.DataFrame) -> list:
    """Calculate historical capacity of cooling technology.
    The data for shares is read from ``cooltech_cost_and_shares_ssp_msg.csv``

    Returns
    -------
    hist_new_capacity(cooling_tech) = historical_new_capacity(parent_technology)*
    share * cooling_fraction
    """
    tech_df = hold_cost[
        hold_cost["technology"].str.startswith(x.technology)
    ]  # [x.node_loc]
    if context.type_reg == "country":
        node_search = context.regions
    else:
        node_search = x["node_loc"]  # R11_EEU
    node_loc = x["node_loc"]
    technology = x["technology"]
    cooling_technologies = list(tech_df["technology"])
    new_values = tech_df[node_search] * x.value

    return [
        [
            node_loc,
            technology,
            cooling_technology,
            x.year_vtg,
            x.value,
            new_value,
            x.unit,
        ]
        for new_value, cooling_technology in zip(new_values, cooling_technologies)
    ]


def relax_growth_constraint(
    ref_hist: pd.DataFrame,
    scen,
    cooling_df: pd.DataFrame,
    g_lo: pd.DataFrame,
    constraint_type: Literal[Union["activity", "new_capacity", "total_capacity"]],
) -> pd.DataFrame:
    """
    Checks if the parent technologies are shut down and require relaxing
    the growth constraint.

    Parameters
    ----------
    ref_hist : pd.DataFrame
        Historical data in the reference scenario.
    scen : Scenario
        Scenario object to retrieve necessary parameters.
    cooling_df : pd.DataFrame
        DataFrame containing information on cooling technologies and their
        parent technologies.
    g_lo : pd.DataFrame
        DataFrame containing growth constraints for each technology.
    constraint_type : {"activity", "new_capacity"}
        Type of constraint to check, either "activity" for operational limits or
        "new_capacity" for capacity expansion limits.

    Returns
    -------
    pd.DataFrame
        Updated `g_lo` DataFrame with relaxed growth constraints.
    """
    year_type = "year_vtg" if constraint_type == "new_capacity" else "year_act"
    year_hist = "year_act" if constraint_type == "activity" else "year_vtg"
    print(year_type)
    bound_param = (
        "bound_activity_lo"
        if constraint_type == "activity"
        else "bound_new_capacity_lo"
        if constraint_type == "new_capacity"
        else "bound_total_capacity_lo"
    )
    print(bound_param)
    # keep rows with max year_type
    max_year_hist = (
        ref_hist.loc[ref_hist.groupby(["node_loc", "technology"])[year_hist].idxmax()]
        .drop(columns="unit")
        .rename(columns={year_hist: "hist_year", "value": "hist_value"})
    )

    # Step 2: Check for bound_activity_up or bound_new_capacity_up conditions
    bound_up_pare = scen.par(bound_param, {"technology": cooling_df["parent_tech"]})
    # Get a set with unique year_type values and order them
    years = np.sort(bound_up_pare[year_type].unique())

    # In max_year_hist add the next year from years matching the hist_year columns
    max_year_hist["next_year"] = max_year_hist["hist_year"].apply(
        lambda x: years[years > x][0] if any(years > x) else None
    )

    # Merge the max_year_hist with bound_up_pare
    bound_up = pd.merge(bound_up_pare, max_year_hist, how="left")
    # subset of first year after the historical
    # if next_year = None (single year test case) bound_up1 is simply empty
    bound_up1 = bound_up[bound_up[year_type] == bound_up["next_year"]]
    # Categories that might break the growth constraints
    bound_up1 = bound_up1[bound_up1["value"] > 0.9 * bound_up1["hist_value"]]
    # not look ad sudden contraints after sthe starting year
    bound_up = bound_up.sort_values(by=["node_loc", "technology", year_type])
    # Check if value for a year is greater than the value of the next year
    bound_up["next_value"] = bound_up.groupby(["node_loc", "technology"])[
        "value"
    ].shift(-1)
    bound_up2 = bound_up[bound_up["value"] < 0.9 * bound_up["next_value"]]
    bound_up2 = bound_up2.drop(columns=["next_value"])
    # combine bound 1 and 2
    combined_bound = (
        pd.concat([bound_up1, bound_up2]).drop_duplicates().reset_index(drop=True)
    )
    # Keep only node_loc, technology, and year_type
    combined_bound = combined_bound[["node_loc", "technology", year_type]]
    # Add columns with value "remove" to be able to use make_matched_dfs
    combined_bound["rem"] = "remove"
    combined_bound.rename(columns={"technology": "parent_tech"}, inplace=True)

    # map_par tec to parent tec
    map_parent = cooling_df[["technology_name", "parent_tech"]]
    map_parent.rename(columns={"technology_name": "technology"}, inplace=True)
    # expand bound_up to all cooling technologies in map_parent
    combined_bound = pd.merge(combined_bound, map_parent, how="left")
    # rename tear_type to year_act, because g_lo use it
    combined_bound.rename(columns={year_type: "year_act"}, inplace=True)

    # Merge to g_lo to be able to remove the technologies
    g_lo = pd.merge(g_lo, combined_bound, how="left")
    g_lo = g_lo[g_lo["rem"] != "remove"]
    # Remove column rem and parent_tech
    g_lo = g_lo.drop(columns=["rem", "parent_tech"])

    return g_lo


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
    #: Name of the input file.
    # The input file mentions water withdrawals and emission heating fractions for
    # cooling technologies alongwith parent technologies:
    FILE = "tech_water_performance_ssp_msg.csv"
    # Investment costs & regional shares of hist. activities of cooling
    # technologies
    FILE1 = (
        "cooltech_cost_and_shares_"
        + (f"ssp_msg_{context.regions}" if context.type_reg == "global" else "country")
        + ".csv"
    )

    # define an empty dictionary
    results = {}

    # Reference to the water configuration
    info = context["water build info"]
    sub_time = context.time

    # reading basin_delineation
    FILE2 = f"basins_by_region_simpl_{context.regions}.csv"
    PATH = package_data_path("water", "delineation", FILE2)

    df_node = pd.read_csv(PATH)
    # Assigning proper nomenclature
    df_node["node"] = "B" + df_node["BCU_name"].astype(str)
    df_node["mode"] = "M" + df_node["BCU_name"].astype(str)
    df_node["region"] = (
        context.map_ISO_c[context.regions]
        if context.type_reg == "country"
        else f"{context.regions}_" + df_node["REGION"].astype(str)
    )

    node_region = df_node["region"].unique()
    # reading ppl cooling tech dataframe
    path = package_data_path("water", "ppl_cooling_tech", FILE)
    df = pd.read_csv(path)
    cooling_df = df.loc[df["technology_group"] == "cooling"].copy()
    # Separate a column for parent technologies of respective cooling
    # techs
    cooling_df["parent_tech"] = (
        cooling_df["technology_name"]
        .apply(lambda x: pd.Series(str(x).split("__")))
        .drop(columns=1)
    )

    scen = context.get_scenario()

    # Extracting input database from scenario for parent technologies
    # Extracting input values from scenario
    ref_input = scen.par("input", {"technology": cooling_df["parent_tech"]})
    # list of tec in cooling_df["parent_tech"] that are not in ref_input
    missing_tec = cooling_df["parent_tech"][
        ~cooling_df["parent_tech"].isin(ref_input["technology"])
    ]
    # some techs only have output, like csp
    ref_output = scen.par("output", {"technology": missing_tec})
    # set columns names of ref_output to be the same as ref_input
    ref_output.columns = ref_input.columns
    # merge ref_input and ref_output
    ref_input = pd.concat([ref_input, ref_output])
    # Extracting historical activity from scenario
    ref_hist_act = scen.par(
        "historical_activity", {"technology": cooling_df["parent_tech"]}
    )
    # Extracting historical capacity from scenario
    ref_hist_cap = scen.par(
        "historical_new_capacity", {"technology": cooling_df["parent_tech"]}
    )

    ref_input[["value", "level"]] = ref_input.apply(missing_tech, axis=1)

    # Combines the input df of parent_tech with water withdrawal data
    input_cool = (
        cooling_df.set_index("parent_tech")
        .combine_first(ref_input.set_index("technology"))
        .reset_index()
    )

    # Drops NA values from the value column
    input_cool = input_cool.dropna(subset=["value"])

    # Convert year values into integers to be compatibel for model
    input_cool.year_vtg = input_cool.year_vtg.astype(int)
    input_cool.year_act = input_cool.year_act.astype(int)
    # Drops extra technologies from the data. backwards compatibility
    input_cool = input_cool[
        (input_cool["level"] != "water_supply") & (input_cool["level"] != "cooling")
    ]
    # heat plants need no cooling
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

    input_cool["cooling_fraction"] = input_cool.apply(cooling_fr, axis=1)

    # Converting water withdrawal units to Km3/GWa
    # this refers to activity per cooling requirement (heat)
    input_cool["value_cool"] = (
        input_cool["water_withdrawal_mid_m3_per_output"]
        * 60
        * 60
        * 24
        * 365
        * 1e-9
        / input_cool["cooling_fraction"]
    )

    input_cool["return_rate"] = 1 - (
        input_cool["water_consumption_mid_m3_per_output"]
        / input_cool["water_withdrawal_mid_m3_per_output"]
    )
    # consumption to be saved in emissions rates for reporting purposes
    input_cool["consumption_rate"] = (
        input_cool["water_consumption_mid_m3_per_output"]
        / input_cool["water_withdrawal_mid_m3_per_output"]
    )

    input_cool["value_return"] = input_cool["return_rate"] * input_cool["value_cool"]

    # only for reporting purposes
    input_cool["value_consumption"] = (
        input_cool["consumption_rate"] * input_cool["value_cool"]
    )

    # def foo3(x):
    #     """
    #     This function is similar to foo2, but it returns electricity values
    #     per unit of cooling for techs that require parasitic electricity demand
    #     """
    #     if "hpl" in x['index']:
    #         return x['parasitic_electricity_demand_fraction']
    #
    #     elif x['parasitic_electricity_demand_fraction'] > 0.0:
    #         return x['parasitic_electricity_demand_fraction'] / x['cooling_fraction']

    # Filter out technologies that requires parasitic electricity
    electr = input_cool[input_cool["parasitic_electricity_demand_fraction"] > 0.0]

    # Make a new column 'value_cool' for calculating values against technologies
    electr["value_cool"] = (
        electr["parasitic_electricity_demand_fraction"] / electr["cooling_fraction"]
    )
    # Filters out technologies requiring saline water supply
    saline_df = input_cool[
        input_cool["technology_name"].str.endswith("ot_saline", na=False)
    ]

    # input_cool_minus_saline_elec_df
    con1 = input_cool["technology_name"].str.endswith("ot_saline", na=False)
    con2 = input_cool["technology_name"].str.endswith("air", na=False)
    icmse_df = input_cool[(~con1) & (~con2)]
    # electricity inputs
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
    # once through and closed loop freshwater
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
                commodity="freshwater",
                level="water_supply",
                time="year",
                time_origin="year",
                value=icmse_df["value_cool"],
                unit="km3/GWa",
            ),
        ]
    )
    # saline cooling technologies
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
                unit="km3/GWa",
            ),
        ]
    )

    # Drops NA values from the value column
    inp = inp.dropna(subset=["value"])

    # append the input data to results
    results["input"] = inp

    # add water consumption as emission factor, also for saline tecs
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
        unit="km3/yr",
    )

    results["emission_factor"] = emi

    # add output for share contraints to introduce SSP assumptions
    # also in the nexus case, the share contraints are at the macro-regions

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

    # add water return flows for cooling tecs
    # Use share of basin availability to distribute the return flow from
    df_sw = map_basin_region_wat(context)
    df_sw.drop(columns={"mode", "date", "MSGREG"}, inplace=True)
    df_sw.rename(
        columns={"region": "node_dest", "time": "time_dest", "year": "year_act"},
        inplace=True,
    )
    df_sw["time_dest"] = df_sw["time_dest"].astype(str)
    if context.nexus_set == "nexus":
        for nn in icmse_df.node_loc.unique():
            # input cooling fresh basin
            icfb_df = icmse_df[icmse_df["node_loc"] == nn]
            bs = list(df_node[df_node["region"] == nn]["node"])

            out_t = (
                make_df(
                    "output",
                    node_loc=icfb_df["node_loc"],
                    technology=icfb_df["technology_name"],
                    year_vtg=icfb_df["year_vtg"],
                    year_act=icfb_df["year_act"],
                    mode=icfb_df["mode"],
                    # node_origin=icmse_df["node_origin"],
                    commodity="surfacewater_basin",
                    level="water_avail_basin",
                    time="year",
                    value=icfb_df["value_return"],
                    unit="km3/GWa",
                )
                .pipe(broadcast, node_dest=bs, time_dest=sub_time)
                .merge(df_sw, how="left")
            )
            # multiply by basin water availability share
            out_t["value"] = out_t["value"] * out_t["share"]
            out_t.drop(columns={"share"}, inplace=True)
            out = pd.concat([out, out_t])

        out = out.dropna(subset=["value"])
        out.reset_index(drop=True, inplace=True)
    # in any case save out into results
    results["output"] = out

    # costs and historical parameters
    path1 = package_data_path("water", "ppl_cooling_tech", FILE1)
    cost = pd.read_csv(path1)
    # Combine technology name to get full cooling tech names
    cost["technology"] = cost["utype"] + "__" + cost["cooling"]
    # Filtering out 2010 data to use for historical values
    input_cool_2020 = input_cool[
        (input_cool["year_act"] == 2020) & (input_cool["year_vtg"] == 2020)
    ]
    # Filter out columns that contain 'mix' in column name
    columns = [col for col in cost.columns if "mix_" in col]
    # Rename column names to R11 to match with the previous df

    cost.rename(columns=lambda name: name.replace("mix_", ""), inplace=True)
    search_cols = [
        col for col in cost.columns if context.regions in col or "technology" in col
    ]
    hold_df = input_cool_2020[
        ["node_loc", "technology_name", "cooling_fraction"]
    ].drop_duplicates()
    search_cols_cooling_fraction = [col for col in search_cols if col != "technology"]

    # Apply function to the
    hold_cost = cost[search_cols].apply(
        shares,
        axis=1,
        context=context,
        search_cols_cooling_fraction=search_cols_cooling_fraction,
        hold_df=hold_df,
        search_cols=search_cols,
    )

    changed_value_series = ref_hist_act.apply(
        hist_act, axis=1, context=context, hold_cost=hold_cost
    )
    changed_value_series_flat = [
        row for series in changed_value_series for row in series
    ]
    columns = [
        "node_loc",
        "technology",
        "cooling_technology",
        "year_act",
        "value",
        "new_value",
        "unit",
    ]
    # dataframe for historical activities of cooling techs
    act_value_df = pd.DataFrame(changed_value_series_flat, columns=columns)
    act_value_df = act_value_df[act_value_df["new_value"] > 0]

    changed_value_series = ref_hist_cap.apply(
        hist_cap, axis=1, context=context, hold_cost=hold_cost
    )
    changed_value_series_flat = [
        row for series in changed_value_series for row in series
    ]
    columns = [
        "node_loc",
        "technology",
        "cooling_technology",
        "year_vtg",
        "value",
        "new_value",
        "unit",
    ]
    cap_value_df = pd.DataFrame(changed_value_series_flat, columns=columns)
    cap_value_df = cap_value_df[cap_value_df["new_value"] > 0]

    # Make model compatible df for historical activitiy
    h_act = make_df(
        "historical_activity",
        node_loc=act_value_df["node_loc"],
        technology=act_value_df["cooling_technology"],
        year_act=act_value_df["year_act"],
        mode="M1",
        time="year",
        value=act_value_df["new_value"],
        # TODO finalize units
        unit="GWa",
    )

    results["historical_activity"] = h_act
    # Make model compatible df for histroical new capacity
    h_cap = make_df(
        "historical_new_capacity",
        node_loc=cap_value_df["node_loc"],
        technology=cap_value_df["cooling_technology"],
        year_vtg=cap_value_df["year_vtg"],
        value=cap_value_df["new_value"],
        unit="GWa",
    )

    results["historical_new_capacity"] = h_cap

    # Manually removing extra technologies not required
    # TODO make it automatic to not include the names manually
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

    from message_ix_models.tools.costs.config import Config
    from message_ix_models.tools.costs.projections import create_cost_projections

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

    # Add the investment costs to the results
    results["inv_cost"] = inv_cost

    # Addon conversion
    adon_df = input_cool.copy()
    # Add 'cooling_' before name of parent technologies that are type_addon
    # nomenclature
    adon_df["tech"] = "cooling__" + adon_df["parent_tech"].astype(str)
    # technology : 'parent technology' and type_addon is type of addons such
    # as 'cooling__bio_hpl'
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
        unit="km3/GWa",
    )

    results["addon_conversion"] = addon_df

    # Addon_lo will remain 1 for all cooling techs so it allows 100% activity of
    # parent technologies
    addon_lo = make_matched_dfs(addon_df, addon_lo=1)
    results["addon_lo"] = addon_lo["addon_lo"]

    # technical lifetime
    # make_matched_dfs didn't map all technologies
    # tl = make_matched_dfs(inv_cost,
    #                       technical_lifetime = 30)
    year = info.Y
    if 2010 in year:
        pass
    else:
        year.insert(0, 2010)

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

    cap_fact = make_matched_dfs(inp, capacity_factor=1)
    # Climate Impacts on freshwater cooling capacity
    # Taken from
    # https://www.sciencedirect.com/science/article/
    #  pii/S0959378016301236?via%3Dihub#sec0080
    if context.RCP == "no_climate":
        df = cap_fact["capacity_factor"]
    else:
        df = cap_fact["capacity_factor"]
        # reading ppl cooling impact dataframe
        path = package_data_path(
            "water", "ppl_cooling_tech", "power_plant_cooling_impact_MESSAGE.xlsx"
        )
        df_impact = pd.read_excel(path, sheet_name=f"{context.regions}_{context.RCP}")

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

    results["capacity_factor"] = df
    # results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    # growth activity up to avoid sudden switch in the cooling techs
    g_up = make_df(
        "growth_activity_up",
        technology=inp["technology"].drop_duplicates(),
        value=0.05,
        unit="%",
        time="year",
    ).pipe(broadcast, year_act=info.Y, node_loc=node_region)

    # relax growth constraints for activity jumps of parent technologies
    g_up = relax_growth_constraint(ref_hist_act, scen, cooling_df, g_up, "activity")
    # relax growth constraints for capacity jumps of parent technologies
    g_up = relax_growth_constraint(ref_hist_cap, scen, cooling_df, g_up, "new_capacity")
    g_up = relax_growth_constraint(
        ref_hist_cap, scen, cooling_df, g_up, "total_capacity"
    )

    results["growth_activity_up"] = g_up

    # add share constraints for cooling technologies based on SSP assumptions
    df_share = cooling_shares_SSP_from_yaml(context)

    if not df_share.empty:
        results["share_commodity_up"] = df_share

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
        non_cool_df["water_withdrawal_mid_m3_per_output"] * 60 * 60 * 24 * 365 * (1e-9)
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
    inp_n_cool = make_df(
        "input",
        technology=n_cool_df_merge["technology"],
        value=n_cool_df_merge["value_y"],
        unit="km3/GWa",
        level="water_supply",
        commodity="freshwater",
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
