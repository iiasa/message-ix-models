"""Prepare data for adding demands"""

import os

import numpy as np
import pandas as pd
import xarray as xr
from message_ix import make_df
from message_ix_models.util import broadcast, private_data_path


def target_rate(df, basin, val):
    """
    Sets target connection and sanitation rates for SDG scenario
    It filters out the basins as developing and developed baed on the countries
    overlapping basins. If the numbers of developing countries in the basins are
    more than basin is cateogirzez as developing and vice versa.
    If the number of developing and developed countries are equal in a basin, then
    the basin is assumed developing.
    For developed basins target is set at 2030.
    For developing basins, the access target is set at 2040 and 2035 target is the average of
    2030 original rate and 2040 target.
    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
    """
    value = []
    for i in df.node.unique():
        temp = basin[basin["BCU_name"] == i]

        sizes = temp.pivot_table(index=["STATUS"], aggfunc="size")

        if len(sizes) > 1:
            if sizes["DEV"] > sizes["IND"] or sizes["DEV"] == sizes["IND"]:

                # for j in urban[urban["node"] == i][urban[urban["node"] == i]["year"] == 2030].index:
                #      if urban[urban["node"] == i][urban[urban["node"] == i]["year"] == 2030].at[j, "value"] < np.float64(0.75):
                #          value.append([j, np.float64(0.75)])

                for ind, j in enumerate(
                    df[df["node"] == i][df[df["node"] == i]["year"] == 2030].index
                ):
                    jj = df[df["node"] == i][df[df["node"] == i]["year"] == 2035].index
                    temp = (
                        df[df["node"] == i][df[df["node"] == i]["year"] == 2030].at[
                            j, "value"
                        ]
                        + val
                    ) / 2
                    value.append([jj[ind], np.float64(temp)])

                for j in df[df["node"] == i][df[df["node"] == i]["year"] >= 2040].index:
                    if df[df["node"] == i][df[df["node"] == i]["year"] >= 2040].at[
                        j, "value"
                    ] < np.float64(val):
                        value.append([j, np.float64(val)])
            else:
                for j in df[df["node"] == i][df[df["node"] == i]["year"] >= 2030].index:
                    if df[df["node"] == i][df[df["node"] == i]["year"] >= 2030].at[
                        j, "value"
                    ] < np.float64(val):
                        value.append([j, np.float64(val)])
        else:
            if sizes.index[0] == "DEV":
                # for j in urban[urban["node"] == i][urban[urban["node"] == i]["year"] == 2030].index:
                #       if urban[urban["node"] == i][urban[urban["node"] == i]["year"] == 2030].at[j, "value"] < np.float64(0.75):
                #           value.append([j, np.float64(0.75)])

                for ind, j in enumerate(
                    df[df["node"] == i][df[df["node"] == i]["year"] == 2030].index
                ):
                    jj = df[df["node"] == i][df[df["node"] == i]["year"] == 2035].index
                    temp = (
                        df[df["node"] == i][df[df["node"] == i]["year"] == 2030].at[
                            j, "value"
                        ]
                        + val
                    ) / 2
                    value.append([jj[ind], np.float64(temp)])

                for j in df[df["node"] == i][df[df["node"] == i]["year"] >= 2040].index:
                    if df[df["node"] == i][df[df["node"] == i]["year"] >= 2040].at[
                        j, "value"
                    ] < np.float64(val):
                        value.append([j, np.float64(val)])
            else:
                for j in df[df["node"] == i][df[df["node"] == i]["year"] >= 2030].index:
                    if df[df["node"] == i][df[df["node"] == i]["year"] >= 2030].at[
                        j, "value"
                    ] < np.float64(val):
                        value.append([j, np.float64(val)])
    valuetest = pd.DataFrame(data=value, columns=["Index", "Value"])

    for i in range(len(valuetest["Index"])):
        df.at[valuetest["Index"][i], "Value"] = valuetest["Value"][i]

    real_value = df["Value"].combine_first(df["value"])

    df.drop(["value", "Value"], axis=1, inplace=True)
    df["value"] = real_value

    return df


def target_rate_trt(df, basin):
    """
    Sets target treatment rates for SDG scenario. The target value for
    developed and developing region is making sure that the amount of untreated
    wastewater is halved beyond 2030 & 2040 respectively.
    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
    """

    value = []
    for i in df.node.unique():
        temp = basin[basin["BCU_name"] == i]

        sizes = temp.pivot_table(index=["STATUS"], aggfunc="size")

        if len(sizes) > 1:
            if sizes["DEV"] > sizes["IND"] or sizes["DEV"] == sizes["IND"]:

                for j in df[df["node"] == i][df[df["node"] == i]["year"] >= 2040].index:
                    temp = df[df["node"] == i][df[df["node"] == i]["year"] >= 2040].at[
                        j, "value"
                    ]
                    temp = temp + (1 - temp) / 2
                    value.append([j, np.float64(temp)])
            else:
                for j in df[df["node"] == i][df[df["node"] == i]["year"] >= 2030].index:
                    temp = df[df["node"] == i][df[df["node"] == i]["year"] >= 2030].at[
                        j, "value"
                    ]
                    temp = temp + (1 - temp) / 2
                    value.append([j, np.float64(temp)])
        else:
            if sizes.index[0] == "DEV":

                for j in df[df["node"] == i][df[df["node"] == i]["year"] >= 2040].index:
                    temp = df[df["node"] == i][df[df["node"] == i]["year"] >= 2040].at[
                        j, "value"
                    ]
                    temp = temp + (1 - temp) / 2
                    value.append([j, np.float64(temp)])
            else:
                for j in df[df["node"] == i][df[df["node"] == i]["year"] >= 2030].index:
                    temp = df[df["node"] == i][df[df["node"] == i]["year"] >= 2030].at[
                        j, "value"
                    ]
                    temp = temp + (1 - temp) / 2
                    value.append([j, np.float64(temp)])

    valuetest = pd.DataFrame(data=value, columns=["Index", "Value"])

    for i in range(len(valuetest["Index"])):
        df.at[valuetest["Index"][i], "Value"] = valuetest["Value"][i]

    real_value = df["Value"].combine_first(df["value"])

    df.drop(["value", "Value"], axis=1, inplace=True)

    df["value"] = real_value
    return df


def add_sectoral_demands(context):
    """
    Adds water sectoral demands
    Parameters
    ----------
    context : .Context
    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'. Values
        are data frames ready for :meth:`~.Scenario.add_par`.
    """

    # define an empty dictionary
    results = {}

    # Reference to the water configuration
    info = context["water build info"]

    # defines path to read in demand data
    region = f"{context.regions}"
    path = private_data_path("water", "water_demands", "harmonized", region, ".")
    # make sure all of the csvs have format, otherwise it might not work
    list_of_csvs = list(path.glob("*_baseline.csv"))
    # define names for variables
    fns = [os.path.splitext(os.path.basename(x))[0] for x in list_of_csvs]
    fns = " ".join(fns).replace("ssp2_regional_", "").split()
    # dictionary for reading csv files
    d = {}

    for i in range(len(fns)):
        d[fns[i]] = pd.read_csv(list_of_csvs[i])

    # d is a dictionary that have ist of dataframes read in this folder
    dfs = {}
    for key, df in d.items():
        df.rename(columns={"Unnamed: 0": "year"}, inplace=True)
        df.index = df["year"]
        df = df.drop(columns=["year"])
        dfs[key] = df

    # convert the dictionary of dataframes to xarray
    df_x = xr.Dataset(dfs).to_array()
    df_x_interp = df_x.interp(year=[2015, 2025, 2035, 2045, 2055])
    df_x_c = df_x.combine_first(df_x_interp)
    # Unstack xarray back to pandas dataframe
    df_f = df_x_c.to_dataframe("").unstack()

    # Format the dataframe to be compatible with message format
    df_dmds = df_f.stack().reset_index(level=0).reset_index()
    df_dmds.columns = ["year", "node", "variable", "value"]
    df_dmds.sort_values(["year", "node", "variable", "value"], inplace=True)

    # Write final interpolated values as csv
    # df2_f.to_csv('final_interpolated_values.csv')

    urban_withdrawal_df = df_dmds[df_dmds["variable"] == "urban_withdrawal_baseline"]
    rual_withdrawal_df = df_dmds[df_dmds["variable"] == "rural_withdrawal_baseline"]
    urban_return_df = df_dmds[df_dmds["variable"] == "urban_return_baseline"]
    urban_return_df.reset_index(drop=True, inplace=True)
    rural_return_df = df_dmds[df_dmds["variable"] == "rural_return_baseline"]
    rural_return_df.reset_index(drop=True, inplace=True)
    urban_connection_rate_df = df_dmds[
        df_dmds["variable"] == "urban_connection_rate_baseline"
    ]
    urban_connection_rate_df.reset_index(drop=True, inplace=True)
    rural_connection_rate_df = df_dmds[
        df_dmds["variable"] == "rural_connection_rate_baseline"
    ]
    rural_connection_rate_df.reset_index(drop=True, inplace=True)

    urban_treatment_rate_df = df_dmds[
        df_dmds["variable"] == "urban_treatment_rate_baseline"
    ]
    urban_treatment_rate_df.reset_index(drop=True, inplace=True)

    rural_treatment_rate_df = df_dmds[
        df_dmds["variable"] == "rural_treatment_rate_baseline"
    ]
    rural_treatment_rate_df.reset_index(drop=True, inplace=True)

    df_recycling = df_dmds[df_dmds["variable"] == "urban_recycling_rate_baseline"]
    df_recycling.reset_index(drop=True, inplace=True)

    all_rates_base = pd.concat(
        [
            urban_connection_rate_df,
            rural_connection_rate_df,
            urban_treatment_rate_df,
            rural_treatment_rate_df,
            df_recycling,
        ]
    )

    if context.SDG:
        # reading basin mapping to countries
        FILE2 = f"basins_country_{context.regions}.csv"
        PATH = private_data_path("water", "delineation", FILE2)

        df_basin = pd.read_csv(PATH)

        # Applying 80% sanitation rate for rural sanitation
        rural_treatment_rate_df = rural_treatment_rate_df_sdg = target_rate(
            rural_treatment_rate_df, df_basin, 0.8
        )
        # Applying 95% sanitation rate for urban sanitation
        urban_treatment_rate_df = urban_treatment_rate_df_sdg = target_rate(
            urban_treatment_rate_df, df_basin, 0.95
        )
        # Applying 99% connection rate for urban infrastructure
        urban_connection_rate_df = urban_connection_rate_df_sdg = target_rate(
            urban_connection_rate_df, df_basin, 0.99
        )
        # Applying 80% connection rate for rural infrastructure
        rural_connection_rate_df = rural_connection_rate_df_sdg = target_rate(
            rural_connection_rate_df, df_basin, 0.8
        )
        # Applying sdg6 waste water treatment target
        df_recycling = df_recycling_sdg = target_rate_trt(df_recycling, df_basin)

        all_rates_sdg = pd.concat(
            [
                urban_connection_rate_df_sdg,
                rural_connection_rate_df_sdg,
                urban_treatment_rate_df_sdg,
                rural_treatment_rate_df_sdg,
                df_recycling_sdg,
            ]
        )
        all_rates_sdg["variable"] = [
            x.replace("baseline", "sdg") for x in all_rates_sdg["variable"]
        ]
        all_rates = pd.concat([all_rates_base, all_rates_sdg])
        save_path = private_data_path(
            "water", "water_demands", "harmonized", context.regions
        )
        # save all the rates for reporting purposes
        all_rates.to_csv(save_path / "all_rates_SSP2.csv", index=False)

    urban_mw = urban_withdrawal_df.reset_index(drop=True)
    urban_mw["value"] = (1e-3 * urban_mw["value"]) * urban_connection_rate_df["value"]

    dmd_df = make_df(
        "demand",
        node="B" + urban_mw["node"],
        commodity="urban_mw",
        level="final",
        year=urban_mw["year"],
        time="year",
        value=urban_mw["value"],
        unit="km3/year",
    )
    urban_dis = urban_withdrawal_df.reset_index(drop=True)
    urban_dis["value"] = (1e-3 * urban_dis["value"]) * (
        1 - urban_connection_rate_df["value"]
    )

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + urban_dis["node"],
            commodity="urban_disconnected",
            level="final",
            year=urban_dis["year"],
            time="year",
            value=urban_dis["value"],
            unit="km3/year",
        )
    )
    rural_mw = rual_withdrawal_df.reset_index(drop=True)
    rural_mw["value"] = (1e-3 * rural_mw["value"]) * rural_connection_rate_df["value"]

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + rural_mw["node"],
            commodity="rural_mw",
            level="final",
            year=rural_mw["year"],
            time="year",
            value=rural_mw["value"],
            unit="km3/year",
        )
    )

    rural_dis = rual_withdrawal_df.reset_index(drop=True)
    rural_dis["value"] = (1e-3 * rural_dis["value"]) * (
        1 - rural_connection_rate_df["value"]
    )

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + rural_dis["node"],
            commodity="rural_disconnected",
            level="final",
            year=rural_dis["year"],
            time="year",
            value=rural_dis["value"],
            unit="km3/year",
        )
    )
    urban_collected_wst = urban_return_df.reset_index(drop=True)
    urban_collected_wst["value"] = (
        1e-3 * urban_return_df["value"]
    ) * urban_treatment_rate_df["value"]

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + urban_collected_wst["node"],
            commodity="urban_collected_wst",
            level="final",
            year=urban_collected_wst["year"],
            time="year",
            value=-urban_collected_wst["value"],
            unit="km3/year",
        )
    )

    rural_collected_wst = rural_return_df.reset_index(drop=True)
    rural_collected_wst["value"] = (
        1e-3 * rural_return_df["value"]
    ) * rural_treatment_rate_df["value"]

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + rural_collected_wst["node"],
            commodity="rural_collected_wst",
            level="final",
            year=rural_collected_wst["year"],
            time="year",
            value=-rural_collected_wst["value"],
            unit="km3/year",
        )
    )
    urban_uncollected_wst = urban_return_df.reset_index(drop=True)
    urban_uncollected_wst["value"] = (1e-3 * urban_return_df["value"]) * (
        1 - urban_treatment_rate_df["value"]
    )

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + urban_uncollected_wst["node"],
            commodity="urban_uncollected_wst",
            level="final",
            year=urban_uncollected_wst["year"],
            time="year",
            value=-urban_uncollected_wst["value"],
            unit="km3/year",
        )
    )

    rural_uncollected_wst = rural_return_df.reset_index(drop=True)
    rural_uncollected_wst["value"] = (1e-3 * rural_return_df["value"]) * (
        1 - rural_treatment_rate_df["value"]
    )

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + rural_uncollected_wst["node"],
            commodity="rural_uncollected_wst",
            level="final",
            year=rural_uncollected_wst["year"],
            time="year",
            value=-rural_uncollected_wst["value"],
            unit="km3/year",
        )
    )

    results["demand"] = dmd_df

    # Add 2010 & 2015 values as historical activities to corresponding technologies
    h_act = dmd_df[dmd_df["year"].isin([2010, 2015])]

    # create a list of our conditions
    conditions = [
        (h_act["commodity"] == "urban_mw"),
        (h_act["commodity"] == "rural_mw"),
        (h_act["commodity"] == "urban_disconnected"),
        (h_act["commodity"] == "rural_disconnected"),
        (h_act["commodity"] == "urban_collected_wst"),
        (h_act["commodity"] == "rural_collected_wst"),
        (h_act["commodity"] == "urban_uncollected_wst"),
        (h_act["commodity"] == "rural_uncollected_wst"),
    ]

    # create a list of the values we want to assign for each condition
    values = [
        "urban_t_d",
        "rural_t_d",
        "urban_unconnected",
        "rural_unconnected",
        "urban_sewerage",
        "rural_sewerage",
        "urban_untreated",
        "rural_untreated",
    ]
    # create a new column and use np.select to assign values to it using our lists as arguments
    h_act["commodity"] = np.select(conditions, values)

    hist_act = make_df(
        "historical_activity",
        node_loc=h_act["node"],
        technology=h_act["commodity"],
        year_act=h_act["year"],
        mode="M1",
        time="year",
        value=h_act["value"].abs(),
        unit="km3/year",
    )
    results["historical_activity"] = hist_act

    h_cap = h_act[h_act["year"] >= 2015]

    hist_cap = make_df(
        "historical_new_capacity",
        node_loc=h_cap["node"],
        technology=h_cap["commodity"],
        year_vtg=h_cap["year"],
        value=h_cap["value"].abs() / 5,
        unit="km3/year",
    )

    results["historical_new_capacity"] = hist_cap

    # share constraint lower bound on urban_Water recycling
    df_share_wat = make_df(
        "share_commodity_lo",
        shares="share_wat_recycle",
        node_share="B" + df_recycling["node"],
        year_act=df_recycling["year"],
        time="year",
        value=df_recycling["value"],
        unit="-",
    )
    results["share_commodity_lo"] = df_share_wat

    # rel = make_df(
    #     "relation_activity",
    #     relation="recycle_rel",
    #     node_rel="B" + df_recycling["node"],
    #     year_rel=df_recycling["year"],
    #     node_loc="B" + df_recycling["node"],
    #     technology="urban_recycle",
    #     year_act=df_recycling["year"],
    #     mode="M1",
    #     value=-df_recycling["value"],
    #     unit="-",
    # )

    # rel = rel.append(
    #     make_df(
    #         "relation_activity",
    #         relation="recycle_rel",
    #         node_rel="B" + df_recycling["node"],
    #         year_rel=df_recycling["year"],
    #         node_loc="B" + df_recycling["node"],
    #         technology="urban_sewerage",
    #         year_act=df_recycling["year"],
    #         mode="M1",
    #         value=1,
    #         unit="-",
    #     )
    # )

    # results["relation_activity"] = rel

    # rel_lo = make_df(
    #     "relation_lower",
    #     relation="recycle_rel",
    #     node_rel="B" + df_recycling["node"],
    #     value=0,
    #     unit="-",
    # ).pipe(broadcast, year_rel=info.Y)

    # results["relation_lower"] = rel_lo

    # rel_up = make_df(
    #     "relation_upper",
    #     relation="recycle_rel",
    #     node_rel="B" + df_recycling["node"],
    #     value=0,
    #     unit="-",
    # ).pipe(broadcast, year_rel=info.Y)

    # results["relation_upper"] = rel_up

    return results


def add_water_availability(context):
    """
    Adds water supply constraints
    Parameters
    ----------
    context : .Context
    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'. Values
        are data frames ready for :meth:`~.Scenario.add_par`.
    """
    # Reference to the water configuration
    info = context["water build info"]

    # define an empty dictionary
    results = {}
    # Adding freshwater supply constraints
    # Reading data, the data is spatially and temprally aggregated from GHMs
    path1 = private_data_path(
        "water", "water_availability", f"qtot_{context.RCP}_{context.REL}.csv"
    )
    df_sw = pd.read_csv(path1)
    # reading sample for assiging basins
    PATH = private_data_path("water", "water_availability", "sample.csv")
    df_x = pd.read_csv(PATH)

    df_sw.drop(["Unnamed: 0"], axis=1, inplace=True)
    years = list(range(2010, 2105, 5))
    df_sw.columns = years
    df_sw.index = df_x["BCU_name"]
    df_sw[2110] = df_sw[2100]
    df_sw.drop(columns=[2065, 2075, 2085, 2095], inplace=True)
    df_sw = df_sw.stack().reset_index()
    df_sw.columns = ["Region", "years", "value"]
    df_sw.sort_values(["Region", "years", "value"], inplace=True)
    df_sw.fillna(0, inplace=True)
    df_sw.reset_index(drop=True, inplace=True)

    # Reading data, the data is spatially and temporally aggregated from GHMs
    path1 = private_data_path(
        "water", "water_availability", f"qr_{context.RCP}_{context.REL}.csv"
    )
    df_gw = pd.read_csv(path1)

    df_gw.drop(["Unnamed: 0"], axis=1, inplace=True)
    df_gw.columns = years
    df_gw.index = df_x["BCU_name"]
    df_gw[2110] = df_gw[2100]
    df_gw.drop(columns=[2065, 2075, 2085, 2095], inplace=True)
    df_gw = df_gw.stack().reset_index()
    df_gw.columns = ["Region", "years", "value"]
    df_gw.sort_values(["Region", "years", "value"], inplace=True)
    df_gw.fillna(0, inplace=True)
    df_gw.reset_index(drop=True, inplace=True)

    dmd_df = make_df(
        "demand",
        node="B" + df_sw["Region"].astype(str),
        commodity="surfacewater_basin",
        level="water_avail_basin",
        year=df_sw["years"],
        time="year",
        value=-df_sw["value"],
        unit="km3/year",
    )

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + df_gw["Region"].astype(str),
            commodity="groundwater_basin",
            level="water_avail_basin",
            year=df_gw["years"],
            time="year",
            value=-df_gw["value"],
            unit="km3/year",
        )
    )

    dmd_df["value"] = dmd_df["value"].apply(lambda x: x if x <= 0 else 0)

    results["demand"] = dmd_df

    # share constraint lower bound on groundwater
    df_share = make_df(
        "share_commodity_lo",
        shares="share_low_lim_GWat",
        node_share="B" + df_gw["Region"],
        year_act=df_gw["years"],
        time="year",
        value=df_gw["value"]
        / (df_sw["value"] + df_gw["value"])
        * 0.95,  # 0.95 buffer factor to avoid numerical error
        unit="-",
    )

    df_share["value"] = df_share["value"].fillna(0)

    results["share_commodity_lo"] = df_share

    return results


def add_irrigation_demand(context):
    """
    Adds endogenous irrigation water demands from GLOBIOM emulator
    Parameters
    ----------
    context : .Context
    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'. Values
        are data frames ready for :meth:`~.Scenario.add_par`.
    """
    # define an empty dictionary
    results = {}

    scen = context.get_scenario()
    # add water for irrigation from globiom
    land_out_1 = scen.par(
        "land_output", {"commodity": "Water|Withdrawal|Irrigation|Cereals"}
    )
    land_out_1["level"] = "irr_cereal"
    land_out_2 = scen.par(
        "land_output", {"commodity": "Water|Withdrawal|Irrigation|Oilcrops"}
    )
    land_out_2["level"] = "irr_oilcrops"
    land_out_3 = scen.par(
        "land_output", {"commodity": "Water|Withdrawal|Irrigation|Sugarcrops"}
    )
    land_out_3["level"] = "irr_sugarcrops"

    land_out = pd.concat([land_out_1, land_out_2, land_out_3])
    land_out["commodity"] = "freshwater"

    land_out["value"] = 1e-3 * land_out["value"]

    # take land_out edited and add as a demand in  land_input
    results["land_input"] = land_out

    return results
