"""Prepare data for adding demands"""

import os

import numpy as np
import pandas as pd
import xarray as xr
from message_ix import make_df

from message_ix_models.util import broadcast, private_data_path


def get_basin_sizes(basin, node):
    """Returns the sizes of developing and developed basins for a given node"""
    temp = basin[basin["BCU_name"] == node]
    sizes = temp.pivot_table(index=["STATUS"], aggfunc="size")
    return sizes["DEV"], sizes["IND"]


def set_target_rate(df, node, year, target):
    """Sets the target value for a given node and year"""
    indices = df[df["node"] == node][df[df["node"] == node]["year"] == year].index
    for index in indices:
        if (
            df[df["node"] == node][df[df["node"] == node]["year"] == year].at[
                index, "value"
            ]
            < target
        ):
            df.at[index, "value"] = target


def set_target_rate_developed(df, node, target):
    """Sets target rate for a developed basin"""
    set_target_rate(df, node, 2030, target)


def set_target_rate_developing(df, node, target):
    """Sets target rate for a developing basin"""
    for i in df.index:
        if df.at[i, "node"] == node and df.at[i, "year"] == 2030:
            value_2030 = df.at[i, "value"]
            break

    set_target_rate(
        df,
        node,
        2035,
        (value_2030 + target) / 2,
    )
    set_target_rate(df, node, 2040, target)


def set_target_rates(df, basin, val):
    """Sets target rates for all nodes in a given basin"""
    for node in df.node.unique():
        dev_size, ind_size = get_basin_sizes(basin, node)
        if dev_size >= ind_size:
            set_target_rate_developed(df, node, val)
        else:
            set_target_rate_developing(df, node, val)


def target_rate(df, basin, val):
    """
    Sets target connection and sanitation rates for SDG scenario.
    The function filters out the basins as developing and
    developed based on the countries overlapping basins.
    If the number of developing countries in the basins are
    more than basin is categorized as developing and vice versa.
    If the number of developing and developed countries are equal
    in a basin, then the basin is assumed developing.
    For developed basins, target is set at 2030.
    For developing basins, the access target is set at
    2040 and 2035 target is the average of
    2030 original rate and 2040 target.
    Returns:
        df (pandas.DataFrame): Data frame with updated value column.
    """
    set_target_rates(df, basin, val)
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
    sub_time = context.time
    path = private_data_path("water", "demands", "harmonized", region, ".")
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

    df_dmds["time"] = "year"

    # Write final interpolated values as csv
    # df2_f.to_csv('final_interpolated_values.csv')

    # if we are using sub-annual timesteps we replace the rural and municipal
    # withdrawals and return flows with monthly data and also add industrial
    if "year" not in context.time:
        PATH = private_data_path(
            "water", "demands", "harmonized", region, "ssp2_m_water_demands.csv"
        )
        df_m = pd.read_csv(PATH)
        df_m.value *= 30  # from mcm/day to mcm/month
        df_m["sector"][df_m["sector"] == "industry"] = "manufacturing"
        df_m["variable"] = df_m["sector"] + "_" + df_m["type"] + "_baseline"
        df_m["variable"].replace(
            "urban_withdrawal_baseline", "urban_withdrawal2_baseline", inplace=True
        )
        df_m["variable"].replace(
            "urban_return_baseline", "urban_return2_baseline", inplace=True
        )
        df_m = df_m[["year", "pid", "variable", "value", "month"]]
        df_m.columns = ["year", "node", "variable", "value", "time"]

        # remove yearly parts from df_dms
        df_dmds = df_dmds[
            ~df_dmds["variable"].isin(
                [
                    "urban_withdrawal2_baseline",
                    "rural_withdrawal_baseline",
                    "manufacturing_withdrawal_baseline",
                    "manufacturing_return_baseline",
                    "urban_return2_baseline",
                    "rural_return_baseline",
                ]
            )
        ]
        # attach the monthly demand
        df_dmds = pd.concat([df_dmds, df_m])

    urban_withdrawal_df = df_dmds[df_dmds["variable"] == "urban_withdrawal2_baseline"]
    rual_withdrawal_df = df_dmds[df_dmds["variable"] == "rural_withdrawal_baseline"]
    industrial_withdrawals_df = df_dmds[
        df_dmds["variable"] == "manufacturing_withdrawal_baseline"
    ]
    industrial_return_df = df_dmds[
        df_dmds["variable"] == "manufacturing_return_baseline"
    ]
    urban_return_df = df_dmds[df_dmds["variable"] == "urban_return2_baseline"]
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
        save_path = private_data_path("water", "demands", "harmonized", context.regions)
        # save all the rates for reporting purposes
        all_rates.to_csv(save_path / "all_rates_SSP2.csv", index=False)

    # urban water demand and return. 1e-3 from mcm to km3
    urban_mw = urban_withdrawal_df.reset_index(drop=True)
    urban_mw = urban_mw.merge(
        urban_connection_rate_df.drop(columns=["variable", "time"]).rename(
            columns={"value": "rate"}
        )
    )
    urban_mw["value"] = (1e-3 * urban_mw["value"]) * urban_mw["rate"]

    dmd_df = make_df(
        "demand",
        node="B" + urban_mw["node"],
        commodity="urban_mw",
        level="final",
        year=urban_mw["year"],
        time=urban_mw["time"],
        value=urban_mw["value"],
        unit="km3/year",
    )
    urban_dis = urban_withdrawal_df.reset_index(drop=True)
    urban_dis = urban_dis.merge(
        urban_connection_rate_df.drop(columns=["variable", "time"]).rename(
            columns={"value": "rate"}
        )
    )
    urban_dis["value"] = (1e-3 * urban_dis["value"]) * (1 - urban_dis["rate"])

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + urban_dis["node"],
            commodity="urban_disconnected",
            level="final",
            year=urban_dis["year"],
            time=urban_dis["time"],
            value=urban_dis["value"],
            unit="km3/year",
        )
    )
    # rural water demand and return
    rural_mw = rual_withdrawal_df.reset_index(drop=True)
    rural_mw = rural_mw.merge(
        rural_connection_rate_df.drop(columns=["variable", "time"]).rename(
            columns={"value": "rate"}
        )
    )
    rural_mw["value"] = (1e-3 * rural_mw["value"]) * rural_mw["rate"]

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + rural_mw["node"],
            commodity="rural_mw",
            level="final",
            year=rural_mw["year"],
            time=rural_mw["time"],
            value=rural_mw["value"],
            unit="km3/year",
        )
    )

    rural_dis = rual_withdrawal_df.reset_index(drop=True)
    rural_dis = rural_dis.merge(
        rural_connection_rate_df.drop(columns=["variable", "time"]).rename(
            columns={"value": "rate"}
        )
    )
    rural_dis["value"] = (1e-3 * rural_dis["value"]) * (1 - rural_dis["rate"])

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + rural_dis["node"],
            commodity="rural_disconnected",
            level="final",
            year=rural_dis["year"],
            time=rural_dis["time"],
            value=rural_dis["value"],
            unit="km3/year",
        )
    )

    # manufactury/ industry water demand and return
    manuf_mw = industrial_withdrawals_df.reset_index(drop=True)
    manuf_mw["value"] = 1e-3 * manuf_mw["value"]

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + manuf_mw["node"],
            commodity="industry_mw",
            level="final",
            year=manuf_mw["year"],
            time=manuf_mw["time"],
            value=manuf_mw["value"],
            unit="km3/year",
        )
    )

    manuf_uncollected_wst = industrial_return_df.reset_index(drop=True)
    manuf_uncollected_wst["value"] = 1e-3 * manuf_uncollected_wst["value"]

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + manuf_uncollected_wst["node"],
            commodity="industry_uncollected_wst",
            level="final",
            year=manuf_uncollected_wst["year"],
            time=manuf_uncollected_wst["time"],
            value=-manuf_uncollected_wst["value"],
            unit="km3/year",
        )
    )

    urban_collected_wst = urban_return_df.reset_index(drop=True)
    urban_collected_wst = urban_collected_wst.merge(
        urban_treatment_rate_df.drop(columns=["variable", "time"]).rename(
            columns={"value": "rate"}
        )
    )
    urban_collected_wst["value"] = (
        1e-3 * urban_collected_wst["value"]
    ) * urban_collected_wst["rate"]

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + urban_collected_wst["node"],
            commodity="urban_collected_wst",
            level="final",
            year=urban_collected_wst["year"],
            time=urban_collected_wst["time"],
            value=-urban_collected_wst["value"],
            unit="km3/year",
        )
    )

    rural_collected_wst = rural_return_df.reset_index(drop=True)
    rural_collected_wst = rural_collected_wst.merge(
        rural_treatment_rate_df.drop(columns=["variable", "time"]).rename(
            columns={"value": "rate"}
        )
    )
    rural_collected_wst["value"] = (
        1e-3 * rural_collected_wst["value"]
    ) * rural_collected_wst["rate"]

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + rural_collected_wst["node"],
            commodity="rural_collected_wst",
            level="final",
            year=rural_collected_wst["year"],
            time=rural_collected_wst["time"],
            value=-rural_collected_wst["value"],
            unit="km3/year",
        )
    )
    urban_uncollected_wst = urban_return_df.reset_index(drop=True)
    urban_uncollected_wst = urban_uncollected_wst.merge(
        urban_treatment_rate_df.drop(columns=["variable", "time"]).rename(
            columns={"value": "rate"}
        )
    )
    urban_uncollected_wst["value"] = (1e-3 * urban_uncollected_wst["value"]) * (
        1 - urban_uncollected_wst["rate"]
    )

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + urban_uncollected_wst["node"],
            commodity="urban_uncollected_wst",
            level="final",
            year=urban_uncollected_wst["year"],
            time=urban_uncollected_wst["time"],
            value=-urban_uncollected_wst["value"],
            unit="km3/year",
        )
    )

    rural_uncollected_wst = rural_return_df.reset_index(drop=True)
    rural_uncollected_wst = rural_uncollected_wst.merge(
        rural_treatment_rate_df.drop(columns=["variable", "time"]).rename(
            columns={"value": "rate"}
        )
    )
    rural_uncollected_wst["value"] = (1e-3 * rural_uncollected_wst["value"]) * (
        1 - rural_uncollected_wst["rate"]
    )

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + rural_uncollected_wst["node"],
            commodity="rural_uncollected_wst",
            level="final",
            year=rural_uncollected_wst["year"],
            time=rural_uncollected_wst["time"],
            value=-rural_uncollected_wst["value"],
            unit="km3/year",
        )
    )
    # Add 2010 & 2015 values as historical activities to corresponding technologies
    h_act = dmd_df[dmd_df["year"].isin([2010, 2015])]

    dmd_df = dmd_df[dmd_df["year"].isin(info.Y)]
    results["demand"] = dmd_df

    # create a list of our conditions
    conditions = [
        (h_act["commodity"] == "urban_mw"),
        (h_act["commodity"] == "industry_mw"),
        (h_act["commodity"] == "rural_mw"),
        (h_act["commodity"] == "urban_disconnected"),
        (h_act["commodity"] == "rural_disconnected"),
        (h_act["commodity"] == "urban_collected_wst"),
        (h_act["commodity"] == "rural_collected_wst"),
        (h_act["commodity"] == "urban_uncollected_wst"),
        (h_act["commodity"] == "industry_uncollected_wst"),
        (h_act["commodity"] == "rural_uncollected_wst"),
    ]

    # create a list of the values we want to assign for each condition
    values = [
        "urban_t_d",
        "industry_unconnected",
        "rural_t_d",
        "urban_unconnected",
        "rural_unconnected",
        "urban_sewerage",
        "rural_sewerage",
        "urban_untreated",
        "industry_untreated",
        "rural_untreated",
    ]
    # create a new column and use np.select to assign
    # values to it using our lists as arguments
    h_act["commodity"] = np.select(conditions, values)
    h_act["value"] = h_act["value"].abs()

    hist_act = make_df(
        "historical_activity",
        node_loc=h_act["node"],
        technology=h_act["commodity"],
        year_act=h_act["year"],
        mode="M1",
        time=h_act["time"],
        value=h_act["value"],
        unit="km3/year",
    )
    results["historical_activity"] = hist_act

    h_cap = h_act[h_act["year"] >= 2015]
    h_cap = (
        h_cap.groupby(["node", "commodity", "level", "year", "unit"])["value"]
        .sum()
        .reset_index()
    )

    hist_cap = make_df(
        "historical_new_capacity",
        node_loc=h_cap["node"],
        technology=h_cap["commodity"],
        year_vtg=h_cap["year"],
        value=h_cap["value"] / 5,
        unit="km3/year",
    )

    results["historical_new_capacity"] = hist_cap

    # share constraint lower bound on urban_Water recycling
    df_share_wat = make_df(
        "share_commodity_lo",
        shares="share_wat_recycle",
        node_share="B" + df_recycling["node"],
        year_act=df_recycling["year"],
        value=df_recycling["value"],
        unit="-",
    ).pipe(
        broadcast,
        time=sub_time,
    )

    df_share_wat = df_share_wat[df_share_wat["year_act"].isin(info.Y)]
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


def read_water_availability(context):
    """
    Reads water availability data and bias correct
    it for the historical years and no climate
    scenario assumptions.

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
    # reading sample for assiging basins
    PATH = private_data_path(
        "water", "delineation", f"basins_by_region_simpl_{context.regions}.csv"
    )
    df_x = pd.read_csv(PATH)

    if "year" in context.time:
        # path for reading basin delineation file
        PATH = private_data_path(
            "water", "delineation", f"basins_by_region_simpl_{context.regions}.csv"
        )
        df_x = pd.read_csv(PATH)
        # Adding freshwater supply constraints
        # Reading data, the data is spatially and temprally aggregated from GHMs
        path1 = private_data_path(
            "water",
            "availability",
            f"qtot_5y_{context.RCP}_{context.REL}_{context.regions}.csv",
        )
        # Read rcp 2.6 data
        df_sw = pd.read_csv(path1)
        df_sw.drop(["Unnamed: 0"], axis=1, inplace=True)

        df_sw.index = df_x["BCU_name"]
        df_sw = df_sw.stack().reset_index()
        df_sw.columns = ["Region", "years", "value"]
        df_sw.fillna(0, inplace=True)
        df_sw.reset_index(drop=True, inplace=True)
        df_sw["year"] = pd.DatetimeIndex(df_sw["years"]).year
        df_sw["time"] = "year"
        df_sw2210 = df_sw[df_sw["year"] == 2100]
        df_sw2210["year"] = 2110
        df_sw = pd.concat([df_sw, df_sw2210])
        df_sw = df_sw[df_sw["year"].isin(info.Y)]

        # Adding groundwater supply constraints
        # Reading data, the data is spatially and temprally aggregated from GHMs
        path1 = private_data_path(
            "water",
            "availability",
            f"qr_5y_{context.RCP}_{context.REL}_{context.regions}.csv",
        )

        # Read groundwater data
        df_gw = pd.read_csv(path1)
        df_gw.drop(["Unnamed: 0"], axis=1, inplace=True)
        df_gw.index = df_x["BCU_name"]
        df_gw = df_gw.stack().reset_index()
        df_gw.columns = ["Region", "years", "value"]
        df_gw.fillna(0, inplace=True)
        df_gw.reset_index(drop=True, inplace=True)
        df_gw["year"] = pd.DatetimeIndex(df_gw["years"]).year
        df_gw["time"] = "year"
        df_gw2210 = df_gw[df_gw["year"] == 2100]
        df_gw2210["year"] = 2110
        df_gw = pd.concat([df_gw, df_gw2210])
        df_gw = df_gw[df_gw["year"].isin(info.Y)]

    else:
        # Adding freshwater supply constraints
        # Reading data, the data is spatially and temprally aggregated from GHMs
        path1 = private_data_path(
            "water",
            "availability",
            f"qtot_5y_m_{context.RCP}_{context.REL}_{context.regions}.csv",
        )
        df_sw = pd.read_csv(path1)
        df_sw.drop(["Unnamed: 0"], axis=1, inplace=True)

        df_sw.index = df_x["BCU_name"]
        df_sw = df_sw.stack().reset_index()
        df_sw.columns = ["Region", "years", "value"]
        df_sw.sort_values(["Region", "years", "value"], inplace=True)
        df_sw.fillna(0, inplace=True)
        df_sw.reset_index(drop=True, inplace=True)
        df_sw["year"] = pd.DatetimeIndex(df_sw["years"]).year
        df_sw["time"] = pd.DatetimeIndex(df_sw["years"]).month
        df_sw2210 = df_sw[df_sw["year"] == 2100]
        df_sw2210["year"] = 2110
        df_sw = pd.concat([df_sw, df_sw2210])
        df_sw = df_sw[df_sw["year"].isin(info.Y)]

        # Reading data, the data is spatially and temporally aggregated from GHMs
        path1 = private_data_path(
            "water",
            "availability",
            f"qr_5y_m_{context.RCP}_{context.REL}_{context.regions}.csv",
        )
        df_gw = pd.read_csv(path1)
        df_gw.drop(["Unnamed: 0"], axis=1, inplace=True)

        df_gw.index = df_x["BCU_name"]
        df_gw = df_gw.stack().reset_index()
        df_gw.columns = ["Region", "years", "value"]
        df_gw.sort_values(["Region", "years", "value"], inplace=True)
        df_gw.fillna(0, inplace=True)
        df_gw.reset_index(drop=True, inplace=True)
        df_gw["year"] = pd.DatetimeIndex(df_gw["years"]).year
        df_gw["time"] = pd.DatetimeIndex(df_gw["years"]).month
        df_gw2210 = df_gw[df_gw["year"] == 2100]
        df_gw2210["year"] = 2110
        df_gw = pd.concat([df_gw, df_sw2210])
        df_gw = df_gw[df_gw["year"].isin(info.Y)]

    return df_sw, df_gw


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

    # define an empty dictionary
    results = {}
    # Adding freshwater supply constraints
    # Reading data, the data is spatially and temprally aggregated from GHMs

    df_sw, df_gw = read_water_availability(context)

    dmd_df = make_df(
        "demand",
        node="B" + df_sw["Region"].astype(str),
        commodity="surfacewater_basin",
        level="water_avail_basin",
        year=df_sw["year"],
        time=df_sw["time"],
        value=-df_sw["value"],
        unit="km3/year",
    )

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node="B" + df_gw["Region"].astype(str),
            commodity="groundwater_basin",
            level="water_avail_basin",
            year=df_gw["year"],
            time=df_gw["time"],
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
        year_act=df_gw["year"],
        time=df_gw["time"],
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
