"""Prepare data for adding demands"""

import os

import pandas as pd
import xarray as xr
from message_ix import make_df
from message_ix_models.util import private_data_path


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

    # defines path to read in demand data
    path = private_data_path("water", "water_demands", "harmonized", "R11", ".")
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
    urban_return_df.reset_index(drop=True, inplace = True)
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

    # urban_desal_fraction_df = df_dmds[df_dmds['variable'] == 'urban_desal_fraction_baseline']
    # rural_desal_fraction_df = df_dmds[df_dmds['variable'] == 'rural_desal_fraction_baseline']
    # urban_reuse_fraction_df = df_dmds[df_dmds['variable'] == 'urban_reuse_fraction_baseline']

    urban_mw = urban_withdrawal_df.reset_index(drop=True)
    urban_mw["value"] = (1e-3 * urban_mw["value"]) * urban_connection_rate_df["value"]

    dmd_df = make_df(
        "demand",
        node='B'+urban_mw["node"],
        commodity="urban_mw",
        level="final",
        year=urban_mw["year"],
        time="year",
        value=urban_mw["value"],
        unit="-",
    )
    urban_dis = urban_withdrawal_df.reset_index(drop=True)
    urban_dis["value"] = (1e-3 * urban_dis["value"]) * (
        1 - urban_connection_rate_df["value"]
    )

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node='B'+urban_dis["node"],
            commodity="urban_disconnected",
            level="final",
            year=urban_dis["year"],
            time="year",
            value=urban_dis["value"],
            unit="-",
        )
    )
    rural_mw = rual_withdrawal_df.reset_index(drop=True)
    rural_mw["value"] = (1e-3 * rural_mw["value"]) * rural_connection_rate_df["value"]

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node='B'+rural_mw["node"],
            commodity="rural_mw",
            level="final",
            year=rural_mw["year"],
            time="year",
            value=rural_mw["value"],
            unit="-",
        )
    )

    rural_dis = rual_withdrawal_df.reset_index(drop=True)
    rural_dis["value"] = (1e-3 * rural_dis["value"]) * (
        1 - rural_connection_rate_df["value"]
    )

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node='B'+rural_dis["node"],
            commodity="rural_disconnected",
            level="final",
            year=rural_dis["year"],
            time="year",
            value=rural_dis["value"],
            unit="-",
        )
    )
    urban_collected_wst = urban_return_df.reset_index(drop=True)
    urban_collected_wst["value"] = (
        1e-3 * urban_return_df["value"]
    ) * urban_treatment_rate_df["value"]

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node='B'+urban_collected_wst["node"],
            commodity="urban_collected_wst",
            level="final",
            year=urban_collected_wst["year"],
            time="year",
            value=-urban_collected_wst["value"],
            unit="-",
        )
    )

    rural_collected_wst = rural_return_df.reset_index(drop=True)
    rural_collected_wst["value"] = (
        1e-3 * rural_return_df["value"]
    ) * rural_treatment_rate_df["value"]

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node='B'+rural_collected_wst["node"],
            commodity="rural_collected_wst",
            level="final",
            year=rural_collected_wst["year"],
            time="year",
            value=-rural_collected_wst["value"],
            unit="-",
        )
    )
    urban_uncollected_wst = urban_return_df.reset_index(drop=True)
    urban_uncollected_wst["value"] = (1e-3 * urban_return_df["value"]) * (
        1 - urban_treatment_rate_df["value"]
    )

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node='B'+urban_uncollected_wst["node"],
            commodity="urban_uncollected_wst",
            level="final",
            year=urban_uncollected_wst["year"],
            time="year",
            value=-urban_uncollected_wst["value"],
            unit="-",
        )
    )

    rural_uncollected_wst = rural_return_df.reset_index(drop=True)
    rural_uncollected_wst["value"] = (1e-3 * rural_return_df["value"]) * (
        1 - rural_treatment_rate_df["value"]
    )

    dmd_df = dmd_df.append(
        make_df(
            "demand",
            node='B'+rural_uncollected_wst["node"],
            commodity="rural_collected_wst",
            level="final",
            year=rural_uncollected_wst["year"],
            time="year",
            value=-rural_uncollected_wst["value"],
            unit="-",
        )
    )

    # TODO Add updated irrigation withdrawal data from GLOBIOM
    # TODO decide whether to include additional GLOBIOM nodes

    # freshwater_supply = irrigation_withdrawal_df.reset_index(drop=True)
    # freshwater_supply["value"] = 1e-3 * freshwater_water_supply["value"]
    #
    # dmd_df = dmd_df.append(
    #     make_df(
    #         "demand",
    #         node=freshwater_supply["node"],
    #         commodity="freshwater_supply",
    #         level="water_supply",
    #         year=freshwater_supply["year"],
    #         time="year",
    #         value=freshwater_supply["value"],
    #         unit="-",
    #     )
    # )


    results["demand"] = dmd_df

    return results

def add_water_availbility(context):
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
    # The water availability here refers to runoff
    # TODO identify whether discharge or runoff needs to be used
    # Reading data, the data is spatially and temprally aggregated from GHMs
    path1 = private_data_path("water", "water_availability", "dis_rcp26_5y_meansd.csv")
    df_wat_ava = pd.read_csv(path1)
    df_wat_ava = df_wat_ava.drop(
        columns=[
            "Unnamed: 0",
            "BASIN_ID",
            "2065",
            "2075",
            "2085",
            "2095",
        ],
        axis=1,
    )
    df_wat_ava["2110"] = 1.1 * df_wat_ava["2100"]
    df_wat_ava = df_wat_ava.set_index(["BCU_name", "unit"])
    df_wat_ava = df_wat_ava.stack().reset_index(level=0).reset_index()
    df_wat_ava.columns = ["unit", "year", "node", "value"]
    df_wat_ava.sort_values(["unit", "year", "node", "value"], inplace=True)
    df_wat_ava.fillna(0, inplace=True)

    dmd_df =  make_df(
            "demand",
            node='B' + df_wat_ava["node"],
            commodity="freshwater_basin",
            level="water_avail_basin",
            year=df_wat_ava["year"],
            time="year",
            value=-df_wat_ava["value"],
            unit="-",
        )


    results["demand"] = dmd_df


    return results