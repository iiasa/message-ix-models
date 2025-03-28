"""Prepare data for adding demands"""

import os
from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, Union

import numpy as np
import pandas as pd
import xarray as xr
from message_ix import make_df as make_df


from message_ix_models.util import broadcast, minimum_version, package_data_path

from message_ix_models.model.water.data.demand_rules import *

if TYPE_CHECKING:
    from message_ix_models import Context



def get_basin_sizes(
    basin: pd.DataFrame, node: str
) -> Sequence[Union[pd.Series, Literal[0]]]:
    """Returns the sizes of developing and developed basins for a given node"""
    temp = basin[basin["BCU_name"] == node]
    sizes = temp.pivot_table(index=["STATUS"], aggfunc="size")
    sizes_dev = sizes["DEV"] if "DEV" in sizes.index else 0
    sizes_ind = sizes["IND"] if "IND" in sizes.index else 0
    return_tuple: tuple[Union[pd.Series, Literal[0]], Union[pd.Series, Literal[0]]] = (
        sizes_dev,
        sizes_ind,
    )  # type: ignore # Somehow, mypy is unable to recognize the proper type without forcing it
    return return_tuple


def set_target_rate(df: pd.DataFrame, node: str, year: int, target: float) -> None:
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


def target_rate(df: pd.DataFrame, basin: pd.DataFrame, val: float) -> pd.DataFrame:
    """
    Sets target rates for all nodes in a given basin
    """
    for node in df.node.unique():
        dev_size, ind_size = get_basin_sizes(basin, node)

        is_developed = dev_size >= ind_size
        match is_developed:
            case True:
                set_target_rate(df, node, 2030, val)
            case False:
                for i in df.index:
                    if df.at[i, "node"] == node and df.at[i, "year"] == 2030:
                        value_2030 = df.at[i, "value"]
                        break
                set_target_rate(df, node, 2035, (value_2030 + val) / 2)
                set_target_rate(df, node, 2040, val)


def target_rate_trt(df: pd.DataFrame, basin: pd.DataFrame) -> pd.DataFrame:
    """
    Sets target treatment rates for SDG scenario. The target value for
    developed and developing regions is making sure that the amount of untreated
    wastewater is halved beyond 2030 & 2040 respectively.

    Returns
    -------
    data : pandas.DataFrame
    """
    updates = []  # Will hold tuples of (index, new_value)
    
    for node in df.node.unique():
        basin_node = basin[basin["BCU_name"] == node]
        sizes = basin_node.pivot_table(index=["STATUS"], aggfunc="size")
        
        # Use pattern matching to decide on the threshold year.
        is_dev = sizes["DEV"] >= sizes["IND"]
        match len(sizes):
            case n if n > 1:
                match is_dev:
                    case True:
                        threshold = 2040
                    case False:
                        threshold = 2030
            case 1:
                match sizes.index[0]:
                    case "DEV":
                        threshold = 2040
                    case _:
                        threshold = 2030
        
        # Filter rows for this node and the chosen threshold year.
        node_rows = df[(df["node"] == node) & (df["year"] >= threshold)]
        for j in node_rows.index:
            old_val = df.at[j, "value"]
            new_val = old_val + (1 - old_val) / 2
            updates.append((j, np.float64(new_val)))
    
    # Create a temporary DataFrame from the updates.
    update_df = pd.DataFrame(updates, columns=["Index", "Value"])
    
    # Update the main DataFrame with new values.
    for _, row in update_df.iterrows():
        df.at[row["Index"], "Value"] = row["Value"]
    
    # Combine new values with original ones.
    real_value = df["Value"].combine_first(df["value"])
    df.drop(["value", "Value"], axis=1, inplace=True)
    df["value"] = real_value
    
    return df

def _preprocess_availability_data(df: pd.DataFrame, monthly: bool = False, df_x: pd.DataFrame = None, info = None) -> pd.DataFrame:
    """
    Preprocesses availability data

    Parameters
    ----------
    df : pd.DataFrame
    monthly : bool
    df_x : pd.DataFrame
    info : Context

    Returns
    -------
    df : pd.DataFrame
    """
    df.drop(["Unnamed: 0"], axis=1, inplace=True)
    df.index = df_x["BCU_name"].index
    df = df.stack().reset_index()
    df.columns = pd.Index(["Region", "years", "value"])
    df.fillna(0, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df["year"] = pd.DatetimeIndex(df["years"]).year
    df["time"] = "year" if not monthly else pd.DatetimeIndex(df["years"]).month
    df["Region"] = df["Region"].map(df_x["BCU_name"])
    df2210 = df[df["year"] == 2100].copy()
    df2210["year"] = 2110
    df = pd.concat([df, df2210])
    df = df[df["year"].isin(info.Y)]
    return df

def read_water_availability(context: "Context") -> Sequence[pd.DataFrame]:
    """
    Reads water availability data and bias correct
    it for the historical years and no climate
    scenario assumptions.

    Parameters
    ----------
    context : .Context

    Returns
    -------
    data : (pd.DataFrame, pd.DataFrame)
    """

    # Reference to the water configuration
    info = context["water build info"]
    # reading sample for assiging basins
    PATH = package_data_path(
        "water", "delineation", f"basins_by_region_simpl_{context.regions}.csv"
    )
    df_x = pd.read_csv(PATH)
    monthly = False
    match context.time:
        case "year":
            # path for reading basin delineation file
            path1 = package_data_path(
                "water",
                "availability",
                f"qtot_5y_{context.RCP}_{context.REL}_{context.regions}.csv",
            )
            # Reading data, the data is spatially and temprally aggregated from GHMs
            path2 = package_data_path(
                "water",
                "availability",
                f"qr_5y_{context.RCP}_{context.REL}_{context.regions}.csv",
            )
        case "month":
            monthly = True
            path1 = package_data_path(
                "water",
                "availability",
                f"qtot_5y_m_{context.RCP}_{context.REL}_{context.regions}.csv",
            )

            # Reading data, the data is spatially and temporally aggregated from GHMs
            path2 = package_data_path(
                "water",
                "availability",
                f"qr_5y_m_{context.RCP}_{context.REL}_{context.regions}.csv",
            )
        case _:
            raise ValueError(f"Invalid time period: {context.time}")

    df_sw = pd.read_csv(path1)
    df_sw = _preprocess_availability_data(df_sw, monthly=monthly, df_x = df_x, info = info)

    df_gw = pd.read_csv(path2)
    df_gw = _preprocess_availability_data(df_gw, monthly=monthly, df_x = df_x, info = info)

    return df_sw, df_gw


def add_water_availability(context: "Context") -> dict[str, pd.DataFrame]:
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
    water_availability = []
    for rule in WATER_AVAILABILITY:
        match rule["df_source"]:
            case "df_sw":
                df_source = df_sw
            case "df_gw":
                df_source = df_gw
            case _:
                raise ValueError(f"Invalid df_source: {rule['df_source']}")

        dmd_df = make_df(
            rule["type"],
            node="B"+ eval_field(rule["node"], df_source),
            commodity=rule["commodity"],
            level=rule["level"],
            year=eval_field(rule["year"], df_source),
            time=eval_field(rule["time"], df_source),
            value= -eval_field(rule["value"], df_source),
            unit=rule["unit"],
        )
        water_availability.append(dmd_df)

    dmd_df = pd.concat(water_availability)

    dmd_df["value"] = dmd_df["value"].apply(lambda x: x if x <= 0 else 0)

    results["demand"] = dmd_df


    share_constraints_gw = []
    for rule in SHARE_CONSTRAINTS_GW:
        # share constraint lower bound on groundwater
        if rule["df_source1"] == "df_gw" and rule["df_source2"] == "df_sw":
            df_source1 = df_gw
            df_source2 = df_sw
        else:
            raise ValueError(f"Invalid df_source: {rule['df_source1']} or {rule['df_source2']}")
        
        df_share = make_df(
            rule["type"],
            shares=rule["shares"],
            node_share="B" + eval_field(rule["node"], df_source1),
            year_act=eval_field(rule["year"], df_source1),
            time=eval_field(rule["time"], df_source1),
            value=eval_field(rule["value"], df_source1, df_source2),             
            unit=rule["unit"],
    )

    df_share["value"] = df_share["value"].fillna(0)

    results["share_commodity_lo"] = df_share

    return results


def add_irrigation_demand(context: "Context") -> dict[str, pd.DataFrame]:
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

    

def _preprocess_demand_data_stage1(context: "Context") -> pd.DataFrame:
    # read and clean raw demand data to standardized format
    region = f"{context.regions}"
    # get data path using package_data_path
    path = package_data_path("water", "demands", "harmonized", region, ".")
    # get the csv files matching format
    list_of_csvs = list(path.glob("ssp2_regional_*.csv"))
    fns = [os.path.splitext(os.path.basename(x))[0] for x in list_of_csvs]
    fns = " ".join(fns).replace("ssp2_regional_", "").split()
    d: dict[str, pd.DataFrame] = {}
    for i in range(len(fns)):
        d[fns[i]] = pd.read_csv(list_of_csvs[i])
    dfs = {}
    for key, df in d.items():
        df.rename(columns={"Unnamed: 0": "year"}, inplace=True)
        df.set_index("year", inplace=True)
        dfs[key] = df
    # combine dataframes using xarray and interpolate selected years
    df_x = xr.Dataset(dfs).to_array()
    df_x_interp = df_x.interp(year=[2015, 2025, 2035, 2045, 2055])
    df_x_c = df_x.combine_first(df_x_interp)
    df_f = df_x_c.to_dataframe("").unstack()
    # stack to obtain standardized dataframe with columns year, node, variable, value
    df_dmds = df_f.stack(future_stack=True).reset_index(level=0).reset_index()
    df_dmds.columns = ["year", "node", "variable", "value"]
    df_dmds.sort_values(["year", "node", "variable", "value"], inplace=True)
    df_dmds["time"] = "year"
    # if sub-annual timesteps are used, merge with monthly data
    if "year" not in context.time:
        PATH = package_data_path("water", "demands", "harmonized", region, "ssp2_m_water_demands.csv")
        df_m: pd.DataFrame = pd.read_csv(PATH)
        df_m.value *= 30  # conversion from mcm/day to mcm/month
        df_m.loc[df_m["sector"] == "industry", "sector"] = "manufacturing"
        df_m["variable"] = df_m["sector"] + "_" + df_m["type"] + "_baseline"
        df_m.loc[df_m["variable"] == "urban_withdrawal_baseline", "variable"] = "urbann_withdrawal2_baseline"
        df_m.loc[df_m["variable"] == "urban_return_baseline", "variable"] = "urbann_return2_baseline"
        df_m = df_m[["year", "pid", "variable", "value", "month"]]
        df_m.columns = pd.Index(["year", "node", "variable", "value", "time"])
        # remove yearly parts before merging with monthly data
        df_dmds = df_dmds[~df_dmds["variable"].isin([
            "urban_withdrawal2_baseline",
            "rural_withdrawal_baseline",
            "manufacturing_withdrawal_baseline",
            "manufacturing_return_baseline",
            "urban_return2_baseline",
            "rural_return_baseline",
        ])]
        df_dmds = pd.concat([df_dmds, df_m])
    return df_dmds

def _preprocess_demand_data_stage2(df_dmds: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Pre-process the DataFrame to prepare it for the rule evaluation.
    """
    variables_operations = {
        "urban_withdrawal2_baseline": {"df_name": "urban_withdrawal_df", "reset_index": False},
        "rural_withdrawal_baseline": {"df_name": "rural_withdrawal_df", "reset_index": False},
        "manufacturing_withdrawal_baseline": {"df_name": "industrial_withdrawals_df", "reset_index": False},
        "manufacturing_return_baseline": {"df_name": "industrial_return_df", "reset_index": False},
        "urban_return2_baseline": {"df_name": "urban_return_df", "reset_index": True},
        "rural_return_baseline": {"df_name": "rural_return_df", "reset_index": True},
        "urban_connection_rate_baseline": {"df_name": "urban_connection_rate_df", "reset_index": True},
        "rural_connection_rate_baseline": {"df_name": "rural_connection_rate_df", "reset_index": True},
        "urban_treatment_rate_baseline": {"df_name": "urban_treatment_rate_df", "reset_index": True},
        "rural_treatment_rate_baseline": {"df_name": "rural_treatment_rate_df", "reset_index": True},
        "urban_recycling_rate_baseline": {"df_name": "df_recycling", "reset_index": True},
    }
    Results = {}
    for variable, attrs in variables_operations.items():
        df_name, reset_index = attrs["df_name"], attrs["reset_index"]
        df_name = df_dmds[df_dmds["variable"] == variable]
        if reset_index:
            df_name.reset_index(drop=True, inplace=True)
        Results[attrs["df_name"]] = df_name

    return Results



@minimum_version("message_ix 3.7")
def add_sectoral_demands(context: "Context") -> dict[str, pd.DataFrame]:
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

    # add water sectoral demands
    results = {}
    info = context["water build info"]
    sub_time = context.time

    # get standardized input data
    df_dmds = _preprocess_demand_data_stage1(context)
    
    # Process data and unpack results
    processed_data = _preprocess_demand_data_stage2(df_dmds)
    urban_withdrawal_df = processed_data["urban_withdrawal_df"]
    rural_withdrawal_df = processed_data["rural_withdrawal_df"]
    industrial_withdrawals_df = processed_data["industrial_withdrawals_df"]
    industrial_return_df = processed_data["industrial_return_df"]
    urban_return_df = processed_data["urban_return_df"]
    rural_return_df = processed_data["rural_return_df"]
    urban_connection_rate_df = processed_data["urban_connection_rate_df"]
    rural_connection_rate_df = processed_data["rural_connection_rate_df"]
    urban_treatment_rate_df = processed_data["urban_treatment_rate_df"]
    rural_treatment_rate_df = processed_data["rural_treatment_rate_df"]
    df_recycling = processed_data["df_recycling"]

    if context.SDG != "baseline":
        if context.SDG == "SDG":
            FILE2 = f"basins_country_{context.regions}.csv"
            PATH = package_data_path("water", "delineation", FILE2)
            df_basin = pd.read_csv(PATH)
            rural_treatment_rate_df = rural_treatment_rate_df_sdg = target_rate(
                rural_treatment_rate_df, df_basin, 0.8
            )
            urban_treatment_rate_df = urban_treatment_rate_df_sdg = target_rate(
                urban_treatment_rate_df, df_basin, 0.95
            )
            urban_connection_rate_df = urban_connection_rate_df_sdg = target_rate(
                urban_connection_rate_df, df_basin, 0.99
            )
            rural_connection_rate_df = rural_connection_rate_df_sdg = target_rate(
                rural_connection_rate_df, df_basin, 0.8
            )
            df_recycling = df_recycling_sdg = target_rate_trt(df_recycling, df_basin)
        else:
            pol_scen = context.SDG
            check_dm = df_dmds[df_dmds["variable"] == "urban_connection_rate_" + pol_scen]
            if check_dm.empty:
                raise ValueError(f"policy data is missing for the {pol_scen} scenario.")
            urban_connection_rate_df = urban_connection_rate_df_sdg = df_dmds[
                df_dmds["variable"] == "urban_connection_rate_" + pol_scen
            ]
            urban_connection_rate_df.reset_index(drop=True, inplace=True)
            rural_connection_rate_df = rural_connection_rate_df_sdg = df_dmds[
                df_dmds["variable"] == "rural_connection_rate_" + pol_scen
            ]
            rural_connection_rate_df.reset_index(drop=True, inplace=True)
            urban_treatment_rate_df = urban_treatment_rate_df_sdg = df_dmds[
                df_dmds["variable"] == "urban_treatment_rate_" + pol_scen
            ]
            urban_treatment_rate_df.reset_index(drop=True, inplace=True)
            rural_treatment_rate_df = rural_treatment_rate_df_sdg = df_dmds[
                df_dmds["variable"] == "rural_treatment_rate_" + pol_scen
            ]
            rural_treatment_rate_df.reset_index(drop=True, inplace=True)
            df_recycling = df_recycling_sdg = df_dmds[
                df_dmds["variable"] == "urban_recycling_rate_" + pol_scen
            ]
            df_recycling.reset_index(drop=True, inplace=True)
        all_rates_sdg = pd.concat(
            [
                urban_connection_rate_df_sdg,
                rural_connection_rate_df_sdg,
                urban_treatment_rate_df_sdg,
                rural_treatment_rate_df_sdg,
                df_recycling_sdg,
            ]
        )
        all_rates_sdg["variable"] = [x.replace("baseline", pol_scen) for x in all_rates_sdg["variable"]]
        all_rates = pd.concat([pd.concat([
            urban_connection_rate_df,
            rural_connection_rate_df,
            urban_treatment_rate_df,
            rural_treatment_rate_df,
            df_recycling,
        ]), all_rates_sdg])
        save_path = package_data_path("water", "demands", "harmonized", context.regions)
        all_rates.to_csv(save_path / "all_rates_SSP2.csv", index=False)

    # urban water demand and return. 1e-3 from mcm to km3
    urban_dmds = []
    for r in URBAN_DEMAND:
        match r["commodity"]:
            case "urban_mw":
                urban_mw = urban_withdrawal_df.reset_index(drop=True)
                urban_mw = urban_mw.merge(
                    urban_connection_rate_df.drop(columns=["variable", "time"]).rename(columns={"value": "rate"})
                )
                urban_mw["value"] = (1e-3 * urban_mw["value"]) * urban_mw["rate"]

                urban_dmds.append(load_rules(r, urban_mw))
            case "urban_disconnected":
                urban_dis = urban_withdrawal_df.reset_index(drop=True)
                urban_dis = urban_dis.merge(
                    urban_connection_rate_df.drop(columns=["variable", "time"]).rename(columns={"value": "rate"})
                )
                urban_dis["value"] = (1e-3 * urban_dis["value"]) * (1 - urban_dis["rate"])

                urban_dmds.append(load_rules(r, urban_dis))
            case _:
                raise ValueError(f"Invalid commodity: {r['commodity']}")
    urban_dmds = pd.concat(urban_dmds)
    dmd_df = urban_dmds
    # rural water demand and return
    rural_dmds = []
    for r in RURAL_DEMAND:
        match r["commodity"]:
            case "rural_mw":
                rural_mw = rural_withdrawal_df.reset_index(drop=True)
                rural_mw = rural_mw.merge(
                    rural_connection_rate_df.drop(columns=["variable", "time"]).rename(columns={"value": "rate"})
                )
                rural_mw["value"] = (1e-3 * rural_mw["value"]) * rural_mw["rate"]

                rural_dmds.append(load_rules(r, rural_mw))
            case "rural_disconnected":
                rural_dis = rural_withdrawal_df.reset_index(drop=True)
                rural_dis = rural_dis.merge(
                    rural_connection_rate_df.drop(columns=["variable", "time"]).rename(columns={"value": "rate"})
                )
                rural_dis["value"] = (1e-3 * rural_dis["value"]) * (1 - rural_dis["rate"])

                rural_dmds.append(load_rules(r, rural_dis))
            case _:
                raise ValueError(f"Invalid commodity: {r['commodity']}")

    rural_dmds = pd.concat(rural_dmds)
    dmd_df = pd.concat([dmd_df, rural_dmds])

    # manufactury/ industry water demand and return
    industrial_dmds = []
    for r in INDUSTRIAL_DEMAND:
        match r["commodity"]:
            case "industry_mw":
                # manufactury/ industry water demand and return
                manuf_mw = industrial_withdrawals_df.reset_index(drop=True)
                manuf_mw["value"] = 1e-3 * manuf_mw["value"]
                industrial_dmds.append(load_rules(r, manuf_mw))
                
            case "industry_uncollected_wst":
                manuf_uncollected_wst = industrial_return_df.reset_index(drop=True)
                manuf_uncollected_wst["value"] = 1e-3 * manuf_uncollected_wst["value"]
                industrial_dmds.append(load_rules(r, manuf_uncollected_wst))
            case _:
                raise ValueError(f"Invalid commodity: {r['commodity']}")
    industrial_dmds = pd.concat(industrial_dmds)
    dmd_df = pd.concat([dmd_df, industrial_dmds])

    # urban collected wastewater
    urban_collected_wst_df = []
    for r in URBAN_COLLECTED_WST:
        urban_collected_wst = urban_return_df.reset_index(drop=True)
        urban_collected_wst = urban_collected_wst.merge(
            urban_treatment_rate_df.drop(columns=["variable", "time"]).rename(columns={"value": "rate"})
        )
        urban_collected_wst["value"] = (
        1e-3 * urban_collected_wst["value"]
        ) * urban_collected_wst["rate"]
        urban_collected_wst_df.append(load_rules(r, urban_collected_wst))

    urban_collected_wst_df = pd.concat(urban_collected_wst_df)    
    dmd_df = pd.concat([dmd_df, urban_collected_wst_df])

    # rural collected wastewater
    rural_collected_wst_df = []
    for r in RURAL_COLLECTED_WST:
        rural_collected_wst = rural_return_df.reset_index(drop=True)
        rural_collected_wst = rural_collected_wst.merge(
            rural_treatment_rate_df.drop(columns=["variable", "time"]).rename(columns={"value": "rate"})
        )
        rural_collected_wst["value"] = (
            1e-3 * rural_collected_wst["value"]
        ) * rural_collected_wst["rate"]
        rural_collected_wst_df.append(load_rules(r, rural_collected_wst))

    rural_collected_wst_df = pd.concat(rural_collected_wst_df)
    dmd_df = pd.concat([dmd_df, rural_collected_wst_df])

    # urban uncollected wastewater
    urban_uncollected_wst_df = []
    for r in URBAN_UNCOLLECTED_WST:
        urban_uncollected_wst = urban_return_df.reset_index(drop=True)
        urban_uncollected_wst = urban_uncollected_wst.merge(
            urban_treatment_rate_df.drop(columns=["variable", "time"]).rename(columns={"value": "rate"})
        )
        urban_uncollected_wst["value"] = (1e-3 * urban_uncollected_wst["value"]) * (
            1 - urban_uncollected_wst["rate"]
        )
        urban_uncollected_wst_df.append(load_rules(r, urban_uncollected_wst))

    urban_uncollected_wst_df = pd.concat(urban_uncollected_wst_df)
    dmd_df = pd.concat([dmd_df, urban_uncollected_wst_df])

    # rural uncollected wastewater
    rural_uncollected_wst_df = []
    for r in RURAL_UNCOLLECTED_WST:
        rural_uncollected_wst = rural_return_df.reset_index(drop=True)
        rural_uncollected_wst = rural_uncollected_wst.merge(
            rural_treatment_rate_df.drop(columns=["variable", "time"]).rename(columns={"value": "rate"})
        )
        rural_uncollected_wst["value"] = (1e-3 * rural_uncollected_wst["value"]) * (
            1 - rural_uncollected_wst["rate"]
        )
        rural_uncollected_wst_df.append(load_rules(r, rural_uncollected_wst))

    rural_uncollected_wst_df = pd.concat(rural_uncollected_wst_df)
    dmd_df = pd.concat([dmd_df, rural_uncollected_wst_df])


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
    h_act["commodity"] = np.select(conditions, values, "Unknown commodity")
    h_act["value"] = h_act["value"].abs()

    # Process historical activities using rules
    historical_activity_df = []
    for rule in HISTORICAL_ACTIVITY:
        hist_act = make_df(
            rule["type"],
            node_loc=eval_field(rule["node"], h_act),
            technology=eval_field(rule["technology"], h_act),
            year_act=eval_field(rule["year"], h_act),
            mode=rule["mode"],
            time=eval_field(rule["time"], h_act),
            value=eval_field(rule["value"], h_act),
            unit=rule["unit"],
        )
        historical_activity_df.append(hist_act)
    results["historical_activity"] = pd.concat(historical_activity_df)

    # Process historical capacities using rules
    h_cap = h_act[h_act["year"] >= 2015]
    h_cap = (
        h_cap.groupby(["node", "commodity", "level", "year", "unit"])["value"]
        .sum()
        .reset_index()
    )
    historical_capacity_df = []
    for rule in HISTORICAL_CAPACITY:
        hist_cap = make_df(
            rule["type"],
            node_loc=eval_field(rule["node"], h_cap),
            technology=eval_field(rule["technology"], h_cap),
            year_vtg=eval_field(rule["year"], h_cap),
            value=eval_field(rule["value"], h_cap),
            unit=rule["unit"],
        )
        historical_capacity_df.append(hist_cap)
    results["historical_new_capacity"] = pd.concat(historical_capacity_df)

    # share constraint lower bound on urban_Water recycling
    share_constraint_df = []
    for rule in SHARE_CONSTRAINTS_RECYCLING:
        df_share_wat = make_df(
            rule["type"],
            shares=rule["shares"],
            node_share="B" + eval_field(rule["node"], df_recycling),
            year_act=eval_field(rule["year"], df_recycling),
            value=eval_field(rule["value"], df_recycling),
            unit=rule["unit"],
    ).pipe(
            broadcast,
            time=pd.Series(sub_time),
        )
        share_constraint_df.append(df_share_wat)
    results["share_commodity_lo"] = pd.concat(share_constraint_df)
    results["share_commodity_lo"] = results["share_commodity_lo"][results["share_commodity_lo"]["year_act"].isin(info.Y)]


    return results



