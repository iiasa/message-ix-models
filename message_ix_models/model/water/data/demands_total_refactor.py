"""prepare data for adding demands

this file sets targets for connection, sanitation and treatment based on basin info
"""

import os
import builtins
from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, Union

import numpy as np
import pandas as pd
import xarray as xr
from message_ix import make_df

from message_ix_models.util import broadcast, minimum_version, package_data_path

if TYPE_CHECKING:
    from message_ix_models import Context

from message_ix_models.model.water.data.Common_Tooling.demand_rules import *
from message_ix_models.model.water.data.Common_Tooling.demand_tooling import *


def get_basin_sizes(basin: pd.DataFrame, node: str) -> tuple[int, int]:
    """return dev and ind sizes for a node"""
    temp = basin[basin["BCU_name"] == node]
    sizes = temp.pivot_table(index=["STATUS"], aggfunc="size")
    sizes_dev = sizes.get("DEV", 0)
    sizes_ind = sizes.get("IND", 0)
    return sizes_dev, sizes_ind


def target_rate(df: pd.DataFrame, basin: pd.DataFrame, target: Union[float, str]) -> pd.DataFrame:
    """
    adjust rate values according to target.

    if target is 'treatment', then for developed basins (dev >= ind) a threshold of 2040 is used,
    otherwise 2030; values are increased by half the gap toward 1.
    
    if target is a float (for connection/sanitation), then in developed basins the rate is set at 2030,
    while in developing basins the 2035 rate is set as the average of the 2030 rate and target, and
    the 2040 value is set to target.
    """
    for node in df["node"].unique():
        sizes_dev, sizes_ind = get_basin_sizes(basin, node)
        is_developed = sizes_dev >= sizes_ind
        
        match target:
            case "treatment":
                # determine threshold based on basin type
                match is_developed:
                    case True:
                        threshold = 2040
                    case False:
                        threshold = 2030
                cond = (df["node"] == node) & (df["year"] >= threshold)
                df.loc[cond, "value"] = df.loc[cond, "value"] + (1 - df.loc[cond, "value"]) / 2

            case float() as t:
                # adjust connection/sanitation targets based on basin type
                match is_developed:
                    case True:
                        cond = (df["node"] == node) & (df["year"] == 2030)
                        df.loc[cond, "value"] = df.loc[cond, "value"].where(df.loc[cond, "value"] >= t, t)
                    case False:
                        cond_2030 = (df["node"] == node) & (df["year"] == 2030)
                        rate_2030 = df.loc[cond_2030, "value"].iloc[0] if not df.loc[cond_2030, "value"].empty else 0
                        cond_2035 = (df["node"] == node) & (df["year"] == 2035)
                        cond_2040 = (df["node"] == node) & (df["year"] == 2040)
                        target_2035 = (rate_2030 + t) / 2
                        df.loc[cond_2035, "value"] = df.loc[cond_2035, "value"].where(df.loc[cond_2035, "value"] >= target_2035, target_2035)
                        df.loc[cond_2040, "value"] = df.loc[cond_2040, "value"].where(df.loc[cond_2040, "value"] >= t, t)
            case _:
                raise ValueError(f"unrecognized target parameter: {target}")
    return df

# constant for buffer factor
BUFFER_FACTOR = 0.95

def _read_basins(context: "Context") -> pd.DataFrame:
    # read basins file
    path = package_data_path("water", "delineation", f"basins_by_region_simpl_{context.regions}.csv")
    return pd.read_csv(path)

def process_availability_data(df: pd.DataFrame, df_basins: pd.DataFrame, time_mode: str, info) -> pd.DataFrame:
    # common transformation pipeline for water data
    df = df.drop(columns=["Unnamed: 0"])
    df.index = df_basins["BCU_name"].to_list()
    df = df.stack().reset_index()
    df = df.rename(columns={"level_0": "Region", "level_1": "years", 0: "value"})
    df = df.fillna(0)
    df = df.reset_index(drop=True)
    match time_mode:
        case "annual":
            df["year"] = pd.DatetimeIndex(df["years"]).year
            df["time"] = "year"
        case "subannual":
            df = df.sort_values(["Region", "years", "value"])
            df["year"] = pd.DatetimeIndex(df["years"]).year
            df["time"] = pd.DatetimeIndex(df["years"]).month
        case _:
            raise ValueError(f"unknown time mode {time_mode}")
    # duplicate 2100 as 2110 and filter relevant years
    df_patch = df[df["year"] == 2100].copy()
    df_patch["year"] = 2110
    df = pd.concat([df, df_patch])
    df = df[df["year"].isin(info.Y)]
    return df

def read_water_availability(context: "Context") -> tuple[pd.DataFrame, pd.DataFrame]:
    # read water availability data in an imperative style
    info = context["water build info"]
    df_basins = _read_basins(context)
    time_mode = "annual" if "year" in context.time else "subannual"
    suffix = "" if time_mode == "annual" else "_m"
    # process surface water data
    path_sw = package_data_path("water", "availability", f"qtot_5y{suffix}_{context.RCP}_{context.REL}_{context.regions}.csv")
    df_sw = pd.read_csv(path_sw)
    df_sw = process_availability_data(df_sw, df_basins, time_mode, info)
    # process groundwater data
    path_gw = package_data_path("water", "availability", f"qr_5y{suffix}_{context.RCP}_{context.REL}_{context.regions}.csv")
    df_gw = pd.read_csv(path_gw)
    df_gw = process_availability_data(df_gw, df_basins, time_mode, info)
    return df_sw, df_gw

def add_water_availability(context: "Context") -> dict[str, pd.DataFrame]:
    # compile water supply constraints using the imperative transforms
    results = {}
    df_sw, df_gw = read_water_availability(context)
    dmd_sw = make_df(
        "demand",
        node="B" + df_sw["Region"].astype(str),
        commodity="surfacewater_basin",
        level="water_avail_basin",
        year=df_sw["year"],
        time=df_sw["time"],
        value=-df_sw["value"],
        unit="km3/year",
    )
    dmd_gw = make_df(
        "demand",
        node="B" + df_gw["Region"].astype(str),
        commodity="groundwater_basin",
        level="water_avail_basin",
        year=df_gw["year"],
        time=df_gw["time"],
        value=-df_gw["value"],
        unit="km3/year",
    )
    dmd_df = pd.concat([dmd_sw, dmd_gw])
    dmd_df["value"] = dmd_df["value"].apply(lambda x: x if x <= 0 else 0)
    results["demand"] = dmd_df
    share_val = (df_gw["value"].abs() / (df_sw["value"].abs() + df_gw["value"].abs())) * BUFFER_FACTOR
    df_share = make_df(
        "share_commodity_lo",
        shares="share_low_lim_GWat",
        node_share="B" + df_gw["Region"].astype(str),
        year_act=df_gw["year"],
        time=df_gw["time"],
        value=share_val,
        unit="-",
    )
    df_share["value"] = df_share["value"].fillna(0)
    results["share_commodity_lo"] = df_share
    return results

def add_irrigation_demand(context: "Context") -> dict[str, pd.DataFrame]:
    # process irrigation water demand from globiom
    results = {}
    scen = context.get_scenario()
    land_out_1 = scen.par("land_output", {"commodity": "Water|Withdrawal|Irrigation|Cereals"})
    land_out_1["level"] = "irr_cereal"
    land_out_2 = scen.par("land_output", {"commodity": "Water|Withdrawal|Irrigation|Oilcrops"})
    land_out_2["level"] = "irr_oilcrops"
    land_out_3 = scen.par("land_output", {"commodity": "Water|Withdrawal|Irrigation|Sugarcrops"})
    land_out_3["level"] = "irr_sugarcrops"
    land_out = pd.concat([land_out_1, land_out_2, land_out_3])
    land_out["commodity"] = "freshwater"
    land_out["value"] = 1e-3 * land_out["value"]
    results["land_input"] = land_out
    return results

def target_rate(df: pd.DataFrame, df_basin: pd.DataFrame, target: Union[float, str], Years: list[int], Start_Year: int, Interp_rate: float, treatment_rate: float = 0.25) -> pd.DataFrame:
    """
    adjust rates to reach a target by 2030 using pattern matching.
    
    parameters
    ----------
    df : dataframe
        contains rate data with year, node, and value columns
    df_basin : dataframe
        basin info for mapping
    target : float or "treatment"
        target value (0 to 1). if "treatment", use 0.25.
        
    returns
    -------
    dataframe
        adjusted rates
    """
    match target:
        case "treatment":
            effective_target = treatment_rate
        case float() as t:
            effective_target = t
        case _:
            raise ValueError(f"unrecognized target parameter: {target}")
    
    df_out = df.copy()
    for node in df_out["node"].unique():
        rate_base_year = df_out.loc[(df_out["node"] == node) & (df_out["year"] == base_year), "value"].values[0]
        for year in Years:
            if year <= base_year:
                new_rate = rate_base_year + (effective_target - rate_base_year) * Interp_rate
            else:
                new_rate = effective_target
            df_out.loc[(df_out["node"] == node) & (df_out["year"] == year), "value"] = new_rate
    return df_out


def load_and_combine_csvs(
    filepaths: list[Path],
    index_col: str = "year",
    interpolation_years: list[int] | None = None,
    rename_map: dict[str, str] | None = None,
    time_label: str = "time",
    default_time_value: str = "year",
) -> pd.DataFrame:
    """load and combine csvs; optionally interpolate and flatten."""
    data_dict = {}
    for fp in filepaths:
        df_temp = pd.read_csv(fp)
        # if expected index is missing but "Unnamed: 0" exists, rename it
        if index_col not in df_temp.columns and "Unnamed: 0" in df_temp.columns:
            df_temp.rename(columns={"Unnamed: 0": index_col}, inplace=True)
        if index_col in df_temp.columns:
            df_temp.set_index(index_col, inplace=True)
            df_temp.index.name = index_col  # ensure index is named "year"
        # set the dataframe's column name to mark node labels
        df_temp.columns.name = "node"
        if rename_map:
            df_temp.rename(columns=rename_map, inplace=True)

        # quick fix: remove prefix to match legacy behavior
        data_key = fp.stem.replace("ssp2_regional_", "")
        data_dict[data_key] = df_temp

    ds = xr.Dataset(data_dict).to_array()
    if interpolation_years:
        ds_interp = ds.interp({index_col: interpolation_years})
        ds = ds.combine_first(ds_interp)

    df_flat = ds.to_dataframe("").unstack()
    df_long = df_flat.stack(future_stack=True).reset_index(level=0).reset_index()
    # retain legacy ordering: year, node, variable, value
    df_long.columns = [index_col, "node", "variable", "value"]
    df_long[time_label] = default_time_value
    # add sorting to match original implementation
    df_long.sort_values([index_col, "node", "variable", "value"], inplace=True)
    return df_long


def filter_and_merge_subannual(
    df: pd.DataFrame,
    df_subannual: pd.DataFrame,
    replace_vars: list[str],
    join_cols: list[str] = ["year", "variable", "node"],
) -> pd.DataFrame:
    """merge base and subannual dfs; replace variables in replace_vars."""
    df_filtered = df[~df["variable"].isin(replace_vars)]
    return pd.concat([df_filtered, df_subannual], ignore_index=True)


def extract_components(
    df: pd.DataFrame,
    component_map: dict[str, str],
    index_cols: list[str] = None,
) -> dict[str, pd.DataFrame]:
    """extract components using component_map; optionally reset index."""
    comps = {}
    for comp_name, var_val in component_map.items():
        tmp = df[df["variable"] == var_val].copy()
        if index_cols is not None:
            tmp.reset_index(drop=True, inplace=True)
        comps[comp_name] = tmp
    return comps



def apply_rate_adjustments(
    comps: dict[str, pd.DataFrame],
    adjusters: dict[str, Callable[[pd.DataFrame], pd.DataFrame]] | None = None
) -> dict[str, pd.DataFrame]:
    """apply adjusters to components; pass unchanged if none provided."""
    if not adjusters:
        return comps
    new_comps = {}
    for k, v in comps.items():
        if k in adjusters:
            new_comps[k] = adjusters[k](v.copy())
        else:
            new_comps[k] = v.copy()
    return new_comps


def assemble_historical_data(
    df_demands: pd.DataFrame,
    commodity_map: dict[str, str],
    hist_years: list[int],
    capacity_year_threshold: int,
    convert_func: Optional[Callable[[pd.Series], pd.Series]] = None,
    activity_label: str = "historical_activity",
    capacity_label: str = "historical_new_capacity",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """assemble historical activity and capacity using commodity_map and years."""
    df_hist = df_demands[df_demands["year"].isin(hist_years)].copy()
    df_hist["commodity"] = df_hist["commodity"].map(commodity_map).fillna("unknown")
    if convert_func:
        df_hist["value"] = convert_func(df_hist["value"])

    df_hist["value"] = df_hist["value"].abs()
    hist_act = make_df(
        activity_label,
        node_loc=df_hist["node"],
        technology=df_hist["commodity"],
        year_act=df_hist["year"],
        mode="M1",
        time=df_hist["time"],
        value=df_hist["value"],
        unit="km3/year",
    )
    df_cap = df_hist[df_hist["year"] >= capacity_year_threshold].copy()
    df_cap_grouped = df_cap.groupby(["node", "commodity", "year"], as_index=False)["value"].sum()
    if convert_func:
        pass
    df_cap_grouped["value"] = df_cap_grouped["value"] / 5.0
    hist_cap = make_df(
        capacity_label,
        node_loc=df_cap_grouped["node"],
        technology=df_cap_grouped["commodity"],
        year_vtg=df_cap_grouped["year"],
        value=df_cap_grouped["value"],
        unit="km3/year",
    )
    return hist_act, hist_cap


def generate_share_constraint(
    df: pd.DataFrame,
    share_name: str,
    sub_times: Sequence,
    year_filter: list[int],
    node_prefix: str = "B",
) -> pd.DataFrame:
    """create share constraint df using share_name and sub_times."""
    df_share = make_df(
        "share_commodity_lo",
        shares=share_name,
        node_share=node_prefix + df["node"],
        year_act=df["year"],
        value=df["value"],
        unit="-",
    )
    df_share = broadcast(df_share, time=pd.Series(sub_times))
    df_share = df_share[df_share["year_act"].isin(year_filter)]
    return df_share


@minimum_version("message_ix 3.7")
def build_sectoral_demands(
    csv_paths: list[Path],
    interpolation_years: list[int],
    subannual_data: Optional[pd.DataFrame] = None,
    subannual_replace_vars: list[str] = (),
    dsl_rules: list[dict] = DSL_RULES,
    component_map: dict[str, str] = None,
    adjusters: dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = None,
    commodity_map: dict[str, str] = None,
    historical_years: list[int] = [2010, 2015],
    capacity_year_threshold: int = 2015,
    share_name: str = "share_wat_recycle",
    sub_times: Sequence = ("year",),
    node_prefix: str = "B",
    time_label: str = "time",
    default_time_value: str = "year",
    model_years: list[int] = None,
    convert_to_mcm: bool = True,
    unit_in: str = "km3/year",
    unit_out: str = "km3/year",
) -> dict[str, pd.DataFrame]:
    """build sectoral demands: load, merge, extract, adjust, transform, and assemble."""
    results = {}
    df_main = load_and_combine_csvs(
        csv_paths,
        index_col="year",
        interpolation_years=interpolation_years,
        rename_map=None,
        time_label=time_label,
        default_time_value=default_time_value,
    )
    if subannual_data is not None:
        df_main = filter_and_merge_subannual(df_main, subannual_data, subannual_replace_vars)
    if component_map:
        comps = extract_components(df_main, component_map)
    else:
        comps = {"all_data": df_main}
    comps = apply_rate_adjustments(comps, adjusters)
    
    # call the dsl engine to apply the declarative transformation rules
    dmd_df = apply_dsl_transformations(dsl_rules, comps, node_prefix=node_prefix)
    if model_years:
        dmd_df = dmd_df[dmd_df["year"].isin(model_years)]
    results["demand"] = dmd_df
    if commodity_map is not None:
        convert_func = None
        if convert_to_mcm:
            convert_func = lambda x: convert_units(x, unit_in, unit_out)
        hist_act, hist_cap = assemble_historical_data(
            dmd_df,
            commodity_map=commodity_map,
            hist_years=historical_years,
            capacity_year_threshold=capacity_year_threshold,
            convert_func=convert_func,
        )
        results["historical_activity"] = hist_act
        results["historical_new_capacity"] = hist_cap
    if "recycling" in comps:
        df_share = generate_share_constraint(
            df=comps["recycling"],
            share_name=share_name,
            sub_times=sub_times,
            year_filter=dmd_df["year"].unique().tolist(),
            node_prefix=node_prefix,
        )
        results["share_commodity_lo"] = df_share
    return results


# ---------------------------------------------------------------------------
# 9. COMPATIBILITY WRAPPER FOR EXISTING CODE
#    This provides a drop-in replacement for the original function
# ---------------------------------------------------------------------------
@minimum_version("message_ix 3.7")
def add_sectoral_demands(context: "Context") -> dict[str, pd.DataFrame]:
    """compat wrapper; call build_sectoral_demands with context."""
    # context info
    info = context["water build info"]
    region = f"{context.regions}"
    sub_time = context.time
    # csv paths
    path = package_data_path("water", "demands", "harmonized", region, ".")
    csv_paths = list(path.glob("ssp2_regional_*.csv"))
    # comp map
    component_map = {
        "urban_withdrawal": "urban_withdrawal2_baseline",
        "rural_withdrawal": "rural_withdrawal_baseline",
        "manufacturing_withdrawal": "manufacturing_withdrawal_baseline",
        "manufacturing_return": "manufacturing_return_baseline",
        "urban_return": "urban_return2_baseline",
        "rural_return": "rural_return_baseline",
        "urban_connection_rate": "urban_connection_rate_baseline",
        "rural_connection_rate": "rural_connection_rate_baseline",
        "urban_treatment_rate": "urban_treatment_rate_baseline",
        "rural_treatment_rate": "rural_treatment_rate_baseline",
        "recycling": "urban_recycling_rate_baseline"
    }
    # subannual data
    subannual_data = None
    subannual_replace_vars = []
    if "year" not in context.time:
        PATH = package_data_path("water", "demands", "harmonized", region, "ssp2_m_water_demands.csv")
        df_m = pd.read_csv(PATH)
        df_m.value *= 30  # mcm/day to mcm/month
        df_m.loc[df_m["sector"] == "industry", "sector"] = "manufacturing"
        df_m["variable"] = df_m["sector"] + "_" + df_m["type"] + "_baseline"
        df_m.loc[df_m["variable"] == "urban_withdrawal_baseline", "variable"] = "urbann_withdrawal2_baseline"
        df_m.loc[df_m["variable"] == "urban_return_baseline", "variable"] = "urbann_return2_baseline"
        df_m = df_m[["year", "pid", "variable", "value", "month"]]
        df_m.columns = pd.Index(["year", "node", "variable", "value", "time"])
        subannual_data = df_m
        subannual_replace_vars = [
            "urban_withdrawal2_baseline",
            "rural_withdrawal_baseline",
            "manufacturing_withdrawal_baseline",
            "manufacturing_return_baseline",
            "urban_return2_baseline",
            "rural_return_baseline",
        ]
    # adjusters for sdg
    adjusters = None
    if context.SDG != "baseline":
        adjusters = {}
        if context.SDG == "sdg":
            file2 = f"basins_country_{context.regions}.csv"
            PATH = package_data_path("water", "delineation", file2)
            df_basin = pd.read_csv(PATH)
            # SDG parameters
            params = {
                "years": [2025, 2035, 2045, 2055],
                "base_year": 2015,
                "interp_rate": 0.6,
                "treatment_rate": 0.25
            }  
            adjusters["rural_treatment_rate"] = lambda df: target_rate(df, df_basin, 0.8, **params)
            adjusters["urban_treatment_rate"] = lambda df: target_rate(df, df_basin, 0.95, **params)
            adjusters["urban_connection_rate"] = lambda df: target_rate(df, df_basin, 0.99, **params)
            adjusters["rural_connection_rate"] = lambda df: target_rate(df, df_basin, 0.8, **params)
            adjusters["recycling"] = lambda df: target_rate(df, df_basin, "treatment", **params)
            pass
        else:
            pass
    # commodity map
    commodity_map = {
        "urban_mw": "urban_t_d",
        "industry_mw": "industry_unconnected",
        "rural_mw": "rural_t_d",
        "urban_disconnected": "urban_unconnected",
        "rural_disconnected": "rural_unconnected",
        "urban_collected_wst": "urban_sewerage",
        "rural_collected_wst": "rural_sewerage",
        "urban_uncollected_wst": "urban_untreated",
        "industry_uncollected_wst": "industry_untreated",
        "rural_uncollected_wst": "rural_untreated",
    }
    # call pipeline
    return build_sectoral_demands(
        csv_paths=csv_paths,
        interpolation_years=[2015, 2025, 2035, 2045, 2055],
        subannual_data=subannual_data,
        subannual_replace_vars=subannual_replace_vars,
        dsl_rules=DSL_RULES,
        component_map=component_map,
        adjusters=adjusters,
        commodity_map=commodity_map,
        historical_years=[2010, 2015],
        capacity_year_threshold=2015,
        share_name="share_wat_recycle",
        sub_times=sub_time,
        node_prefix="B",
        model_years=info.Y,
        convert_to_mcm=False
    )
