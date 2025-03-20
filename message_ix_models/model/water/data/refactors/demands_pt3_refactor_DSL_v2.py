from __future__ import annotations

import os
from pathlib import Path
from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, Union, Callable, Optional

import numpy as np
import pandas as pd
import xarray as xr
from message_ix import make_df
from message_ix_models.util import broadcast, minimum_version, package_data_path
from message_ix_models.model.water.data.data_transformers import DSL_RULES, convert_units, apply_transformation_rule, apply_dsl_transformations


# ---------------------------------------------------------------------------
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
