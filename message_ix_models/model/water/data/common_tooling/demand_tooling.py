import pandas as pd
import xarray as xr
from typing import Callable, Optional, Sequence, Union
from pathlib import Path


# new: unit conversion helper using a reference dict for common unit conversions
def convert_units(value, from_unit: str, to_unit: str):
    # convert between common water demand units
    conversion_factors = {
        ("km3/year", "mcm/year"): 1000,
        ("mcm/year", "km3/year"): 0.001,
        ("km3", "mcm"): 1000,
        ("mcm", "km3"): 0.001,
    }
    if from_unit.lower() == to_unit.lower():
        return value
    key = (from_unit.lower(), to_unit.lower())
    if key in conversion_factors:
        return value * conversion_factors[key]
    raise ValueError(f"conversion from {from_unit} to {to_unit} not defined")

def _compute_value(withdrawal, rate, conversion, rate_op):
    # use pattern matching to choose the appropriate rate adjustment
    match rate_op:
        case "identity":
            return conversion * withdrawal * rate
        case "invert":
            return conversion * withdrawal * (1 - rate)
        case None:
            return conversion * withdrawal
        case _:
            raise ValueError(f"unknown rate_op {rate_op}")

def apply_transformation_rule(rule: dict, comps: dict, node_prefix: str = "B", CONVERT_TO_MCM: bool = False) -> pd.DataFrame:
    # get the withdrawal dataframe; key names come from the dsl rule
    df_withd = comps[rule["withdrawal"]].reset_index(drop=True)
    if rule.get("rate") is not None:
        df_rate = comps[rule["rate"]].drop(columns=["variable", "time"]).rename(columns={"value": "rate"})
        df = df_withd.merge(df_rate)
        df["value"] = _compute_value(df["value"], df["rate"], rule["conversion"], rule["rate_op"])
    else:
        df = df_withd.copy()
        df["value"] = rule["conversion"] * df["value"]
    if rule.get("sign", 1) < 0:
        df["value"] = -df["value"]

    # convert output from km3/year to mcm/year
    if CONVERT_TO_MCM:
        df_converted_value = convert_units(df["value"], "km3/year", "mcm/year")
        unit = "mcm/year"
    else:
        df_converted_value = df["value"]
        unit = "km3/year"
    return make_df(
        "demand",
        node=node_prefix + df["node"],
        commodity=rule["commodity"],
        level="final",
        year=df["year"],
        time=df["time"],
        value=df_converted_value,
        unit=unit,
    ) 


def parse_dsl_rules(dsl_rules: list[dict], comps: dict) -> list[pd.DataFrame]:
    # apply each dsl rule and return the resulting dfs
    return [apply_transformation_rule(rule, comps) for rule in dsl_rules]

def apply_dsl_transformations(dsl_rules: list[dict], comps: dict, node_prefix: str = "B") -> pd.DataFrame:
    # combine the results and add node prefix where necessary
    dfs = parse_dsl_rules(dsl_rules, comps)
    combined_df = pd.concat(dfs)
    mask = ~combined_df["node"].astype(str).str.startswith(node_prefix)
    combined_df.loc[mask, "node"] = node_prefix + combined_df.loc[mask, "node"].astype(str)
    return combined_df 

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