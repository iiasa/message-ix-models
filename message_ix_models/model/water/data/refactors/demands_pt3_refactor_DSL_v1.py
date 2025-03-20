import os
import builtins
from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, Union

import numpy as np
import pandas as pd
import xarray as xr
from message_ix import make_df

from message_ix_models.util import broadcast, minimum_version, package_data_path

from message_ix_models.model.water.data.data_transformers import DSL_RULES, apply_transformation_rule, convert_units
from message_ix_models.model.water.data.demands_pt3_refactor_DSL_v2 import (
    load_and_combine_csvs,
    filter_and_merge_subannual,
    extract_components,
    apply_rate_adjustments,
    target_rate,
    assemble_historical_data,
    generate_share_constraint
)
if TYPE_CHECKING:
    from message_ix_models import Context

CONVERT_TO_MCM = False
  # flag to convert km3/year to mcm/year

@minimum_version("message_ix 3.7")
def add_sectoral_demands(context: "Context") -> dict[str, pd.DataFrame]:
    # facade function wrapping dsl-based water demand processing

    results = {}
    info = context["water build info"]
    region = f"{context.regions}"
    sub_time = context.time

    # load and transform yearly csv data
    df_dmds = _load_yearly_csvs(context, region)
    # apply monthly adjustments for sub-annual timesteps
    df_dmds = _apply_subannual_adjustments(df_dmds, context, region)
    # extract data components from combined data
    comps = _extract_components(df_dmds)
    # adjust rates based on policy configuration if needed
    comps = _apply_sdg_adjustments(comps, df_dmds, context)

    # construct demand dataframes using dsl rules
    dmd_dfs = [apply_transformation_rule(rule, comps) for rule in DSL_RULES]
    dmd_df = pd.concat(dmd_dfs)
    dmd_df = dmd_df[dmd_df["year"].isin(info.Y)]
    results["demand"] = dmd_df

    hist_act, hist_cap = _assemble_historical_data(dmd_df, info)
    results["historical_activity"] = hist_act
    results["historical_new_capacity"] = hist_cap

    df_share_wat = _generate_share_constraint(comps["recycling"], sub_time, info)
    results["share_commodity_lo"] = df_share_wat

    return results


def _load_yearly_csvs(context, region) -> pd.DataFrame:
    # load csv files using generic helper
    path = package_data_path("water", "demands", "harmonized", region, ".")
    filepaths = list(path.glob("ssp2_regional_*.csv"))
    df_dmds = load_and_combine_csvs(
        filepaths=filepaths,
        index_col="year",
        interpolation_years=[2015, 2025, 2035, 2045, 2055],
        rename_map=None,
        time_label="time",
        default_time_value="year",
    )
    return df_dmds


def _apply_subannual_adjustments(df_dmds, context, region) -> pd.DataFrame:
    # merge monthly data if timesteps are sub-annual using generic helper
    if "year" not in context.time:
        PATH = package_data_path("water", "demands", "harmonized", region, "ssp2_m_water_demands.csv")
        df_m = pd.read_csv(PATH)
        df_m.value *= 30  # converting mcm/day to mcm/month
        df_m.loc[df_m["sector"] == "industry", "sector"] = "manufacturing"
        df_m["variable"] = df_m["sector"] + "_" + df_m["type"] + "_baseline"
        df_m.loc[df_m["variable"] == "urban_withdrawal_baseline", "variable"] = "urbann_withdrawal2_baseline"
        df_m.loc[df_m["variable"] == "urban_return_baseline", "variable"] = "urbann_return2_baseline"
        df_m = df_m[["year", "pid", "variable", "value", "month"]]
        df_m.columns = pd.Index(["year", "node", "variable", "value", "time"])
        replace_vars = [
            "urban_withdrawal2_baseline",
            "rural_withdrawal_baseline",
            "manufacturing_withdrawal_baseline",
            "manufacturing_return_baseline",
            "urban_return2_baseline",
            "rural_return_baseline",
        ]
        df_dmds = filter_and_merge_subannual(df_dmds, df_m, replace_vars)
    return df_dmds


def _extract_components(df_dmds) -> dict:
    # extract demand and rate data using generic helper
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
    return extract_components(df_dmds, component_map, index_cols=["force-reset"])


def _apply_sdg_adjustments(comps, df_dmds, context) -> dict:
    # adjust rate components for sdg or alternative policy scenarios
    if context.SDG != "baseline":
        file2 = f"basins_country_{context.regions}.csv"
        PATH = package_data_path("water", "delineation", file2)
        df_basin = pd.read_csv(PATH)
        if context.SDG.upper() == "SDG":
            adjusters = {
                "rural_treatment_rate": lambda df: target_rate(df, df_basin, 0.8),
                "urban_treatment_rate": lambda df: target_rate(df, df_basin, 0.95),
                "urban_connection_rate": lambda df: target_rate(df, df_basin, 0.99),
                "rural_connection_rate": lambda df: target_rate(df, df_basin, 0.8),
                "recycling": lambda df: target_rate(df, df_basin, "treatment")
            }
            suffix = "SDG"
            comps = apply_rate_adjustments(comps, adjusters)
        else:
            pol_scen = context.SDG
            comps["urban_connection_rate"] = df_dmds[df_dmds["variable"] == "urban_connection_rate_" + pol_scen]
            comps["urban_connection_rate"].reset_index(drop=True, inplace=True)
            comps["rural_connection_rate"] = df_dmds[df_dmds["variable"] == "rural_connection_rate_" + pol_scen]
            comps["rural_connection_rate"].reset_index(drop=True, inplace=True)
            comps["urban_treatment_rate"] = df_dmds[df_dmds["variable"] == "urban_treatment_rate_" + pol_scen]
            comps["urban_treatment_rate"].reset_index(drop=True, inplace=True)
            comps["rural_treatment_rate"] = df_dmds[df_dmds["variable"] == "rural_treatment_rate_" + pol_scen]
            comps["rural_treatment_rate"].reset_index(drop=True, inplace=True)
            comps["recycling"] = df_dmds[df_dmds["variable"] == "urban_recycling_rate_" + pol_scen]
            comps["recycling"].reset_index(drop=True, inplace=True)
            suffix = pol_scen
        all_rates = pd.concat([
            comps["urban_connection_rate"],
            comps["rural_connection_rate"],
            comps["urban_treatment_rate"],
            comps["rural_treatment_rate"],
            comps["recycling"],
        ])
        all_rates["variable"] = [x.replace("baseline", suffix) for x in all_rates["variable"]]
        save_path = package_data_path("water", "demands", "harmonized", context.regions)
        all_rates.to_csv(save_path / "all_rates_SSP2.csv", index=False)
    return comps


def _assemble_historical_data(dmd_df, info):
    # revert to native historical data aggregation
    h_act = dmd_df[dmd_df["year"].isin([2010, 2015])]
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
    h_act["commodity"] = np.select(conditions, values, "unknown commodity")
    h_act["value"] = h_act["value"].abs()
    if CONVERT_TO_MCM:
        h_act_converted = convert_units(h_act["value"], "km3/year", "mcm/year")
        hist_unit = "mcm/year"
    else:
        h_act_converted = h_act["value"]
        hist_unit = "km3/year"
    hist_act = make_df(
        "historical_activity",
        node_loc=h_act["node"],
        technology=h_act["commodity"],
        year_act=h_act["year"],
        mode="M1",
        time=h_act["time"],
        value=h_act_converted,
        unit=hist_unit,
    )
    h_cap = h_act[h_act["year"] >= 2015]
    h_cap = (
        h_cap.groupby(["node", "commodity", "level", "year", "unit"])["value"]
        .sum()
        .reset_index()
    )
    if CONVERT_TO_MCM:
        h_cap_value = convert_units(h_cap["value"] / 5, "km3/year", "mcm/year")
        cap_unit = "mcm/year"
    else:
        h_cap_value = h_cap["value"] / 5
        cap_unit = "km3/year"
    hist_cap = make_df(
        "historical_new_capacity",
        node_loc=h_cap["node"],
        technology=h_cap["commodity"],
        year_vtg=h_cap["year"],
        value=h_cap_value,
        unit=cap_unit,
    )
    return hist_act, hist_cap


def _generate_share_constraint(df_recycling, sub_time, info):
    # build share constraint using generic helper
    df_share_wat = generate_share_constraint(
        df=df_recycling,
        share_name="share_wat_recycle",
        sub_times=sub_time,
        year_filter=info.Y,
        node_prefix="B",
    )
    return df_share_wat