from typing import Any

import numpy as np
import pandas as pd
from message_ix import Scenario

from message_ix_models import Context
from message_ix_models.model.water.data.demands import read_water_availability
from message_ix_models.model.water.data.water_supply_rules import (
    COOLING_SUPPLY_RULES,
    DUMMY_BASIN_TO_REG_OUTPUT_RULES,
    DUMMY_VARIABLE_COST_RULES,
    E_FLOW_RULES_BOUND,
    E_FLOW_RULES_DMD,
    EXTRACTION_INPUT_RULES,
    EXTRACTION_OUTPUT_RULES,
    FIXED_COST_RULES,
    HISTORICAL_NEW_CAPACITY_RULES,
    INVESTMENT_COST_RULES,
    SHARE_MODE_RULES,
    SLACK_TECHNOLOGY_RULES,
    TECHNICAL_LIFETIME_RULES,
)
from message_ix_models.model.water.dsl_engine import run_standard
from message_ix_models.model.water.utils import (
    map_yv_ya_lt,
    safe_concat,
)
from message_ix_models.util import (
    minimum_version,
    package_data_path,
)

WS_CONST = {"R11_SAS": 0.5, "MAX_ENV_FLOW_RATIO": 0.7}


def _basin_region_preprocess(
    df: pd.DataFrame,
    df_x: pd.DataFrame,
    context: "Context",
    info: None,
    monthly: bool = False,
) -> pd.DataFrame:
    """
    Preprocess the basin region data for map_basin_region_wat function.
    """
    df = df.copy()
    df.drop(columns=["Unnamed: 0"], inplace=True)

    df["BCU_name"] = df_x["BCU_name"]

    df["MSGREG"] = (
        context.map_ISO_c[context.regions]
        if context.type_reg == "country"
        else f"{context.regions}_" + df["BCU_name"].str.split("|").str[-1]
    )

    df = df.set_index(["MSGREG", "BCU_name"])

    # Calculating ratio of water availability in basin by region
    df = df.groupby(["MSGREG"]).apply(lambda x: x / x.sum())
    df.reset_index(level=0, drop=True, inplace=True)
    df.reset_index(inplace=True)
    df["Region"] = "B" + df["BCU_name"].astype(str)
    df["Mode"] = df["Region"].replace(regex=["^B"], value="M")
    df.drop(columns=["BCU_name"], inplace=True)
    df.set_index(["MSGREG", "Region", "Mode"], inplace=True)
    df = df.stack().reset_index(level=0).reset_index()
    df.columns = pd.Index(["region", "mode", "date", "MSGREG", "share"])
    df.sort_values(["region", "date", "MSGREG", "share"], inplace=True)
    df["year"] = pd.DatetimeIndex(df["date"]).year
    df["time"] = "year" if not monthly else pd.DatetimeIndex(df["date"]).month
    df = df[df["year"].isin(info.Y)]
    df.reset_index(drop=True, inplace=True)

    return df


@minimum_version("message_ix 3.7")
@minimum_version("python 3.10")
def map_basin_region_wat(context: "Context") -> pd.DataFrame:
    """
    Calculate share of water availability of basins per each parent region.

    The parent region could be global message regions or country

    Parameters
    ----------
        context : .Context

    Returns
    -------
        data : pandas.DataFrame

    Requires Python 3.10+ for pattern matching support.
    """
    info = context["water build info"]

    PATH1 = package_data_path(
        "water", "delineation", f"basins_by_region_simpl_{context.regions}.csv"
    )

    match context.time:
        case (
            "year" | "year" | ["year"]
        ):  # ['year'] is used in context.time when called from water for ppl.
            # Adding freshwater supply constraints
            # Reading data, the data is spatially and temprally aggregated from GHMs
            monthly = False
            path_context = package_data_path(
                "water",
                "availability",
                f"qtot_5y_{context.RCP}_{context.REL}_{context.regions}.csv",
            )
        case "month" | "month":
            # add water return flows for cooling tecs
            # Use share of basin availability to distribute the return flow from
            monthly = True
            path_context = package_data_path(
                "water",
                "availability",
                f"qtot_5y_m_{context.RCP}_{context.REL}_{context.regions}.csv",
            )
        case _:
            raise ValueError(f"Invalid time: {context.time}")
    # Read both CSV files concurrently using asyncio.gather for better performance
    df_sw, df_x = pd.read_csv(path_context), pd.read_csv(PATH1)
    df_sw = _basin_region_preprocess(df_sw, df_x, context, info, monthly)

    return df_sw


# Helper for processing cooling supply rules
def _process_cooling_supply(
    context: "Context", year_wat: tuple, node_region: Any
) -> dict[str, pd.DataFrame]:
    """Processes rules specific to the 'cooling' nexus setting."""
    results: dict[str, pd.DataFrame] = {}
    cooling_outputs = []
    dummy_df = pd.DataFrame()
    base_cooling = {
        "rule_dfs": {"df_node": dummy_df, "runtime_vals": {"year_wat": year_wat}},
        "node_loc": node_region,
    }  # no dfs used in cooling rules
    for r in COOLING_SUPPLY_RULES.get_rule():
        df_rule = run_standard(r, base_cooling)
        cooling_outputs.append(df_rule)
    results["output"] = safe_concat(cooling_outputs)
    return results


# Helper for processing slack technology rules
def _process_slack_rules(
    df_node: pd.DataFrame, sub_time_series: pd.Series, year_wat: tuple
) -> pd.DataFrame:
    """Processes slack technology rules."""
    slack_inputs = []
    base_slack = {
        "rule_dfs": {"df_node": df_node, "runtime_vals": {"year_wat": year_wat}},
        "sub_time": sub_time_series,
        "node_loc": df_node["node"],
    }
    for r in SLACK_TECHNOLOGY_RULES.get_rule():
        match r["technology"]:
            case "return_flow" | "gw_recharge":
                df_rule = run_standard(r, base_slack)
                slack_inputs.append(df_rule)
            case "basin_to_reg":
                df_rule = run_standard(r, base_slack, extra_args={"year_vtg": year_wat})
                slack_inputs.append(df_rule)
            case "salinewater_return":
                continue  # Skip this technology
            case _:
                raise ValueError(f"Invalid technology: {r['technology']}")
    slack_df = safe_concat(slack_inputs)
    slack_df["year_act"] = slack_df["year_vtg"]
    return slack_df


# Helper for processing extraction input rules
def _process_extraction_input_rules(
    df_node: pd.DataFrame,
    df_gwt: pd.DataFrame,
    sub_time_series: pd.Series,
    yv_ya_sw: pd.DataFrame,
    yv_ya_gw: pd.DataFrame,
) -> pd.DataFrame:
    """Processes extraction input rules."""
    extraction_inputs = []
    for r in EXTRACTION_INPUT_RULES.get_rule():
        bcast = yv_ya_sw if r["technology"] == "extract_surfacewater" else yv_ya_gw
        base_extract = {
            "rule_dfs": {"df_node": df_node, "df_gwt": df_gwt},
            "broadcast_year": bcast,
            "sub_time": sub_time_series,
        }
        df_rule = run_standard(r, base_extract)
        extraction_inputs.append(df_rule)
    return safe_concat(extraction_inputs)


# Helper to apply global specific scaling
def _apply_global_scaling(inp: pd.DataFrame, context: "Context") -> pd.DataFrame:
    """Applies global-specific scaling adjustments."""
    if context.type_reg == "global":
        cond = (
            (inp["technology"].str.contains("extract_gw_fossil"))
            & (inp["year_act"] == 2020)
            & (inp["node_loc"] == "R11_SAS")
        )
        inp.loc[cond, "value"] *= WS_CONST["R11_SAS"]  # Use constant
    return inp


# Helper for processing extraction output rules
def _process_extraction_output_rules(
    df_node: pd.DataFrame,
    node_region: np.ndarray,
    year_wat: tuple,
    sub_time_series: pd.Series,
    yv_ya_sw: pd.DataFrame,
    yv_ya_gw: pd.DataFrame,
) -> pd.DataFrame:
    """Processes extraction output rules."""
    extraction_outputs = []
    base_extract_out = {
        "rule_dfs": {"df_node": df_node, "runtime_vals": {"year_wat": year_wat}},
        "node_loc": node_region,
    }
    for r in EXTRACTION_OUTPUT_RULES.get_rule():
        r["type"] = "output"
        is_gw = r["technology"] in ["extract_gw_fossil", "extract_groundwater"]
        bcast = yv_ya_gw if is_gw else yv_ya_sw
        current_base = base_extract_out.copy()
        if r["technology"] == "extract_salinewater":
            df_rule = run_standard(r, current_base)
        else:
            current_base["sub_time"] = sub_time_series
            df_rule = run_standard(r, current_base, broadcast_year=bcast)
        extraction_outputs.append(df_rule)
    return safe_concat(extraction_outputs)


# Helper for processing historical new capacity rules
def _process_hist_cap_rules(df_hist: pd.DataFrame) -> pd.DataFrame:
    """Processes historical new capacity rules."""
    return safe_concat(
        [
            run_standard(r, {"rule_dfs": df_hist})
            for r in HISTORICAL_NEW_CAPACITY_RULES.get_rule()
        ]
    )


# Helper for processing dummy basin-to-region output rules
def _process_dummy_basin_output_rules(
    df_node: pd.DataFrame, sub_time_series: pd.Series, year_wat: tuple
) -> pd.DataFrame:
    """Processes dummy basin-to-region output rules."""
    dummy_outputs = []
    base_dummy = {"rule_dfs": df_node, "sub_time": sub_time_series}
    for r in DUMMY_BASIN_TO_REG_OUTPUT_RULES.get_rule():
        df_rule = run_standard(r, base_dummy, extra_args={"year_vtg": year_wat})
        df_rule["year_act"] = df_rule["year_vtg"]
        dummy_outputs.append(df_rule)
    return safe_concat(dummy_outputs) if len(dummy_outputs) > 1 else dummy_outputs[0]


# Helper for processing dummy variable cost rules
def _process_var_cost_rules(
    df_node: pd.DataFrame, sub_time_series: pd.Series, year_wat: tuple
) -> pd.DataFrame:
    """Processes dummy variable cost rules."""
    var_costs = []
    base_var_cost = {"rule_dfs": df_node, "sub_time": sub_time_series}
    for r in DUMMY_VARIABLE_COST_RULES.get_rule():
        df_rule = run_standard(r, base_var_cost, extra_args={"year_vtg": year_wat})
        df_rule["year_act"] = df_rule["year_vtg"]
        var_costs.append(df_rule)
    return safe_concat(var_costs)


# Helper for processing fixed cost rules
def _process_fix_cost_rules(
    df_node: pd.DataFrame, sub_time_series: pd.Series, yv_ya_gw: pd.DataFrame
) -> pd.DataFrame:
    """Processes fixed cost rules."""
    base_fix_cost = {
        "rule_dfs": df_node,
        "sub_time": sub_time_series,
        "node_loc": df_node["node"],
        "broadcast_year": yv_ya_gw,
    }
    return safe_concat(
        [run_standard(r, base_fix_cost) for r in FIXED_COST_RULES.get_rule()]
    )


# Helper for processing share mode rules
def _process_share_mode_rules(context: "Context") -> pd.DataFrame:
    """Processes share mode rules."""
    df_sw = map_basin_region_wat(context)
    base_share = {"rule_dfs": df_sw}
    return safe_concat(
        [run_standard(r, base_share) for r in SHARE_MODE_RULES.get_rule()]
    )


# Helper for processing technical lifetime rules
def _process_tl_rules(df_node: pd.DataFrame, year_wat: tuple) -> pd.DataFrame:
    """Processes technical lifetime rules."""
    base_tl = {"rule_dfs": df_node, "node_loc": df_node["node"]}
    return safe_concat(
        [
            run_standard(r, base_tl, extra_args={"year_vtg": year_wat})
            for r in TECHNICAL_LIFETIME_RULES.get_rule()
        ]
    )


# Helper for processing investment cost rules
def _process_inv_cost_rules(df_node: pd.DataFrame, year_wat: tuple) -> pd.DataFrame:
    """Processes investment cost rules."""
    base_inv = {"rule_dfs": df_node, "node_loc": df_node["node"]}
    return safe_concat(
        [
            run_standard(r, base_inv, extra_args={"year_vtg": year_wat})
            for r in INVESTMENT_COST_RULES.get_rule()
        ]
    )


# Helper function for the 'nexus' branch
def _process_nexus_supply(
    context: "Context",
    info: Any,  # Assuming info is some object/dict
    year_wat: tuple,
    sub_time: str,
    yv_ya_sw: pd.DataFrame,
    yv_ya_gw: pd.DataFrame,
    df_node: pd.DataFrame,
    df_gwt: pd.DataFrame,
    df_hist: pd.DataFrame,
    node_region: np.ndarray,
    sub_time_series: pd.Series,
) -> dict[str, pd.DataFrame]:
    """Processes rules specific to the 'nexus' nexus setting."""
    results: dict[str, pd.DataFrame] = {}

    # Process slack rules
    slack_inputs_df = _process_slack_rules(df_node, sub_time_series, year_wat)
    results["input"] = slack_inputs_df

    # Process extraction input rules
    extraction_inputs_df = _process_extraction_input_rules(
        df_node, df_gwt, sub_time_series, yv_ya_sw, yv_ya_gw
    )

    # Combine slack and extraction inputs, apply scaling
    inp = safe_concat([slack_inputs_df, extraction_inputs_df])
    inp["value"] = pd.to_numeric(inp["value"], errors="raise")
    inp = _apply_global_scaling(inp, context)
    results["input"] = inp

    # Process extraction output rules
    extraction_outputs_df = _process_extraction_output_rules(
        df_node, node_region, year_wat, sub_time_series, yv_ya_sw, yv_ya_gw
    )
    results["output"] = extraction_outputs_df

    # Process historical new capacity rules
    results["historical_new_capacity"] = _process_hist_cap_rules(df_hist)

    # Process dummy basin-to-region output rules
    dummy_basin_output_df = _process_dummy_basin_output_rules(
        df_node, sub_time_series, year_wat
    )
    results["output"] = safe_concat([results["output"], dummy_basin_output_df])
    # Synchronize output activity year with vintage year -
    # Apply to the final combined output
    results["output"]["year_act"] = results["output"]["year_vtg"]

    # Process dummy variable cost rules
    results["var_cost"] = _process_var_cost_rules(df_node, sub_time_series, year_wat)

    # Process fixed cost rules
    results["fix_cost"] = _process_fix_cost_rules(df_node, sub_time_series, yv_ya_gw)

    # Process share mode rules
    results["share_mode_up"] = _process_share_mode_rules(context)

    # Process technical lifetime rules
    results["technical_lifetime"] = _process_tl_rules(df_node, year_wat)

    # Process investment cost rules
    results["inv_cost"] = _process_inv_cost_rules(df_node, year_wat)

    return results


@minimum_version("python 3.10")
def add_water_supply(context: "Context") -> dict[str, pd.DataFrame]:
    """Add Water supply infrastructure (Refactored for reduced complexity).

    This function links the water supply based on different settings and options.
    It defines the supply linkages for freshwater, groundwater and salinewater.

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
    Requires Python 3.10+ for pattern matching support.
    """
    # Reference to the water configuration
    info = context["water build info"]
    # load the scenario from context (ensure platform is available)
    scen = Scenario(context.get_platform(), **context.core.scenario_info)
    first_year = scen.firstmodelyear
    # Determine first model year safely

    # Create the year tuple and set key time values
    year_wat = (2010, 2015, *info.Y)
    sub_time = context.time

    # Build broadcast year mappings for different lifetimes
    yv_ya_sw = map_yv_ya_lt(year_wat, 50, first_year)  # freshwater
    yv_ya_gw = map_yv_ya_lt(year_wat, 20, first_year)  # groundwater

    # Read common input CSV files
    FILE = f"basins_by_region_simpl_{context.regions}.csv"
    PATH = package_data_path("water", "delineation", FILE)
    df_node = pd.read_csv(PATH)
    df_node["node"] = "B" + df_node["BCU_name"].astype(str)
    df_node["mode"] = "M" + df_node["BCU_name"].astype(str)
    df_node["region"] = (
        context.map_ISO_c[context.regions]
        if context.type_reg == "country"
        else f"{context.regions}_" + df_node["REGION"].astype(str)
    )
    node_region = df_node["region"].unique()

    FILE1 = f"gw_energy_intensity_depth_{context.regions}.csv"
    PATH1 = package_data_path("water", "availability", FILE1)
    df_gwt = pd.read_csv(PATH1)
    df_gwt["region"] = (
        context.map_ISO_c[context.regions]
        if context.type_reg == "country"
        else f"{context.regions}_" + df_gwt["REGION"].astype(str)
    )

    FILE2 = f"historical_new_cap_gw_sw_km3_year_{context.regions}.csv"
    PATH2 = package_data_path("water", "availability", FILE2)
    df_hist = pd.read_csv(PATH2)
    df_hist["BCU_name"] = "B" + df_hist["BCU_name"].astype(str)

    sub_time_series = pd.Series(sub_time)

    # Select processing branch based on nexus_set
    if context.nexus_set == "cooling":
        results = _process_cooling_supply(context, year_wat, node_region)
    elif context.nexus_set == "nexus":
        results = _process_nexus_supply(
            context,
            info,
            year_wat,
            sub_time,
            yv_ya_sw,
            yv_ya_gw,
            df_node,
            df_gwt,
            df_hist,
            node_region,
            sub_time_series,
        )
    else:
        # Optionally handle other nexus_set values or raise an error
        raise ValueError(f"Unsupported nexus_set: {context.nexus_set}")

    return results


def _e_flow_preprocess(
    df: pd.DataFrame,
    df_x: pd.DataFrame,
    context: "Context",
    info: None,
    monthly: bool = False,
) -> pd.DataFrame:
    """
    Preprocess the e-flow data for add_e_flow function.
    """
    df = df.copy()
    df.drop(columns=["Unnamed: 0"], inplace=True)
    df.index = df_x["BCU_name"].index
    df = df.stack().reset_index()
    df.columns = pd.Index(["Region", "years", "value"])
    df.sort_values(["Region", "years", "value"], inplace=True)
    df.fillna(0, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df["year"] = pd.DatetimeIndex(df["years"]).year
    df["time"] = "year" if not monthly else pd.DatetimeIndex(df["years"]).month
    df["Region"] = df["Region"].map(df_x["BCU_name"])
    df2210 = df[df["year"] == 2100].copy()
    df2210["year"] = 2110
    df = safe_concat([df, df2210])
    df = df[df["year"].isin(info.Y)]

    return df


@minimum_version("python 3.10")
def add_e_flow(context: "Context") -> dict[str, pd.DataFrame]:
    """Add environmental flows
    This function bounds the available water and allocates the environmental
    flows.Environmental flow bounds are calculated using Variable Monthly Flow
    (VMF) method. The VMF method is applied to wet and dry seasonal runoff
    values. These wet and dry seasonal values are then aggregated to annual
    values.Environmental flows in the model will be incorporated as bounds on
    'return_flow' technology. The lower bound on this technology will ensure
    that certain amount of water remain

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
    Requires Python 3.10+ for pattern matching support.
    """
    # define an empty dictionary
    results = {}

    info = context["water build info"]

    # Adding freshwater supply constraints
    # Reading data, the data is spatially and temprally aggregated from GHMs
    df_sw, df_gw = read_water_availability(context)

    # reading sample for assiging basins
    PATH = package_data_path(
        "water", "delineation", f"basins_by_region_simpl_{context.regions}.csv"
    )
    df_x = pd.read_csv(PATH)

    eflow_dmd_df = []
    for r in E_FLOW_RULES_DMD.get_rule():
        dmd_df = run_standard(
            r,
            base_args={"rule_dfs": df_sw},
        )
        eflow_dmd_df.append(dmd_df)
    dmd_df = safe_concat(eflow_dmd_df)
    dmd_df = dmd_df[dmd_df["year"] >= 2025].reset_index(drop=True)
    dmd_df["value"] = dmd_df["value"].apply(lambda x: x if x >= 0 else 0)

    match context.time:
        case "year":
            # Reading data, the data is spatially and temporally aggregated from GHMs
            monthly = False
            path = package_data_path(
                "water",
                "availability",
                f"e-flow_{context.RCP}_{context.regions}.csv",
            )
        case "month":
            monthly = True
            path = package_data_path(
                "water",
                "availability",
                f"e-flow_5y_m_{context.RCP}_{context.regions}.csv",
            )
        case _:
            raise ValueError(f"Invalid time: {context.time}")

    df_env = pd.read_csv(path)
    df_env = _e_flow_preprocess(df_env, df_x, context, info, monthly)

    # Return a processed dataframe for env flow calculations
    if context.SDG != "baseline":
        # dataframe to put constraints on env flows
        eflow_df = []
        for r in E_FLOW_RULES_BOUND.get_rule():
            base_args = {
                "rule_dfs": df_env,
            }
            eflow_df.append(run_standard(r, base_args))
        eflow_df = safe_concat(eflow_df)

        eflow_df["value"] = eflow_df["value"].apply(lambda x: x if x >= 0 else 0)
        eflow_df = eflow_df[eflow_df["year_act"] >= 2025].reset_index(drop=True)

        dmd_df.sort_values(by=["node", "year"], inplace=True)
        dmd_df.reset_index(drop=True, inplace=True)
        eflow_df.sort_values(by=["node_loc", "year_act"], inplace=True)
        eflow_df.reset_index(drop=True, inplace=True)

        # Cap environmental flow requirements at 70% of water demand.

        eflow_df["value"] = np.where(
            eflow_df["value"] >= WS_CONST["MAX_ENV_FLOW_RATIO"] * dmd_df["value"],
            WS_CONST["MAX_ENV_FLOW_RATIO"] * dmd_df["value"],
            eflow_df["value"],
        )

        results["bound_activity_lo"] = eflow_df

    return results
