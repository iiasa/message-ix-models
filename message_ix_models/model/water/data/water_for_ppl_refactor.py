"""Prepare data for water use for cooling & energy technologies."""

import logging
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yaml
from message_ix import Scenario, make_df

from message_ix_models import Context
from message_ix_models.model.water.data.water_supply import map_basin_region_wat
from message_ix_models.tools.costs.config import Config
from message_ix_models.tools.costs.projections import create_cost_projections
from message_ix_models.util import (
    broadcast,
    make_matched_dfs,
    minimum_version,
    package_data_path,
    same_node,
)

log = logging.getLogger(__name__)

# --- Configuration ---
# Set to False to use the original implementations for debugging
USE_REFACTORED_IMPLEMENTATION = True
# --- End Configuration ---


# --- Constants ---
# File Names
TECH_PERFORMANCE_FILE = "tech_water_performance_ssp_msg.csv"
COST_SHARES_FILE_PATTERN = "cooltech_cost_and_shares_ssp_msg_{regions}.csv"
COST_SHARES_COUNTRY_FILE = "cooltech_cost_and_shares_country.csv"
BASIN_DELINEATION_FILE_PATTERN = "basins_by_region_simpl_{regions}.csv"
SSP_CONFIG_FILE = "ssp.yaml"
COOLING_IMPACT_FILE = "power_plant_cooling_impact_MESSAGE.xlsx"

# Parameter Names
PARAM_INPUT = "input"
PARAM_OUTPUT = "output"
PARAM_EMISSION_FACTOR = "emission_factor"
PARAM_CAPACITY_FACTOR = "capacity_factor"
PARAM_TECHNICAL_LIFETIME = "technical_lifetime"
PARAM_INV_COST = "inv_cost"
PARAM_ADDON_CONVERSION = "addon_conversion"
PARAM_ADDON_LO = "addon_lo"
PARAM_SHARE_COMMODITY_UP = "share_commodity_up"
PARAM_HIST_ACTIVITY = "historical_activity"
PARAM_HIST_NEW_CAPACITY = "historical_new_capacity"
PARAM_INIT_ACTIVITY_UP = "initial_activity_up"
PARAM_INIT_ACTIVITY_LO = "initial_activity_lo"
PARAM_INIT_NEW_CAPACITY_UP = "initial_new_capacity_up"
PARAM_SOFT_ACTIVITY_UP = "soft_activity_up"
PARAM_SOFT_ACTIVITY_LO = "soft_activity_lo"
PARAM_SOFT_NEW_CAPACITY_UP = "soft_new_capacity_up"
PARAM_LEVEL_COST_ACTIVITY_SOFT_UP = "level_cost_activity_soft_up"
PARAM_LEVEL_COST_ACTIVITY_SOFT_LO = "level_cost_activity_soft_lo"
PARAM_GROWTH_ACTIVITY_LO = "growth_activity_lo"
PARAM_GROWTH_ACTIVITY_UP = "growth_activity_up"
PARAM_GROWTH_NEW_CAPACITY_UP = "growth_new_capacity_up"


# MESSAGE Sets & Keywords
COMMODITY_ELECTR = "electr"
COMMODITY_FRESHWATER = "freshwater"
COMMODITY_SALINE_PPL = "saline_ppl"
COMMODITY_SURFACEWATER_BASIN = "surfacewater_basin"
LEVEL_SECONDARY = "secondary"
LEVEL_WATER_SUPPLY = "water_supply"
LEVEL_SALINE_SUPPLY = "saline_supply"
LEVEL_SHARE = "share"
LEVEL_WATER_AVAIL_BASIN = "water_avail_basin"
LEVEL_DUMMY_SUPPLY = "dummy_supply"
LEVEL_COOLING = "cooling"
EMISSION_FRESH_RETURN = "fresh_return"
MODE_STANDARD = "M1"
TIME_YEAR = "year"
UNIT_GWa = "GWa"
UNIT_MCM_GWa = "MCM/GWa"
UNIT_YEAR = "year"
UNIT_DIMENSIONLESS = "-"
TECH_GROUP_COOLING = "cooling"

# Technology Name Parts/Suffixes
SUFFIX_OT_FRESH = "__ot_fresh"
SUFFIX_CL_FRESH = "__cl_fresh"
SUFFIX_AIR = "__air"
SUFFIX_OT_SALINE = "__ot_saline"
TECH_SUFFIXES = [SUFFIX_OT_FRESH, SUFFIX_CL_FRESH, SUFFIX_AIR, SUFFIX_OT_SALINE]

# Other Constants
SECONDS_PER_YEAR = 60 * 60 * 24 * 365
MCM_PER_M3 = 1e-6
DEFAULT_TECH_LIFETIME = 30
HIST_YEAR_REF = 2015
HIST_YEAR_FALLBACKS = [2020, 2010, 2030, 2050, 2000, 2080, 1990]
SHARE_FLEXIBILITY = 1.05
ZERO_SHARE_REPLACEMENT = 0.0001
MIN_SALINE_SHARE_FUT = 0.45
SALINE_SHARE_YEAR_THRESHOLD = 2050
CAP_FACT_MULTIPLIER_FLEXIBILITY = 1.2
SHARE_CALIB_YEARS = [2020, 2025]


# --- Helper Functions (Existing, Refined) ---


def missing_tech(x: pd.Series) -> pd.Series:
    """Assign default input efficiencies for technologies missing them in the scenario.

    Uses a predefined dictionary for specific technology substrings.
    """
    # Predefined efficiencies (input/output)
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

    tech = x["technology"]
    current_value = x["value"]
    current_level = x["level"]

    if pd.notna(tech):
        matched_key = next((key for key in data_dic if key in tech), None)
        if matched_key:
            default_value = data_dic[matched_key]
            # Use the higher efficiency if the current one is very low (avoids <1 input)
            value = max(current_value, default_value) if current_value < 1 else default_value
            # Preserve original level unless it was 'cooling' (legacy behavior)
            level = LEVEL_DUMMY_SUPPLY if current_level == LEVEL_COOLING else current_level
            return pd.Series({"value": value, "level": level})

    # Return original values if no match or tech is NaN
    return pd.Series({"value": current_value, "level": current_level})


def cooling_fr(x: pd.Series) -> float:
    """Calculate cooling fraction (heat rejection relative to useful output).

    Assumes 10% flue gas losses for non-heat-plant technologies.
    """
    # Input efficiency (energy in / energy out)
    input_efficiency = x["value"]

    # Heat plants: Cooling = Input - Heat Output (already accounts for losses)
    # Assuming Heat Output = 1 unit for normalization
    is_heat_plant = "parent_tech" in x and "hpl" in x["parent_tech"]
    if is_heat_plant:
        return max(input_efficiency - 1, 0)  # Ensure non-negative

    # Other plants: Cooling = Input - Flue Losses - Useful Output
    # Assuming Useful Output = 1 unit for normalization
    flue_gas_loss_fraction = 0.1
    cooling_fraction = (
        input_efficiency - (input_efficiency * flue_gas_loss_fraction) - 1
    )
    return max(cooling_fraction, 0)  # Ensure non-negative


def shares(
    x: pd.Series,
    context: "Context",
    search_cols_cooling_fraction: list,
    hold_df: pd.DataFrame,
    search_cols: list,
) -> pd.Series:
    """Applies cooling fraction multipliers to regional share data.

    Modifies the input Series `x` in place for regional columns, then returns a
    new Series with the updated values.
    """
    tech = x["technology"]
    output_data = x.to_dict()

    for region_col in search_cols_cooling_fraction:
        # Map column name if needed (country context)
        node_loc = (
            context.map_ISO_c[region_col] if context.type_reg == "country" else region_col
        )

        # Find the corresponding cooling fraction for this technology and node
        cooling_fraction_series = hold_df.loc[
            (hold_df["node_loc"] == node_loc) & (hold_df["technology_name"] == tech),
            "cooling_fraction",
        ]

        if cooling_fraction_series.empty:
            log.debug(
                f"No cooling_fraction found for node_loc: {node_loc}, "
                f"technology: {tech}. Using 0."
            )
            cooling_fraction = 0.0
        else:
            # Take the first value if multiple matches (shouldn't happen with drop_duplicates)
            cooling_fraction = cooling_fraction_series.iloc[0]

        # Multiply the share value by the cooling fraction
        output_data[region_col] *= cooling_fraction

    # Construct the output Series ensuring correct types
    results = {
        key: val if isinstance(val, str) else float(val)
        for key, val in output_data.items()
    }

    return pd.Series(results, index=search_cols)


def apply_act_cap_multiplier(
    df: pd.DataFrame,
    hold_cost: pd.DataFrame,
    cap_fact_parent: Optional[pd.DataFrame] = None,
    param_name: str = "",
) -> pd.DataFrame:
    """Applies share and capacity factor multipliers to historical/initial parameters.

    Modifies parameter values based on cooling technology shares (`hold_cost`) and,
    for capacity-related parameters, the parent technology's capacity factor.
    """
    if df.empty:
        log.warning(f"Input df for apply_act_cap_multiplier for {param_name} is empty.")
        return df

    # Prepare share multipliers (hold_cost) in long format
    id_vars = ["utype", "technology"]
    region_cols = [col for col in hold_cost.columns if col not in id_vars]
    if not region_cols:
        log.warning(f"No region columns found in hold_cost for {param_name}.")
        # Return df unmodified or handle error appropriately
        return df.drop(columns=[c for c in ['utype', 'multiplier'] if c in df.columns], errors='ignore')

    hold_cost_long = hold_cost.melt(
        id_vars=id_vars,
        var_name="node_loc",
        value_name="multiplier",
        value_vars=region_cols, # Explicitly specify value_vars
    )

    # Merge multipliers with the parameter data
    # Ensure 'node_loc' exists and has compatible type in df
    if 'node_loc' not in df.columns:
         # Attempt to identify the correct node column if named differently, e.g., 'node'
        node_col = next((c for c in df.columns if 'node' in c and c != 'node_origin'), None)
        if node_col:
            log.debug(f"Using column '{node_col}' as 'node_loc' for merge in {param_name}")
            df = df.rename(columns={node_col: 'node_loc'})
        else:
            log.error(f"'node_loc' column missing in df for parameter {param_name}. Cannot apply multipliers.")
            return pd.DataFrame(columns=df.columns) # Return empty DataFrame with original columns


    # Ensure data types are compatible for merge
    df['node_loc'] = df['node_loc'].astype(str)
    hold_cost_long['node_loc'] = hold_cost_long['node_loc'].astype(str)
    df['technology'] = df['technology'].astype(str)
    hold_cost_long['technology'] = hold_cost_long['technology'].astype(str)

    # Perform the merge
    merged_df = df.merge(hold_cost_long, on=["node_loc", "technology"], how="left")


    # Apply share multiplier: NewValue = OriginalValue * ShareMultiplier
    # Fill missing multipliers with 1 (or 0 depending on desired behavior - 1 seems safer)
    merged_df["multiplier"].fillna(1, inplace=True)
    # Ensure value is numeric
    merged_df["value"] = pd.to_numeric(merged_df["value"], errors='coerce')
    merged_df["value"] *= merged_df["multiplier"]


    # Apply parent capacity factor adjustment for capacity parameters
    # NewValue = NewValue * ParentCapFactor * FlexibilityFactor
    is_capacity_param = any(kw in param_name for kw in ["capacity", "Capacity"])

    if is_capacity_param and cap_fact_parent is not None and not cap_fact_parent.empty:
        # Prepare cap_fact_parent for merge
        # Assuming cap_fact_parent has ['node_loc', 'utype', 'year_vtg', 'cap_fact']
        if 'utype' not in merged_df.columns:
             # Create 'utype' from 'technology' if missing (parent tech before '__')
             merged_df['utype'] = merged_df['technology'].str.split('__').str[0]

        # Ensure compatible types for merge
        cap_fact_parent['utype'] = cap_fact_parent['utype'].astype(str)
        cap_fact_parent['node_loc'] = cap_fact_parent['node_loc'].astype(str)
        cap_fact_parent['year_vtg'] = pd.to_numeric(cap_fact_parent['year_vtg'], errors='coerce').astype('Int64') # Use Int64 for nullable integers
        merged_df['utype'] = merged_df['utype'].astype(str)
        merged_df['node_loc'] = merged_df['node_loc'].astype(str)
        # Ensure year_vtg exists and is numeric, handling potential missing column or non-numeric data
        if 'year_vtg' in merged_df.columns:
             merged_df['year_vtg'] = pd.to_numeric(merged_df['year_vtg'], errors='coerce').astype('Int64')
        else:
             log.warning(f"'year_vtg' column missing in df for capacity parameter {param_name}. Cannot apply capacity factor.")
             # Decide how to handle: skip CF adjustment, error out, or use a default year? Skipping for now.


        # Merge capacity factors only if 'year_vtg' is present in merged_df
        if 'year_vtg' in merged_df.columns:
            # Check if cap_fact_parent has the required columns before merge
            required_cf_cols = ['node_loc', 'utype', 'year_vtg', 'cap_fact']
            if all(col in cap_fact_parent.columns for col in required_cf_cols):
                 merged_df = merged_df.merge(
                     cap_fact_parent[required_cf_cols],
                     on=["node_loc", "utype", "year_vtg"],
                     how="left"
                 )
                 # Apply capacity factor adjustment
                 # Fill missing cap_fact with 1 (no adjustment)
                 merged_df["cap_fact"].fillna(1, inplace=True)
                 merged_df["value"] *= merged_df["cap_fact"] * CAP_FACT_MULTIPLIER_FLEXIBILITY
                 merged_df.drop(columns="cap_fact", inplace=True, errors='ignore')
            else:
                 log.warning(f"Capacity factor data (cap_fact_parent) is missing required columns: {required_cf_cols}. Skipping CF adjustment for {param_name}.")
        else:
            # If year_vtg was missing, we already logged a warning.
            pass # Continue without CF adjustment

    # Clean up: remove NaNs/empty strings introduced, drop helper columns
    initial_rows = len(merged_df)
    # Explicitly check for NaN and potentially empty strings if 'value' can be object type
    if pd.api.types.is_numeric_dtype(merged_df['value']):
        invalid_values = merged_df['value'].isna() | (merged_df['value'] <= 0) # Filter non-positive values too
    else:
         # Handle object type: check for NaN, None, empty strings
        invalid_values = merged_df['value'].isna() | (merged_df['value'].astype(str).str.strip() == "") | \
                        (pd.to_numeric(merged_df['value'], errors='coerce') <= 0)


    if invalid_values.any():
        log.warning(
            f"Invalid/non-positive values found after applying multipliers for {param_name}. "
             f"Removing {invalid_values.sum()} rows. Example invalid row:\n"
             f"{merged_df[invalid_values].head(1)}"
        )
        merged_df = merged_df[~invalid_values].copy() # Use .copy() to avoid SettingWithCopyWarning
        log.info(f"Parameter {param_name}: {initial_rows - len(merged_df)} rows removed due to invalid values.")


    # Drop helper columns, ignore errors if they don't exist
    columns_to_drop = ["utype", "multiplier"]
    merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df.columns], inplace=True)


    return merged_df


@lru_cache()
def _load_ssp_config(yaml_file_path: str) -> Optional[dict]:
    """Loads and caches the SSP configuration YAML file."""
    try:
        with open(yaml_file_path, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        log.error(f"SSP YAML file '{yaml_file_path}' not found.")
        return None
    except yaml.YAMLError as e:
        log.error(f"Error parsing SSP YAML file '{yaml_file_path}': {e}")
        return None


def cooling_shares_SSP_from_yaml(context: "Context") -> pd.DataFrame:
    """Generates 'share_commodity_up' DataFrame based on SSP assumptions from YAML."""
    yaml_file_path = package_data_path("water", SSP_CONFIG_FILE)
    yaml_data = _load_ssp_config(yaml_file_path)

    if not yaml_data:
        log.warning("Could not load SSP config. Returning empty DataFrame.")
        return pd.DataFrame()

    ssp = context.ssp
    macro_regions_map = yaml_data.get("macro-regions", {})
    scenarios = yaml_data.get("scenarios", {})

    if ssp not in scenarios:
        log.warning(
            f"SSP '{ssp}' not found in the 'scenarios' section of '{SSP_CONFIG_FILE}'. "
            "Cannot apply SSP-specific cooling shares."
        )
        return pd.DataFrame()

    ssp_cooling_data = scenarios[ssp].get("cooling_tech", {})
    if not ssp_cooling_data:
        log.info(f"No 'cooling_tech' data found for SSP '{ssp}'.")
        return pd.DataFrame()

    all_shares_df = []
    info = context["water build info"]
    # Constraint years: 2050 and later within the model horizon
    year_constraint = [year for year in info.Y if year >= SALINE_SHARE_YEAR_THRESHOLD]
    model_nodes = info.N # All nodes in the model context

    for macro_region, region_data in ssp_cooling_data.items():
        share_data = region_data.get(PARAM_SHARE_COMMODITY_UP, {})
        if not share_data:
            continue

        # Get the list of specific model nodes belonging to this macro-region
        macro_region_nodes = macro_regions_map.get(macro_region, [])
        # Filter these nodes to include only those present in the current model context
        relevant_nodes = [
            node for node in model_nodes if any(node.endswith(mr_node) for mr_node in macro_region_nodes)
        ]

        if not relevant_nodes:
            log.debug(f"No model nodes match macro-region '{macro_region}' definitions.")
            continue

        for share_name, share_value in share_data.items():
            df_share = make_df(
                PARAM_SHARE_COMMODITY_UP,
                shares=[share_name], # Note: 'shares' is the column name in make_df
                time=[TIME_YEAR],
                value=[share_value],
                unit=[UNIT_DIMENSIONLESS],
            ).pipe(
                broadcast,
                year_act=year_constraint,
                node_share=relevant_nodes # Use the filtered list of model nodes
            )
            all_shares_df.append(df_share)

    if not all_shares_df:
        log.info(f"No specific SSP share constraints generated for SSP '{ssp}'.")
        return pd.DataFrame()

    return pd.concat(all_shares_df, ignore_index=True)


def _compose_capacity_factor(inp: pd.DataFrame, context: "Context") -> pd.DataFrame:
    """Creates the 'capacity_factor' parameter, optionally applying climate impacts."""
    # Start with default capacity factor of 1
    if inp.empty:
        log.warning("Input 'inp' for _compose_capacity_factor is empty. Returning empty DataFrame.")
        return pd.DataFrame()

    cap_fact_base = make_matched_dfs(inp, capacity_factor=1)[PARAM_CAPACITY_FACTOR]

    if context.RCP == "no_climate":
        log.info("No climate impacts applied to capacity factors (RCP = no_climate).")
        return cap_fact_base
    else:
        log.info(f"Applying climate impacts ({context.RCP}) to freshwater cooling capacity factors.")
        # Construct path to the climate impact Excel file
        try:
            impact_file_path = package_data_path(
                "water", "ppl_cooling_tech", COOLING_IMPACT_FILE
            )
            sheet_name = f"{context.regions}_{context.RCP}"
            df_impact = pd.read_excel(impact_file_path, sheet_name=sheet_name)
        except FileNotFoundError:
            log.error(f"Climate impact file not found: {impact_file_path}")
            return cap_fact_base
        except ValueError as e: # Handles incorrect sheet name
             log.error(f"Error reading climate impact sheet '{sheet_name}' from {impact_file_path}: {e}")
             log.warning("Proceeding without climate impact adjustments.")
             return cap_fact_base


        df = cap_fact_base.copy()

        # Ensure 'year_act' is numeric for comparisons
        df["year_act"] = pd.to_numeric(df["year_act"], errors='coerce')
        df_impact_melted = df_impact.melt(id_vars=['node'], var_name='period', value_name='impact_factor')

        # Define period boundaries based on column names (e.g., '2025s')
        df_impact_melted['start_year'] = df_impact_melted['period'].str.extract(r'(\d{4})').astype(int)
        # Define end year (e.g., 2025s covers 2025-2049) - adjust logic as needed
        # Assuming '2025s' -> 2025-2049, '2050s' -> 2050-2069, '2070s' -> 2070+
        period_map = {
            '2025s': (2025, 2050),
            '2050s': (2050, 2070),
            '2070s': (2070, 9999) # Use a large number for infinity
        }

        df_impact_melted[['period_start', 'period_end']] = df_impact_melted['period'].map(period_map).apply(pd.Series)


        # Apply impacts iteratively or via merge (merge is generally faster)
        df['impact_factor'] = 1.0 # Default: no impact

        for _, row in df_impact_melted.dropna(subset=['period_start', 'period_end']).iterrows():
            node = row['node']
            start_year = row['period_start']
            end_year = row['period_end']
            impact = row['impact_factor']

            # Conditions for applying the impact
            mask = (
                df["technology"].str.contains("fresh") & # Only freshwater technologies
                (df["year_act"] >= start_year) &
                (df["year_act"] < end_year) &
                (df["node_loc"] == node)
            )
            df.loc[mask, 'impact_factor'] = impact

        # Apply the impact factor (adjusting the base capacity factor of 1)
        df["value"] *= df['impact_factor']
        df.drop(columns='impact_factor', inplace=True)

        # Log how many values were changed
        changed_count = (df["value"] != 1.0).sum()
        log.info(f"Applied climate impacts to {changed_count} capacity factor entries.")

        return df


# --- Refactored Internal Helper Functions for cool_tech ---

def _load_and_prepare_cooling_data(
    context: "Context", scen: Scenario
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Loads cooling tech performance data, merges with scenario inputs/outputs,
       calculates efficiencies, cooling fractions, and water metrics."""

    log.info("Loading and preparing cooling technology performance data.")
    # Load base cooling technology data
    perf_path = package_data_path("water", "ppl_cooling_tech", TECH_PERFORMANCE_FILE)
    try:
        df_perf = pd.read_csv(perf_path)
    except FileNotFoundError:
        log.exception(f"Cooling performance file not found at {perf_path}")
        raise

    cooling_df = df_perf.loc[df_perf["technology_group"] == TECH_GROUP_COOLING].copy()
    cooling_df["parent_tech"] = cooling_df["technology_name"].str.split("__").str[0]
    parent_techs = cooling_df["parent_tech"].unique()

    # Get parent technology input/output data from scenario
    log.debug(f"Fetching input/output for parent techs: {parent_techs}")
    ref_input = scen.par(PARAM_INPUT, {"technology": parent_techs})
    if ref_input.empty:
        log.warning(f"No '{PARAM_INPUT}' data found for parent technologies.")
        # Handle potentially missing techs - check output as well
        missing_parent_tec = parent_techs
    else:
        missing_parent_tec = parent_techs[~np.isin(parent_techs, ref_input["technology"].unique())]


    ref_output = pd.DataFrame()
    if len(missing_parent_tec) > 0:
        log.debug(f"Checking '{PARAM_OUTPUT}' for missing parent techs: {missing_parent_tec}")
        ref_output = scen.par(PARAM_OUTPUT, {"technology": missing_parent_tec})
        if not ref_output.empty:
            # Align columns for concatenation
             # Identify common columns, prioritizing those in ref_input
            common_cols = list(ref_input.columns) if not ref_input.empty else list(ref_output.columns)
            ref_output = ref_output.rename(columns={ # Simple rename heuristic
                c_out: c_in for c_in, c_out in zip(ref_input.columns, ref_output.columns) if c_in != c_out
            })
             # Ensure all necessary columns exist, adding NaNs if needed
            for col in common_cols:
                 if col not in ref_output.columns:
                     ref_output[col] = pd.NA

            ref_output = ref_output[common_cols] # Reorder and select


    # Combine input and output data for parents
    ref_combined = pd.concat([ref_input, ref_output], ignore_index=True)

    # Apply default efficiencies where missing
    log.debug("Applying default efficiencies for missing parent tech data.")
    if not ref_combined.empty:
        value_level_df = ref_combined.apply(missing_tech, axis=1)
        # Check if the DataFrame returned by apply has the expected columns
        if not value_level_df.empty and all(col in value_level_df.columns for col in ['value', 'level']):
             ref_combined[["value", "level"]] = value_level_df
        else:
             log.warning("Applying missing_tech did not return expected columns. Skipping update.")


    # Combine base cooling data with parent efficiency data
    # Use parent tech as index for combining
    input_cool = (
        cooling_df.set_index("parent_tech")
        .combine_first(ref_combined.set_index("technology"))
        .reset_index()
        .rename(columns={"index": "parent_tech"}) # Ensure parent_tech column is named correctly
    )


    # Data Cleaning and Preparation
    input_cool.dropna(subset=["value"], inplace=True) # Drop rows where efficiency is missing
    input_cool[["year_vtg", "year_act"]] = input_cool[["year_vtg", "year_act"]].astype(int)

    # Filter out non-power plants and irrelevant levels
    input_cool = input_cool[
        (input_cool["level"] != LEVEL_WATER_SUPPLY) & (input_cool["level"] != LEVEL_COOLING) # Legacy filter
    ]
    input_cool = input_cool[~input_cool["technology_name"].str.contains("hpl", na=False)]

    # Handle global node names (replace with origin/destination)
    glb_node = f"{context.regions}_GLB"
    is_glb_loc = input_cool["node_loc"] == glb_node
    input_cool.loc[is_glb_loc, "node_loc"] = input_cool.loc[is_glb_loc, "node_origin"]
    is_glb_origin = input_cool["node_origin"] == glb_node
    input_cool.loc[is_glb_origin, "node_origin"] = input_cool.loc[is_glb_origin, "node_loc"]

    # Calculate Cooling Fraction
    log.debug("Calculating cooling fractions.")
    input_cool["cooling_fraction"] = input_cool.apply(cooling_fr, axis=1)
    input_cool["cooling_fraction"] = input_cool["cooling_fraction"].replace([np.inf, -np.inf], np.nan).fillna(0) # Handle potential division by zero in cooling_fr if value is 0


    # Calculate Water Withdrawal/Return/Consumption Rates per GWA of COOLING needed
    log.debug("Calculating water metrics per GWA of cooling.")
    # Ensure cooling_fraction is not zero to avoid division errors
    safe_cooling_fraction = np.where(input_cool["cooling_fraction"] <= 0, 1e-9, input_cool["cooling_fraction"]) # Use small number instead of 0

    input_cool["value_cool"] = ( # Water withdrawal per GWA cooling
        input_cool["water_withdrawal_mid_m3_per_output"]
        * SECONDS_PER_YEAR * MCM_PER_M3
        / safe_cooling_fraction
    )
    input_cool["value_cool"] = np.where(input_cool["value_cool"] < 0, 1e-6, input_cool["value_cool"])


    # Calculate Return Rate (fraction of withdrawal returned)
    # Handle division by zero if withdrawal is zero
    safe_withdrawal = np.where(input_cool["water_withdrawal_mid_m3_per_output"] <= 0, 1e-9, input_cool["water_withdrawal_mid_m3_per_output"])
    input_cool["return_rate"] = 1 - (
        input_cool["water_consumption_mid_m3_per_output"] / safe_withdrawal
    )
    # Ensure return rate is within [0, 1]
    input_cool["return_rate"] = np.clip(input_cool["return_rate"], 0, 1)


    # Water returned per GWA cooling
    input_cool["value_return"] = input_cool["return_rate"] * input_cool["value_cool"]
    input_cool["value_return"] = np.where(input_cool["value_return"] < 0, 0, input_cool["value_return"]) # Ensure non-negative


    # Water consumed per GWA cooling (for potential reporting, not direct use in params yet)
    input_cool["consumption_rate"] = 1 - input_cool["return_rate"]
    input_cool["value_consumption"] = input_cool["consumption_rate"] * input_cool["value_cool"]
    input_cool["value_consumption"] = np.where(input_cool["value_consumption"] < 0, 0, input_cool["value_consumption"]) # Ensure non-negative


    # Calculate Parasitic Electricity Demand per GWA of COOLING needed
    log.debug("Calculating parasitic electricity demand per GWA of cooling.")
    input_cool["parasitic_demand_per_cool"] = (
         input_cool["parasitic_electricity_demand_fraction"] / safe_cooling_fraction
     )
    input_cool["parasitic_demand_per_cool"] = np.where(
         input_cool["parasitic_demand_per_cool"] < 0, 1e-6, input_cool["parasitic_demand_per_cool"]
     )


    log.info(f"Prepared initial cooling data with {len(input_cool)} rows.")
    return input_cool, cooling_df


def _create_input_parameter(input_cool: pd.DataFrame) -> pd.DataFrame:
    """Creates the 'input' parameter DataFrame for cooling technologies."""
    log.info("Creating 'input' parameter data.")
    # Technologies with parasitic electricity demand
    electr_df = input_cool[input_cool["parasitic_demand_per_cool"] > 1e-9].copy()
    inp_elec = make_df(
        PARAM_INPUT,
        node_loc=electr_df["node_loc"],
        technology=electr_df["technology_name"],
        year_vtg=electr_df["year_vtg"],
        year_act=electr_df["year_act"],
        mode=electr_df["mode"],
        node_origin=electr_df["node_origin"],
        commodity=COMMODITY_ELECTR,
        level=LEVEL_SECONDARY,
        time=TIME_YEAR,
        time_origin=TIME_YEAR,
        value=electr_df["parasitic_demand_per_cool"],
        unit=UNIT_GWa, # GWA elec per GWA cooling
    )

    # Freshwater technologies (Once-through and Closed-loop)
    is_saline = input_cool["technology_name"].str.endswith(SUFFIX_OT_SALINE, na=False)
    is_air = input_cool["technology_name"].str.endswith(SUFFIX_AIR, na=False)
    fresh_df = input_cool[(~is_saline) & (~is_air)].copy()
    inp_fresh = make_df(
        PARAM_INPUT,
        node_loc=fresh_df["node_loc"],
        technology=fresh_df["technology_name"],
        year_vtg=fresh_df["year_vtg"],
        year_act=fresh_df["year_act"],
        mode=fresh_df["mode"],
        node_origin=fresh_df["node_origin"], # Where the water comes from
        commodity=COMMODITY_FRESHWATER,
        level=LEVEL_WATER_SUPPLY,
        time=TIME_YEAR,
        time_origin=TIME_YEAR,
        value=fresh_df["value_cool"], # MCM water per GWA cooling
        unit=UNIT_MCM_GWa,
    )

    # Saline technologies
    saline_df = input_cool[is_saline].copy()
    inp_saline = make_df(
        PARAM_INPUT,
        node_loc=saline_df["node_loc"],
        technology=saline_df["technology_name"],
        year_vtg=saline_df["year_vtg"],
        year_act=saline_df["year_act"],
        mode=saline_df["mode"],
        node_origin=saline_df["node_origin"], # Where saline water comes from
        commodity=COMMODITY_SALINE_PPL, # Specific commodity for saline
        level=LEVEL_SALINE_SUPPLY,
        time=TIME_YEAR,
        time_origin=TIME_YEAR,
        value=saline_df["value_cool"], # MCM saline water per GWA cooling
        unit=UNIT_MCM_GWa,
    )

    # Combine all input types
    inp_all = pd.concat([inp_elec, inp_fresh, inp_saline], ignore_index=True)
    inp_all.dropna(subset=["value"], inplace=True)
    log.info(f"Created 'input' parameter with {len(inp_all)} entries.")
    return inp_all


def _create_emission_factor_parameter(input_cool: pd.DataFrame) -> pd.DataFrame:
    """Creates the 'emission_factor' parameter for freshwater return flows."""
    log.info("Creating 'emission_factor' parameter for water return.")
    # Include all water-using techs (fresh and saline) for return flow 'emission'
    # Air cooling does not have water return flows
    is_air = input_cool["technology_name"].str.endswith(SUFFIX_AIR, na=False)
    return_df = input_cool[~is_air].copy()

    emi = make_df(
        PARAM_EMISSION_FACTOR,
        node_loc=return_df["node_loc"],
        technology=return_df["technology_name"],
        year_vtg=return_df["year_vtg"],
        year_act=return_df["year_act"],
        mode=return_df["mode"],
        emission=EMISSION_FRESH_RETURN, # Represents water returned to environment
        value=return_df["value_return"], # MCM returned per GWA cooling
        unit=UNIT_MCM_GWa,
    )
    emi.dropna(subset=["value"], inplace=True)
    log.info(f"Created 'emission_factor' parameter with {len(emi)} entries.")
    return emi

def _create_output_parameter(
    context: "Context", input_cool: pd.DataFrame, info, df_node: pd.DataFrame
) -> pd.DataFrame:
    """Creates the 'output' parameter, including base cooling output and nexus return flows."""
    log.info("Creating 'output' parameter data.")

    # Base output: Commodity representing the type of cooling provided (for shares)
    out_base = make_df(
        PARAM_OUTPUT,
        node_loc=input_cool["node_loc"],
        technology=input_cool["technology_name"],
        year_vtg=input_cool["year_vtg"],
        year_act=input_cool["year_act"],
        mode=input_cool["mode"],
        node_dest=input_cool["node_origin"], # Output 'cooling type' usable in the node
        commodity=input_cool["technology_name"].str.split("__").str[1], # e.g., 'ot_fresh'
        level=LEVEL_SHARE, # Used for share constraints
        time=TIME_YEAR,
        time_dest=TIME_YEAR,
        value=1, # Fixed value for share definition
        unit=UNIT_DIMENSIONLESS,
    )

    # Nexus-specific output: Return flow distribution to basins
    out_nexus = pd.DataFrame()
    if context.nexus_set == "nexus":
        log.info("Adding nexus-specific water return flows to basins.")
        # Prepare surface water availability shares by basin
        df_sw = map_basin_region_wat(context)
        # Expected columns: region, node, time, year, share, (mode, date, MSGREG - dropped)
        df_sw = df_sw.rename(columns={
            "region": "node_dest", # Basin node is the destination
            "time": "time_dest",
            "year": "year_act" # Year for matching with technology activity
        }).drop(columns=["mode", "date", "MSGREG"], errors='ignore')
        df_sw["time_dest"] = df_sw["time_dest"].astype(str)


        # Filter for freshwater cooling technologies (excluding air/saline)
        is_saline = input_cool["technology_name"].str.endswith(SUFFIX_OT_SALINE, na=False)
        is_air = input_cool["technology_name"].str.endswith(SUFFIX_AIR, na=False)
        fresh_return_df = input_cool[(~is_saline) & (~is_air) & (input_cool["value_return"] > 0)].copy()

        # Check if df_node and df_sw are ready
        if df_node.empty or df_sw.empty or fresh_return_df.empty:
             log.warning("Missing data for nexus return flow calculation (df_node, df_sw, or fresh_return_df). Skipping.")
        else:
            nexus_outputs = []
            # Group by operating node to handle basin mapping efficiently
            for node_loc, group_df in fresh_return_df.groupby("node_loc"):
                 # Find basins associated with this operational node_loc
                 associated_basins = list(df_node[df_node["region"] == node_loc]["node"].unique()) # 'node' here is the basin ID like 'BXXX'
                 if not associated_basins:
                     log.debug(f"No basins found for node_loc '{node_loc}' in df_node. Skipping return flow.")
                     continue

                 # Create the base output structure for this group
                 out_group = make_df(
                     PARAM_OUTPUT,
                     node_loc=group_df["node_loc"],
                     technology=group_df["technology_name"],
                     year_vtg=group_df["year_vtg"],
                     year_act=group_df["year_act"],
                     mode=group_df["mode"],
                     commodity=COMMODITY_SURFACEWATER_BASIN,
                     level=LEVEL_WATER_AVAIL_BASIN,
                     time=TIME_YEAR,
                     value=group_df["value_return"], # MCM returned per GWA cooling
                     unit=UNIT_MCM_GWa,
                 )

                 # Broadcast to associated basins and all relevant sub-year time slices
                 out_group_bcast = out_group.pipe(
                     broadcast,
                     node_dest=associated_basins,
                     time_dest=context.time # Use context.time for sub-year slices
                 )

                 # Merge with basin water availability shares
                 # Ensure merge keys are correct type
                 out_group_bcast['node_dest'] = out_group_bcast['node_dest'].astype(str)
                 out_group_bcast['time_dest'] = out_group_bcast['time_dest'].astype(str)
                 out_group_bcast['year_act'] = out_group_bcast['year_act'].astype(int)
                 df_sw_filtered = df_sw[['node_dest', 'time_dest', 'year_act', 'share']].copy()
                 df_sw_filtered['node_dest'] = df_sw_filtered['node_dest'].astype(str)
                 df_sw_filtered['time_dest'] = df_sw_filtered['time_dest'].astype(str)
                 df_sw_filtered['year_act'] = df_sw_filtered['year_act'].astype(int)


                 out_merged = pd.merge(out_group_bcast, df_sw_filtered,
                                      on=["node_dest", "time_dest", "year_act"],
                                      how="left")


                 # Apply share: Value = ReturnFlow * BasinShare
                 # Fill missing shares with 0 (no flow if basin share not defined)
                 out_merged["share"].fillna(0, inplace=True)
                 out_merged["value"] *= out_merged["share"]
                 out_merged.drop(columns=["share"], inplace=True)
                 nexus_outputs.append(out_merged)


            if nexus_outputs:
                 out_nexus = pd.concat(nexus_outputs, ignore_index=True)
                 out_nexus.dropna(subset=["value"], inplace=True)
                 # Filter out zero values which are meaningless
                 out_nexus = out_nexus[out_nexus["value"] > 1e-9].copy()
                 log.info(f"Created {len(out_nexus)} nexus return flow output entries.")
            else:
                 log.info("No nexus return flow output entries generated.")


    # Combine base output and nexus output
    out_all = pd.concat([out_base, out_nexus], ignore_index=True)
    out_all.dropna(subset=["value"], inplace=True)
    # Filter out near-zero values that might cause solver issues
    out_all = out_all[out_all["value"].abs() > 1e-9].copy()


    log.info(f"Created 'output' parameter with {len(out_all)} total entries.")
    return out_all


def _load_cost_and_shares_data(context: Context) -> Optional[pd.DataFrame]:
    """Loads the cooling technology cost and shares CSV file."""
    if context.type_reg == "global":
        filename = COST_SHARES_FILE_PATTERN.format(regions=context.regions)
    else:
        filename = COST_SHARES_COUNTRY_FILE

    path = package_data_path("water", "ppl_cooling_tech", filename)
    log.info(f"Loading cost and shares data from: {path}")
    try:
        cost_share_df = pd.read_csv(path)
        # Basic validation
        if 'utype' not in cost_share_df.columns or 'cooling' not in cost_share_df.columns:
            log.error(f"Missing required columns 'utype' or 'cooling' in {filename}")
            return None
        cost_share_df["technology"] = cost_share_df["utype"] + "__" + cost_share_df["cooling"]
        cost_share_df["share_name_calib"] = "share_calib_" + cost_share_df["utype"] + "_" + cost_share_df["cooling"]

        return cost_share_df
    except FileNotFoundError:
        log.exception(f"Cost and shares file not found: {path}")
        return None
    except Exception as e:
        log.exception(f"Failed to load or process cost/shares file {path}: {e}")
        return None

def _prepare_historical_input_data(input_cool: pd.DataFrame) -> pd.DataFrame:
    """Prepares a representative input_cool dataframe for the historical reference year."""
    log.debug(f"Preparing historical input data reference ({HIST_YEAR_REF}).")
    # Prioritize the reference year
    input_cool_hist = input_cool[
        (input_cool["year_act"] == HIST_YEAR_REF) & (input_cool["year_vtg"] == HIST_YEAR_REF)
    ].copy()

    # Identify unique parent_tech/node_loc combinations needed
    required_combos = set(zip(input_cool["parent_tech"], input_cool["node_loc"]))
    # Identify combos present in the reference year data
    hist_combos = set(zip(input_cool_hist["parent_tech"], input_cool_hist["node_loc"]))

    missing_combos = required_combos - hist_combos

    if missing_combos:
        log.debug(f"{len(missing_combos)} parent_tech/node_loc combos missing from {HIST_YEAR_REF}. Searching fallback years.")
        found_rows_fallback = []
        remaining_combos = missing_combos.copy()

        for year in HIST_YEAR_FALLBACKS:
            if not remaining_combos: break # Stop if all filled

            log.debug(f"Checking fallback year {year} for {len(remaining_combos)} missing combos.")
            fallback_rows = input_cool[
                (input_cool["year_act"] == year) & (input_cool["year_vtg"] == year) &
                 input_cool.apply(lambda row: (row["parent_tech"], row["node_loc"]) in remaining_combos, axis=1)
            ].copy()

            if not fallback_rows.empty:
                # Update years to reference year
                fallback_rows["year_act"] = HIST_YEAR_REF
                fallback_rows["year_vtg"] = HIST_YEAR_REF
                found_rows_fallback.append(fallback_rows)

                # Update remaining combos
                found_in_year = set(zip(fallback_rows["parent_tech"], fallback_rows["node_loc"]))
                remaining_combos -= found_in_year
                log.debug(f"Found {len(found_in_year)} combos in {year}. {len(remaining_combos)} still missing.")

        if found_rows_fallback:
            input_cool_hist = pd.concat([input_cool_hist] + found_rows_fallback, ignore_index=True)

        if remaining_combos:
            log.warning(
                f"Could not find historical data ({HIST_YEAR_REF} or fallbacks) for "
                f"{len(remaining_combos)} parent_tech/node_loc combinations. "
                f"Example missing: {next(iter(remaining_combos))}"
            )

    # Use the prepared historical data (dropping duplicates just in case)
    input_cool_hist = input_cool_hist.drop_duplicates(subset=['node_loc', 'technology_name'])
    log.info(f"Prepared historical input data reference with {len(input_cool_hist)} unique node/tech entries.")
    return input_cool_hist


def _create_share_parameters(
    context: Context,
    cost_share_df: pd.DataFrame,
    input_cool: pd.DataFrame,
    input_cool_hist_ref: pd.DataFrame,
    info
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Creates 'share_commodity_up' constraints and calculates the 'hold_cost' multiplier DataFrame."""
    log.info("Creating share parameters and hold_cost multipliers.")

    # 1. Prepare 'hold_cost' DataFrame (shares * cooling_fraction multiplier)
    # These multipliers are applied to historical/initial activity/capacity
    log.debug("Preparing 'hold_cost' multipliers.")
    # Identify regional share columns (prefixed with 'mix_')
    mix_cols = [col for col in cost_share_df.columns if col.startswith("mix_")]
    if not mix_cols:
        log.error("No 'mix_' columns found in cost/share data. Cannot calculate hold_cost.")
        # Return empty DataFrames or handle error appropriately
        return pd.DataFrame(), pd.DataFrame()

    cost_regions_df = cost_share_df[["utype", "technology"] + mix_cols].copy()
    # Rename 'mix_REGION' columns to just 'REGION'
    cost_regions_df.rename(columns=lambda name: name.replace("mix_", ""), inplace=True)
    region_cols = [col for col in cost_regions_df.columns if col not in ["utype", "technology"]]

    # Prepare the historical cooling fraction data needed by the 'shares' function
    hold_df_hist_cf = input_cool_hist_ref[
        ["node_loc", "technology_name", "cooling_fraction"]
    ].drop_duplicates()


    # Apply the 'shares' function row-wise to calculate multipliers
    hold_cost = cost_regions_df.apply(
        shares, # The helper function defined earlier
        axis=1,
        context=context,
        search_cols_cooling_fraction=region_cols, # Columns to apply CF to
        hold_df=hold_df_hist_cf,                 # Historical CF lookup
        search_cols=cost_regions_df.columns,      # All columns to return
    )
    log.debug(f"'hold_cost' multipliers calculated with shape {hold_cost.shape}.")


    # 2. Create 'share_commodity_up' constraints
    log.debug("Creating 'share_commodity_up' constraints.")
    # Filter relevant columns for share calculation
    share_calib_filtered = cost_share_df[["utype", "share_name_calib"] + mix_cols].copy()

    # Melt to long format
    share_long = share_calib_filtered.melt(
        id_vars=["utype", "share_name_calib"],
        var_name="node_share",
        value_name="value",
        value_vars=mix_cols # Explicitly use mix_cols
    )


    # Filter for parent technologies present in the processed input_cool data
    valid_parents = input_cool["parent_tech"].unique()
    share_long = share_long[share_long["utype"].isin(valid_parents)].reset_index(drop=True)


    # Clean up node names and apply flexibility/floor
    share_long["node_share"] = share_long["node_share"].str.replace("mix_", "", regex=False)
    share_long["value"] *= SHARE_FLEXIBILITY
    share_long["value"] = share_long["value"].replace(0, ZERO_SHARE_REPLACEMENT)


    # Assign standard columns for make_df/broadcasting
    share_long.rename(columns={"share_name_calib": "shares"}, inplace=True) # 'shares' is the target column name
    share_long.drop(columns={"utype"}, inplace=True)
    share_long["time"] = TIME_YEAR
    share_long["unit"] = UNIT_DIMENSIONLESS


    # Create Calibration Shares (e.g., 2020, 2025)
    share_calib = share_long.copy()
    share_calib = share_calib.loc[share_calib.index.repeat(len(SHARE_CALIB_YEARS))].reset_index(drop=True)
    share_calib["year_act"] = SHARE_CALIB_YEARS * (len(share_calib) // len(SHARE_CALIB_YEARS))


    # Create Future Share Constraints (for saline tech, post-2050)
    years_fut = [year for year in info.Y if year not in SHARE_CALIB_YEARS and year >= SALINE_SHARE_YEAR_THRESHOLD]
    share_fut = pd.DataFrame() # Initialize as empty
    if years_fut:
        share_fut_base = share_long.copy()
        # Filter only shares related to saline cooling
        share_fut_base = share_fut_base[share_fut_base["shares"].str.contains("ot_saline")]

        if not share_fut_base.empty:
            share_fut = share_fut_base.loc[share_fut_base.index.repeat(len(years_fut))].reset_index(drop=True)
            share_fut["year_act"] = years_fut * (len(share_fut) // len(years_fut))
            # Apply minimum share floor for saline in the future
            share_fut["value"] = np.maximum(share_fut["value"], MIN_SALINE_SHARE_FUT)
            # Filter only years >= threshold (redundant due to years_fut definition but safe)
            # share_fut = share_fut[share_fut["year_act"] >= SALINE_SHARE_YEAR_THRESHOLD] # Already handled by years_fut


    # Combine Calibration, Future Saline, and SSP-based shares
    ssp_shares = cooling_shares_SSP_from_yaml(context)


    # Ensure all parts have the same columns before concat
    final_share_dfs = []
    expected_cols = [ "node_share", "shares", "value", "time", "unit", "year_act"] # From make_df output
    if not share_calib.empty:
        share_calib = share_calib.reindex(columns=expected_cols)
        final_share_dfs.append(share_calib)
    if not share_fut.empty:
        share_fut = share_fut.reindex(columns=expected_cols)
        final_share_dfs.append(share_fut)
    if not ssp_shares.empty:
        # Check if ssp_shares columns match expected_cols, rename/reindex if necessary
        # Assuming ssp_shares has the correct columns from make_df
        ssp_shares = ssp_shares.reindex(columns=expected_cols)
        final_share_dfs.append(ssp_shares)


    if not final_share_dfs:
        log.warning("No share_commodity_up constraints were generated.")
        results_share_commodity_up = pd.DataFrame()
    else:
        results_share_commodity_up = pd.concat(final_share_dfs, ignore_index=True)
        results_share_commodity_up.dropna(subset=["value", "year_act"], inplace=True)


    log.info(f"Created 'share_commodity_up' parameter with {len(results_share_commodity_up)} entries.")
    return results_share_commodity_up, hold_cost


def _create_cost_parameters(context: Context, techs_to_remove: List[str]) -> pd.DataFrame:
    """Generates 'inv_cost' parameter using the cost projection tools."""
    log.info("Creating cost parameters using cost projection tools.")
    # Set up configuration for cost projections
    cfg = Config(
        module=TECH_GROUP_COOLING, # 'cooling' module
        scenario=context.ssp,
        method="gdp", # As used in the original code
        node=context.regions,
    )

    try:
        # Generate projected costs (investment and fixed O&M)
        cost_proj = create_cost_projections(cfg)
    except Exception as e:
        log.exception(f"Failed to generate cost projections: {e}")
        return pd.DataFrame() # Return empty DataFrame on failure

    # Extract investment costs
    if PARAM_INV_COST not in cost_proj or cost_proj[PARAM_INV_COST].empty:
        log.warning("Cost projection did not return 'inv_cost' data.")
        return pd.DataFrame()

    inv_cost = cost_proj[PARAM_INV_COST][
        ["year_vtg", "node_loc", "technology", "value", "unit"] # Select relevant columns
    ].copy()

    # Filter out manually specified technologies to remove
    inv_cost = inv_cost[~inv_cost["technology"].isin(techs_to_remove)]

    # Keep only cooling technologies (containing '__')
    inv_cost = inv_cost[inv_cost["technology"].str.contains("__", na=False)]

    log.info(f"Created 'inv_cost' parameter with {len(inv_cost)} entries.")
    return inv_cost


def _create_addon_parameters(input_cool: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Generates 'addon_conversion' and 'addon_lo' parameters."""
    log.info("Creating addon conversion and lower bound parameters.")

    # Addon Conversion: Links parent activity to required cooling addon activity
    addon_conv_df = make_df(
        PARAM_ADDON_CONVERSION,
        node=input_cool["node_loc"],
        technology=input_cool["parent_tech"], # Parent technology
        year_vtg=input_cool["year_vtg"],
        year_act=input_cool["year_act"],
        mode=input_cool["mode"],
        time=TIME_YEAR,
        type_addon="cooling__" + input_cool["parent_tech"], # Addon identifier
        value=input_cool["cooling_fraction"], # Cooling needed per unit parent output
        unit=UNIT_DIMENSIONLESS,
    )
    # Handle NaNs or infinities that might result from cooling_fraction calculation
    addon_conv_df['value'] = addon_conv_df['value'].replace([np.inf, -np.inf], np.nan).fillna(0)
    addon_conv_df = addon_conv_df[addon_conv_df['value'] > 1e-9] # Keep only positive conversion factors

    # Addon Lower Bound: Allows 100% of parent activity (addon is optional/proportional)
    # Create based on the structure of addon_conversion
    if addon_conv_df.empty:
        log.warning("Addon conversion data is empty, cannot create addon_lo.")
        addon_lo_df = pd.DataFrame()
    else:
        # Use make_matched_dfs to ensure all dimensions are covered
        addon_lo_base = make_matched_dfs(addon_conv_df, addon_lo=1)
        addon_lo_df = addon_lo_base[PARAM_ADDON_LO]


    log.info(f"Created '{PARAM_ADDON_CONVERSION}' ({len(addon_conv_df)} entries) "
             f"and '{PARAM_ADDON_LO}' ({len(addon_lo_df)} entries).")
    return addon_conv_df, addon_lo_df


def _create_lifetime_parameter(
    inp: pd.DataFrame, info, node_region: np.ndarray
) -> pd.DataFrame:
    """Generates the 'technical_lifetime' parameter."""
    log.info("Creating 'technical_lifetime' parameter.")
    if inp.empty or node_region.size == 0:
        log.warning("Cannot create technical_lifetime: input data or node_region is empty.")
        return pd.DataFrame()

    # Get unique cooling technologies from the input parameter data
    cooling_techs = inp["technology"].unique()

    # Use model years for broadcasting vintage years
    model_years = info.Y # Or info.yv_ya.year_vtg.unique() ? Check 'info' structure
    # Filter years >= 1990 as in original code
    years_vtg = [y for y in model_years if y >= 1990]


    if not years_vtg:
        log.warning("No valid vintage years (>= 1990) found in model info.")
        return pd.DataFrame()


    tl = (
        make_df(
            PARAM_TECHNICAL_LIFETIME,
            technology=cooling_techs,
            value=DEFAULT_TECH_LIFETIME,
            unit=UNIT_YEAR,
        )
        .pipe(broadcast, year_vtg=years_vtg, node_loc=node_region)
        .pipe(same_node) # Ensure node_loc == node_origin if node_origin exists
    )

    log.info(f"Created 'technical_lifetime' parameter with {len(tl)} entries.")
    return tl


def _prepare_parent_capacity_factor(scen: Scenario, parent_techs: List[str]) -> pd.DataFrame:
    """Fetches and prepares parent technology capacity factors needed for scaling."""
    log.debug("Preparing parent technology capacity factors.")
    cap_fact_parent_raw = scen.par(PARAM_CAPACITY_FACTOR, {"technology": parent_techs})

    if cap_fact_parent_raw.empty:
        log.warning("Could not retrieve parent capacity factors from scenario.")
        return pd.DataFrame()

    first_model_year = scen.firstmodelyear

    # Prepare historical CFs (use minimum value per group)
    cf_hist = cap_fact_parent_raw[
        cap_fact_parent_raw["year_vtg"] < first_model_year
    ][["node_loc", "technology", "year_vtg", "value"]].copy()
    cf_hist = cf_hist.groupby(
        ["node_loc", "technology", "year_vtg"], as_index=False
    ).min()


    # Prepare future/model-year CFs (use year_act as proxy for vtg, take minimum)
    # This addresses parameters like initial_new_capacity_up defined by year_act
    cf_fut = cap_fact_parent_raw[
        cap_fact_parent_raw["year_act"] >= first_model_year
    ][["node_loc", "technology", "year_act", "value"]].copy()
    cf_fut = cf_fut.groupby(
        ["node_loc", "technology", "year_act"], as_index=False
    ).min()
    # Rename year_act to year_vtg for merging consistency
    cf_fut.rename(columns={"year_act": "year_vtg"}, inplace=True)

    # Combine historical and future, rename columns for use in apply_act_cap_multiplier
    cap_fact_parent = pd.concat([cf_hist, cf_fut], ignore_index=True)
    cap_fact_parent.rename(
        columns={"value": "cap_fact", "technology": "utype"}, inplace=True
    )

    # Ensure correct types for merging later
    cap_fact_parent['node_loc'] = cap_fact_parent['node_loc'].astype(str)
    cap_fact_parent['utype'] = cap_fact_parent['utype'].astype(str)
    cap_fact_parent['year_vtg'] = pd.to_numeric(cap_fact_parent['year_vtg'], errors='coerce').astype('Int64')
    cap_fact_parent['cap_fact'] = pd.to_numeric(cap_fact_parent['cap_fact'], errors='coerce')
    cap_fact_parent.dropna(subset=['node_loc', 'utype', 'year_vtg', 'cap_fact'], inplace=True)


    log.debug(f"Prepared parent capacity factor lookup table with {len(cap_fact_parent)} entries.")
    return cap_fact_parent

def _create_historical_and_bound_parameters(
    scen: Scenario,
    parent_techs: List[str],
    hold_cost: pd.DataFrame,
    cap_fact_parent: pd.DataFrame,
) -> Dict[str, pd.DataFrame]:
    """Extracts, expands, and scales historical, initial, and growth parameters."""
    log.info("Creating historical, initial, and growth/bound parameters.")
    results = {}

    # Define parameter groups
    params_to_fetch = [
        PARAM_HIST_ACTIVITY, PARAM_HIST_NEW_CAPACITY,
        PARAM_INIT_ACTIVITY_UP, PARAM_INIT_ACTIVITY_LO, PARAM_INIT_NEW_CAPACITY_UP,
        PARAM_SOFT_ACTIVITY_UP, PARAM_SOFT_ACTIVITY_LO, PARAM_SOFT_NEW_CAPACITY_UP,
        PARAM_LEVEL_COST_ACTIVITY_SOFT_UP, PARAM_LEVEL_COST_ACTIVITY_SOFT_LO,
        PARAM_GROWTH_ACTIVITY_LO, PARAM_GROWTH_ACTIVITY_UP, PARAM_GROWTH_NEW_CAPACITY_UP,
    ]
    # Parameters requiring multiplication by shares and potentially capacity factor
    params_to_multiply = [
        PARAM_HIST_ACTIVITY, PARAM_HIST_NEW_CAPACITY,
        PARAM_INIT_ACTIVITY_UP, PARAM_INIT_ACTIVITY_LO, PARAM_INIT_NEW_CAPACITY_UP,
    ]

    # Fetch parameters for parent technologies
    parent_params = {}
    for param_name in params_to_fetch:
        log.debug(f"Fetching parent parameter: {param_name}")
        df = scen.par(param_name, {"technology": parent_techs})
        if df is None or df.empty:
             log.warning(f"No data found for parent parameter: {param_name}")
             # Add empty dataframe to avoid KeyErrors later if needed, or skip
             parent_params[param_name] = pd.DataFrame() # Store empty df
        else:
             parent_params[param_name] = df


    # Expand parameters for each cooling technology suffix
    for param_name, base_df in parent_params.items():
        if base_df.empty:
             log.debug(f"Skipping expansion for empty parent parameter: {param_name}")
             results[param_name] = base_df # Store the empty df in results
             continue

        expanded_dfs = []
        for suffix in TECH_SUFFIXES:
            df_add = base_df.copy()
            # Ensure 'technology' column exists before modification
            if 'technology' in df_add.columns:
                df_add["technology"] = df_add["technology"] + suffix
                expanded_dfs.append(df_add)
            else:
                 log.error(f"Base DataFrame for parameter '{param_name}' is missing 'technology' column. Cannot expand.")
                 # Decide handling: skip this suffix, skip the param, or error out
                 # Skipping suffix for now
                 continue


        if not expanded_dfs:
             log.warning(f"No expanded DataFrames generated for {param_name}. Base df might lack 'technology' column or TECH_SUFFIXES is empty.")
             results[param_name] = pd.DataFrame(columns=base_df.columns) # Empty df with correct columns
             continue


        df_expanded = pd.concat(expanded_dfs, ignore_index=True)


        # Apply multipliers if needed
        if param_name in params_to_multiply:
            log.debug(f"Applying multipliers to expanded parameter: {param_name}")
            df_scaled = apply_act_cap_multiplier(
                df=df_expanded,
                hold_cost=hold_cost,
                cap_fact_parent=cap_fact_parent,
                param_name=param_name,
            )
            results[param_name] = df_scaled
            log.info(f"Generated scaled parameter '{param_name}' with {len(df_scaled)} entries.")
        else:
            # No scaling needed for bounds/costs, just use the expanded version
            results[param_name] = df_expanded
            log.info(f"Generated expanded parameter '{param_name}' with {len(df_expanded)} entries.")

    return results

# --- Main Refactored Function ---

def _cool_tech_refactored(context: "Context") -> Dict[str, pd.DataFrame]:
    """Refactored implementation of cool_tech."""
    log.info("--- Starting Refactored cool_tech ---")
    results: Dict[str, pd.DataFrame] = {}
    scen = context.get_scenario()
    info = context["water build info"] # General model info (years, nodes, etc.)

    # Load basin delineation mapping needed for nexus outputs and node lists
    try:
        delineation_file = BASIN_DELINEATION_FILE_PATTERN.format(regions=context.regions)
        delineation_path = package_data_path("water", "delineation", delineation_file)
        df_node = pd.read_csv(delineation_path)
        df_node["node"] = "B" + df_node["BCU_name"].astype(str) # Basin ID
        df_node["mode"] = MODE_STANDARD # Assuming standard mode for basins
        # Determine region naming convention based on context
        if context.type_reg == "country":
             # This mapping might need adjustment based on actual country node names
             # Assuming context.map_ISO_c maps region column (e.g., 'REGION') to country node
             region_col = 'REGION' # Adjust if the column name in CSV is different
             df_node["region"] = df_node[region_col].map(context.map_ISO_c).fillna(df_node[region_col])
        else:
             df_node["region"] = f"{context.regions}_" + df_node["REGION"].astype(str)
        node_region = df_node["region"].unique() # Unique MESSAGE node names
    except FileNotFoundError:
        log.exception(f"Basin delineation file not found: {delineation_path}")
        raise # Or handle gracefully depending on requirements
    except Exception as e:
        log.exception(f"Error loading or processing basin delineation file: {e}")
        raise


    # 1. Load and Prepare Base Cooling Data
    input_cool, cooling_df = _load_and_prepare_cooling_data(context, scen)
    parent_techs = list(cooling_df["parent_tech"].unique()) # Used multiple times

    # 2. Create Core Parameters derived directly from input_cool
    results[PARAM_INPUT] = _create_input_parameter(input_cool)
    results[PARAM_EMISSION_FACTOR] = _create_emission_factor_parameter(input_cool)
    # Pass df_node needed for nexus output generation
    results[PARAM_OUTPUT] = _create_output_parameter(context, input_cool, info, df_node)
    results[PARAM_CAPACITY_FACTOR] = _compose_capacity_factor(results[PARAM_INPUT], context)
    results[PARAM_ADDON_CONVERSION], results[PARAM_ADDON_LO] = _create_addon_parameters(input_cool)
    results[PARAM_TECHNICAL_LIFETIME] = _create_lifetime_parameter(results[PARAM_INPUT], info, node_region)


    # 3. Load Cost/Shares and Prepare Historical/Multiplier Data
    cost_share_df = _load_cost_and_shares_data(context)
    if cost_share_df is None:
        log.error("Failed to load cost/share data. Aborting cool_tech.")
        return {} # Return empty dict on critical failure

    input_cool_hist_ref = _prepare_historical_input_data(input_cool)

    # 4. Create Share Constraints and hold_cost multiplier table
    results[PARAM_SHARE_COMMODITY_UP], hold_cost = _create_share_parameters(
        context, cost_share_df, input_cool, input_cool_hist_ref, info
    )

    # 5. Create Cost Parameters
    # Define technologies to exclude from cost projections (as in original code)
    # TODO: Automate this list based on scenario content if possible
    techs_to_remove_costs = [
        "mw_ppl__ot_fresh", "mw_ppl__ot_saline", "mw_ppl__cl_fresh", "mw_ppl__air",
        "nuc_fbr__ot_fresh", "nuc_fbr__ot_saline", "nuc_fbr__cl_fresh", "nuc_fbr__air",
        "nuc_htemp__ot_fresh", "nuc_htemp__ot_saline", "nuc_htemp__cl_fresh", "nuc_htemp__air",
    ]
    results[PARAM_INV_COST] = _create_cost_parameters(context, techs_to_remove_costs)

    # 6. Prepare Parent Capacity Factor lookup table
    cap_fact_parent = _prepare_parent_capacity_factor(scen, parent_techs)


    # 7. Create Historical/Initial/Bound Parameters (fetch, expand, scale)
    historical_bound_params = _create_historical_and_bound_parameters(
        scen, parent_techs, hold_cost, cap_fact_parent
    )
    results.update(historical_bound_params)


    # Final check and cleanup (optional: remove empty DataFrames)
    final_results = {k: v for k, v in results.items() if not v.empty}
    log.info(f"--- Finished Refactored cool_tech: Generated data for {len(final_results)} parameters. ---")
    return final_results

# --- Legacy Implementations (Renamed with _legacy suffix) ---

@minimum_version("message_ix 3.7")
def _cool_tech_legacy(context: "Context") -> dict[str, pd.DataFrame]:
    """Original implementation of cool_tech (renamed for backup/toggle)."""
    # ----- START OF ORIGINAL cool_tech CODE -----
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
    ref_input = scen.par("input", {"technology": cooling_df["parent_tech"].unique()}) # Use unique()
    # list of tec in cooling_df["parent_tech"] that are not in ref_input
    missing_tec = cooling_df["parent_tech"].unique()[
        ~np.isin(cooling_df["parent_tech"].unique(), ref_input["technology"].unique())
    ]
    # some techs only have output, like csp
    ref_output = pd.DataFrame()
    if len(missing_tec) > 0:
        ref_output = scen.par("output", {"technology": missing_tec})
        # set columns names of ref_output to be the same as ref_input
        if not ref_output.empty and not ref_input.empty:
            # Basic column alignment, might need refinement
            ref_output.columns = ref_input.columns[:len(ref_output.columns)]
    # merge ref_input and ref_output
    ref_input = pd.concat([ref_input, ref_output], ignore_index=True)

    # Original had apply directly on ref_input, ensure it handles empty df
    if not ref_input.empty:
         value_level_update = ref_input.apply(missing_tech, axis=1)
         # Check if apply returned expected columns before assigning
         if not value_level_update.empty and all(col in value_level_update for col in ['value', 'level']):
             ref_input[["value", "level"]] = value_level_update
         else:
             log.warning("Legacy: missing_tech did not return expected columns. Skipping update.")

    # Combines the input df of parent_tech with water withdrawal data
    input_cool = (
        cooling_df.set_index("parent_tech")
        .combine_first(ref_input.set_index("technology"))
        .reset_index()
        .rename(columns={'index': 'parent_tech'}) # Ensure correct column name
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
    glb_node_name = f"{context.regions}_GLB" # Define the name
    is_glb_loc = input_cool["node_loc"] == glb_node_name
    input_cool.loc[is_glb_loc, "node_loc"] = input_cool.loc[is_glb_loc, "node_origin"]

    is_glb_origin = input_cool["node_origin"] == glb_node_name
    input_cool.loc[is_glb_origin, "node_origin"] = input_cool.loc[is_glb_origin, "node_loc"]


    input_cool["cooling_fraction"] = input_cool.apply(cooling_fr, axis=1)
    # Handle potential inf/-inf from cooling_fr if value is 0
    input_cool["cooling_fraction"] = input_cool["cooling_fraction"].replace([np.inf, -np.inf], np.nan).fillna(0)

    # Converting water withdrawal units to MCM/GWa
    # this refers to activity per cooling requirement (heat)
    # Avoid division by zero
    safe_cooling_fraction = np.where(input_cool["cooling_fraction"] <= 0, 1e-9, input_cool["cooling_fraction"])
    input_cool["value_cool"] = (
        input_cool["water_withdrawal_mid_m3_per_output"]
        * SECONDS_PER_YEAR * MCM_PER_M3 # Use constants
        / safe_cooling_fraction
    )
    # set to 1e-6 if value_cool is negative
    input_cool["value_cool"] = np.where(
        input_cool["value_cool"] < 0, 1e-6, input_cool["value_cool"]
    )
    # Avoid division by zero
    safe_withdrawal = np.where(input_cool["water_withdrawal_mid_m3_per_output"] <= 0, 1e-9, input_cool["water_withdrawal_mid_m3_per_output"])
    input_cool["return_rate"] = 1 - (
        input_cool["water_consumption_mid_m3_per_output"]
        / safe_withdrawal
    )
    # Clip return rate to [0, 1]
    input_cool["return_rate"] = np.clip(input_cool["return_rate"], 0, 1)

    # consumption to be saved in emissions rates for reporting purposes
    input_cool["consumption_rate"] = 1 - input_cool["return_rate"] # Use calculated return rate


    input_cool["value_return"] = input_cool["return_rate"] * input_cool["value_cool"]
    input_cool["value_return"] = np.where(input_cool["value_return"] < 0, 0, input_cool["value_return"]) # Ensure non-negative


    # only for reporting purposes
    input_cool["value_consumption"] = (
        input_cool["consumption_rate"] * input_cool["value_cool"]
    )
    input_cool["value_consumption"] = np.where(input_cool["value_consumption"] < 0, 0, input_cool["value_consumption"]) # Ensure non-negative


    # Filter out technologies that requires parasitic electricity
    electr = input_cool[input_cool["parasitic_electricity_demand_fraction"] > 0.0].copy() # Use copy


    # Make a new column 'value_cool' for calculating values against technologies
    # Avoid division by zero
    safe_cooling_fraction_elec = np.where(electr["cooling_fraction"] <= 0, 1e-9, electr["cooling_fraction"])
    electr["parasitic_demand_per_cool"] = ( # Rename for clarity
        electr["parasitic_electricity_demand_fraction"] / safe_cooling_fraction_elec
    )
    # set to 1e-6 if value_cool is negative
    electr["parasitic_demand_per_cool"] = np.where(
        electr["parasitic_demand_per_cool"] < 0, 1e-6, electr["parasitic_demand_per_cool"]
    )
    # Filters out technologies requiring saline water supply
    saline_df = input_cool[
        input_cool["technology_name"].str.endswith("ot_saline", na=False)
    ].copy() # Use copy


    # input_cool_minus_saline_elec_df
    con1 = input_cool["technology_name"].str.endswith("ot_saline", na=False)
    con2 = input_cool["technology_name"].str.endswith("air", na=False)
    icmse_df = input_cool[(~con1) & (~con2)].copy() # Use copy
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
        value=electr["parasitic_demand_per_cool"], # Use renamed column
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
                unit="MCM/GWa",
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
                unit="MCM/GWa",
            ),
        ]
    )

    # Drops NA values from the value column
    inp = inp.dropna(subset=["value"])

    # append the input data to results
    results["input"] = inp

    # add water consumption as emission factor, also for saline tecs
    emiss_df = input_cool[(~con2)] # Exclude air cooling
    emi = make_df(
        "emission_factor",
        node_loc=emiss_df["node_loc"],
        technology=emiss_df["technology_name"],
        year_vtg=emiss_df["year_vtg"],
        year_act=emiss_df["year_act"],
        mode=emiss_df["mode"],
        emission="fresh_return",
        value=emiss_df["value_return"],
        unit="MCM/GWa",
    )
    emi = emi.dropna(subset=["value"]) # Drop NA values
    emi = emi[emi['value'] > 1e-9] # Drop zero/neg values

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
    if not df_sw.empty:
        # Check required columns before dropping
        cols_to_drop = [col for col in ["mode", "date", "MSGREG"] if col in df_sw.columns]
        df_sw.drop(columns=cols_to_drop, inplace=True)
        df_sw.rename(
            columns={"region": "node_dest", "time": "time_dest", "year": "year_act"},
            inplace=True,
        )
        df_sw["time_dest"] = df_sw["time_dest"].astype(str)

        if context.nexus_set == "nexus":
            nexus_outputs_legacy = []
            # Check if df_node is available
            if 'df_node' in locals() and not df_node.empty:
                # Ensure icmse_df is available
                 if 'icmse_df' in locals() and not icmse_df.empty:
                     for nn in icmse_df.node_loc.unique():
                         # input cooling fresh basin
                         icfb_df = icmse_df[icmse_df["node_loc"] == nn]
                         # Ensure 'region' column exists in df_node
                         if 'region' in df_node.columns:
                             bs = list(df_node[df_node["region"] == nn]["node"])
                             if bs: # Only proceed if basins are found
                                 out_t = (
                                     make_df(
                                         "output",
                                         node_loc=icfb_df["node_loc"],
                                         technology=icfb_df["technology_name"],
                                         year_vtg=icfb_df["year_vtg"],
                                         year_act=icfb_df["year_act"],
                                         mode=icfb_df["mode"],
                                         commodity="surfacewater_basin",
                                         level="water_avail_basin",
                                         time="year",
                                         value=icfb_df["value_return"],
                                         unit="MCM/GWa",
                                     )
                                     .pipe(broadcast, node_dest=bs, time_dest=sub_time)
                                 )
                                 # Prepare for merge - ensure types match df_sw
                                 out_t['node_dest'] = out_t['node_dest'].astype(str)
                                 out_t['time_dest'] = out_t['time_dest'].astype(str)
                                 out_t['year_act'] = out_t['year_act'].astype(int)
                                 df_sw_merge = df_sw[['node_dest', 'time_dest', 'year_act', 'share']].copy()
                                 df_sw_merge['node_dest'] = df_sw_merge['node_dest'].astype(str)
                                 df_sw_merge['time_dest'] = df_sw_merge['time_dest'].astype(str)
                                 df_sw_merge['year_act'] = df_sw_merge['year_act'].astype(int)

                                 out_t = out_t.merge(df_sw_merge, how="left", on=["node_dest", "time_dest", "year_act"])

                                 # multiply by basin water availability share
                                 out_t["share"].fillna(0, inplace=True) # Handle missing shares
                                 out_t["value"] = out_t["value"] * out_t["share"]
                                 out_t.drop(columns={"share"}, inplace=True)
                                 nexus_outputs_legacy.append(out_t)
                         else:
                             log.warning("Legacy: 'region' column missing in df_node for nexus output.")


                     if nexus_outputs_legacy:
                         out_nexus_legacy = pd.concat(nexus_outputs_legacy, ignore_index=True)
                         out = pd.concat([out, out_nexus_legacy], ignore_index=True)

                 else:
                      log.warning("Legacy: icmse_df not available for nexus output calculation.")
            else:
                log.warning("Legacy: df_node not available for nexus output calculation.")


        # Common cleanup for 'output'
        out = out.dropna(subset=["value"])
        out = out[out['value'].abs() > 1e-9] # Filter near-zero values
        out.reset_index(drop=True, inplace=True)
    else:
        log.warning("Legacy: map_basin_region_wat(context) returned empty DataFrame. Cannot process nexus outputs.")


    # in any case save out into results
    results["output"] = out

    # costs and historical parameters
    path1 = package_data_path("water", "ppl_cooling_tech", FILE1)
    try:
        cost = pd.read_csv(path1)
    except FileNotFoundError:
        log.error(f"Legacy: Cost/share file not found: {path1}")
        cost = pd.DataFrame() # Assign empty DataFrame

    if not cost.empty:
        # Combine technology name to get full cooling tech names
        cost["technology"] = cost["utype"] + "__" + cost["cooling"]
        cost["share"] = cost["utype"] + "_" + cost["cooling"] # Original naming for share_calib

        # add contraint with relation, based on shares
        # Keep only "utype", "technology", and columns starting with "mix_"
        # Ensure columns exist before selecting
        mix_cols_legacy = [col for col in cost.columns if col.startswith("mix_")]
        if 'utype' in cost.columns and 'share' in cost.columns and mix_cols_legacy:
            share_filtered = cost.loc[:, ["utype", "share"] + mix_cols_legacy]

            # Melt to long format
            share_long = share_filtered.melt(
                id_vars=["utype", "share"], var_name="node_share", value_name="value"
            )
            # filter with uypt in cooling_df["parent_tech"]
            # Ensure input_cool is available
            if 'input_cool' in locals() and not input_cool.empty:
                 share_long = share_long[
                     share_long["utype"].isin(input_cool["parent_tech"].unique())
                 ].reset_index(drop=True)
            else:
                log.warning("Legacy: input_cool not available for filtering shares.")
                share_long = pd.DataFrame() # Empty df if input_cool is missing


            if not share_long.empty:
                # Remove "mix_" prefix from region names
                share_long["node_share"] = share_long["node_share"].str.replace(
                    "mix_", "", regex=False
                )
                # Replace 0 values with 0.0001
                share_long["value"] = share_long["value"] * SHARE_FLEXIBILITY  # Use constant
                share_long["value"] = share_long["value"].replace(0, ZERO_SHARE_REPLACEMENT) # Use constant

                share_long["shares"] = "share_calib_" + share_long["share"]
                share_long.drop(columns={"utype", "share"}, inplace=True)
                share_long["time"] = "year"
                share_long["unit"] = "-"
                share_calib = share_long.copy()
                # Expand for years [2020, 2025]
                calib_years = SHARE_CALIB_YEARS # Use constant
                if not share_calib.empty: # Check if df is not empty before repeat
                     share_calib = share_calib.loc[share_calib.index.repeat(len(calib_years))].reset_index(drop=True)
                     share_calib["year_act"] = calib_years * (len(share_calib) // len(calib_years))
                else:
                     share_calib = pd.DataFrame(columns=share_long.columns.tolist() + ['year_act']) # Empty df with columns


                # take year in info.N but not years_calib
                years_fut = [year for year in info.Y if year not in calib_years]
                share_fut = pd.DataFrame()
                if years_fut and not share_long.empty: # Check if years and base df exist
                    share_fut = share_long.copy()
                    share_fut = share_fut.loc[share_fut.index.repeat(len(years_fut))].reset_index(drop=True)
                    share_fut["year_act"] = years_fut * (len(share_fut) // len(years_fut))
                    # filter only shares that contain "ot_saline"
                    share_fut = share_fut[share_fut["shares"].str.contains("ot_saline")]
                    # Apply floor value
                    share_fut["value"] = np.maximum(share_fut["value"], MIN_SALINE_SHARE_FUT) # Use constant and maximum
                    # keep only after 2050 (or threshold year)
                    share_fut = share_fut[share_fut["year_act"] >= SALINE_SHARE_YEAR_THRESHOLD] # Use constant
                else:
                     share_fut = pd.DataFrame(columns=share_long.columns.tolist() + ['year_act']) # Empty df


                # append share_calib and (share_fut only to add constraints on ot_saline)
                # Ensure DataFrames have compatible columns before concat
                share_cols = ["node_share", "value", "shares", "time", "unit", "year_act"]
                share_calib = share_calib.reindex(columns=share_cols)
                share_fut = share_fut.reindex(columns=share_cols)
                results["share_commodity_up"] = pd.concat([share_calib, share_fut], ignore_index=True)
                results["share_commodity_up"].dropna(subset=['value', 'year_act'], inplace=True)
            else:
                 results["share_commodity_up"] = pd.DataFrame() # Set empty if share_long was empty

        else:
             log.warning("Legacy: Missing required columns in cost/share data or mix_cols empty. Skipping share parameter generation.")
             results["share_commodity_up"] = pd.DataFrame()
             cost.drop(columns="share", inplace=True, errors='ignore') # Still drop share if it exists

        # restore cost as it was for future use (drop the added 'share' column)
        cost.drop(columns="share", inplace=True, errors='ignore')

    else: # If cost df was empty initially
        results["share_commodity_up"] = pd.DataFrame()


    # Prepare historical data reference (original logic)
    input_cool_2015 = pd.DataFrame()
    if 'input_cool' in locals() and not input_cool.empty:
        input_cool_hist_ref_legacy = _prepare_historical_input_data(input_cool) # Use the helper
        # The original code continues to use input_cool_2015 name, so assign it
        input_cool_2015 = input_cool_hist_ref_legacy
    else:
         log.warning("Legacy: input_cool not available for historical data preparation.")
         input_cool_2015 = pd.DataFrame() # Ensure it's an empty DF


    # Prepare hold_cost (original logic relies on 'cost' DataFrame)
    hold_cost_legacy = pd.DataFrame()
    if 'cost' in locals() and not cost.empty and not input_cool_2015.empty:
        # Filter out columns that contain 'mix' in column name - RENAME FIRST
        cost.rename(columns=lambda name: name.replace("mix_", ""), inplace=True)
        # Now identify columns based on region name or standard names
        search_cols_legacy = [
            col for col in cost.columns
            if (context.regions in col if context.regions else False) or col in ["technology", "utype"]
        ]
        if search_cols_legacy: # Check if any columns were selected
            hold_df_legacy = input_cool_2015[
                ["node_loc", "technology_name", "cooling_fraction"]
            ].drop_duplicates()
            search_cols_cooling_fraction_legacy = [
                col for col in search_cols_legacy if col not in ["technology", "utype"]
            ]
            # Check if apply is possible
            if not cost[search_cols_legacy].empty and search_cols_cooling_fraction_legacy:
                # multiplication factor with cooling factor and shares
                hold_cost_legacy = cost[search_cols_legacy].apply(
                    shares,
                    axis=1,
                    context=context,
                    search_cols_cooling_fraction=search_cols_cooling_fraction_legacy,
                    hold_df=hold_df_legacy,
                    search_cols=search_cols_legacy,
                )
            else:
                 log.warning("Legacy: Cannot apply 'shares' function for hold_cost - input empty or no region columns.")
        else:
             log.warning("Legacy: No valid search_cols found for hold_cost calculation.")


    # Prepare parent capacity factor (original logic)
    cap_fact_parent_legacy = pd.DataFrame()
    if 'cooling_df' in locals() and not cooling_df.empty:
        parent_techs_legacy = list(cooling_df['parent_tech'].unique())
        if parent_techs_legacy:
             cap_fact_parent_legacy = _prepare_parent_capacity_factor(scen, parent_techs_legacy) # Use helper
    else:
         log.warning("Legacy: cooling_df not available for parent capacity factor preparation.")


    # Manually removing extra technologies not required
    # TODO make it automatic to not include the names manually
    techs_to_remove_legacy = [
        "mw_ppl__ot_fresh", "mw_ppl__ot_saline", "mw_ppl__cl_fresh", "mw_ppl__air",
        "nuc_fbr__ot_fresh", "nuc_fbr__ot_saline", "nuc_fbr__cl_fresh", "nuc_fbr__air",
        "nuc_htemp__ot_fresh", "nuc_htemp__ot_saline", "nuc_htemp__cl_fresh", "nuc_htemp__air",
    ]

    # Investment cost (original logic)
    results[PARAM_INV_COST] = _create_cost_parameters(context, techs_to_remove_legacy) # Use helper


    # Addon conversion (original logic)
    if 'input_cool' in locals() and not input_cool.empty:
        results[PARAM_ADDON_CONVERSION], results[PARAM_ADDON_LO] = _create_addon_parameters(input_cool) # Use helper
    else:
         log.warning("Legacy: input_cool not available for addon parameter creation.")
         results[PARAM_ADDON_CONVERSION], results[PARAM_ADDON_LO] = pd.DataFrame(), pd.DataFrame()


    # technical lifetime (original logic)
    if 'inp' in locals() and not inp.empty and 'node_region' in locals() and node_region.size > 0:
        results[PARAM_TECHNICAL_LIFETIME] = _create_lifetime_parameter(inp, info, node_region) # Use helper
    else:
        log.warning("Legacy: inp or node_region not available for technical lifetime.")
        results[PARAM_TECHNICAL_LIFETIME] = pd.DataFrame()


    # capacity factor (original logic)
    if 'inp' in locals() and not inp.empty:
        results[PARAM_CAPACITY_FACTOR] = _compose_capacity_factor(inp=inp, context=context) # Use helper
    else:
        log.warning("Legacy: inp not available for capacity factor.")
        results[PARAM_CAPACITY_FACTOR] = pd.DataFrame()


    # Historical and bounds parameters (original logic)
    if ('cooling_df' in locals() and not cooling_df.empty and
        'hold_cost_legacy' in locals() and # Use the legacy hold_cost prepared earlier
        'cap_fact_parent_legacy' in locals()): # Use the legacy cap_fact prepared earlier

        parent_techs_legacy = list(cooling_df['parent_tech'].unique())
        historical_bound_params_legacy = _create_historical_and_bound_parameters( # Use helper
            scen,
            parent_techs_legacy,
            hold_cost_legacy, # Pass the specific legacy version
            cap_fact_parent_legacy # Pass the specific legacy version
        )
        # Update results, potentially overwriting empty DFs created if helpers failed
        for param, df in historical_bound_params_legacy.items():
             if not df.empty or param not in results: # Update if new data or param wasn't set
                 results[param] = df
    else:
         log.warning("Legacy: Missing data (cooling_df, hold_cost, cap_fact_parent) for historical/bound params.")
         # Ensure parameter keys exist even if empty
         params_hist_bound = [
             PARAM_HIST_ACTIVITY, PARAM_HIST_NEW_CAPACITY,
             PARAM_INIT_ACTIVITY_UP, PARAM_INIT_ACTIVITY_LO, PARAM_INIT_NEW_CAPACITY_UP,
             PARAM_SOFT_ACTIVITY_UP, PARAM_SOFT_ACTIVITY_LO, PARAM_SOFT_NEW_CAPACITY_UP,
             PARAM_LEVEL_COST_ACTIVITY_SOFT_UP, PARAM_LEVEL_COST_ACTIVITY_SOFT_LO,
             PARAM_GROWTH_ACTIVITY_LO, PARAM_GROWTH_ACTIVITY_UP, PARAM_GROWTH_NEW_CAPACITY_UP,
         ]
         for p_name in params_hist_bound:
              if p_name not in results:
                  results[p_name] = pd.DataFrame()



    # add share constraints for cooling technologies based on SSP assumptions (original logic)
    df_share_ssp = cooling_shares_SSP_from_yaml(context) # Use helper

    if not df_share_ssp.empty:
        # pd concat to the existing results["share_commodity_up"]
        if PARAM_SHARE_COMMODITY_UP in results and not results[PARAM_SHARE_COMMODITY_UP].empty:
             # Ensure columns match before concat
             share_cols = results[PARAM_SHARE_COMMODITY_UP].columns
             df_share_ssp = df_share_ssp.reindex(columns=share_cols)
             results[PARAM_SHARE_COMMODITY_UP] = pd.concat(
                 [results[PARAM_SHARE_COMMODITY_UP], df_share_ssp], ignore_index=True
             )
        else: # If existing share data was empty or missing
             results[PARAM_SHARE_COMMODITY_UP] = df_share_ssp

    # Final cleanup of empty DataFrames generated during legacy execution
    final_results_legacy = {k: v for k, v in results.items() if isinstance(v, pd.DataFrame) and not v.empty}
    log.info(f"--- Finished Legacy cool_tech: Generated data for {len(final_results_legacy)} parameters. ---")
    return final_results_legacy
    # ----- END OF ORIGINAL cool_tech CODE -----


# --- Refactored Non-Cooling Tech Function ---

def _non_cooling_tec_refactored(context: "Context") -> dict[str, pd.DataFrame]:
    """Refactored implementation of non_cooling_tec (minor improvements)."""
    log.info("--- Starting Refactored non_cooling_tec ---")
    results = {}
    scen = context.get_scenario()

    # Load performance data
    perf_path = package_data_path("water", "ppl_cooling_tech", TECH_PERFORMANCE_FILE)
    try:
        df_perf = pd.read_csv(perf_path)
    except FileNotFoundError:
        log.exception(f"Performance file not found at {perf_path}")
        return {}

    # Filter for non-cooling freshwater supply technologies
    non_cool_df = df_perf[
        (df_perf["technology_group"] != TECH_GROUP_COOLING)
        & (df_perf["water_supply_type"] == "freshwater_supply") # Original logic filter
    ].copy()

    # Filter against technologies present in the scenario (using technical_lifetime as proxy)
    try:
        tec_lt = scen.par("technical_lifetime")
        if tec_lt is None or tec_lt.empty:
             log.warning("Could not retrieve 'technical_lifetime' from scenario. Cannot filter non-cooling techs.")
             # Proceed without filtering or return empty? Proceeding for now.
             all_tech_in_scen = non_cool_df['technology_name'].unique() # Assume all are valid
        else:
             all_tech_in_scen = list(tec_lt["technology"].unique())

        tech_non_cool_csv = list(non_cool_df["technology_name"].unique())
        techs_to_keep = [tec for tec in tech_non_cool_csv if tec in all_tech_in_scen]


        if len(techs_to_keep) < len(tech_non_cool_csv):
            removed_count = len(tech_non_cool_csv) - len(techs_to_keep)
            log.info(f"Removed {removed_count} non-cooling techs not found in scenario's technical_lifetime.")


        non_cool_df = non_cool_df[non_cool_df["technology_name"].isin(techs_to_keep)]
        non_cool_df = non_cool_df.rename(columns={"technology_name": "technology"})
    except Exception as e:
        log.exception(f"Error retrieving or processing 'technical_lifetime': {e}. Proceeding without tech filtering.")
        non_cool_df = non_cool_df.rename(columns={"technology_name": "technology"})


    # Calculate water withdrawal value in MCM/GWa
    non_cool_df["value"] = (
        non_cool_df["water_withdrawal_mid_m3_per_output"]
        * SECONDS_PER_YEAR * MCM_PER_M3 # Use constants
    )
    non_cool_df.dropna(subset=["value"], inplace=True)


    # Fetch corresponding output parameter data from scenario
    non_cool_tech_list = list(non_cool_df["technology"].unique())
    if not non_cool_tech_list:
        log.warning("No valid non-cooling technologies found after filtering. Returning empty results.")
        return {}

    n_cool_output_df = scen.par("output", {"technology": non_cool_tech_list})


    if n_cool_output_df is None or n_cool_output_df.empty:
        log.warning("No 'output' parameter data found for non-cooling technologies. Cannot generate inputs.")
        return {}


    # Filter out global nodes (as in original logic)
    glb_node_name = f"{context.regions}_GLB"
    n_cool_output_df = n_cool_output_df[
        (n_cool_output_df["node_loc"] != glb_node_name)
        & (n_cool_output_df["node_dest"] != glb_node_name)
    ]


    # Merge scenario output data with calculated withdrawal values
    # Use suffixes to distinguish value columns if names clash ('value_x' from output, 'value_y' from non_cool_df calc)
    n_cool_df_merged = pd.merge(
        n_cool_output_df,
        non_cool_df[["technology", "value"]], # Select only needed columns
        on="technology",
        how="inner", # Use inner merge to keep only matching techs
        suffixes=("_scen", "_calc")
    )
    n_cool_df_merged.dropna(subset=["value_calc"], inplace=True) # Drop if calculated value is missing


    # Create the input parameter DataFrame
    inp_n_cool = make_df(
        PARAM_INPUT,
        technology=n_cool_df_merged["technology"],
        value=n_cool_df_merged["value_calc"], # Use the calculated MCM/GWa value
        unit=UNIT_MCM_GWa,
        level=LEVEL_WATER_SUPPLY,
        commodity=COMMODITY_FRESHWATER,
        time_origin=TIME_YEAR,
        mode=MODE_STANDARD, # Assuming standard mode 'M1'
        time=TIME_YEAR,
        year_vtg=n_cool_df_merged["year_vtg"].astype(int),
        year_act=n_cool_df_merged["year_act"].astype(int),
        node_loc=n_cool_df_merged["node_loc"],
        node_origin=n_cool_df_merged["node_dest"], # Water comes from the destination node of the output? Verify logic.
    )


    # Append the input data to results
    results[PARAM_INPUT] = inp_n_cool
    log.info(f"--- Finished Refactored non_cooling_tec: Generated {len(inp_n_cool)} input entries. ---")
    return results


# --- Legacy Non-Cooling Tech Function ---

def _non_cooling_tec_legacy(context: "Context") -> dict[str, pd.DataFrame]:
    """Original implementation of non_cooling_tec (renamed for backup/toggle)."""
    # ----- START OF ORIGINAL non_cooling_tec CODE -----
    results = {}

    FILE = "tech_water_performance_ssp_msg.csv"
    path = package_data_path("water", "ppl_cooling_tech", FILE)
    try:
        df = pd.read_csv(path)
    except FileNotFoundError:
        log.error(f"Legacy NonCooling: File not found {path}")
        return {}

    # Original filtering logic
    cooling_df_check = df.copy() # Need a copy to avoid modifying df used later?
    cooling_df_check = cooling_df_check.loc[cooling_df_check["technology_group"] == "cooling"]
    if not cooling_df_check.empty:
        cooling_df_check["parent_tech"] = (
            cooling_df_check["technology_name"]
            .apply(lambda x: pd.Series(str(x).split("__")))
            .drop(columns=1)
        )

    non_cool_df = df[
        (df["technology_group"] != "cooling")
        & (df["water_supply_type"] == "freshwater_supply")
    ].copy() # Use copy


    scen = context.get_scenario()
    tec_lt = scen.par("technical_lifetime")
    all_tech = []
    if tec_lt is not None and not tec_lt.empty:
        all_tech = list(tec_lt["technology"].unique())
    else:
        log.warning("Legacy NonCooling: Cannot get tech list from technical_lifetime.")
        # Fallback or error handling? Original assumes it exists.
        # Using all techs from non_cool_df as fallback
        all_tech = list(non_cool_df["technology_name"].unique())


    tech_non_cool_csv = list(non_cool_df["technology_name"].unique())
    # Original filtering based on presence in all_tech list
    techs_to_keep_legacy = [tec for tec in tech_non_cool_csv if tec in all_tech]
    non_cool_df = non_cool_df[non_cool_df["technology_name"].isin(techs_to_keep_legacy)]


    non_cool_df = non_cool_df.rename(columns={"technology_name": "technology"})


    non_cool_df["value"] = (
        non_cool_df["water_withdrawal_mid_m3_per_output"]
        * 60 * 60 * 24 * 365 * (1e-6)  # MCM - Use constants
    )
    non_cool_df.dropna(subset=['value'], inplace=True) # Drop NAs


    non_cool_tech = list(non_cool_df["technology"].unique())


    n_cool_df = pd.DataFrame()
    if non_cool_tech: # Check if list is not empty
        n_cool_df_raw = scen.par("output", {"technology": non_cool_tech})
        if n_cool_df_raw is not None and not n_cool_df_raw.empty:
             # Original global node filtering
             glb_node_name = f"{context.regions}_GLB"
             n_cool_df = n_cool_df_raw[
                 (n_cool_df_raw["node_loc"] != glb_node_name)
                 & (n_cool_df_raw["node_dest"] != glb_node_name)
             ].copy() # Use copy
        else:
             log.warning("Legacy NonCooling: No 'output' data found for non-cooling techs.")
    else:
         log.warning("Legacy NonCooling: No valid non-cooling techs found after filtering.")


    n_cool_df_merge = pd.DataFrame()
    if not n_cool_df.empty and not non_cool_df.empty:
        # Original merge, assumes 'value' column names won't clash badly?
        # Using suffixes is safer. Original used 'right' merge.
        n_cool_df_merge = pd.merge(n_cool_df, non_cool_df[['technology', 'value']],
                                   on="technology", how="inner", # Changed to inner for safety
                                   suffixes=("_scen", "_calc"))
        # Original code had dropna after merge, potentially dropping rows kept by 'right' merge
        n_cool_df_merge.dropna(inplace=True) # Original inplace dropna
    else:
         log.warning("Legacy NonCooling: Cannot merge output data with calculated values.")


    # Input dataframe for non cooling technologies
    if not n_cool_df_merge.empty:
        # Original uses 'value_y' which implies suffixes were expected or handled implicitly
        # Assuming the calculated value is the one intended ('value_calc' with suffixes)
        inp_n_cool = make_df(
            "input",
            technology=n_cool_df_merge["technology"],
            value=n_cool_df_merge["value_calc"], # Use calculated value
            unit="MCM/GWa",
            level="water_supply",
            commodity="freshwater",
            time_origin="year",
            mode="M1",
            time="year",
            year_vtg=n_cool_df_merge["year_vtg"].astype(int),
            year_act=n_cool_df_merge["year_act"].astype(int),
            node_loc=n_cool_df_merge["node_loc"],
            node_origin=n_cool_df_merge["node_dest"], # As per original
        )
        # append the input data to results
        results["input"] = inp_n_cool
        log.info(f"--- Finished Legacy non_cooling_tec: Generated {len(inp_n_cool)} input entries. ---")
    else:
        log.warning("Legacy NonCooling: Merged data empty, cannot create input parameter.")
        results["input"] = pd.DataFrame()

    return results
    # ----- END OF ORIGINAL non_cooling_tec CODE -----


# --- Public API Wrappers ---

@minimum_version("message_ix 3.7")
def cool_tech(context: "Context") -> dict[str, pd.DataFrame]:
    """Process cooling technology data for a scenario instance.

    Delegates to either the refactored or legacy implementation based on the
    `USE_REFACTORED_IMPLEMENTATION` flag.
    """
    if USE_REFACTORED_IMPLEMENTATION:
        log.info("Using refactored cool_tech implementation.")
        try:
            return _cool_tech_refactored(context)
        except Exception as e:
            log.exception("Error during refactored cool_tech execution. Falling back to legacy.")
            # Fallback to legacy if refactored version fails critically
            try:
                return _cool_tech_legacy(context)
            except Exception as e_legacy:
                log.exception("Error during legacy cool_tech execution after fallback.")
                return {} # Return empty dict if both fail
    else:
        log.info("Using legacy cool_tech implementation.")
        try:
            return _cool_tech_legacy(context)
        except Exception as e_legacy:
            log.exception("Error during legacy cool_tech execution.")
            return {} # Return empty dict if legacy fails


def non_cooling_tec(context: "Context") -> dict[str, pd.DataFrame]:
    """Process data for water usage of power plants (non-cooling technology related).

    Delegates to either the refactored or legacy implementation based on the
    `USE_REFACTORED_IMPLEMENTATION` flag.
    """
    if USE_REFACTORED_IMPLEMENTATION:
        log.info("Using refactored non_cooling_tec implementation.")
        try:
            return _non_cooling_tec_refactored(context)
        except Exception as e:
            log.exception("Error during refactored non_cooling_tec execution. Falling back to legacy.")
            try:
                return _non_cooling_tec_legacy(context)
            except Exception as e_legacy:
                 log.exception("Error during legacy non_cooling_tec execution after fallback.")
                 return {}
    else:
        log.info("Using legacy non_cooling_tec implementation.")
        try:
            return _non_cooling_tec_legacy(context)
        except Exception as e_legacy:
             log.exception("Error during legacy non_cooling_tec execution.")
             return {}