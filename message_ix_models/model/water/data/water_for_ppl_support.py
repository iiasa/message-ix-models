"""Prepare data for water use for cooling & energy technologies."""

import logging
from typing import Optional

import numpy as np
import pandas as pd
import yaml

from message_ix_models import Context
from message_ix_models.model.water.data.water_for_ppl_rules import COOL_SHARE_RULES
from message_ix_models.model.water.dsl_engine import run_standard
from message_ix_models.util import (
    make_matched_dfs,
    package_data_path,
)
from message_ix_models.util.citation_wrapper import citation_wrapper

log = logging.getLogger(__name__)


def missing_tech(x: pd.Series) -> pd.Series:
    """Assign values to missing data.
    It goes through the input data frame and extract the technologies which
    don't have input values and then assign manual  values to those technologies
    along with assigning them an arbitrary level i.e dummy supply
    """
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

    if pd.notna(x["technology"]):
        # Find a matching key in `data_dic` using substring matching
        matched_key = next((key for key in data_dic if key in x["technology"]), None)

        if matched_key:
            value = data_dic[matched_key]
            if x["value"] < 1:
                value = max(x["value"], value)
            # for backwards compatibility
            return (
                pd.Series({"value": value, "level": "dummy_supply"})
                if x["level"] == "cooling"
                else pd.Series({"value": value, "level": x["level"]})
            )

    # Return the original values if no match is found
    return pd.Series({"value": x["value"], "level": x["level"]})


def cooling_fr(x: pd.Series) -> float:
    """Calculate cooling fraction

    Returns
    -------
    The calculated cooling fraction after for two categories;
    1. Technologies that produce heat as an output
        cooling_fraction(h_cool) = input value(hi) - 1
    Simply subtract 1 from the heating value since the rest of the part is already
    accounted in the heating value
    2. Rest of technologies
        h_cool  =  hi -Hi* h_fg - 1,
        where:
            h_fg (flue gasses losses) = 0.1 (10% assumed losses)
    """
    try:
        if "hpl" in x["parent_tech"]:
            return x["value"] - 1
        else:
            return x["value"] - (x["value"] * 0.1) - 1
    except KeyError:
        return x["value"] - (x["value"] * 0.1) - 1


def shares(
    x: pd.Series,
    context: "Context",
    search_cols_cooling_fraction: list,
    hold_df: pd.DataFrame,
    search_cols: list,
) -> pd.Series:
    """Process share and cooling fraction.

    Returns
    -------
    Product of value of shares of cooling technology types of regions with
    corresponding cooling fraction
    """
    for col in search_cols_cooling_fraction:
        col2 = context.map_ISO_c[col] if context.type_reg == "country" else col

        # Filter the cooling fraction
        cooling_fraction = hold_df[
            (hold_df["node_loc"] == col2)
            & (hold_df["technology_name"] == x["technology"])
        ]["cooling_fraction"]

        # Log unmatched rows
        if cooling_fraction.empty:
            log.info(
                f"No cooling_fraction found for node_loc: {col2}, "
                f"technology: {x['technology']}"
            )
            cooling_fraction = pd.Series([0])

        # Ensure the Series is not empty before accessing its first element
        # # Default to 0 if cooling_fraction is empty
        x[col] = (
            x[col] * cooling_fraction.iloc[0]
            if not cooling_fraction.empty
            else x[col] * 0
        )

    # Construct the output
    results = []
    for i in x:
        if isinstance(i, str):
            results.append(i)
        else:
            results.append(float(i) if not isinstance(i, pd.Series) else i.iloc[0])

    return pd.Series(results, index=search_cols)


def apply_act_cap_multiplier(
    df: pd.DataFrame,
    hold_cost: pd.DataFrame,
    cap_fact_parent: Optional[pd.DataFrame] = None,
    param_name: str = "",
) -> pd.DataFrame:
    """
    Generalized function to apply hold_cost factors and optionally divide by cap factor.
    hold cost contain the share per cooling technologies and their activity factors
    compared to parent technologies.
    """

    # Melt hold_cost to long format
    hold_cost_long = hold_cost.melt(
        id_vars=["utype", "technology"], var_name="node_loc", value_name="multiplier"
    )

    # Merge and apply hold_cost multipliers: share * addon_factor
    # ACT,c = ACT,p * share * addon_factor
    df = df.merge(hold_cost_long, how="left")
    df["value"] *= df["multiplier"]

    # filter with value > 0
    df = df[df["value"] > 0]

    # If parameter is capacity-related, multiply by cap_fact
    # CAP,c * cf,c(=1) = CAP,p * share * addon_factor * cf,p
    if "capacity" in param_name and cap_fact_parent is not None:
        df = df.merge(cap_fact_parent, how="left")
        df["value"] *= df["cap_fact"] * 1.2  # flexibility
        df.drop(columns="cap_fact", inplace=True)
    # remove if there are Nan values, but write a log that inform on the parameter and
    # the head of the data
    # Identify missing or invalid values
    missing_values = (
        df["value"].isna()
        | (df["value"] == "")
        | (df["value"].astype(str).str.strip() == "")
    )

    if missing_values.any():
        print("diobo")
        log.warning(
            f"Missing or empty values found in {param_name}.head(1):\n"
            f"{df[missing_values].head(1)}"
        )
        df = df[~missing_values]  # Remove rows with missing/empty values

    df.drop(columns=["utype", "multiplier"], inplace=True)

    return df


def cooling_shares_SSP_from_yaml(
    context: "Context",  # Aligning with the style of the functions provided
) -> pd.DataFrame:
    """
    Populate a DataFrame for 'share_commodity_up' from a YAML configuration file.
    """
    # Load the YAML file
    FILE = "ssp.yaml"
    yaml_file_path = package_data_path("water", FILE)
    try:
        with open(yaml_file_path, "r") as file:
            yaml_data = yaml.safe_load(file)
    except FileNotFoundError:
        log.warning(f"YAML file '{FILE}' not found. Please, check your data.")

    # Read the SSP from the context
    ssp = context.ssp

    # Navigate to the scenarios section in the YAML file
    macro_regions_data = yaml_data.get("macro-regions", {})
    scenarios = yaml_data.get("scenarios", {})

    # Validate that the SSP exists in the YAML data
    if ssp not in scenarios:
        log.warning(
            f"SSP '{ssp}' not found in the 'scenarios' section of the YAML file."
        )
        return pd.DataFrame()

    # Extract data for the specified SSP
    ssp_data = scenarios[ssp]["cooling_tech"]

    # Initialize an empty list to hold DataFrames
    df_region = pd.DataFrame()
    info = context["water build info"]
    year_constraint = [year for year in info.Y if year >= 2050]

    # Loop through all regions and shares
    for macro_region, region_data in ssp_data.items():
        share_data = region_data.get("share_commodity_up", {})
        reg_shares = macro_regions_data[macro_region]
        # filter reg shares that are also in info.N
        reg_shares = [
            node
            for node in info.N
            if any(node.endswith(reg_share) for reg_share in reg_shares)
        ]
        for share, value in share_data.items():
            # Create a DataFrame for the current region and share
            # Need to pass [share] and [value] as a DF else our DSL engine will not work.
            df_share = pd.DataFrame(
                {
                    "shares": [share],
                    "time": ["year"],
                    "value": [value],
                    "unit": ["-"],
                }
            )
            args = {
                "rule_dfs": df_share,
            }
            extra_args = {
                "year_act": year_constraint,
                "node_share": reg_shares,
            }
            df_region = pd.concat(
                [
                    df_region,
                    run_standard(COOL_SHARE_RULES.get_rule()[0], args, extra_args),
                ]
            )

    return df_region


@citation_wrapper(
    "van Vliet et al. 2016",
    "https://doi.org/10.1016/j.gloenvcha.2016.07.007",
    description="""Multi-model assessment of global hydropower
    and cooling water discharge potential under climate change""",
    metadata={"RCP": "no_climate"},
)
def _compose_capacity_factor(inp: pd.DataFrame, context: "Context") -> pd.DataFrame:
    """Create the capacity_factor base on data in `inp` and `context."""
    cap_fact = make_matched_dfs(inp, capacity_factor=1)["capacity_factor"]

    # nothing to do for the counter‑factual run
    if context.RCP == "no_climate":
        return cap_fact

    # --- climate‑impact case -------------------------------------------------
    path = package_data_path(
        "water", "ppl_cooling_tech", "power_plant_cooling_impact_MESSAGE.xlsx"
    )
    df_impact = pd.read_excel(path, sheet_name=f"{context.regions}_{context.RCP}")

    # Precompute the “freshwater” mask once
    is_fresh = cap_fact["technology"].str.contains("fresh", na=False)

    # Loop over each node that has an impact entry
    for node in df_impact["node"].unique():
        # Mask for all fresh‐water tech in this node
        base_mask = is_fresh & (cap_fact["node_loc"] == node)
        if not base_mask.any():
            continue  # skip nodes with no matching rows

        # Fetch the row of impact factors for this node
        impact_row = df_impact.loc[df_impact["node"] == node].iloc[0]

        # Build the new 'value' Series via a single np.select call
        periods = np.select(
            [
                base_mask & cap_fact["year_act"].between(2025, 2049),
                base_mask & cap_fact["year_act"].between(2050, 2069),
                base_mask & (cap_fact["year_act"] >= 2070),
            ],
            [
                impact_row["2025s"],
                impact_row["2050s"],
                impact_row["2070s"],
            ],
            default=cap_fact["value"],  # leave everything else unchanged
        )

        # Overwrite the DataFrame’s 'value' column in one shot
        cap_fact["value"] = periods

    return cap_fact
