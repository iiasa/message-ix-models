"""Replace cooling capacity factor parameters with climate impact projections.

Implements Jones et al. thermodynamic constraints on freshwater cooling
via relation_activity bounds in MESSAGE scenarios.
"""

import logging

import numpy as np
import pandas as pd
from message_ix import Scenario

from message_ix_models.project.alps.cid_utils import (
    cached_rime_prediction,
    extract_region_code,
)
from message_ix_models.project.alps.constants import BASELINE_GWL, R12_REGIONS
from message_ix_models.project.alps.rime import predict_rime
from message_ix_models.util import package_data_path

log = logging.getLogger(__name__)


def _extract_years_from_magicc(magicc_df: pd.DataFrame) -> list[int]:
    """Extract year columns from MAGICC IAMC-format DataFrame."""
    # IAMC format has years as column names (integers or strings of integers)
    year_cols = [c for c in magicc_df.columns if str(c).isdigit()]
    return sorted(int(y) for y in year_cols)


def add_jones_relation_constraints(
    scen: Scenario,
    jones_factors: pd.DataFrame,
    commit_message: str = "Add Jones freshwater cooling constraints",
    baseline_gwl: float = BASELINE_GWL,
) -> Scenario:
    """Add relation_activity constraints to bound freshwater cooling by Jones factors.

    Constraint form:
        ACT[fresh_cooling] <= jones_ratio * fresh_share_ref * cooling_fraction * ACT[parent]

    where:
        jones_ratio = CF(region, year, GMT) / CF(region, baseline_gwl)
        fresh_share_ref = baseline freshwater share from cooltech_cost_and_shares data
        cooling_fraction = addon_conversion (cooling per unit electricity)

    The constraint bounds the freshwater share of total cooling to:
        fresh_share <= jones_ratio * fresh_share_ref

    Parameters
    ----------
    scen : Scenario
        MESSAGE scenario with cooling module (must have addon_conversion params)
    jones_factors : pd.DataFrame
        Jones capacity factors with columns: region + year columns (2025-2100)
        Values are absolute capacity factors (0-1)
    baseline_gwl : float
        Baseline global warming level for normalization (default: 1.0C)

    Returns
    -------
    Scenario
        Modified scenario with relation constraints added
    """
    log.info("Extracting cooling structure from scenario...")

    # Get baseline CF at reference GWL for normalization
    cf_baseline = predict_rime(np.array([baseline_gwl]), "capacity_factor")  # (12, 1)
    cf_baseline = cf_baseline.flatten()  # (12,)

    # Build region -> baseline_cf lookup
    baseline_lookup = dict(zip(R12_REGIONS, cf_baseline))
    log.debug(
        f"Baseline CF at GWL={baseline_gwl}C: {dict(list(baseline_lookup.items())[:3])}..."
    )

    # Get addon_conversion to find parent techs and cooling_fraction values
    addon = scen.par("addon_conversion")
    cooling_addon = addon[addon["type_addon"].str.startswith("cooling__")].copy()

    if len(cooling_addon) == 0:
        raise ValueError("No cooling addon_conversion found in scenario")

    # Extract parent tech from type_addon (e.g., "cooling__coal_ppl" -> "coal_ppl")
    cooling_addon["parent_tech"] = cooling_addon["type_addon"].str.replace(
        "cooling__", "", regex=False
    )

    # Get cooling_fraction per (region, parent_tech, year_act)
    cooling_frac = (
        cooling_addon.groupby(["node", "parent_tech", "year_act"])["value"]
        .first()
        .reset_index()
    )
    cooling_frac.columns = ["node", "parent_tech", "year_act", "cooling_fraction"]

    # Get capacity_factor to find freshwater cooling tech names
    cf = scen.par("capacity_factor")
    fresh_cf = cf[cf["technology"].str.contains("_fresh", na=False)].copy()
    fresh_cf["parent_tech"] = fresh_cf["technology"].str.rsplit("__", n=1).str[0]

    # Build lookup: parent_tech -> list of freshwater cooling variants
    parent_to_fresh = {}
    for parent in fresh_cf["parent_tech"].unique():
        variants = fresh_cf[fresh_cf["parent_tech"] == parent]["technology"].unique()
        parent_to_fresh[parent] = list(variants)

    log.info(
        f"Found {len(parent_to_fresh)} parent technologies with freshwater cooling"
    )

    # Load baseline freshwater share from cooltech_cost_and_shares data
    share_file = package_data_path(
        "water", "ppl_cooling_tech", "cooltech_cost_and_shares_ssp_msg_R12.csv"
    )
    share_df = pd.read_csv(share_file)

    # Melt to long format: (utype, cooling, region) -> share
    mix_cols = [c for c in share_df.columns if c.startswith("mix_R12_")]
    share_long = share_df.melt(
        id_vars=["utype", "cooling"],
        value_vars=mix_cols,
        var_name="region",
        value_name="share",
    )
    share_long["region"] = share_long["region"].str.replace("mix_", "")

    # Sum cl_fresh + ot_fresh to get total freshwater share
    fresh_types = ["cl_fresh", "ot_fresh"]
    fresh_share_df = (
        share_long[share_long["cooling"].isin(fresh_types)]
        .groupby(["region", "utype"])["share"]
        .sum()
        .reset_index()
    )
    fresh_share_df.columns = ["region", "parent_tech", "fresh_share_ref"]

    # Build lookup: (region, parent_tech) -> fresh_share_ref
    fresh_share_lookup = {}
    for _, row in fresh_share_df.iterrows():
        fresh_share_lookup[(row["region"], row["parent_tech"])] = row["fresh_share_ref"]

    log.info(
        f"   Loaded baseline freshwater shares for {len(fresh_share_lookup)} (region, tech) pairs"
    )

    # Parse jones_factors to get year columns and region mapping
    year_cols = [c for c in jones_factors.columns if isinstance(c, (int, np.integer))]

    # Build jones factor lookup: (region_short, year) -> factor
    jones_lookup = {}
    for _, row in jones_factors.iterrows():
        region = row.get("region", row.name)
        for year in year_cols:
            jones_lookup[(region, year)] = row[year]

    # Get valid (node, parent_tech, year) combinations from technical_lifetime
    tl = scen.par("technical_lifetime")
    parent_techs = list(parent_to_fresh.keys())
    tl_parents = tl[tl["technology"].isin(parent_techs)]
    valid_combinations = set(
        zip(tl_parents["node_loc"], tl_parents["technology"], tl_parents["year_vtg"])
    )
    log.info(f"   Valid (node, tech, year) combinations: {len(valid_combinations)}")

    # Get model years from scenario (filtered to Jones data range)
    model_years = [int(y) for y in scen.set("year") if int(y) >= min(year_cols)]
    regions = cooling_frac["node"].unique()

    log.info(
        f"   Building constraints for {len(model_years)} years, {len(regions)} regions..."
    )

    # Prepare parameter DataFrames
    relation_set_entries = []
    relation_activity_rows = []
    relation_upper_rows = []

    for parent_tech, fresh_variants in parent_to_fresh.items():
        rel_name = f"fresh_cool_bound_{parent_tech}"
        relation_set_entries.append(rel_name)

        for region in regions:
            region_short = extract_region_code(region)

            # Get baseline CF for this region
            baseline_cf = baseline_lookup.get(region_short, 1.0)

            for year in model_years:
                if (region, parent_tech, year) not in valid_combinations:
                    continue

                # Get year-specific cooling_fraction from addon_conversion
                cf_row = cooling_frac[
                    (cooling_frac["node"] == region)
                    & (cooling_frac["parent_tech"] == parent_tech)
                    & (cooling_frac["year_act"] == year)
                ]
                if len(cf_row) == 0:
                    continue
                cooling_fraction = cf_row.iloc[0]["cooling_fraction"]

                # Get baseline freshwater share for this (region, parent_tech)
                fresh_share_ref = fresh_share_lookup.get((region, parent_tech), None)
                if fresh_share_ref is None:
                    raise KeyError(
                        f"No baseline freshwater share for ({region}, {parent_tech})"
                    )
                if fresh_share_ref <= 0:
                    continue

                # Get jones_factor for this (region, year)
                if (region_short, year) in jones_lookup:
                    jones_factor = jones_lookup[(region_short, year)]
                else:
                    nearest_year = min(year_cols, key=lambda y: abs(y - year))
                    jones_factor = jones_lookup.get((region_short, nearest_year), 1.0)

                # Normalize to baseline GWL
                jones_ratio = jones_factor / baseline_cf if baseline_cf > 0 else 1.0

                # Skip if no constraint needed (ratio >= 1 means no degradation)
                if jones_ratio >= 1.0:
                    continue

                # Add relation_activity for freshwater variants: coefficient = +1
                for fresh_tech in fresh_variants:
                    relation_activity_rows.append(
                        {
                            "relation": rel_name,
                            "node_rel": region,
                            "year_rel": year,
                            "node_loc": region,
                            "technology": fresh_tech,
                            "year_act": year,
                            "mode": "M1",
                            "value": 1.0,
                            "unit": "-",
                        }
                    )

                # Add relation_activity for parent tech:
                # coefficient = -jones_ratio * fresh_share_ref * cooling_fraction
                relation_activity_rows.append(
                    {
                        "relation": rel_name,
                        "node_rel": region,
                        "year_rel": year,
                        "node_loc": region,
                        "technology": parent_tech,
                        "year_act": year,
                        "mode": "M1",
                        "value": -jones_ratio * fresh_share_ref * cooling_fraction,
                        "unit": "-",
                    }
                )

                # Add relation_upper: bound = 0
                relation_upper_rows.append(
                    {
                        "relation": rel_name,
                        "node_rel": region,
                        "year_rel": year,
                        "value": 0.0,
                        "unit": "-",
                    }
                )

    log.info(f"   Created {len(relation_set_entries)} relations")
    log.info(f"   Created {len(relation_activity_rows)} relation_activity rows")
    log.info(f"   Created {len(relation_upper_rows)} relation_upper rows")

    if len(relation_activity_rows) == 0:
        log.info("   No constraints needed (jones_ratio >= 1.0 for all cases)")
        return scen

    rel_act_df = pd.DataFrame(relation_activity_rows)
    rel_upper_df = pd.DataFrame(relation_upper_rows)

    with scen.transact(commit_message):
        for rel_name in set(relation_set_entries):
            scen.add_set("relation", rel_name)
        scen.add_par("relation_activity", rel_act_df)
        scen.add_par("relation_upper", rel_upper_df)

    scen.set_as_default()
    log.info(f"   Committed version {scen.version}")

    return scen


def generate_cooling_cid_scenario(
    scen: Scenario,
    magicc_df: pd.DataFrame,
    run_ids: tuple,
    n_runs: int,
    baseline_gwl: float = BASELINE_GWL,
) -> Scenario:
    """Generate cooling CID scenario with thermodynamic constraints.

    Parameters
    ----------
    scen : Scenario
        MESSAGE scenario to modify
    magicc_df : pd.DataFrame
        MAGICC output DataFrame (IAMC format)
    run_ids : tuple
        Run IDs for RIME prediction
    n_runs : int
        Number of runs for expectation computation
    baseline_gwl : float
        Baseline GWL for jones_ratio normalization (default: 1.0C)

    Returns
    -------
    Scenario
        Modified scenario with cooling CID constraints
    """
    from message_ix_models.project.alps.scenario_generator import _build_cooling_module

    # Build cooling module first
    log.info("Building cooling module...")
    scen = _build_cooling_module(scen)

    log.info("Running RIME predictions for capacity_factor (annual)...")
    cf_array = cached_rime_prediction(
        magicc_df, run_ids, "capacity_factor", temporal_res="annual"
    )
    log.info(f"   Got capacity_factor array shape: {cf_array.shape}")

    # Convert ndarray to DataFrame with proper format
    years = _extract_years_from_magicc(magicc_df)
    cf_expected = pd.DataFrame(cf_array, index=R12_REGIONS, columns=years)
    log.info(f"   capacity_factor DataFrame shape: {cf_expected.shape}")

    # Get source name from DataFrame
    source_name = (
        magicc_df["Scenario"].iloc[0] if "Scenario" in magicc_df.columns else "unknown"
    )

    log.info("Adding Jones relation constraints...")
    commit_msg = (
        f"CID cooling: Jones relation constraints for {source_name}\n"
        f"RIME: n_runs={n_runs}, variable=capacity_factor"
    )
    scen_updated = add_jones_relation_constraints(
        scen, cf_expected, commit_msg, baseline_gwl=baseline_gwl
    )

    log.info(f"   Committed version {scen_updated.version}")
    return scen_updated
