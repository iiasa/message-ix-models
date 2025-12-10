"""Replace cooling capacity factor parameters with climate impact projections.

Implements Jones et al. thermodynamic constraints on freshwater cooling
via relation_activity bounds in MESSAGE scenarios.

Constraint equation (per parent technology, region, year):

    ACT[cl_fresh] + ACT[ot_fresh] <= r_jones * s_ref * f_cool * ACT[parent]

Rearranged as relation_activity with upper bound = 0:

    (+1) * ACT[cl_fresh] + (+1) * ACT[ot_fresh]
    + (-r_jones * s_ref * f_cool) * ACT[parent] <= 0
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
    year_cols = [c for c in magicc_df.columns if str(c).isdigit()]
    return sorted(int(y) for y in year_cols)


def _build_lookups(
    jones_factors: pd.DataFrame, baseline_gwl: float
) -> tuple[dict, dict, dict, list]:
    """Build all lookup dictionaries for constraint computation.

    Returns
    -------
    baseline_lookup : dict[str, float]
        Region short code -> baseline capacity factor at reference GWL
    fresh_share_lookup : dict[tuple[str, str], float]
        (region, parent_tech) -> baseline freshwater cooling share
    jones_lookup : dict[tuple[str, int], float]
        (region_short, year) -> Jones capacity factor
    year_cols : list[int]
        Year columns from jones_factors
    """
    # Baseline CF at reference GWL
    cf_baseline = predict_rime(np.array([baseline_gwl]), "capacity_factor").flatten()
    baseline_lookup = dict(zip(R12_REGIONS, cf_baseline))

    # Freshwater share from reference data
    share_file = package_data_path(
        "water", "ppl_cooling_tech", "cooltech_cost_and_shares_ssp_msg_R12.csv"
    )
    share_df = pd.read_csv(share_file)
    mix_cols = [c for c in share_df.columns if c.startswith("mix_R12_")]
    share_long = share_df.melt(
        id_vars=["utype", "cooling"],
        value_vars=mix_cols,
        var_name="region",
        value_name="share",
    )
    share_long["region"] = share_long["region"].str.replace("mix_", "")
    fresh_share_df = (
        share_long[share_long["cooling"].isin(["cl_fresh", "ot_fresh"])]
        .groupby(["region", "utype"])["share"]
        .sum()
        .reset_index()
    )
    fresh_share_lookup = {
        (row["region"], row["utype"]): row["share"]
        for _, row in fresh_share_df.iterrows()
    }

    # Jones factors by (region, year)
    year_cols = [c for c in jones_factors.columns if isinstance(c, (int, np.integer))]
    jones_lookup = {}
    for _, row in jones_factors.iterrows():
        region = row.get("region", row.name)
        for year in year_cols:
            jones_lookup[(region, year)] = row[year]

    return baseline_lookup, fresh_share_lookup, jones_lookup, year_cols


def _get_freshwater_cooling_mapping(scen: Scenario) -> dict[str, list[str]]:
    """Extract parent technology -> freshwater cooling variants mapping.

    Returns dict like {'coal_ppl': ['coal_ppl__cl_fresh', 'coal_ppl__ot_fresh']}
    """
    cf = scen.par("capacity_factor")
    fresh_cf = cf[cf["technology"].str.contains("_fresh", na=False)].copy()
    fresh_cf["parent_tech"] = fresh_cf["technology"].str.rsplit("__", n=1).str[0]

    return {
        parent: list(fresh_cf[fresh_cf["parent_tech"] == parent]["technology"].unique())
        for parent in fresh_cf["parent_tech"].unique()
    }


def _get_cooling_fraction_df(scen: Scenario) -> pd.DataFrame:
    """Extract cooling fraction per (node, parent_tech, year_act) from addon_conversion.

    Returns DataFrame with columns: node, parent_tech, year_act, cooling_fraction
    """
    addon = scen.par("addon_conversion")
    cooling_addon = addon[addon["type_addon"].str.startswith("cooling__")].copy()

    if len(cooling_addon) == 0:
        raise ValueError("No cooling addon_conversion found in scenario")

    cooling_addon["parent_tech"] = cooling_addon["type_addon"].str.replace(
        "cooling__", "", regex=False
    )

    cooling_frac = (
        cooling_addon.groupby(["node", "parent_tech", "year_act"])["value"]
        .first()
        .reset_index()
    )
    cooling_frac.columns = ["node", "parent_tech", "year_act", "cooling_fraction"]
    return cooling_frac


def _compute_jones_ratio(
    region_short: str,
    year: int,
    baseline_lookup: dict,
    jones_lookup: dict,
    year_cols: list,
) -> float | None:
    """Compute Jones ratio (CF / baseline_CF). Returns None if ratio >= 1 (no constraint)."""
    baseline_cf = baseline_lookup.get(region_short, 1.0)

    if (region_short, year) in jones_lookup:
        jones_factor = jones_lookup[(region_short, year)]
    else:
        nearest_year = min(year_cols, key=lambda y: abs(y - year))
        jones_factor = jones_lookup.get((region_short, nearest_year), 1.0)

    jones_ratio = jones_factor / baseline_cf if baseline_cf > 0 else 1.0
    return None if jones_ratio >= 1.0 else jones_ratio


def add_jones_relation_constraints(
    scen: Scenario,
    jones_factors: pd.DataFrame,
    commit_message: str = "Add Jones freshwater cooling constraints",
    baseline_gwl: float = BASELINE_GWL,
) -> Scenario:
    """Add relation_activity constraints bounding freshwater cooling by Jones factors.

    Adds constraint: ACT[cl_fresh] + ACT[ot_fresh] <= r * s * f * ACT[parent]

    where r = jones_ratio, s = fresh_share_ref, f = cooling_fraction.

    Parameters
    ----------
    scen : Scenario
        MESSAGE scenario with cooling module
    jones_factors : pd.DataFrame
        Jones capacity factors with region index and year columns
    baseline_gwl : float
        Baseline GWL for normalization

    Returns
    -------
    Scenario
        Modified scenario with relation constraints added
    """
    log.info("Building constraint lookups...")
    baseline_lookup, fresh_share_lookup, jones_lookup, year_cols = _build_lookups(
        jones_factors, baseline_gwl
    )

    log.info("Extracting cooling structure from scenario...")
    parent_to_fresh = _get_freshwater_cooling_mapping(scen)
    cooling_frac = _get_cooling_fraction_df(scen)
    log.info(f"Found {len(parent_to_fresh)} parent technologies with freshwater cooling")

    # Valid (node, tech, year) from technical_lifetime
    tl = scen.par("technical_lifetime")
    tl_parents = tl[tl["technology"].isin(parent_to_fresh.keys())]
    valid_combinations = set(
        zip(tl_parents["node_loc"], tl_parents["technology"], tl_parents["year_vtg"])
    )

    model_years = [int(y) for y in scen.set("year") if int(y) >= min(year_cols)]
    regions = cooling_frac["node"].unique()
    log.info(f"Building constraints for {len(model_years)} years, {len(regions)} regions")

    # Build constraint rows
    relation_set_entries = []
    relation_activity_rows = []
    relation_upper_rows = []

    for parent_tech, fresh_variants in parent_to_fresh.items():
        rel_name = f"fresh_cool_bound_{parent_tech}"
        relation_set_entries.append(rel_name)

        for region in regions:
            region_short = extract_region_code(region)

            for year in model_years:
                if (region, parent_tech, year) not in valid_combinations:
                    continue

                # Get cooling fraction
                cf_row = cooling_frac[
                    (cooling_frac["node"] == region)
                    & (cooling_frac["parent_tech"] == parent_tech)
                    & (cooling_frac["year_act"] == year)
                ]
                if len(cf_row) == 0:
                    continue
                cooling_fraction = cf_row.iloc[0]["cooling_fraction"]

                # Get freshwater share
                fresh_share_ref = fresh_share_lookup.get((region, parent_tech))
                if fresh_share_ref is None or fresh_share_ref <= 0:
                    continue

                # Compute Jones ratio
                jones_ratio = _compute_jones_ratio(
                    region_short, year, baseline_lookup, jones_lookup, year_cols
                )
                if jones_ratio is None:
                    continue

                # Freshwater variants: coefficient = +1
                for fresh_tech in fresh_variants:
                    relation_activity_rows.append({
                        "relation": rel_name,
                        "node_rel": region,
                        "year_rel": year,
                        "node_loc": region,
                        "technology": fresh_tech,
                        "year_act": year,
                        "mode": "M1",
                        "value": 1.0,
                        "unit": "-",
                    })

                # Parent: coefficient = -jones_ratio * fresh_share_ref * cooling_fraction
                relation_activity_rows.append({
                    "relation": rel_name,
                    "node_rel": region,
                    "year_rel": year,
                    "node_loc": region,
                    "technology": parent_tech,
                    "year_act": year,
                    "mode": "M1",
                    "value": -jones_ratio * fresh_share_ref * cooling_fraction,
                    "unit": "-",
                })

                # Upper bound = 0
                relation_upper_rows.append({
                    "relation": rel_name,
                    "node_rel": region,
                    "year_rel": year,
                    "value": 0.0,
                    "unit": "-",
                })

    log.info(f"Created {len(set(relation_set_entries))} relations, "
             f"{len(relation_activity_rows)} activity rows, "
             f"{len(relation_upper_rows)} upper bound rows")

    if len(relation_activity_rows) == 0:
        log.info("No constraints needed (jones_ratio >= 1.0 for all cases)")
        return scen

    with scen.transact(commit_message):
        for rel_name in set(relation_set_entries):
            scen.add_set("relation", rel_name)
        scen.add_par("relation_activity", pd.DataFrame(relation_activity_rows))
        scen.add_par("relation_upper", pd.DataFrame(relation_upper_rows))

    scen.set_as_default()
    log.info(f"Committed version {scen.version}")
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
