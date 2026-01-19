"""Replace building energy demand CIDs in MESSAGE scenarios.

Replaces climate-driven portions of rc_spec (cooling) and rc_therm (heating)
with RIME-based projections.
"""

import logging
from typing import Tuple

import pandas as pd
import pint
from message_ix import Scenario

from message_ix_models.util import package_data_path

from .building_energy import compute_energy_demand_ensemble
from .cid_utils import sample_to_message_years
from .rime import get_gmt_ensemble

log = logging.getLogger(__name__)

# Unit conversion using pint: demand parameter expects GWa, building_energy outputs EJ
_ureg = pint.UnitRegistry()
EJ_TO_GWA = (1 * _ureg.EJ).to(_ureg.GW * _ureg.year).magnitude


def load_sector_fractions() -> pd.DataFrame:
    """Load sector fractions for cooling/heating demand decomposition."""
    filepath = package_data_path("alps", "rc_sector_fractions.csv")
    return pd.read_csv(filepath, comment="#")


def _sample_long_format(df: pd.DataFrame, method: str) -> pd.DataFrame:
    """Apply year sampling to long-format DataFrame (node, year, value)."""
    from message_ix_models.project.alps.constants import MESSAGE_YEARS

    # Pivot to wide: node as index, years as columns
    wide = df.pivot(index="node", columns="year", values="value").reset_index()
    # Sample to MESSAGE years
    sampled = sample_to_message_years(wide, ["node"], method=method)
    # Pivot back to long
    return sampled.melt(
        id_vars=["node"], value_vars=MESSAGE_YEARS, var_name="year", value_name="value"
    )


def compute_building_cids(
    magicc_df: pd.DataFrame,
    n_runs: int = 100,
    coeff_scenario: str = "S1",
    year_sampling: str = "point",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Compute building energy CIDs from MAGICC temperature ensemble.

    Uses ensemble averaging (E computed per-run, then averaged) to smooth
    bin-edge artifacts from the EI emulator's discrete GWL bins.

    Parameters
    ----------
    magicc_df : pd.DataFrame
        MAGICC output with temperature trajectories (IAMC format)
    n_runs : int
        Number of MAGICC runs for ensemble (default: 100)
    coeff_scenario : str
        Correction coefficient scenario ('S1', 'S2', 'S3')
    year_sampling : str
        'point' - sample at MESSAGE years, 'average' - average preceding period

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        (cooling_demand, heating_demand) with columns: node, year, value (EJ)
    """
    # Extract GMT ensemble from MAGICC
    gmt_trajectories, years = get_gmt_ensemble(magicc_df, n_runs=n_runs)

    # Compute cooling demand (resid + comm) with ensemble averaging
    cooling_resid = compute_energy_demand_ensemble(
        mode="cool",
        scenario=coeff_scenario,
        gmt_trajectories=gmt_trajectories,
        years=years,
        sector="resid",
    )
    cooling_comm = compute_energy_demand_ensemble(
        mode="cool",
        scenario=coeff_scenario,
        gmt_trajectories=gmt_trajectories,
        years=years,
        sector="comm",
    )
    cooling_total = pd.concat([cooling_resid, cooling_comm])
    cooling_total = (
        cooling_total.groupby(["region", "year"])["E_cool_mean_EJ"].sum().reset_index()
    )
    cooling_total.rename(
        columns={"region": "node", "E_cool_mean_EJ": "value"}, inplace=True
    )
    cooling_total["node"] = "R12_" + cooling_total["node"]

    # Compute heating demand (resid + comm) with ensemble averaging
    heating_resid = compute_energy_demand_ensemble(
        mode="heat",
        scenario=coeff_scenario,
        gmt_trajectories=gmt_trajectories,
        years=years,
        sector="resid",
    )
    heating_comm = compute_energy_demand_ensemble(
        mode="heat",
        scenario=coeff_scenario,
        gmt_trajectories=gmt_trajectories,
        years=years,
        sector="comm",
    )
    heating_total = pd.concat([heating_resid, heating_comm])
    heating_total = (
        heating_total.groupby(["region", "year"])["E_heat_mean_EJ"].sum().reset_index()
    )
    heating_total.rename(
        columns={"region": "node", "E_heat_mean_EJ": "value"}, inplace=True
    )
    heating_total["node"] = "R12_" + heating_total["node"]

    # Apply year sampling
    cooling_sampled = _sample_long_format(cooling_total, year_sampling)
    heating_sampled = _sample_long_format(heating_total, year_sampling)

    return cooling_sampled, heating_sampled


def generate_building_cid_scenario(
    scen: Scenario,
    magicc_df: pd.DataFrame,
    n_runs: int = 100,
    coeff_scenario: str = "S1",
    year_sampling: str = "point",
) -> Scenario:
    """Inject building energy CIDs into scenario.

    Parameters
    ----------
    scen : Scenario
        MESSAGE scenario (must be checked out)
    magicc_df : pd.DataFrame
        MAGICC output with temperature trajectories
    n_runs : int
        Number of MAGICC runs for GMT expectation
    coeff_scenario : str
        Correction coefficient scenario ('S1', 'S2', 'S3')
    year_sampling : str
        'point' - sample at MESSAGE years, 'average' - average preceding period

    Returns
    -------
    Scenario
        Modified scenario with building CIDs
    """
    # Compute building CIDs (returns EJ)
    cooling_total, heating_total = compute_building_cids(
        magicc_df, n_runs, coeff_scenario, year_sampling
    )

    # Convert EJ to GWa for MESSAGE demand parameter
    cooling_total["value"] = cooling_total["value"] * EJ_TO_GWA
    heating_total["value"] = heating_total["value"] * EJ_TO_GWA

    # Load sector fractions
    fractions = load_sector_fractions()

    # Replace in single transact
    with scen.transact(f"Inject building CIDs ({coeff_scenario})"):
        # Get existing demand and filter by commodity
        demand = scen.par("demand")
        rc_spec = demand[demand["commodity"] == "rc_spec"].copy()
        rc_therm = demand[demand["commodity"] == "rc_therm"].copy()

        # Merge fractions
        rc_spec = rc_spec.merge(
            fractions[["node", "year", "frac_resid_cool", "frac_comm_cool"]],
            on=["node", "year"],
            how="left",
        )
        rc_therm = rc_therm.merge(
            fractions[["node", "year", "frac_resid_heat", "frac_comm_heat"]],
            on=["node", "year"],
            how="left",
        )

        # Fill missing fractions with 0 (no reduction) - log if any missing
        missing_cool_frac = rc_spec["frac_resid_cool"].isna().sum()
        missing_heat_frac = rc_therm["frac_resid_heat"].isna().sum()
        if missing_cool_frac > 0:
            missing_nodes = rc_spec.loc[
                rc_spec["frac_resid_cool"].isna(), "node"
            ].unique()
            log.warning(
                "Missing cooling sector fractions for %d rows (%d nodes), using 0: %s",
                missing_cool_frac,
                len(missing_nodes),
                list(missing_nodes)[:5],
            )
        if missing_heat_frac > 0:
            missing_nodes = rc_therm.loc[
                rc_therm["frac_resid_heat"].isna(), "node"
            ].unique()
            log.warning(
                "Missing heating sector fractions for %d rows (%d nodes), using 0: %s",
                missing_heat_frac,
                len(missing_nodes),
                list(missing_nodes)[:5],
            )

        rc_spec["frac_resid_cool"] = rc_spec["frac_resid_cool"].fillna(0)
        rc_spec["frac_comm_cool"] = rc_spec["frac_comm_cool"].fillna(0)
        rc_therm["frac_resid_heat"] = rc_therm["frac_resid_heat"].fillna(0)
        rc_therm["frac_comm_heat"] = rc_therm["frac_comm_heat"].fillna(0)

        # Total climate fraction to remove
        rc_spec["cool_frac"] = rc_spec["frac_resid_cool"] + rc_spec["frac_comm_cool"]
        rc_therm["heat_frac"] = rc_therm["frac_resid_heat"] + rc_therm["frac_comm_heat"]

        # Reduce existing by fraction
        rc_spec["value"] = rc_spec["value"] * (1 - rc_spec["cool_frac"])
        rc_therm["value"] = rc_therm["value"] * (1 - rc_therm["heat_frac"])

        # Merge in new climate demand
        rc_spec = rc_spec.merge(
            cooling_total.rename(columns={"value": "cool_add"}),
            on=["node", "year"],
            how="left",
        )
        rc_therm = rc_therm.merge(
            heating_total.rename(columns={"value": "heat_add"}),
            on=["node", "year"],
            how="left",
        )

        # Log if climate demand missing for any regions
        missing_cool_add = rc_spec["cool_add"].isna().sum()
        missing_heat_add = rc_therm["heat_add"].isna().sum()
        if missing_cool_add > 0:
            missing_nodes = rc_spec.loc[rc_spec["cool_add"].isna(), "node"].unique()
            log.warning(
                "Missing RIME cooling demand for %d rows (%d nodes), using 0: %s",
                missing_cool_add,
                len(missing_nodes),
                list(missing_nodes)[:5],
            )
        if missing_heat_add > 0:
            missing_nodes = rc_therm.loc[rc_therm["heat_add"].isna(), "node"].unique()
            log.warning(
                "Missing RIME heating demand for %d rows (%d nodes), using 0: %s",
                missing_heat_add,
                len(missing_nodes),
                list(missing_nodes)[:5],
            )

        rc_spec["cool_add"] = rc_spec["cool_add"].fillna(0)
        rc_therm["heat_add"] = rc_therm["heat_add"].fillna(0)

        # Add climate demand
        rc_spec["value"] = rc_spec["value"] + rc_spec["cool_add"]
        rc_therm["value"] = rc_therm["value"] + rc_therm["heat_add"]

        # Clean up columns for add_par - keep only demand parameter columns
        demand_cols = ["node", "commodity", "level", "year", "time", "value", "unit"]
        rc_spec_clean = rc_spec[demand_cols].copy()
        rc_therm_clean = rc_therm[demand_cols].copy()

        # Remove old rc_spec/rc_therm rows and add updated ones
        old_rc_spec = demand[demand["commodity"] == "rc_spec"]
        old_rc_therm = demand[demand["commodity"] == "rc_therm"]

        scen.remove_par("demand", old_rc_spec)
        scen.add_par("demand", rc_spec_clean)

        scen.remove_par("demand", old_rc_therm)
        scen.add_par("demand", rc_therm_clean)

    scen.set_as_default()
    return scen
