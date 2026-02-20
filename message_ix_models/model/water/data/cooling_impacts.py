"""Cooling CID: Jones et al. freshwater cooling efficiency degradation.

Warming reduces the thermal margin between river discharge limits and intake
temperature, degrading freshwater cooling capacity. This module predicts
regional capacity factors from GMT via RIME emulators and builds MESSAGE
``relation_activity`` constraints that bound freshwater cooling activity
as a function of warming.

Data: ``r12_capacity_gwl_ensemble.nc`` (12 R12 regions x 47 GWL bins).
No resolution expansion needed — RIME cooling data is native R12.

Constraint per parent technology *p*, region *r*, year *t* (t >= min_year)::

    ACT[p__cl_fresh] + ACT[p__ot_fresh]
        <= r_jones(r,t) * s_ref(r) * f_cool(p) * ACT[p]

Where *r_jones* = CF(GMT) / CF(baseline), *s_ref* = reference freshwater
share, *f_cool* = cooling fraction from ``addon_conversion``.

Design note — relation_activity vs addon_up
-------------------------------------------
The existing addon mechanism (``cooling__<parent>``) links all four cooling
variants to parent heat rejection and is indexed by timeslice. An
``addon_up`` constraint would apply *per timeslice*, which is stricter than
needed when the Jones ratio is annual. Using it would also require a new
addon type (``cooling_fw__<parent>``) covering only freshwater variants, with
new ``cat_addon``, ``map_tec_addon``, and ``addon_conversion`` entries — more
invasive than the relation approach, not simpler.

``RELATION_EQUIVALENCE`` has no timeslice index, so it constrains the annual
aggregate, which matches the annual RIME emulator. The ``REL`` auxiliary
variable is also directly inspectable in GDX output for binding analysis.

If seasonal Jones ratios are derived (separate summer/winter CF degradation,
e.g. for the EGU subannual cooling extension), the addon_up path becomes the
right framework: per-timeslice constraint application would then be correct
and the set restructuring would be warranted.
"""

import functools
import logging

import numpy as np
import pandas as pd

from message_ix_models.tools.impacts import (
    clip_gmt,
    impacts_data_path,
    predict_rime,
)
from message_ix_models.util import package_data_path
from message_ix_models.util.node import extract_region_code

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DATASET = "r12_capacity_gwl_ensemble.nc"
_VAR = "capacity_factor"
_DEFAULT_BASELINE_GWL = 1.0
_DEFAULT_MIN_YEAR = 2045


# ---------------------------------------------------------------------------
# Cached data loaders
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _region_codes() -> list[str]:
    """Short region codes from the capacity factor dataset."""
    import xarray as xr

    ds = xr.open_dataset(str(impacts_data_path("rime", _DATASET)))
    return list(ds.region.values)


@functools.lru_cache(maxsize=1)
def _freshwater_reference_shares() -> pd.Series:
    """Regional average freshwater share (cl_fresh + ot_fresh).

    Returns Series indexed by short region code (e.g. "AFR").
    """
    path = package_data_path(
        "water", "ppl_cooling_tech", "cooltech_cost_and_shares_ssp_msg_R12.csv"
    )
    df = pd.read_csv(path)
    mix_cols = [c for c in df.columns if c.startswith("mix_")]

    fresh = df[df["cooling"].isin(["cl_fresh", "ot_fresh"])]
    # Sum cl_fresh + ot_fresh shares per region, averaged across parent techs
    regional_fresh = fresh.groupby("cooling")[mix_cols].mean().sum()

    # Convert column names: "mix_R12_AFR" -> "AFR"
    return regional_fresh.rename(index=lambda c: c.replace("mix_R12_", ""))


# ---------------------------------------------------------------------------
# Prediction
# ---------------------------------------------------------------------------


def predict_cooling_cf(gmt_array: np.ndarray) -> pd.DataFrame:
    """Predict regional capacity factors from GMT.

    Parameters
    ----------
    gmt_array
        GMT values in degC above pre-industrial. Shape ``(n_years,)`` or
        ``(n_runs, n_years)`` for ensemble (returns expectation).

    Returns
    -------
    pd.DataFrame
        Wide DataFrame with ``region`` index (short codes) and one column
        per GMT input position. Values are capacity factors (fractions).
    """
    gmt_array = np.asarray(gmt_array)
    gmt_clipped = clip_gmt(gmt_array, gmt_min=0.6, gmt_ceil=0.9)

    dataset_path = impacts_data_path("rime", _DATASET)
    raw = predict_rime(gmt_clipped, dataset_path, _VAR)
    # raw shape: (12, n_years) — regions x time positions

    regions = _region_codes()
    return pd.DataFrame(raw, index=pd.Index(regions, name="region"))


def compute_jones_ratios(
    gmt_array: np.ndarray,
    baseline_gwl: float = _DEFAULT_BASELINE_GWL,
) -> pd.DataFrame:
    """Compute Jones degradation ratios: CF(GMT) / CF(baseline).

    Parameters
    ----------
    gmt_array
        GMT trajectory. Shape ``(n_years,)`` or ``(n_runs, n_years)``.
    baseline_gwl
        Reference warming level (degC). Default 1.0.

    Returns
    -------
    pd.DataFrame
        Same shape as :func:`predict_cooling_cf` output. Values are ratios
        relative to baseline — below 1 under warming, above 1 if GMT
        dips below baseline.
    """
    cf = predict_cooling_cf(gmt_array)

    # Baseline CF: single-point prediction at baseline_gwl
    dataset_path = impacts_data_path("rime", _DATASET)
    cf_baseline = predict_rime(np.array([baseline_gwl]), dataset_path, _VAR)[
        :, 0
    ]  # (12,)

    # Ratio: broadcast baseline across time axis
    ratios = cf.values / cf_baseline[:, np.newaxis]
    return pd.DataFrame(ratios, index=cf.index.copy(), columns=cf.columns.copy())


# ---------------------------------------------------------------------------
# Constraint building
# ---------------------------------------------------------------------------


def _read_cooling_structure(
    addon_df: pd.DataFrame,
    technologies: set[str],
) -> pd.DataFrame:
    """Extract freshwater cooling techs and their parent-tech cooling fractions.

    Parameters
    ----------
    addon_df
        The ``addon_conversion`` parameter DataFrame from a MESSAGE scenario.
    technologies
        Set of technology names present in the scenario.

    Returns
    -------
    pd.DataFrame
        Columns: parent_tech, cl_fresh_tech, ot_fresh_tech, node_loc,
        cooling_fraction. One row per (parent_tech, node_loc).
    """
    if addon_df.empty:
        raise ValueError("addon_conversion is empty — cooling module not built?")

    # addon_conversion has type_addon = "cooling__<parent>", value = cooling_fraction
    cooling_addon = addon_df[addon_df["type_addon"].str.startswith("cooling__")].copy()
    cooling_addon["parent_tech"] = cooling_addon["type_addon"].str.replace(
        "cooling__", "", n=1
    )

    # Deduplicate: one cooling_fraction per (parent_tech, node_loc)
    # Take mean across vintage/year combinations — cooling_fraction is
    # physically constant for a given parent tech
    grouped = (
        cooling_addon.groupby(["parent_tech", "node"])["value"]
        .mean()
        .reset_index()
        .rename(columns={"node": "node_loc", "value": "cooling_fraction"})
    )

    rows = []
    for _, row in grouped.iterrows():
        parent = row["parent_tech"]
        cl = f"{parent}__cl_fresh"
        ot = f"{parent}__ot_fresh"
        if cl in technologies or ot in technologies:
            rows.append(
                {
                    "parent_tech": parent,
                    "cl_fresh_tech": cl if cl in technologies else None,
                    "ot_fresh_tech": ot if ot in technologies else None,
                    "node_loc": row["node_loc"],
                    "cooling_fraction": row["cooling_fraction"],
                }
            )

    if not rows:
        raise ValueError("No freshwater cooling technologies found")

    return pd.DataFrame(rows)


def build_cooling_constraints(
    addon_df: pd.DataFrame,
    technologies: set[str],
    jones_ratios: pd.DataFrame,
    model_years: list[int] | None = None,
    min_year: int = _DEFAULT_MIN_YEAR,
) -> dict:
    """Build ``relation_activity`` and ``relation_upper`` for Jones constraints.

    Parameters
    ----------
    addon_df
        The ``addon_conversion`` parameter DataFrame from a MESSAGE scenario.
    technologies
        Set of technology names present in the scenario.
    jones_ratios
        Output of :func:`compute_jones_ratios`. Rows = regions (short codes),
        columns = model years.
    model_years
        Which years to constrain. If *None*, uses jones_ratios columns.
    min_year
        Earliest year for constraints. Default 2045.

    Returns
    -------
    dict
        ``"relation_activity"``: DataFrame of relation coefficients.
        ``"relation_upper"``: DataFrame of upper bounds (all zero).
        ``"relation_names"``: list of relation name strings to add to
        the ``relation`` set.
    """
    structure = _read_cooling_structure(addon_df, technologies)
    s_ref = _freshwater_reference_shares()

    if model_years is None:
        model_years = [int(c) for c in jones_ratios.columns]
    constrained_years = [y for y in model_years if y >= min_year]

    if not constrained_years:
        log.warning("No model years >= %d; returning empty constraints", min_year)
        return {
            "relation_activity": pd.DataFrame(),
            "relation_upper": pd.DataFrame(),
            "relation_names": [],
        }

    rel_act_rows = []
    rel_up_rows = []
    relation_names = set()

    # Group by parent tech — one relation per parent type
    for parent_tech, group in structure.groupby("parent_tech"):
        rel_name = f"jones_cool_{parent_tech}"
        relation_names.add(rel_name)

        for _, row in group.iterrows():
            node = row["node_loc"]  # "R12_AFR"
            region_short = extract_region_code(node)
            f_cool = row["cooling_fraction"]

            # Reference freshwater share for this region
            if region_short not in s_ref.index:
                log.warning("No freshwater share for region %s, skipping", region_short)
                continue
            share = float(s_ref[region_short])

            for year in constrained_years:
                # Jones ratio for this region-year
                if region_short not in jones_ratios.index:
                    continue
                if year not in jones_ratios.columns:
                    log.warning("Year %d not in jones_ratios columns, skipping", year)
                    continue
                r_jones = float(jones_ratios.loc[region_short, year])

                # Parent tech coefficient: negative (RHS of inequality)
                parent_coeff = -(r_jones * share * f_cool)

                rel_act_rows.append(
                    {
                        "relation": rel_name,
                        "node_rel": node,
                        "year_rel": year,
                        "node_loc": node,
                        "technology": row["parent_tech"],
                        "year_act": year,
                        "mode": "M1",
                        "value": parent_coeff,
                        "unit": "-",
                    }
                )

                # Freshwater variant coefficients: +1 each
                for tech in (row["cl_fresh_tech"], row["ot_fresh_tech"]):
                    if tech is not None:
                        rel_act_rows.append(
                            {
                                "relation": rel_name,
                                "node_rel": node,
                                "year_rel": year,
                                "node_loc": node,
                                "technology": tech,
                                "year_act": year,
                                "mode": "M1",
                                "value": 1.0,
                                "unit": "-",
                            }
                        )

                # Upper bound = 0
                rel_up_rows.append(
                    {
                        "relation": rel_name,
                        "node_rel": node,
                        "year_rel": year,
                        "value": 0.0,
                        "unit": "-",
                    }
                )

    rel_act = pd.DataFrame(rel_act_rows)
    rel_up = pd.DataFrame(rel_up_rows)

    n_parents = len(relation_names)
    n_entries = len(rel_act)
    log.info(
        "Built Jones cooling constraints: %d relations, %d relation_activity entries, "
        "years %d-%d",
        n_parents,
        n_entries,
        min(constrained_years),
        max(constrained_years),
    )

    return {
        "relation_activity": rel_act,
        "relation_upper": rel_up,
        "relation_names": sorted(relation_names),
    }
