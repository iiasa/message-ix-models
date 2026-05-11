"""Cooling CID: thermoelectric capacity-factor degradation under warming.

Wet cooling enters as ``relation_activity`` bounds on freshwater cooling
technologies — the bound is constructed so that, given the regional
freshwater share, total freshwater-cooled activity from a parent power
technology cannot exceed the warming-impaired capacity factor times that
parent's activity. Dry cooling enters as a multiplicative ``capacity_factor``
derating on ``__air`` technologies.

Source dataset: ``r12_thermoelectric_gwl.nc`` — regional capacity-factor
ratios as a function of GWL, for wet and dry cooling.

Impact-kernel citations:

- Wet cooling: Li et al. (2025), "Global hydroclimatic risks and strategic
  decommissioning pathways for thermal power units." *Nature Sustainability*.
  doi:10.1038/s41893-025-01692-9
- Dry cooling: Qin et al. (2023), "Global assessment of the carbon-water
  tradeoff of dry cooling for thermal power generation." *Nature Water*.
  doi:10.1038/s44221-023-00120-6
"""

import functools
import logging

import numpy as np
import pandas as pd

from message_ix_models.tools.iamc import frame_to_iamc
from message_ix_models.tools.impacts import (
    GmtArray,
    clip_gmt,
    open_rime_dataset,
    predict_rime,
)
from message_ix_models.util import package_data_path
from message_ix_models.util.node import extract_region_code

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# R12 regional emulator with two cooling-axis values: "wet" and "dry".
_DATASET = "r12_thermoelectric_gwl.nc"
_VAR = "capacity_factor"

# Ratio denominator: CF(GWL) / CF(1.0 degC). Other baselines are possible
# only if the calibration is re-derived against that reference level.
_DEFAULT_BASELINE_GWL = 1.0

# Earlier wet-cooling bounds produced infeasibilities in preliminary runs.
_DEFAULT_MIN_YEAR = 2045

# Dataset selectors. "wet" covers freshwater once-through + closed-loop;
# "dry" covers air cooling. Saline cooling is not represented in this file.
_WET_SEL = {"cooling": "wet"}
_DRY_SEL = {"cooling": "dry"}
_WET_TIMESERIES_VARIABLE = (
    "Physical Climate Impact|Thermoelectric Cooling|"
    "Freshwater Cooling Activity Limit Ratio"
)
_DRY_TIMESERIES_VARIABLE = (
    "Physical Climate Impact|Thermoelectric Cooling|Dry Cooling Capacity Factor Ratio"
)


# ---------------------------------------------------------------------------
# Cached data loaders
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _region_codes() -> list[str]:
    """Short region codes from the capacity factor dataset."""
    return list(open_rime_dataset(_DATASET).region.values)


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


def predict_cooling_cf(
    gmt_array: np.ndarray,
    cooling: str = "wet",
) -> pd.DataFrame:
    """Predict regional capacity factors from GMT, mean-reduced.

    Parameters
    ----------
    gmt_array
        GMT values in degC above pre-industrial. Shape ``(n_years,)`` or
        ``(n_runs, n_years)`` for ensemble.
    cooling
        ``"wet"`` (freshwater) or ``"dry"`` (air).

    Returns
    -------
    pd.DataFrame
        Wide DataFrame with ``region`` index (short codes) and one column
        per GMT input position. Values are capacity factors (fractions).
    """
    sel = _WET_SEL if cooling == "wet" else _DRY_SEL
    gmt_array = np.asarray(gmt_array)
    gmt_clipped = clip_gmt(gmt_array, gmt_min=0.6, gmt_ceil=0.9)

    raw = predict_rime(gmt_clipped, _DATASET, _VAR, sel=sel, aggregate="mean")
    # raw shape: (12, n_years) — regions x time positions

    regions = _region_codes()
    return pd.DataFrame(raw, index=pd.Index(regions, name="region"))


def compute_degradation_ratios(
    gmt_array: np.ndarray,
    years: list[int],
    cooling: str = "wet",
    baseline_gwl: float = _DEFAULT_BASELINE_GWL,
) -> pd.DataFrame:
    """Compute degradation ratios: CF(GMT) / CF(baseline).

    Parameters
    ----------
    gmt_array
        GMT trajectory. Shape ``(n_years,)`` or ``(n_runs, n_years)``.
    years
        Year labels for the time axis. Length must match ``n_years``.
    cooling
        ``"wet"`` (freshwater) or ``"dry"`` (air).
    baseline_gwl
        Reference warming level (degC). Default 1.0.

    Returns
    -------
    pd.DataFrame
        Rows = R12 region short codes, columns = *years*. Values are ratios
        relative to baseline — below 1 under warming, above 1 if GMT
        dips below baseline.
    """
    sel = _WET_SEL if cooling == "wet" else _DRY_SEL
    cf = predict_cooling_cf(gmt_array, cooling=cooling)

    cf_baseline = predict_rime(
        np.array([baseline_gwl]), _DATASET, _VAR, sel=sel, aggregate="mean"
    )[:, 0]  # (12,)

    ratios = cf.values / cf_baseline[:, np.newaxis]
    return pd.DataFrame(ratios, index=cf.index.copy(), columns=years)


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


def _max_vintage_from_lifetime(
    tl: pd.DataFrame,
) -> dict[tuple[str, str], int]:
    """Last vintage year with ``technical_lifetime`` defined, per (node, tech).

    GAMS evaluates ``relation_activity`` by iterating all model years
    ``<= year_act`` as candidate vintages.  If a candidate vintage has no
    ``technical_lifetime`` row, compilation fails ("Technical lifetime not
    defined for node|tech|year").  The safe bound for ``year_act`` is
    therefore ``max(year_vtg)`` where ``technical_lifetime`` is defined —
    any ``year_act`` beyond that will cause GAMS to probe a vintage year
    with no lifetime entry.
    """
    return {
        (node, tech): int(grp["year_vtg"].max())
        for (node, tech), grp in tl.groupby(["node_loc", "technology"])
    }


_RELATION_PREFIX = "wet_cooling_cf_"


def _emit_constraint_rows(
    *,
    rel_name: str,
    node: str,
    year: int,
    parent_tech: str,
    cl_fresh: str | None,
    ot_fresh: str | None,
    parent_coeff: float,
) -> tuple[list[dict], dict]:
    """Build the (relation_activity rows, relation_upper row) for one cell.

    The constraint shape is::

        sum(freshwater variants) - r * share * f_cool * parent <= 0

    so the parent enters with a negative coefficient (passed in
    pre-multiplied as *parent_coeff*) and each freshwater variant tech
    enters with +1.
    """
    rel_act = [
        {
            "relation": rel_name,
            "node_rel": node,
            "year_rel": year,
            "node_loc": node,
            "technology": parent_tech,
            "year_act": year,
            "mode": "M1",
            "value": parent_coeff,
            "unit": "-",
        }
    ]
    for tech in (cl_fresh, ot_fresh):
        if tech is not None:
            rel_act.append(
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
    rel_up = {
        "relation": rel_name,
        "node_rel": node,
        "year_rel": year,
        "value": 0.0,
        "unit": "-",
    }
    return rel_act, rel_up


def _emit_per_region_year(
    *,
    row,
    rel_name: str,
    parent_tech: str,
    constrained_years: list[int],
    wet_cf_ratios: pd.DataFrame,
    s_ref: pd.Series,
    max_vtg: dict[tuple[str, str], int] | None,
) -> tuple[list[dict], list[dict], int]:
    """Emit relation rows for one (parent_tech, region) entry across years."""
    node = row["node_loc"]
    region_short = extract_region_code(node)
    f_cool = row["cooling_fraction"]

    if region_short not in s_ref.index:
        log.warning("No freshwater share for region %s, skipping", region_short)
        return [], [], 0
    share = float(s_ref[region_short])

    vtg_cap = max_vtg.get((node, parent_tech)) if max_vtg is not None else None

    rel_act_rows: list[dict] = []
    rel_up_rows: list[dict] = []
    n_skipped = 0
    for year in constrained_years:
        if vtg_cap is not None and year > vtg_cap:
            n_skipped += 1
            continue
        if region_short not in wet_cf_ratios.index:
            continue
        if year not in wet_cf_ratios.columns:
            log.warning("Year %d not in wet_cf_ratios columns, skipping", year)
            continue
        r = float(wet_cf_ratios.loc[region_short, year])
        parent_coeff = -(r * share * f_cool)
        act, up = _emit_constraint_rows(
            rel_name=rel_name,
            node=node,
            year=year,
            parent_tech=parent_tech,
            cl_fresh=row["cl_fresh_tech"],
            ot_fresh=row["ot_fresh_tech"],
            parent_coeff=parent_coeff,
        )
        rel_act_rows.extend(act)
        rel_up_rows.append(up)
    return rel_act_rows, rel_up_rows, n_skipped


def build_wet_cooling_constraints(
    addon_df: pd.DataFrame,
    technologies: set[str],
    wet_cf_ratios: pd.DataFrame,
    model_years: list[int] | None = None,
    min_year: int = _DEFAULT_MIN_YEAR,
    technical_lifetime: pd.DataFrame | None = None,
) -> dict:
    """Build wet-cooling ``relation_activity`` and ``relation_upper`` rows.

    For each (parent power technology, region, year) where the parent has
    a freshwater cooling addon, emit one relation that bounds total
    freshwater-cooled activity below the warming-impaired capacity:
    ``cl_fresh + ot_fresh ≤ r * share * f_cool * parent``.

    Parameters
    ----------
    addon_df
        The ``addon_conversion`` parameter DataFrame from a MESSAGE scenario.
    technologies
        Set of technology names present in the scenario.
    wet_cf_ratios
        Output of :func:`compute_degradation_ratios` with ``cooling="wet"``.
        Rows = regions (short codes), columns = model years; values are
        capacity-factor ratios relative to the baseline GWL.
    model_years
        Which years to constrain. If *None*, uses *wet_cf_ratios* columns.
    min_year
        Earliest year for constraints. Default 2045.
    technical_lifetime
        The ``technical_lifetime`` parameter DataFrame. When provided,
        ``relation_activity`` rows are only emitted for ``year_act`` values
        where the parent tech has active vintage capacity.  Without this,
        phase-out techs (e.g. ``coal_ppl_u``) produce GAMS errors:
        "Technical lifetime not defined for node|tech|year".

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
        model_years = [int(c) for c in wet_cf_ratios.columns]
    constrained_years = [y for y in model_years if y >= min_year]

    if not constrained_years:
        log.warning("No model years >= %d; returning empty constraints", min_year)
        return {
            "relation_activity": pd.DataFrame(),
            "relation_upper": pd.DataFrame(),
            "relation_names": [],
        }

    max_vtg = (
        _max_vintage_from_lifetime(technical_lifetime)
        if technical_lifetime is not None
        else None
    )

    rel_act_rows: list[dict] = []
    rel_up_rows: list[dict] = []
    relation_names: set[str] = set()
    n_skipped = 0

    for parent_tech, group in structure.groupby("parent_tech"):
        rel_name = f"{_RELATION_PREFIX}{parent_tech}"
        relation_names.add(rel_name)
        for _, row in group.iterrows():
            act, up, skipped = _emit_per_region_year(
                row=row,
                rel_name=rel_name,
                parent_tech=parent_tech,
                constrained_years=constrained_years,
                wet_cf_ratios=wet_cf_ratios,
                s_ref=s_ref,
                max_vtg=max_vtg,
            )
            rel_act_rows.extend(act)
            rel_up_rows.extend(up)
            n_skipped += skipped

    rel_act = pd.DataFrame(rel_act_rows)
    rel_up = pd.DataFrame(rel_up_rows)

    log.info(
        "Built wet-cooling constraints: %d relations, %d relation_activity entries, "
        "%d skipped (no active vintage), years %d-%d",
        len(relation_names),
        len(rel_act),
        n_skipped,
        min(constrained_years),
        max(constrained_years),
    )

    return {
        "relation_activity": rel_act,
        "relation_upper": rel_up,
        "relation_names": sorted(relation_names),
    }


# ---------------------------------------------------------------------------
# Dry cooling: capacity_factor replacement
# ---------------------------------------------------------------------------


def build_dry_cooling_factors(
    cf_air: pd.DataFrame,
    dry_ratios: pd.DataFrame,
    model_years: list[int],
    min_year: int = _DEFAULT_MIN_YEAR,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build replacement ``capacity_factor`` rows for ``__air`` technologies.

    Parameters
    ----------
    cf_air
        Existing ``capacity_factor`` rows for ``__air`` technologies,
        as returned by ``scenario.par("capacity_factor", ...)``.
    dry_ratios
        Output of :func:`compute_degradation_ratios` with ``cooling="dry"``.
        Rows = R12 short codes, columns = model years.
    model_years
        Which years the ratios cover.
    min_year
        Earliest year for modification. Default 2045.

    Returns
    -------
    old_cf
        Rows that will be removed (affected years only).
    new_cf
        Replacement rows with values scaled by dry degradation ratio.
    """
    if cf_air.empty:
        log.warning("No __air capacity_factor rows provided")
        return pd.DataFrame(), pd.DataFrame()

    constrained_years = [y for y in model_years if y >= min_year]
    if not constrained_years:
        log.warning("No model years >= %d for dry cooling", min_year)
        return pd.DataFrame(), pd.DataFrame()

    # Filter to affected years
    cf_affected = cf_air[cf_air["year_act"].isin(constrained_years)].copy()
    if cf_affected.empty:
        log.warning("No __air capacity_factor rows for years %s", constrained_years)
        return pd.DataFrame(), pd.DataFrame()

    old_cf = cf_affected.copy()

    # Apply dry degradation ratio per (node, year)
    new_cf = cf_affected.copy()
    for idx, row in new_cf.iterrows():
        region_short = extract_region_code(row["node_loc"])
        year = row["year_act"]
        if region_short in dry_ratios.index and year in dry_ratios.columns:
            ratio = float(dry_ratios.loc[region_short, year])
            new_cf.at[idx, "value"] = row["value"] * ratio

    n_modified = len(new_cf)
    log.info(
        "Built dry cooling factors: %d rows, years %d-%d",
        n_modified,
        min(constrained_years),
        max(constrained_years),
    )

    return old_cf, new_cf


# ---------------------------------------------------------------------------
# Scenario application
# ---------------------------------------------------------------------------


def _ratios_to_long(ratios: pd.DataFrame, regions: str) -> pd.DataFrame:
    """Wide (region x year) ratios → long (region, year, value), node-prefixed.

    Region values come from the RIME dataset as short codes (``"AFR"``);
    they are reprefixed with ``f"{regions}_"`` to match scenario node labels.
    """
    long = ratios.reset_index().melt(
        id_vars="region", var_name="year", value_name="value"
    )
    long["region"] = f"{regions}_" + long["region"].astype(str)
    return long


def apply_cooling_cids(
    scen,
    gmt: GmtArray,
    min_year: int = _DEFAULT_MIN_YEAR,
    commit_message: str | None = None,
    regions: str = "R12",
) -> None:
    """Apply wet + dry cooling CIDs to *scen* in place.

    Wet effects bound freshwater cooling activity via capacity-factor
    ratios written as ``relation_activity`` rows. Dry effects derate
    ``capacity_factor`` on ``__air`` technologies. The underlying
    wet-cooling activity-limit ratios and dry-cooling capacity-factor ratios
    are also persisted as scenario timeseries so downstream reporting can
    read them without re-running RIME.

    Parameters
    ----------
    scen
        MESSAGE scenario (must not be checked out).
    gmt
        GMT ensemble + year labels, as returned by :func:`load_magicc_gmt`.
    min_year
        Earliest year touched by the CID. Default 2045.
    commit_message
        Commit message for the parameter writes. Default
        ``"Apply cooling CIDs (wet+dry)"``.
    regions
        MESSAGE node codelist. Only ``"R12"`` is supported by the shipped
        RIME and freshwater-share data.
    """
    if regions != "R12":
        raise NotImplementedError(
            f"regions={regions!r}; this kernel currently only supports R12 "
            "(input RIME and freshwater-share data files are R12-coded)."
        )
    from message_ix_models.util import ScenarioInfo

    info = ScenarioInfo(scen)
    model_years = info.Y

    values = np.asarray(gmt.values)
    years = np.asarray(gmt.years, dtype=int)
    year_index = {int(year): i for i, year in enumerate(years)}
    last_year = int(years.max())
    matched_years = [
        int(year)
        for year in sorted(model_years)
        if int(year) in year_index or int(year) > last_year
    ]
    if not matched_years:
        raise ValueError(
            "No MESSAGE model years overlap with or follow GMT input years"
        )
    positions = [year_index.get(year, year_index[last_year]) for year in matched_years]
    gmt_model = values[:, positions] if values.ndim == 2 else values[positions]

    log.info("Computing wet cooling degradation ratios...")
    wet_ratios = compute_degradation_ratios(
        gmt_model,
        cooling="wet",
        years=matched_years,
    )

    log.info("Computing dry cooling degradation ratios...")
    dry_ratios = compute_degradation_ratios(
        gmt_model,
        cooling="dry",
        years=matched_years,
    )

    addon_df = scen.par("addon_conversion")
    technologies = set(scen.set("technology").tolist())
    tl = scen.par("technical_lifetime")
    constraints = build_wet_cooling_constraints(
        addon_df,
        technologies,
        wet_ratios,
        model_years=matched_years,
        min_year=min_year,
        technical_lifetime=tl,
    )
    rel_act = constraints["relation_activity"]
    rel_up = constraints["relation_upper"]
    rel_names = constraints["relation_names"]

    cf_all = scen.par("capacity_factor")
    cf_air = cf_all[cf_all["technology"].str.endswith("__air")].copy()
    old_cf, new_cf = build_dry_cooling_factors(
        cf_air,
        dry_ratios,
        model_years=matched_years,
        min_year=min_year,
    )

    has_wet = not rel_act.empty
    has_dry = not old_cf.empty

    if not has_wet and not has_dry:
        log.info("No cooling constraints needed")
        return

    msg = commit_message or "Apply cooling CIDs (wet+dry)"
    with scen.transact(msg):
        if has_wet:
            for name in rel_names:
                scen.add_set("relation", name)
            scen.add_par("relation_activity", rel_act)
            scen.add_par("relation_upper", rel_up)
        if has_dry:
            scen.remove_par("capacity_factor", old_cf)
            scen.add_par("capacity_factor", new_cf)

    # Persist the resolved cooling CID inputs as scenario timeseries. Wet and
    # dry are named by the MESSAGE-side control they feed, not only by the
    # RIME capacity-factor source variable.
    constrained = [y for y in matched_years if y >= min_year]
    ts_wet = frame_to_iamc(
        _ratios_to_long(wet_ratios.loc[:, constrained], regions),
        _WET_TIMESERIES_VARIABLE,
        "dimensionless",
    )
    ts_dry = frame_to_iamc(
        _ratios_to_long(dry_ratios.loc[:, constrained], regions),
        _DRY_TIMESERIES_VARIABLE,
        "dimensionless",
    )
    ts = pd.concat([ts_wet, ts_dry], ignore_index=True)
    if not ts.empty:
        scen.check_out(timeseries_only=True)
        try:
            scen.add_timeseries(ts)
            scen.commit(
                "Persist cooling CID inputs "
                "(Physical Climate Impact|Thermoelectric Cooling|*)"
            )
        except BaseException:
            scen.discard_changes()
            raise

    log.info(
        "Cooling CIDs applied: %d wet relations, %d wet constraints, "
        "%d dry capacity_factor rows, %d CID-input timeseries",
        len(rel_names) if has_wet else 0,
        len(rel_act) if has_wet else 0,
        len(new_cf) if has_dry else 0,
        len(ts),
    )
