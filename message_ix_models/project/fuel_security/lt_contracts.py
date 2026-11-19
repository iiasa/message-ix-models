# -*- coding: utf-8 -*-
"""
Add long-term LNG and pipeline gas contracts as lower activity bounds

Contracts are read from ``data/fuel_security/lt_gas_contracts/lt_gas_contracts.csv``
(country level), collapsed to MESSAGE regions, averaged over each model period and
added as ``bound_activity_lo`` on the bilateral export technologies
(``LNG_shipped_exp_<imp>``, ``gas_piped_exp_<imp>``) at the exporting node.
"""
import logging
from typing import Iterable, Optional

import numpy as np
import pandas as pd
import yaml
import message_ix

from message_ix_models.util import package_data_path

log = logging.getLogger(__name__)

# Energy conversion (net calorific value, consistent with MESSAGE energy units).
# The dataset already expresses every contract in Mt LNG-equivalent, using
# 1 Mt LNG = 1.36 bcm (GIIGNL). LNG NCV ~48.6 GJ/t -> 48.6 PJ per Mt.
PJ_PER_GWA = 31.536
GWA_PER_MT_LNG = 48.6 / PJ_PER_GWA  # ~1.54 GWa per Mt LNG (~1.13 GWa per bcm)

# Contract statuses treated as binding commitments
BINDING_STATUSES = ("in force", "contracted, future start", "expiring", "force majeure")

TRADE_TECHNOLOGY = {"LNG": "LNG_shipped", "Pipeline": "gas_piped"}


def load_contracts(
    path=None,
    statuses: Iterable[str] = BINDING_STATUSES,
    include_pre_fid: bool = False,
    exclude_force_majeure: bool = False,
) -> pd.DataFrame:
    """Load the country-level contract dataset and keep binding, dated contracts."""
    path = path or package_data_path("fuel_security", "lt_gas_contracts", "lt_gas_contracts.csv")
    df = pd.read_csv(path)

    keep = set(statuses) | ({"contracted, project pre-FID"} if include_pre_fid else set())
    df = df[df["Status"].isin(keep)]
    if exclude_force_majeure:
        df = df[~df["Notes"].fillna("").str.contains("force majeure")]

    undated = df["Contract Start Year"].isna() | df["Contract End Year"].isna()
    if undated.any():
        log.info(f"Dropping {undated.sum()} contracts without start/end year "
                 f"({df.loc[undated, 'Trade Amount (Mt LNG-eq/yr)'].sum():.1f} Mt LNG-eq/yr)")
    df = df[~undated & df["Trade Amount (Mt LNG-eq/yr)"].notna()]

    return df.astype({"Contract Start Year": int, "Contract End Year": int})


def iso_to_region(message_regions: str = "R12") -> dict:
    """Map ISO3 code -> MESSAGE region (e.g. 'USA' -> 'R12_NAM')."""
    with open(package_data_path("node", message_regions + ".yaml"), "r") as f:
        regions = yaml.safe_load(f)
    return {iso: r for r, v in regions.items() if r != "World" for iso in v.get("child", [])}


def contracts_to_regions(contracts: pd.DataFrame, message_regions: str = "R12") -> pd.DataFrame:
    """Collapse contracts to exporter/importer regions and assign trade technologies.

    Contracts with a PORTFOLIO (destination-flexible or aggregator) counterpart,
    unmapped countries, or exporter and importer in the same region are dropped.
    """
    rmap = iso_to_region(message_regions)
    df = contracts.copy()
    df["exporter"] = df["Exporter ISO3"].map(rmap)
    df["importer"] = df["Importer ISO3"].map(rmap)

    portfolio = (df["Exporter ISO3"] == "PORTFOLIO") | (df["Importer ISO3"] == "PORTFOLIO")
    unmapped = ~portfolio & (df["exporter"].isna() | df["importer"].isna())
    intra = ~portfolio & ~unmapped & (df["exporter"] == df["importer"])
    for mask, why in ((portfolio, "PORTFOLIO counterpart"), (unmapped, "unmapped country"),
                      (intra, "intra-region")):
        if mask.any():
            log.info(f"Dropping {mask.sum()} contracts ({why}): "
                     f"{df.loc[mask, 'Trade Amount (Mt LNG-eq/yr)'].sum():.1f} Mt LNG-eq/yr")
    if unmapped.any():
        log.warning("Unmapped ISO3 codes: "
                    f"{sorted(set(df.loc[unmapped, ['Exporter ISO3', 'Importer ISO3']].values.ravel()) - set(rmap))}")
    df = df[~(portfolio | unmapped | intra)]

    df["node_loc"] = df["exporter"]
    df["technology"] = (df["Pipeline or LNG"].map(TRADE_TECHNOLOGY) + "_exp_"
                        + df["importer"].str.split("_").str[-1].str.lower())
    df["value_gwa"] = df["Trade Amount (Mt LNG-eq/yr)"] * GWA_PER_MT_LNG
    return df


def period_average(df: pd.DataFrame, years: Iterable[int], model_years: Iterable[int]) -> pd.DataFrame:
    """Average contracted volumes over each model period.

    Model year ``y`` represents the years ``(p, y]``, where ``p`` is the preceding
    year in ``years`` (works for 5- and 10-year steps alike). A contract running for
    ``n`` of those years contributes ``value * n / (y - p)``.
    """
    years = sorted(set(years))
    out = []
    for y in sorted(model_years):
        idx = years.index(y)
        if idx == 0:
            raise ValueError(f"No preceding year to define the period length of {y}")
        p = years[idx - 1]
        n = (np.minimum(df["Contract End Year"], y) - np.maximum(df["Contract Start Year"], p + 1) + 1).clip(lower=0)
        share = n / (y - p)
        out.append(df.assign(year_act=y, value=df["value_gwa"] * share)[share > 0])
    if not out:
        return df.iloc[0:0].assign(year_act=[], value=[])
    return (pd.concat(out)
            .groupby(["node_loc", "technology", "year_act"], as_index=False)["value"].sum())


def contract_bounds(
    scenario: message_ix.Scenario,
    contracts: Optional[pd.DataFrame] = None,
    message_regions: str = "R12",
    contract_share: float = 0.9,
    last_year: Optional[int] = None,
) -> pd.DataFrame:
    """Build ``bound_activity_lo`` for `scenario` from the contract dataset.

    Args:
        contracts: Output of :func:`load_contracts`; loaded with defaults if None.
        contract_share: Fraction of contracted volume enforced (default 0.9, i.e.
            ~10% downward quantity tolerance under take-or-pay contracts).
        last_year: Optionally stop bounding after this year.
    """
    contracts = load_contracts() if contracts is None else contracts
    df = contracts_to_regions(contracts, message_regions)

    years = [int(y) for y in scenario.set("year")]
    model_years = [y for y in years if y >= scenario.firstmodelyear and (last_year is None or y <= last_year)]
    bounds = period_average(df, years, model_years)
    bounds["value"] *= contract_share

    # Keep only export technologies that exist at the exporting node
    existing = (scenario.par("output", filters={"technology": list(bounds["technology"].unique())})
                [["node_loc", "technology"]].drop_duplicates())
    merged = bounds.merge(existing, on=["node_loc", "technology"], how="left", indicator=True)
    missing = merged["_merge"] == "left_only"
    if missing.any():
        log.warning("No trade technology in scenario for (dropped): "
                    + ", ".join(sorted({f"{n}:{t}" for n, t in merged.loc[missing, ['node_loc', 'technology']].values})))
    bounds = merged[~missing].drop(columns="_merge")

    return message_ix.make_df(
        "bound_activity_lo",
        node_loc=bounds["node_loc"],
        technology=bounds["technology"],
        year_act=bounds["year_act"],
        mode="M1",
        time="year",
        value=bounds["value"].round(4),
        unit="GWa",
    )


def remove_contract_bounds(scenario: message_ix.Scenario, shocked: pd.DataFrame) -> pd.DataFrame:
    """Let a shock supersede contracts: drop ``bound_activity_lo`` on shocked entries.

    Call this from a shock scenario (FSU/MEA/NAM) *inside* or after the transaction
    that adds the shock. `shocked` needs columns ``node_loc``, ``technology`` and
    ``year_act`` (e.g. the shock's own bound data); every lower bound on those
    entries is removed, so the shock alone constrains them.

    Returns the removed rows.
    """
    keys = ["node_loc", "technology", "year_act"]
    shocked = shocked[keys].drop_duplicates()
    lo = scenario.par("bound_activity_lo", filters={"technology": list(shocked["technology"].unique())})
    rem = lo.merge(shocked, on=keys) if not lo.empty else lo
    if rem.empty:
        return rem

    log.info(f"Shock supersedes {len(rem)} contract bounds "
             f"({rem['value'].sum():.1f} GWa over all years)")
    with scenario.transact("Remove contract bounds superseded by shock"):
        scenario.remove_par("bound_activity_lo", rem)
    return rem


def resolve_bound_conflicts(scenario: message_ix.Scenario) -> pd.DataFrame:
    """Clip ``bound_activity_lo`` to ``bound_activity_up`` where both are set.

    Restriction/conflict sensitivities (FSU, MEA) add upper bounds, possibly 0, on the
    same export technologies; without this the scenario becomes infeasible.
    Returns the rows that were changed (with their original lo value).
    """
    idx = ["node_loc", "technology", "year_act", "mode", "time"]
    lo = scenario.par("bound_activity_lo")
    up = scenario.par("bound_activity_up")
    if lo.empty or up.empty:
        return lo.iloc[0:0]
    m = lo.merge(up[idx + ["value"]], on=idx, suffixes=("", "_up"))
    m = m[m["value"] > m["value_up"]]
    if m.empty:
        return m

    log.info(f"Clipping {len(m)} bound_activity_lo values to bound_activity_up")
    new = m.assign(value=m["value_up"]).drop(columns="value_up")
    with scenario.transact("Clip bound_activity_lo to bound_activity_up"):
        scenario.remove_par("bound_activity_lo", m[idx])
        new = new[new["value"] > 0]
        if not new.empty:
            scenario.add_par("bound_activity_lo", new)
    return m


def fit_dynamic_constraints(scenario: message_ix.Scenario, bounds: pd.DataFrame) -> pd.DataFrame:
    """``initial_activity_up`` needed so each lower bound in `bounds` is reachable.

    MESSAGE's ACTIVITY_CONSTRAINT_UP is, per (node, technology, year, time)::

        ACT(y) <= initial * A + ACT(y-1) * (1 + g)^d,   A = ((1 + g)^d - 1) / g  (d if g = 0)

    with g = ``growth_activity_up``, d = ``duration_period``, and ACT(y-1) replaced by
    ``historical_activity`` before the first model year. Taking ACT(y-1) at its
    smallest allowed value (its lower bound, or its history) gives the initial term
    that guarantees ACT(y) can reach the bound in `bounds`:

        initial >= (bound(y) - base(y-1) * (1 + g)^d) / A

    Only rows that already have a dynamic constraint are considered, and an existing
    ``initial_activity_up`` is only ever raised. The soft relaxation is ignored, so the
    result is conservative. Growth is still capped by g above the contracted volume.

    Returns:
        ``initial_activity_up`` rows to add (empty if the bounds already fit).
    """
    keys = ["node_loc", "technology", "year_act", "time"]
    techs = list(bounds["technology"].unique())
    g = scenario.par("growth_activity_up", filters={"technology": techs})
    i = scenario.par("initial_activity_up", filters={"technology": techs})
    dyn = pd.concat([g[keys], i[keys]]).drop_duplicates()
    need = bounds.groupby(keys, as_index=False)["value"].sum().merge(dyn, on=keys)
    if need.empty:
        return i.iloc[0:0]

    years = sorted(int(y) for y in scenario.set("year"))
    prev = dict(zip(years[1:], years[:-1]))
    fmy = scenario.firstmodelyear
    dp = scenario.par("duration_period")
    dur = dict(zip(dp["year"].astype(int), dp["value"]))
    # Lower bounds as they stand after the contracts were added, summed over modes
    lo = scenario.par("bound_activity_lo", filters={"technology": techs}).groupby(keys)["value"].sum()
    hist = scenario.par("historical_activity", filters={"technology": techs}).groupby(keys)["value"].sum()
    gv = g.set_index(keys)["value"]
    iv = i.set_index(keys)["value"]

    rows = []
    for r in need.itertuples(index=False):
        k = (r.node_loc, r.technology, r.year_act, r.time)
        py = prev.get(r.year_act)
        base = (lo if py is not None and py >= fmy else hist).get(
            (r.node_loc, r.technology, py, r.time), 0.0)
        d, gr = dur[r.year_act], gv.get(k, 0.0)
        growth = (1 + gr) ** d
        a = (growth - 1) / gr if gr else d
        req = (r.value - base * growth) / a
        if req > iv.get(k, 0.0) + 1e-9:
            # Small margin so the bound is not exactly on the dynamic limit
            rows.append(dict(zip(keys, k), value=round(req * 1.001 + 1e-4, 4)))

    unit = i["unit"].iloc[0] if not i.empty else "GWa"
    return pd.DataFrame(rows, columns=keys + ["value"]).assign(unit=unit)


def add_contract_bounds(
    scenario: message_ix.Scenario,
    dynamic_constraints: str = "fit",
    **kwargs,
) -> pd.DataFrame:
    """Add long-term contract lower bounds to `scenario` (in place).

    Args:
        dynamic_constraints: How to keep growth limits (``growth_activity_up`` /
            ``initial_activity_up``) from blocking contract volumes:

            - "fit" (default): keep them, and raise ``initial_activity_up`` only where
              a bound would otherwise be unreachable (:func:`fit_dynamic_constraints`).
            - "remove": delete them on every bounded (node, technology, year). This
              leaves those routes with no upper growth limit at all and lets them
              expand far beyond the contracts (FSU->CHN reached 1,747 GWa by 2070).
            - "keep": leave them unchanged (may be infeasible).
        **kwargs: Passed to :func:`contract_bounds`.

    Returns:
        The ``bound_activity_lo`` data added.
    """
    if dynamic_constraints not in ("fit", "remove", "keep"):
        raise ValueError(f"dynamic_constraints={dynamic_constraints!r}")
    bounds = contract_bounds(scenario, **kwargs)
    log.info(f"Adding {len(bounds)} contract bounds; total by year (GWa):\n"
             f"{bounds.groupby('year_act')['value'].sum().round(1).to_string()}")

    with scenario.transact("Add long-term gas contract bounds"):
        # Replace (not stack on) any existing lo bound on the same index
        idx = ["node_loc", "technology", "year_act", "mode", "time"]
        old = scenario.par("bound_activity_lo", filters={"technology": list(bounds["technology"].unique())})
        if not old.empty:
            old = old.merge(bounds[idx], on=idx)
            if not old.empty:
                scenario.remove_par("bound_activity_lo", old[idx])
        scenario.add_par("bound_activity_lo", bounds)

        if dynamic_constraints == "fit":
            init = fit_dynamic_constraints(scenario, bounds)
            if not init.empty:
                log.info(f"...raising initial_activity_up on {len(init)} entries so "
                         "contract bounds are reachable under the growth limits")
                scenario.add_par("initial_activity_up", init)
        elif dynamic_constraints == "remove":
            for par in ("growth_activity_up", "initial_activity_up"):
                cur = scenario.par(par, filters={"technology": list(bounds["technology"].unique())})
                rem = cur.merge(bounds[["node_loc", "technology", "year_act"]], on=["node_loc", "technology", "year_act"])
                if not rem.empty:
                    log.info(f"...removing {len(rem)} {par}")
                    scenario.remove_par(par, rem)

    resolve_bound_conflicts(scenario)
    return bounds

