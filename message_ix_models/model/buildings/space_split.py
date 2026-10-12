"""Carve space conditioning out of ``rc_spec``/``rc_therm`` as its own demand.

The buildings climate-impact kernel (:mod:`.buildings.impacts`) rewrites the
space-conditioning component of the aggregate residential/commercial demands
``rc_spec`` and ``rc_therm``. Those two commodities are MACRO sectors, so a
climate-driven increment written there enters MACRO's elastic demand pool and
is traded away against energy prices — which is wrong for a physical impact.

This module moves the space-conditioning component onto two new demand
commodities, ``space_cooling`` and ``space_heating``, at the same level
(``useful``) as their parents. They are deliberately *not* MACRO sectors, so
the MESSAGE-MACRO iteration never sees them: every commodity-to-sector
transfer in ``MESSAGE-MACRO_run.gms`` is gated on ``mapping_macro_sector``,
including the demand write-back, so unmapped ``demand`` keys are left
untouched. ``rc_spec``/``rc_therm`` keep the non-space residual and remain
elastic.

Three edits make the split, and all three are required:

Demand
    Per ``(node, year)`` the parent demand is multiplied by the retained
    share ``s = 1 - f`` and the carved share ``f`` lands on the new
    commodity, for model years at or after ``first_split_year``. ``f`` is the
    space-conditioning fraction of the aggregate from the buildings
    calibration (see :func:`space_split_shares`).

Supply
    One free identity technology per new commodity converts the parent
    commodity 1:1 into the new one. Total supply capability is therefore
    unchanged: the same unmodified upstream chain must produce
    ``s * D + f * D = D`` of the parent commodity, exactly as before. Cloning
    the fifteen serving technologies, or giving them a parallel mode, would
    instead duplicate their capacity, growth and relation envelopes.

MACRO reference
    ``aeei`` is rebased so MACRO's own target level for the parent sector
    falls to the residual. Without this, MACRO's first-order condition —
    calibrated against the full parent level through ``prfconst`` — drives
    the residual back up and the new demand ends up added to the service
    instead of carved out of it. See :func:`rebase_aeei` for why ``aeei`` is
    the only admissible surface and why the rebase leaves GDP invariant.

Nothing here touches the ``sector`` set, ``mapping_macro_sector``,
``prfconst``, ``demand_MESSAGE``, ``price_MESSAGE``, ``grow`` or any other
MACRO calibration parameter. ``grow`` in particular carries the GDP damages
of the ``_GDP_CI_*`` starters and must survive untouched.

Reporting: activity-derived tables are complete without any special
handling — the bridge draws the parent commodity 1:1, so the upstream chain
still produces the full parent demand and every fuel-side final-energy row
includes the space component (the bridge is also free, so the useful-level
price of the carved commodity equals the parent's). Tables keyed on the
``demand`` parameter or ``DEMAND`` variable per commodity (e.g. the legacy
useful-energy tables, inactive in the shipping configs) see only the
non-space residual on ``rc_spec``/``rc_therm``; if such a table is ever
activated, aggregate ``space_*`` into its parent there.
"""

import logging
from typing import Any

import pandas as pd
from iam_units import registry
from message_ix import Scenario

from message_ix_models.model.buildings.impacts import load_sector_fractions
from message_ix_models.tools.iamc import frame_to_iamc
from message_ix_models.tools.impacts.temporal import sample_to_model_years

log = logging.getLogger(__name__)

#: Parent commodity -> carved commodity.
SPLIT_MAP: dict[str, str] = {
    "rc_spec": "space_cooling",
    "rc_therm": "space_heating",
}

#: Carved commodity -> the identity technology that serves it.
BRIDGE_TEC: dict[str, str] = {
    "space_cooling": "space_cool_bridge",
    "space_heating": "space_heat_bridge",
}

#: Parent commodity -> the sector-fraction columns that size its space
#: component in ``rc_sector_fractions_<SSP>.csv``.
FRACTION_COLUMNS: dict[str, list[str]] = {
    "rc_spec": ["frac_resid_cool", "frac_comm_cool"],
    "rc_therm": ["frac_resid_heat", "frac_comm_heat"],
}

_LEVEL = "useful"
_MODE = "M1"
_TIME = "year"
_FINAL_ENERGY_UNIT = "EJ/yr"

#: MACRO sector-indexed items that must never gain a carved-commodity row.
MACRO_SECTOR_ITEMS: tuple[str, ...] = (
    "aeei",
    "demand_MESSAGE",
    "price_MESSAGE",
    "prfconst",
)


def first_model_year(scen: Scenario) -> int:
    """Return the scenario's ``firstmodelyear``."""
    cat = scen.set("cat_year")
    return int(cat.loc[cat["type_year"] == "firstmodelyear", "year"].iloc[0])


def space_split_shares(
    reference_scenario: str,
    years: list[int],
) -> pd.DataFrame:
    """Space-conditioning fractions of the parent demands, per (node, year).

    The fractions come from ``rc_sector_fractions_<SSP>.csv``, whose header
    records their construction: ``beta = buildings_sector_GWa /
    rc_aggregate_GWa``, split between residential and commercial, scoped to
    space heating (excluding cooking and hot water) and to
    ``resid_cool_electr + comm_cool_electr`` for cooling. Summing the
    residential and commercial columns therefore gives the share of the
    aggregate that space conditioning accounts for.

    Resampling matches :mod:`.buildings.impacts`: linear interpolation
    between source years, no extrapolation below the first source year, and
    forward-fill above the last.

    Parameters
    ----------
    reference_scenario
        SSP label keying the calibration table, e.g. ``"SSP3"``.
    years
        Years to return fractions for. Every year must be at or above the
        table's first year, else it is silently absent from the result.

    Returns
    -------
    pandas.DataFrame
        Columns ``node``, ``year``, and for each parent commodity
        ``f_<commodity>`` (carved share) and ``s_<commodity>`` (retained
        share).
    """
    frac_cols = [c for cols in FRACTION_COLUMNS.values() for c in cols]
    fractions = sample_to_model_years(
        load_sector_fractions(reference_scenario),
        ["node"],
        sorted(years),
        method="interpolate",
        value_cols=frac_cols,
    )

    out = fractions[["node", "year"]].copy()
    for parent, cols in FRACTION_COLUMNS.items():
        out[f"f_{parent}"] = fractions[cols].sum(axis=1)
        out[f"s_{parent}"] = 1.0 - out[f"f_{parent}"]

    missing = [y for y in years if y not in set(out["year"])]
    if missing:
        raise ValueError(
            f"No sector fractions for {reference_scenario} in years {missing}; "
            "the calibration table starts later than the requested split."
        )
    bad = out[[f"s_{p}" for p in FRACTION_COLUMNS]].min().min()
    if not bad > 0.0:
        raise ValueError(
            f"Retained share must be strictly positive; minimum is {bad}. "
            "A zero or negative residual would make the aeei rebase undefined."
        )
    return out


def split_demand(
    demand: pd.DataFrame,
    shares: pd.DataFrame,
    split_years: list[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split *demand* into residual parent rows and carved-commodity rows.

    Pure data operation. Only parent-commodity rows are read; those outside
    *split_years* are returned unchanged in the residual frame.

    Returns
    -------
    tuple of pandas.DataFrame
        ``(residual, carved)`` on the same columns as *demand*.
    """
    columns = list(demand.columns)
    keys = ["node", "year"]

    residual: list[pd.DataFrame] = []
    carved: list[pd.DataFrame] = []

    for parent, child in SPLIT_MAP.items():
        rows = demand[demand["commodity"] == parent]
        if rows.empty:
            raise ValueError(f"No {parent} rows in demand — buildings module absent?")

        inside = rows[rows["year"].isin(split_years)].merge(
            shares[keys + [f"s_{parent}", f"f_{parent}"]], on=keys, how="left"
        )
        if inside[f"s_{parent}"].isna().any():
            gaps = inside.loc[inside[f"s_{parent}"].isna(), keys].drop_duplicates()
            raise ValueError(f"Missing shares for {parent}: {gaps.to_dict('records')}")

        residual.append(rows[~rows["year"].isin(split_years)][columns])
        residual.append(
            inside.assign(value=lambda df: df["value"] * df[f"s_{parent}"])[columns]
        )
        carved.append(
            inside.assign(
                commodity=child,
                value=lambda df: df["value"] * df[f"f_{parent}"],
            )[columns]
        )

    return (
        pd.concat(residual, ignore_index=True),
        pd.concat(carved, ignore_index=True),
    )


def bridge_parameters(
    nodes: list[str],
    split_years: list[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build ``input``/``output`` rows for the identity bridge technologies.

    One unit of activity consumes one unit of the parent commodity and
    produces one unit of the carved commodity, in the same node and period.
    No cost, lifetime, capacity or bound parameters are written: the bridge
    must not add a capacity envelope of its own, since the whole point is
    that supply capability is unchanged. ``capacity_factor`` defaults to 1
    and the ``map_tec*`` sets are derived from these keys.
    """
    frames_in: list[pd.DataFrame] = []
    frames_out: list[pd.DataFrame] = []

    for parent, child in SPLIT_MAP.items():
        tec = BRIDGE_TEC[child]
        grid = pd.MultiIndex.from_product(
            [nodes, split_years], names=["node_loc", "year_act"]
        ).to_frame(index=False)
        common = grid.assign(
            technology=tec,
            year_vtg=lambda df: df["year_act"],
            mode=_MODE,
            time=_TIME,
            value=1.0,
            unit="-",
        )
        frames_in.append(
            common.assign(
                node_origin=lambda df: df["node_loc"],
                commodity=parent,
                level=_LEVEL,
                time_origin=_TIME,
            )
        )
        frames_out.append(
            common.assign(
                node_dest=lambda df: df["node_loc"],
                commodity=child,
                level=_LEVEL,
                time_dest=_TIME,
            )
        )

    return (
        pd.concat(frames_in, ignore_index=True),
        pd.concat(frames_out, ignore_index=True),
    )


def rebase_aeei(
    aeei: pd.DataFrame,
    shares: pd.DataFrame,
    duration_period: pd.DataFrame,
    split_years: list[int],
) -> pd.DataFrame:
    """Rebase ``aeei`` so MACRO's physical target for the parents is the residual.

    MACRO links its economic energy input to the MESSAGE demand through
    ``PHYSENE >= PRODENE * aeei_factor`` (``MACRO/macro_core.gms``), with
    ``aeei_factor`` the cumulative product of ``(1 - aeei) ** duration_period``
    from 1 at ``initializeyear_macro`` (``MACRO/macro_data_load.gms``). Scaling
    ``aeei_factor`` by the retained share ``s`` from the first split year on
    makes ``PHYSENE`` equal the residual demand while leaving ``PRODENE``,
    and therefore the CES production function and GDP, untouched:

    - ``NEW_PRODUCTION`` and ``NEW_ENERGY`` do not involve ``PHYSENE``.
    - ``COST_ENERGY``'s linear and quadratic terms both vanish where
      ``PHYSENE == enestart``, which is the converged state; both are now the
      residual, so ``EC`` is unchanged.
    - ``PRODENE`` is fixed at the MACRO base period from ``demand_base``, which
      the split leaves whole because *split_years* starts at
      ``firstmodelyear`` — one period after the base period.
    - ``PRODENE.LO = lotol * enestart / aeei_factor`` is invariant: numerator
      and denominator both scale by ``s``.

    The alternatives are not admissible. ``demand_MESSAGE`` only reaches the
    solve through its base-period value, which must stay at the calibrated
    level. ``prfconst`` is the CES share coefficient: moving it to shift
    MACRO's target level also moves the CES aggregate, recalibrating the
    production function and displacing the starter's damage-bearing GDP.

    Returns
    -------
    pandas.DataFrame
        Replacement rows for the parent sectors over *split_years* only, on
        the columns of *aeei*.
    """
    duration = duration_period.set_index("year")["value"].to_dict()
    ordered = sorted(split_years)

    out: list[pd.DataFrame] = []
    for parent in SPLIT_MAP:
        share = shares.set_index(["node", "year"])[f"s_{parent}"]
        rows = aeei[(aeei["sector"] == parent) & aeei["year"].isin(ordered)].copy()
        if len(rows) != len(share.index.get_level_values("node").unique()) * len(
            ordered
        ):
            raise ValueError(
                f"aeei rows for {parent} do not cover every (node, year) in the "
                f"split: {len(rows)} rows for {len(ordered)} years."
            )

        def ratio(row: "pd.Series[Any]") -> float:
            year = int(row["year"])
            previous = ordered[ordered.index(year) - 1] if year != ordered[0] else None
            s_now = float(share.loc[(row["node"], year)])
            s_prev = (
                1.0 if previous is None else float(share.loc[(row["node"], previous)])
            )
            return (s_now / s_prev) ** (1.0 / duration[year])

        out.append(
            rows.assign(
                value=lambda df: 1.0 - (1.0 - df["value"]) * df.apply(ratio, axis=1)  # type: ignore[arg-type]
            )
        )

    return pd.concat(out, ignore_index=True)


def aeei_factor(aeei: pd.DataFrame, duration_period: pd.DataFrame) -> pd.DataFrame:
    """Cumulative AEEI factor, as ``MACRO/macro_data_load.gms`` computes it.

    Used only to falsify the rebase: the post-split factor must equal the
    retained share times the pre-split factor for the parent sectors, and be
    unchanged for every other sector and for the base period.
    """
    duration = duration_period.set_index("year")["value"].to_dict()
    frame = aeei.sort_values(["node", "sector", "year"]).copy()
    frame["step"] = [
        (1.0 - v) ** duration[int(y)]
        for v, y in zip(frame["value"], frame["year"], strict=True)
    ]
    frame["factor"] = frame.groupby(["node", "sector"])["step"].cumprod()
    return frame[["node", "sector", "year", "factor"]]


def apply_space_demand_split(
    scen: Scenario,
    *,
    reference_scenario: str,
    first_split_year: int | None = None,
    commit_message: str | None = None,
) -> dict[str, Any]:
    """Carve space conditioning out of ``rc_spec``/``rc_therm`` on *scen*, in place.

    *scen* must be a clone: this rewrites ``demand`` and ``aeei`` and adds
    two commodities and two technologies. It does **not** set the scenario as
    default. The verification runs after the split is committed, so a failed
    result leaves the split on *scen*; discard the clone in that case.

    Parameters
    ----------
    scen
        Target scenario, not checked out.
    reference_scenario
        SSP label keying the buildings calibration, e.g. ``"SSP3"``.
    first_split_year
        First year to split. Defaults to the scenario's ``firstmodelyear``,
        which keeps the MACRO base period whole — required for the ``aeei``
        rebase to leave the calibration reference intact.
    commit_message
        Passed to :meth:`~message_ix.Scenario.transact`.

    Returns
    -------
    dict
        Record of what was written, plus the pre-solve verification result
        under ``"verification"``.
    """
    existing = set(scen.set("commodity"))
    clash = sorted(set(SPLIT_MAP.values()) & existing)
    if clash:
        raise ValueError(f"Commodities {clash} already on the scenario; refusing.")

    y0 = first_split_year or first_model_year(scen)
    demand = scen.par("demand")
    parent_rows = demand[demand["commodity"].isin(SPLIT_MAP)]
    split_years = sorted(y for y in parent_rows["year"].unique() if int(y) >= y0)
    nodes = sorted(parent_rows["node"].unique())

    shares = space_split_shares(reference_scenario, [int(y) for y in split_years])
    shares = shares[shares["node"].isin(nodes)]
    residual, carved = split_demand(parent_rows, shares, split_years)
    bridge_in, bridge_out = bridge_parameters(nodes, [int(y) for y in split_years])

    aeei_before = scen.par("aeei")
    aeei_new = rebase_aeei(
        aeei_before, shares, scen.par("duration_period"), [int(y) for y in split_years]
    )

    with scen.transact(
        commit_message or "Split space conditioning out of rc_spec/rc_therm"
    ):
        scen.add_set("commodity", sorted(SPLIT_MAP.values()))
        scen.add_set("technology", sorted(BRIDGE_TEC.values()))
        scen.remove_par("demand", parent_rows)
        scen.add_par("demand", pd.concat([residual, carved], ignore_index=True))
        scen.add_par("input", bridge_in)
        scen.add_par("output", bridge_out)
        scen.add_par("aeei", aeei_new)

    record: dict[str, Any] = {
        "reference_scenario": reference_scenario,
        "first_split_year": int(y0),
        "split_years": [int(y) for y in split_years],
        "nodes": nodes,
        "demand_rows_residual": len(residual),
        "demand_rows_carved": len(carved),
        "bridge_input_rows": len(bridge_in),
        "bridge_output_rows": len(bridge_out),
        "aeei_rows_rebased": len(aeei_new),
        "share_range": {
            f"s_{p}": [float(shares[f"s_{p}"].min()), float(shares[f"s_{p}"].max())]
            for p in SPLIT_MAP
        },
    }
    record["verification"] = verify_space_split(
        scen,
        reference_demand=parent_rows,
        reference_aeei=aeei_before,
        reference_scenario=reference_scenario,
        first_split_year=int(y0),
        source="demand",
    )
    log.info("Space split applied: %s", record)
    return record


def _demand_to_final_energy_iamc(
    demand: pd.DataFrame,
    variable: str,
) -> pd.DataFrame:
    """Convert CID demand from MESSAGE GWa to IAMC final-energy rows."""
    converted = demand.assign(
        value=lambda df: (
            registry.Quantity(df["value"].to_numpy(), "GWa/year")
            .to(_FINAL_ENERGY_UNIT)
            .magnitude
        )
    )
    return frame_to_iamc(
        converted,
        variable,
        _FINAL_ENERGY_UNIT,
        region_col="node",
    )


def apply_space_cids(
    scen: Scenario,
    cooling_demand: pd.DataFrame,
    heating_demand: pd.DataFrame,
    *,
    commit_message: str | None = None,
) -> dict[str, Any]:
    """Write the RIME building CIDs onto the carved space demands, in place.

    The split leaves ``space_cooling``/``space_heating`` carrying the
    STURM-calibrated baseline share ``f * D``. This replaces those values
    with the absolute RIME-driven service level from
    :func:`.buildings.impacts.compute_building_cids` — the same substitution
    the pre-split kernel made inside ``rc_spec``/``rc_therm``, now landing on
    commodities outside ``mapping_macro_sector`` so MACRO never trades the
    climate increment away. The ``rc_*`` residual is not touched.

    Every existing carved-demand row must receive a CID value: a partially
    covered rewrite would silently mix baseline and impact years.

    Also persists the CID levels as ``Final Energy|Residential and
    Commercial|{Cooling,Heating}`` timeseries in EJ/yr, matching the
    pre-split kernel. Does **not** set the scenario as default.
    """
    cids = {"space_cooling": cooling_demand, "space_heating": heating_demand}
    demand = scen.par("demand")
    replacement: list[pd.DataFrame] = []
    record: dict[str, Any] = {}

    for child, cid in cids.items():
        rows = demand[demand["commodity"] == child]
        if rows.empty:
            raise ValueError(f"No {child} rows in demand — split not applied?")
        merged = rows.merge(
            cid[["node", "year", "value"]].rename(columns={"value": "_cid"}),
            on=["node", "year"],
            how="left",
        )
        if merged["_cid"].isna().any():
            gaps = merged.loc[merged["_cid"].isna(), ["node", "year"]]
            raise ValueError(
                f"CID does not cover every {child} demand row: "
                f"{gaps.to_dict('records')}"
            )
        replacement.append(
            merged.assign(value=lambda df: df["_cid"])[list(rows.columns)]
        )
        record[child] = {
            "rows": int(len(rows)),
            "baseline_total_GWa": float(rows["value"].sum()),
            "cid_total_GWa": float(merged["_cid"].sum()),
        }

    with scen.transact(commit_message or "Write building CIDs to space demands"):
        for child in cids:
            scen.remove_par("demand", demand[demand["commodity"] == child])
        scen.add_par("demand", pd.concat(replacement, ignore_index=True))

    ts = pd.concat(
        [
            _demand_to_final_energy_iamc(
                cooling_demand, "Final Energy|Residential and Commercial|Cooling"
            ),
            _demand_to_final_energy_iamc(
                heating_demand, "Final Energy|Residential and Commercial|Heating"
            ),
        ],
        ignore_index=True,
    )
    if not ts.empty:
        scen.check_out(timeseries_only=True)
        try:
            scen.add_timeseries(ts)
            scen.commit(
                "Persist buildings CID demand "
                "(Final Energy|Residential and Commercial|{Cooling,Heating})"
            )
        except BaseException:
            scen.discard_changes()
            raise

    log.info("Space CIDs applied: %s", record)
    return record


def verify_space_split(
    scen: Scenario,
    *,
    reference_demand: pd.DataFrame,
    reference_aeei: pd.DataFrame,
    reference_scenario: str,
    first_split_year: int,
    source: str = "demand",
) -> dict[str, Any]:
    """Falsify the split on *scen*.

    Four independent checks, each able to fail on a plausible defect:

    1. **Conservation.** Carved plus residual must equal the pre-split parent
       demand for every ``(node, year)``. Catches a mis-joined share table, a
       dropped year, or double application.
    2. **MACRO invisibility.** ``sector`` and ``mapping_macro_sector`` must be
       unchanged, and no sector-indexed MACRO parameter may name a carved
       commodity. Catches the fatal mistake of registering the carved
       commodities as MACRO sectors.
    3. **AEEI rebase identity.** The recomputed cumulative factor must equal
       the retained share times the pre-split factor for the parents, be
       unchanged for the other sectors, and be unchanged before the first
       split year. Catches an off-by-one in the period recursion or the wrong
       duration exponent.
    4. **Supply reachability.** Each carved commodity must have exactly one
       producing technology whose input is its parent at 1:1.

    Parameters
    ----------
    reference_demand
        Pre-split ``demand`` rows for the parent commodities.
    reference_aeei
        Pre-split ``aeei`` rows.
    source
        ``"demand"`` to read the parameter, ``"DEMAND"`` to read the solved
        variable. A MESSAGE-MACRO solve never writes the ``demand``
        parameter back to the database — only the ``DEMAND`` variable
        reflects MACRO's converged demands — so a post-solve check must use
        ``"DEMAND"``.

    Returns
    -------
    dict
        ``{"passed": bool, "checks": {...}}``, each check carrying the worst
        deviation it found.
    """
    checks: dict[str, Any] = {}
    keys = ["node", "year"]

    if source == "demand":
        current = scen.par("demand")
    else:
        current = (
            scen.var("DEMAND")
            .rename(columns={"lvl": "value"})
            .drop(columns=["mrg"], errors="ignore")
        )
    current = current[
        (current["level"] == _LEVEL)
        & (current["year"] >= first_split_year)
        & current["commodity"].isin(list(SPLIT_MAP) + list(SPLIT_MAP.values()))
    ]

    conservation: dict[str, Any] = {}
    for parent, child in SPLIT_MAP.items():
        want = (
            reference_demand[
                (reference_demand["commodity"] == parent)
                & (reference_demand["year"] >= first_split_year)
            ]
            .groupby(keys, as_index=False)["value"]
            .sum()
        )
        got = (
            current[current["commodity"].isin([parent, child])]
            .groupby(keys, as_index=False)["value"]
            .sum()
        )
        merged = want.merge(got, on=keys, how="outer", suffixes=("_ref", "_now"))
        merged["rel"] = (merged["value_now"] - merged["value_ref"]).abs() / merged[
            "value_ref"
        ].abs()
        conservation[parent] = {
            "keys": int(len(merged)),
            "unmatched": int(
                merged[["value_ref", "value_now"]].isna().any(axis=1).sum()
            ),
            "max_rel_deviation": float(merged["rel"].max()),
        }
    checks["conservation"] = conservation

    carved = list(SPLIT_MAP.values())
    invisibility: dict[str, Any] = {
        "sector_set": sorted(scen.set("sector")),
        "mapping_rows": int(len(scen.set("mapping_macro_sector"))),
        "carved_in_sector_set": sorted(set(carved) & set(scen.set("sector"))),
    }
    for item in MACRO_SECTOR_ITEMS:
        frame = scen.par(item)
        invisibility[f"{item}_carved_rows"] = int(
            frame["sector"].isin(carved).sum() if "sector" in frame else 0
        )
    checks["macro_invisibility"] = invisibility

    duration = scen.par("duration_period")
    before = aeei_factor(reference_aeei, duration).set_index(["node", "sector", "year"])
    after = aeei_factor(scen.par("aeei"), duration).set_index(
        ["node", "sector", "year"]
    )
    rebase_years = sorted(
        int(y)
        for y in after.index.get_level_values("year").unique()
        if int(y) >= first_split_year
    )
    shares = space_split_shares(reference_scenario, rebase_years).set_index(
        ["node", "year"]
    )
    rebase: dict[str, Any] = {}
    for sector in sorted(after.index.get_level_values("sector").unique()):
        pair = before.xs(sector, level="sector").join(
            after.xs(sector, level="sector"), lsuffix="_ref", rsuffix="_now"
        )
        expected = pd.Series(1.0, index=pair.index)
        if sector in SPLIT_MAP:
            s = shares[f"s_{sector}"]
            for key in pair.index:
                if int(key[1]) >= first_split_year:
                    expected.loc[key] = float(s.loc[key])
        deviation = (pair["factor_now"] / pair["factor_ref"] - expected).abs().max()
        rebase[sector] = float(deviation)
    checks["aeei_rebase"] = rebase

    output = scen.par("output", filters={"commodity": carved})
    supply: dict[str, Any] = {}
    for child in carved:
        rows = output[output["commodity"] == child]
        tecs = sorted(rows["technology"].unique())
        inputs = scen.par("input", filters={"technology": tecs})
        supply[child] = {
            "technologies": tecs,
            "output_coefficients": sorted(rows["value"].unique().tolist()),
            "input_commodities": sorted(inputs["commodity"].unique().tolist()),
            "input_coefficients": sorted(inputs["value"].unique().tolist()),
        }
    checks["supply"] = supply

    passed = (
        all(
            c["max_rel_deviation"] < 1e-9 and c["unmatched"] == 0
            for c in conservation.values()
        )
        and not invisibility["carved_in_sector_set"]
        and all(invisibility[f"{i}_carved_rows"] == 0 for i in MACRO_SECTOR_ITEMS)
        and all(v < 1e-9 for v in rebase.values())
        and all(
            supply[child]["technologies"] == [BRIDGE_TEC[child]]
            and supply[child]["output_coefficients"] == [1.0]
            and supply[child]["input_commodities"] == [parent]
            and supply[child]["input_coefficients"] == [1.0]
            for parent, child in SPLIT_MAP.items()
        )
    )
    return {"passed": bool(passed), "checks": checks}
