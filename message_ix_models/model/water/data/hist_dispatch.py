"""Per-basin merit-order dispatch for historical supply-side activity."""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

import numpy as np
import pandas as pd
from message_ix import make_df

from message_ix_models.model.water.config import Config
from message_ix_models.model.water.data.demands import (
    groundwater_share_floor,
    read_water_availability,
)
from message_ix_models.model.water.utils import (
    GW_ELEC_DEPTH_ADDER_GWA_KM3,
    GW_FOSSIL_ELEC_MULTIPLIER,
    GW_FOSSIL_VAR_COST_USD_KM3,
    HIST_DISPATCH_ELEC_PRICE_USD_GWA,
    KM3_TO_MCM,
    SW_ELEC_INTENSITY_GWA_KM3,
    USD_KM3_TO_USD_MCM,
    GWa_KM3_TO_GWa_MCM,
)
from message_ix_models.util import broadcast, package_data_path

if TYPE_CHECKING:
    from message_ix_models import Context


Numeric = Union[float, pd.Series]


# Sectoral withdrawal at the historical anchor currently uses SSP2 baseline
# files for every build.
_WITHDRAWAL_CSVS = (
    "ssp2_regional_urban_withdrawal_baseline.csv",
    "ssp2_regional_rural_withdrawal_baseline.csv",
    "ssp2_regional_manufacturing_withdrawal_baseline.csv",
)


def _as_series(value: Numeric, index: pd.Index) -> pd.Series:
    if isinstance(value, pd.Series):
        return value.reindex(index).fillna(0).astype(float)
    return pd.Series(float(value), index=index)


def read_hist_capacity(context: "Context") -> pd.DataFrame:
    """Historical SW/GW basin capacity (km3/yr), filtered to the build's valid basins.

    ``BCU_name`` is left unprefixed; callers add the ``B`` prefix and unit
    scaling they need.
    """
    cap_path = package_data_path(
        "water",
        "availability",
        f"historical_new_cap_gw_sw_km3_year_{context.regions}.csv",
    )
    df = pd.read_csv(cap_path)
    valid = Config.from_context(context).valid_basins
    return df[df["BCU_name"].isin(valid)].copy()


def merit_order_dispatch(
    capacity: dict[str, pd.Series],
    inputs: dict[str, dict[str, Numeric]],
    var_cost: dict[str, Numeric],
    demand: pd.Series,
    commodity_prices: dict[str, float],
) -> dict[str, pd.Series]:
    """Dispatch ``demand`` against ``capacity`` ordered by per-unit operating cost.

    Per-node opex is ``sum_c inputs[t][c] * commodity_prices[c] + var_cost[t]``.
    Ranking is computed per node and may differ across nodes when an input
    or var_cost is itself per-node. Raises ``ValueError`` when residual
    demand at any node exceeds total capacity.
    """
    techs = list(capacity)
    if not techs:
        raise ValueError("capacity must contain at least one technology")

    served_demand = demand.fillna(0).clip(lower=0)
    nodes = served_demand.index

    opex_df = pd.DataFrame(index=nodes, columns=techs, dtype=float)
    for tec in techs:
        cost = _as_series(var_cost.get(tec, 0.0), nodes)
        for commod, val in inputs.get(tec, {}).items():
            if commod not in commodity_prices:
                raise ValueError(
                    f"Commodity {commod!r} (used by tech {tec!r}) has no entry "
                    f"in commodity_prices"
                )
            cost = cost + _as_series(val, nodes) * commodity_prices[commod]
        opex_df[tec] = cost

    cap_df = pd.DataFrame(
        {tec: _as_series(capacity[tec], nodes).clip(lower=0) for tec in techs}
    )

    activity = {tec: pd.Series(0.0, index=nodes) for tec in techs}
    tol = 1e-9

    for node in nodes:
        residual = float(served_demand[node])
        if residual <= 0:
            continue
        for tec in opex_df.loc[node].sort_values().index:
            cap = float(cap_df.at[node, tec])
            if cap <= 0:
                continue
            served = min(residual, cap)
            activity[tec].at[node] = served
            residual -= served
            if residual <= tol:
                break
        if residual > tol:
            total_cap = float(cap_df.loc[node].sum())
            raise ValueError(
                f"Infeasible dispatch at node {node!r}: residual demand "
                f"{residual:.4g} after exhausting all techs. "
                f"Total capacity {total_cap:.4g}, demand "
                f"{float(served_demand[node]):.4g}."
            )

    return activity


def cap_surfacewater_for_gw_floor(
    sw_cap: pd.Series, demand: pd.Series, gw_floor: pd.Series
) -> pd.Series:
    """Lower the surface-water dispatch ceiling so groundwater clears its floor.

    The cost-only merit order fills surface water first, which can seed total
    groundwater (renewable + fossil) below the ``share_low_lim_GWat`` share the
    LP enforces in-horizon. Capping surface water at ``(1 - gw_floor) * demand``
    reserves the rest of demand for groundwater, so total GW >= ``gw_floor`` after
    dispatch. Returns ``min(sw_cap, (1 - gw_floor) * demand)`` per node; the cap
    only tightens where surface water would otherwise serve more than its share.
    """
    idx = sw_cap.index
    gw_floor = gw_floor.reindex(idx).fillna(0.0).clip(lower=0, upper=1)
    demand = demand.reindex(idx).fillna(0.0)
    sw_ceiling = (1.0 - gw_floor) * demand
    return pd.concat([sw_cap, sw_ceiling], axis=1).min(axis=1)


def read_historical_demand(context: "Context", year: int) -> pd.Series:
    """Return per-basin freshwater_basin demand at ``year`` (MCM/yr).

    Sums:
      * sectoral water withdrawal from the three urban / rural / manuf
        CSVs (linearly interpolated when ``year`` is not in the rows);
      * GLOBIOM irrigation read from the source scenario's ``land_output``
        rows, with regional totals allocated to basins by each basin's
        share of regional historical SW capacity.

    Returns demand keyed by raw ``BCU_name`` (no ``B`` prefix).
    """
    sectoral = _read_sectoral_demand(context, year)
    irrigation = _read_irrigation_demand(context, year)
    total = sectoral.add(irrigation, fill_value=0).clip(lower=0)
    total.index.name = "BCU_name"
    return total


def _read_sectoral_demand(context: "Context", year: int) -> pd.Series:
    """Per-basin sectoral (urban + rural + manuf) withdrawal at ``year``."""
    path = package_data_path("water", "demands", "harmonized", context.regions, ".")
    total: pd.Series | None = None

    for fname in _WITHDRAWAL_CSVS:
        df = pd.read_csv(path / fname).rename(columns={"Unnamed: 0": "year"})
        df["year"] = df["year"].astype(int)
        years = df["year"].to_numpy()
        if year in years:
            row = df[df["year"] == year].drop(columns="year").iloc[0]
        else:
            below = years[years < year]
            above = years[years > year]
            if below.size == 0 or above.size == 0:
                raise ValueError(
                    f"Cannot bracket year {year} in {fname}; available "
                    f"[{years.min()}, {years.max()}]"
                )
            yr_below = int(below.max())
            yr_above = int(above.min())
            row_below = df[df["year"] == yr_below].drop(columns="year").iloc[0]
            row_above = df[df["year"] == yr_above].drop(columns="year").iloc[0]
            frac = (year - yr_below) / (yr_above - yr_below)
            row = row_below + (row_above - row_below) * frac
        total = row if total is None else total.add(row, fill_value=0)

    if total is None:
        raise ValueError("no withdrawal CSVs configured to read")
    return total.clip(lower=0).rename_axis("BCU_name")


def _read_irrigation_demand(context: "Context", year: int) -> pd.Series:
    """Per-basin irrigation withdrawal at ``year`` from GLOBIOM ``land_output``.

    Reads the source-scenario ``land_output`` rows for the three irrigation
    commodities directly. The equivalent post-processed ``land_input`` is
    written in-memory by ``add_irrigation_demand`` during the same build
    but may not be visible when ``context.get_scenario()`` reloads from
    the DB; ``land_output`` is the upstream source.

    Returns an empty Series when no rows are present for ``year``
    (e.g. country-mode builds where irrigation is not coupled).
    """
    scen = context.get_scenario()
    irr_commodities = [
        "Water|Withdrawal|Irrigation|Cereals",
        "Water|Withdrawal|Irrigation|Oilcrops",
        "Water|Withdrawal|Irrigation|Sugarcrops",
    ]
    li = scen.par(
        "land_output",
        filters={"commodity": irr_commodities, "year": year},
    )
    empty = pd.Series(dtype=float).rename_axis("BCU_name")
    if li.empty:
        return empty

    # Sum crops within each land scenario, then average the scenario totals.
    per_scenario = li.groupby(["node", "land_scenario"], as_index=False)["value"].sum()
    regional_irr = per_scenario.groupby("node")["value"].mean()

    df_hist = read_hist_capacity(context)
    if df_hist.empty:
        return empty

    df_hist["region"] = (
        f"{context.regions}_" + df_hist["BCU_name"].str.split("|").str[-1]
    )
    cap = df_hist["hist_cap_sw_km3_year"].clip(lower=0)
    region_cap = cap.groupby(df_hist["region"]).transform("sum")
    share = (cap / region_cap.where(region_cap > 0)).fillna(0)
    irr = share * df_hist["region"].map(regional_irr).fillna(0)
    return pd.Series(irr.to_numpy(), index=df_hist["BCU_name"].to_numpy()).rename_axis(
        "BCU_name"
    )


def add_water_hist_dispatch(context: "Context") -> dict[str, pd.DataFrame]:
    """Seed basin extraction ``historical_activity`` by merit-order dispatch."""
    cfg = Config.from_context(context)
    info = context["water build info"]
    last_vtg_yr = info.Y[0] - 5
    sub_time = list(cfg.time)

    df_hist = read_hist_capacity(context)
    df_hist["BCU_name"] = "B" + df_hist["BCU_name"].astype(str)

    # hist_cap_*_km3_year is installed historical capacity; the dispatch uses it
    # as an annual activity ceiling. The duration-period scaling belongs only
    # to historical_new_capacity.
    sw_cap = (
        (KM3_TO_MCM * df_hist["hist_cap_sw_km3_year"])
        .set_axis(df_hist["BCU_name"])
        .rename_axis("BCU_name")
    )
    gw_cap = (
        (KM3_TO_MCM * df_hist["hist_cap_gw_km3_year"])
        .set_axis(df_hist["BCU_name"])
        .rename_axis("BCU_name")
    )
    fossil_cap = pd.Series(np.inf, index=df_hist["BCU_name"])

    gw_energy_path = package_data_path(
        "water", "availability", f"gw_energy_intensity_depth_{context.regions}.csv"
    )
    df_gwt = pd.read_csv(gw_energy_path)
    gw_elec_intensity = (
        (
            (df_gwt["GW_per_km3_per_year"] + GW_ELEC_DEPTH_ADDER_GWA_KM3)
            * GWa_KM3_TO_GWa_MCM
        )
        .set_axis("B" + df_gwt["BCU_name"].astype(str))
        .rename_axis("BCU_name")
    )

    demand = read_historical_demand(context, last_vtg_yr)
    demand.index = "B" + demand.index.astype(str)
    demand = demand.reindex(df_hist["BCU_name"].to_numpy()).fillna(0)

    # Reserve groundwater its sustainability-floor share before merit-ordering,
    # mirroring the share_low_lim_GWat lower bound the LP enforces in-horizon.
    # The floor is sourced from the same availability data as the constraint
    # (groundwater_share_floor) so the seed and the constraint cannot drift.
    # Availability has no pre-horizon row, so use the firstmodelyear value.
    df_sw_av, df_gw_av = read_water_availability(context)
    floor_all = groundwater_share_floor(df_sw_av, df_gw_av)
    first_year = df_gw_av.loc[df_gw_av["year"] == info.Y[0], ["Region"]].copy()
    first_year["floor"] = floor_all.loc[first_year.index].to_numpy()
    first_year["BCU_name"] = "B" + first_year["Region"].astype(str)
    gw_floor = first_year.groupby("BCU_name")["floor"].first()
    sw_cap = cap_surfacewater_for_gw_floor(sw_cap, demand, gw_floor)

    capacity = {
        "extract_surfacewater": sw_cap,
        "extract_groundwater": gw_cap,
        "extract_gw_fossil": fossil_cap,
    }
    inputs = {
        "extract_surfacewater": {
            "electr": SW_ELEC_INTENSITY_GWA_KM3 * GWa_KM3_TO_GWa_MCM
        },
        "extract_groundwater": {"electr": gw_elec_intensity},
        "extract_gw_fossil": {"electr": gw_elec_intensity * GW_FOSSIL_ELEC_MULTIPLIER},
    }
    var_cost = {"extract_gw_fossil": GW_FOSSIL_VAR_COST_USD_KM3 * USD_KM3_TO_USD_MCM}
    commodity_prices = {"electr": HIST_DISPATCH_ELEC_PRICE_USD_GWA}

    activity = merit_order_dispatch(
        capacity=capacity,
        inputs=inputs,
        var_cost=var_cost,
        demand=demand,
        commodity_prices=commodity_prices,
    )

    n_bins = max(len(sub_time), 1)
    frames = []
    for tec, series in activity.items():
        per_bin = series / n_bins
        frames.append(
            make_df(
                "historical_activity",
                node_loc=per_bin.index,
                technology=tec,
                year_act=last_vtg_yr,
                mode="M1",
                value=per_bin.values,
                unit="MCM/year",
            ).pipe(broadcast, time=pd.Series(sub_time))
        )

    hist_act = pd.concat(frames, ignore_index=True)
    hist_act = hist_act[hist_act["value"] > 0].reset_index(drop=True)
    return {"historical_activity": hist_act}
