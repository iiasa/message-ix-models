import logging

import pandas as pd
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.util import (
    broadcast,
    nodes_ex_world,
)

log = logging.getLogger(__name__)

# Freight-road truck technologies (F ROAD) — edit placeholders as needed
_TRUCK_TECHNOLOGIES: tuple[str, ...] = (
    "f road electr",
    "f road gas fc",
    "f road gas ic",
    "f road methanol",
    "FR_FCH",
    "FR_ICAe",
    "FR_ICE_H",
    "FR_ICE_L",
    "FR_ICE_M",
    "FR_ICH",
)


def freeze_truck_history(context, scenario, year_freeze: int = 2030):
    """Temporary function to freeze truck activity, deprecated after MixT update.

    Option A (commented): equality bounds from solved ``ACT`` in year_freeze.
    Option B (active): copy all ``bound_activity_up`` rows for truck techs to
    ``bound_activity_lo`` in year_freeze.
    """
    del context
    del year_freeze  # used by Option A only

    # Option B
    bound_up = scenario.par(
        "bound_activity_up",
        filters={"technology": list(_TRUCK_TECHNOLOGIES)},
    )
    if bound_up.empty:
        log.warning(
            "No bound_activity_up for truck technologies in %s/%s; skipping",
            scenario.model,
            scenario.scenario,
        )
        return scenario

    bound_lo = bound_up.copy()

    # scenario.remove_solution()

    with scenario.transact("Freeze truck activity from bound_activity_up (option B)"):
        scenario.add_par("bound_activity_lo", bound_lo)

    log.info(
        "freeze_truck_history (B): added %d bound_activity_lo rows "
        "from bound_activity_up (%d technologies) for %s/%s",
        len(bound_lo),
        len(_TRUCK_TECHNOLOGIES),
        scenario.model,
        scenario.scenario,
    )

    return scenario

    # Option A
    # if not scenario.has_solution():
    #     log.warning(
    #         "No solution on %s/%s; skipping freeze_truck_history",
    #         scenario.model,
    #         scenario.scenario,
    #     )
    #     return scenario

    # fmy = int(scenario.firstmodelyear)
    # if year_freeze < fmy:
    #     raise ValueError(
    #         f"year_freeze={year_freeze} must be >= firstmodelyear ({fmy})"
    #     )

    # act = scenario.var("ACT", filters={"technology": list(_TRUCK_TECHNOLOGIES)})
    # act = act.loc[
    #     (act["year_act"].astype(int) >= fmy)
    #     & (act["year_act"].astype(int) <= year_freeze)
    # ]
    # if act.empty:
    #     log.warning(
    #         "No ACT in [%s, %s] for truck technologies in %s/%s; skipping",
    #         fmy,
    #         year_freeze,
    #         scenario.model,
    #         scenario.scenario,
    #     )
    #     return scenario

    # unit = "???"
    # for tech in _TRUCK_TECHNOLOGIES:
    #     for par_name in ("bound_activity_up", "bound_activity_lo", "output"):
    #         sample = scenario.par(par_name, filters={"technology": tech})
    #         if len(sample):
    #             unit = sample.iloc[0]["unit"]
    #             break
    #     if unit != "???":
    #         break

    # act = act.loc[act["lvl"] != 0]
    # if act.empty:
    #     log.warning(
    #         "No non-zero ACT in [%s, %s] for truck technologies in %s/%s; skipping",
    #         fmy,
    #         year_freeze,
    #         scenario.model,
    #         scenario.scenario,
    #     )
    #     return scenario

    # bound = act[
    #     ["node_loc", "technology", "mode", "time", "year_act"]
    # ].assign(value=act["lvl"].astype(float), unit=unit)

    # scenario.remove_solution()

    # with scenario.transact(
    #     f"Freeze truck activity [{fmy}, {year_freeze}] from solved ACT"
    # ):
    #     scenario.add_par("bound_activity_lo", bound)
    #     scenario.add_par("bound_activity_up", bound.copy())

    # log.info(
    #     "freeze_truck_history: added %d bound_activity rows per bound "
    #     "(%s technologies, year_act %s–%s) for %s/%s",
    #     len(bound),
    #     len(_TRUCK_TECHNOLOGIES),
    #     fmy,
    #     year_freeze,
    #     scenario.model,
    #     scenario.scenario,
    # )

    # return scenario


def aas_co2_storage_growth(
    context,
    scenario,
    technologies: list[str] = ["co2_stor"],
    *,
    start_year: int = 2035,
    midpoint_year: int = 2055,
    end_year: int = 2110,
    limit_early: float = 0.028,
    limit_late: float = 0.01,
):
    """Add ``growth_activity_up`` for CO2 storage technologies.

    ``limit_early`` applies for ``start_year`` <= ``year_act`` < ``midpoint_year``;
    ``limit_late`` applies for ``midpoint_year`` <= ``year_act`` <= ``end_year``.
    """
    if not (start_year < midpoint_year <= end_year):
        raise ValueError(
            f"Require start_year < midpoint_year <= end_year; got "
            f"{start_year}, {midpoint_year}, {end_year}"
        )

    info = ScenarioInfo(scenario)
    nodes = nodes_ex_world(info.N)
    years_all = [y for y in info.Y if start_year <= y <= end_year]
    years_early = [y for y in years_all if y < midpoint_year]
    years_late = [y for y in years_all if y >= midpoint_year]

    common = dict(technology=technologies, time="year", unit="???")
    parts: list[pd.DataFrame] = []
    if years_early:
        parts.append(
            make_df("growth_activity_up", value=limit_early, **common).pipe(
                broadcast, node_loc=nodes, year_act=years_early
            )
        )
    if years_late:
        parts.append(
            make_df("growth_activity_up", value=limit_late, **common).pipe(
                broadcast, node_loc=nodes, year_act=years_late
            )
        )
    if not parts:
        log.warning(
            "No model years in [%s, %s] for %s/%s; skipping growth_activity_up",
            start_year,
            end_year,
            scenario.model,
            scenario.scenario,
        )
        return scenario

    df = pd.concat(parts, ignore_index=True)

    with scenario.transact("Add growth_activity_up for CO2 storage technologies."):
        scenario.add_par("growth_activity_up", df)

    log.info(
        "Added growth_activity_up for technologies %s: "
        "%s in [%s, %s) (%d periods), %s in [%s, %s] (%d periods), "
        "all regions (%d nodes)",
        technologies,
        limit_early,
        start_year,
        midpoint_year,
        len(years_early),
        limit_late,
        midpoint_year,
        end_year,
        len(years_late),
        len(nodes),
    )

    return scenario


def aas_co2_storage_share_mode(
    context,
    scenario,
    technology: str = "co2_stor",
    *,
    share: str = "co2_stor_aas1",
):
    """Add ``share_mode_up`` for ``co2_stor`` mode shares ``co2_stor_aas1``."""
    m2_limit = 0.72
    m3_limit = 0.14
    m3_limit_near_term = 0.01
    m3_near_start = 2020
    m3_near_end = 2040

    info = ScenarioInfo(scenario)
    nodes = nodes_ex_world(info.N)
    years_all = sorted(int(y) for y in info.set["year"])
    years_m3_near = [y for y in years_all if m3_near_start <= y <= m3_near_end]
    years_m3_other = [y for y in years_all if y not in years_m3_near]

    def _share_mode_up(mode: str, value: float, year_act: list[int]) -> pd.DataFrame:
        return make_df(
            "share_mode_up",
            shares=share,
            technology=technology,
            mode=mode,
            time="year",
            value=value,
            unit="???",
        ).pipe(broadcast, node_share=nodes, year_act=year_act)

    parts = [_share_mode_up("M2", m2_limit, years_all)]
    if years_m3_near:
        parts.append(_share_mode_up("M3", m3_limit_near_term, years_m3_near))
    if years_m3_other:
        parts.append(_share_mode_up("M3", m3_limit, years_m3_other))
    df = pd.concat(parts, ignore_index=True)

    with scenario.transact(f"Add share_mode_up for {share} on {technology}."):
        if share not in scenario.set("shares").tolist():
            scenario.add_set("shares", share)
        scenario.add_par("share_mode_up", df)

    log.info(
        "Added share_mode_up for %s on %s, %d rows",
        share,
        technology,
        len(df),
    )

    return scenario


def aas_coal_growth_near_term(
    context,
    scenario,
    technologies: list[str] = [
        "coal_adv",
        # "coal_adv_ccs",
        "coal_ppl",
        # "igcc",
        # "igcc_ccs",
    ],
    *,
    start_year: int = 2020,
    end_year: int = 2035,
    limit: float = 0.03,
):
    """Add ``growth_activity_lo`` for coal power technologies.

    Removes overlapping ``growth_activity_up`` entries for the same technologies,
    nodes, and years before adding the lower bound.
    """
    info = ScenarioInfo(scenario)
    nodes = ["R12_CHN", "R12_SAS"]
    years = [y for y in info.Y if start_year <= y <= end_year]

    growth_up = scenario.par(
        "growth_activity_up",
        filters={
            "technology": technologies,
            "node_loc": nodes,
            "year_act": years,
        },
    )

    df = make_df(
        "growth_activity_lo",
        technology=technologies,
        time="year",
        value=limit,
        unit="???",
    ).pipe(
        broadcast,
        node_loc=nodes,
        year_act=years,
    )

    with scenario.transact("Add growth_activity_lo for coal power technologies."):
        if len(growth_up):
            scenario.remove_par("growth_activity_up", growth_up)
        scenario.add_par("growth_activity_lo", df)

    log.info(
        "Added growth_activity_lo=%s for technologies %s, "
        "year_act %s–%s, nodes R12_CHN and R12_SAS (%d periods); "
        "removed %d growth_activity_up rows",
        limit,
        technologies,
        start_year,
        end_year,
        len(years),
        len(growth_up),
    )

    return scenario
