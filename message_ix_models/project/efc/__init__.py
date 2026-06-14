import logging

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

    scenario.remove_solution()

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
