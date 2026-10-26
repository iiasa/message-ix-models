import logging

log = logging.getLogger(__name__)


def lock_agg_ref_from_act(
    scenario,
    *,
    technology: str = "agg_ref",
    node_loc: str | None = None,
):
    """Lock *technology* activity to solved ``ACT``.

    Reads ``ACT``, removes the solution, then sets equal
    ``bound_activity_lo`` / ``bound_activity_up``.

    Parameters
    ----------
    node_loc :
        Region to lock. ``None`` (default) locks all regions with ACT.
    """
    if not scenario.has_solution():
        raise ValueError(
            f"Cannot lock {technology} ACT: no solution on "
            f"{scenario.model}/{scenario.scenario} "
            "(clone this step with keep_solution=True)"
        )

    filters: dict = {"technology": technology}
    if node_loc is not None:
        filters["node_loc"] = node_loc

    act = scenario.var("ACT", filters=filters)
    where = node_loc if node_loc is not None else "all regions"
    if act.empty:
        raise ValueError(
            f"No ACT for {technology} in {where} on "
            f"{scenario.model}/{scenario.scenario}"
        )

    unit = "???"
    for par_name in ("bound_activity_up", "bound_activity_lo", "output"):
        sample = scenario.par(par_name, filters=filters)
        if len(sample):
            unit = str(sample.iloc[0]["unit"])
            break

    bound = act[["node_loc", "technology", "mode", "time", "year_act"]].assign(
        value=act["lvl"].astype(float),
        unit=unit,
    )

    scenario.remove_solution()

    with scenario.transact(
        f"Lock {technology} ACT in {where} via bound_activity_lo/up"
    ):
        scenario.add_par("bound_activity_lo", bound)
        scenario.add_par("bound_activity_up", bound.copy())

    log.info(
        "Locked %s in %s to solved ACT (%d bound rows) on %s/%s",
        technology,
        where,
        len(bound),
        scenario.model,
        scenario.scenario,
    )
