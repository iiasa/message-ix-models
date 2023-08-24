import logging

log = logging.getLogger(__name__)


def main(
    scen,
    budget: float,
    adjust_cumulative=False,
    type_emission="TCE",
    type_tec="all",
    type_year="cumulative",
    region="World",
    unit="tC",
):
    """Adds a budget constraint to a given region.

    Parameters
    ----------

    scen : :class:`message_ix.Scenario`
        Scenario to which budget should be applied
    budget : numeric
        Budget in average tC
    adjust_cumulative : bool, optional
        Option whether to adjust cumulative years to which the budget is applied to
        the optimization time horizon.
    type_emission : str (default: 'TCE')
        type_emission for which the constraint should be applied. This element must
        already be defined in `scen`.
    type_tec : str (default: 'all')
        technology type for which the bound applies
    region : str (default: 'World')
        region to which the bound applies
    unit : str (default: 'tC')
        unit in which the bound is provided
    """

    scen.check_out()

    if adjust_cumulative:
        current_cumulative_years = scen.set("cat_year", {"type_year": ["cumulative"]})

        remove_cumulative_years = current_cumulative_years[
            current_cumulative_years["year"] < scen.firstmodelyear
        ]

        if not remove_cumulative_years.empty:
            scen.remove_set("cat_year", remove_cumulative_years)

    args = [region, type_emission, type_tec, type_year], budget, unit
    log.info(repr(args))

    with scen.transact(f"bound_emission {budget} added"):
        scen.add_par("bound_emission", *args)


if __name__ == "__main__":  # pragma: no cover
    main("test", "test")
