def main(scenario, year_min=1990):
    """Retrieves historical time periods for a given scenario.

    Parameters
    ----------
    scenario : :class:`message_ix.Scenario`
        scenario for which the historical time period should be retrieved
    year_min : int
        starting year of historical time period.

    Returns
    -------
    years : list
        all historical time periods
    """

    firstmodelyear = int(
        scenario.set("cat_year", {"type_year": ["firstmodelyear"]})["year"]
    )
    model_years = [int(x) for x in scenario.set("year")]
    years = [y for y in model_years if y < firstmodelyear and y >= year_min]
    return years
