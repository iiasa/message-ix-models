def main(scen):
    """Retrieves optimization horizon for a given scenario.

    Parameters
    ----------
    scen : :class:`message_ix.Scenario`
        Scenario for which the optimization period should be determined

    Returns
    -------
    years : list
        all model years for which the model will carry out the optimization
    """

    firstmodelyear = int(
        scen.set("cat_year", {"type_year": ["firstmodelyear"]})["year"]
    )
    model_years = scen.set("cat_year").year.unique().tolist()
    years = [y for y in model_years if y >= firstmodelyear]
    return years


if __name__ == "__main__":
    main("test")
