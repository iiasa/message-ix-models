def get_nodes(scen):
    """Retrieve all the nodes defined in a scenario, excluding 'WORLD'.

    Parameters
    ----------
    scen : :class:`message_ix.Scenario`
        Scenario from which nodes should be retrieved.

    Returns
    -------
    list of str
        Regions in the scenario, excluding 'WORLD'.
    """
    return [r for r in scen.set("node").tolist() if r not in ["World"]]
