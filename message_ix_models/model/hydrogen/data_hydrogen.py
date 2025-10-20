from message_ix_models.util import ScenarioInfo

def gen_data_hydrogen(scenario: ScenarioInfo) -> dict:
    """Generate hydrogen technology data.

    Parameters
    ----------
    scenario : .ScenarioInfo
        Scenario information.

    Returns
    -------
    dict
        Dictionary of parameter data.
    """
    # Initialize a dictionary to hold the parameter data
    data = {
        "inv_cost": [],
        "fix_cost": [],
        "var_cost": [],
        "technical_lifetime": [],
        "input": [],
        "output": [],
        "emission_factor": [],
    }

    # In the next steps, we will populate these lists with dataframes

    return data