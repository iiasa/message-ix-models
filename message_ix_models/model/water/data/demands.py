"""Prepare demands data."""

import pandas as pd

from message_data.model.water import read_config
from message_data.model.water.build import get_water_reference_scenario
from message_data.tools import (
    broadcast,
    get_context,
    make_df,
    make_matched_dfs,
    same_node,
)


def add_demand(info):
    """
    Parameters
    ----------
    info : .ScenarioInfo
        Information about target Scenario.

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'. Values
        are data frames ready for :meth:`~.Scenario.add_par`.
    """

    # define an empty dictionary
    results = {}

    context = read_config()
