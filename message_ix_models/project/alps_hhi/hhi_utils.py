# -*- coding: utf-8 -*-
"""
HHI with hard constraint
"""
# Import packages
from typing import Any

import logging
import numpy as np
import pandas as pd
import ixmp
from ixmp import Platform
import message_ix

from message_ix_models.tools.bilateralize.utils import load_config, get_logger

def hhi_df_build(hhi_scenario: message_ix.Scenario,
                 hhi_config: dict,
                 log: logging.Logger) -> pd.DataFrame:
    """Build HHI dataframe
    Parameters
    ----------
    scenario: message_ix.Scenario
        Scenario to build HHI dataframe on
    hhi_config: dict
        Configuration for HHI dataframe
        Should be a dictionary with the following structure:
            {"commodity": str,
             "level": str,
             "nodes": list[str],
             "value": float, ...}
        It must have a 'nodes' key with a list of nodes.
    """
    # Build HHI constraint dataframe
    hhi_df = pd.DataFrame()
    for k in hhi_config.keys():
        log.info(f"Adding HHI dataframe for {k}")
        hhi_dfk = pd.DataFrame(hhi_config[k]['nodes'], columns = ['node'])
        for j in [v for v in hhi_config[k].keys() if v not in ['nodes']]:
            hhi_dfk[j] = hhi_config[k][j]
        hhi_df = pd.concat([hhi_df, hhi_dfk])

    year_list = [y for y in list(hhi_scenario.set("year"))
                if y > 2025]
    hhi_df = hhi_df.assign(key=1).merge(
        pd.DataFrame({'year': year_list, 'key': 1}),
        on = 'key').drop('key', axis = 1)

    return hhi_df