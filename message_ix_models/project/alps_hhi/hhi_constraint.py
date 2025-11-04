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
from message_ix_models.project.alps_hhi.hhi_utils import hhi_df_build

def hhi_constraint_run(project_name: str, 
                       config_name: str,
                       hhi_config_name: str):
    """Run HHI constraint"""
    """
    Parameters
    ----------
    project_name: str
        Name of the project
    config_name: str
        Name of the config file
    hhi_config_name: str
        Name of the HHI config file
    """
    log = get_logger(__name__)

    # Import configurations
    config, config_path = load_config(project_name = project_name,
                                      config_name = config_name)
    hhi_config, hhi_config_path = load_config(project_name = project_name,
                                              config_name = hhi_config_name)

    # Create platform
    mp = ixmp.Platform()

    for k in ['baseline']:
        log.info(f"HHI option: HC")

        base_model_name = 'alps_hhi'
        base_scen_name = k
        target_model_name = 'alps_hhi'
        target_scen_name = k + f'_hhi_HC'

        log.info(f"Base scenario: {base_model_name}/{base_scen_name}")
        log.info(f"Target scenario: {target_model_name}/{target_scen_name}")

        base_scenario = message_ix.Scenario(mp, model=base_model_name, scenario=base_scen_name)
        hhi_scenario = base_scenario.clone(target_model_name, target_scen_name, 
                                           keep_solution = False)
        hhi_scenario.set_as_default()

        hhi_df = hhi_df_build(hhi_scenario = hhi_scenario,
                              hhi_config = hhi_config,
                              log = log)

        with hhi_scenario.transact("Add HHI constraint"):
            hhi_scenario.add_par('hhi_limit', hhi_df)

        hhi_scenario.solve(gams_args = ['--HHI_CONSTRAINT=1'], quiet = False)


hhi_constraint_run(project_name = 'alps_hhi', 
                   config_name = 'config.yaml',
                   hhi_config_name = 'hhi_constraint_config.yaml')