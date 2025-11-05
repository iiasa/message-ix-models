# -*- coding: utf-8 -*-
"""
HHI workflow
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
from message_ix_models.project.alps_hhi.hhi_constraint import hhi_constraint_run
from message_ix_models.project.alps_hhi.hhi_weight import hhi_weightsum_run

# Run HHI constraint workflow on SSP2
hhi_constraint_run(project_name = 'alps_hhi', 
                   config_name = 'config.yaml',
                   base_model = 'alps_hhi',
                   base_scenario = 'SSP2',
                   hhi_config_name = 'hhi_config.yaml')

# Run HHI weight sum workflow on SSP2
hhi_weightsum_run(project_name = 'alps_hhi', 
                  config_name = 'config.yaml',
                  base_model = 'alps_hhi',
                  base_scenario = 'SSP2',
                  hhi_config_name = 'hhi_config.yaml')
