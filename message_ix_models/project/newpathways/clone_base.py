# -*- coding: utf-8 -*-
"""
Run baseline

This clones the non-bilateralized version of the model/scenario (base model)
"""

# Import packages
import os
import sys
import pandas as pd
import logging
import yaml
import message_ix
import ixmp
import itertools
import pickle

from pathlib import Path
from message_ix_models.util import package_data_path
from message_ix_models.tools.bilateralize.bilateralize import *
from message_ix_models.tools.bilateralize.historical_calibration import *
from message_ix_models.tools.bilateralize.pull_gem import *
from message_ix_models.tools.bilateralize.mariteam_calibration import *

# Bring in configuration
config, config_path = load_config(project_name = 'newpathways', 
                                  config_name = 'config.yaml')

covered_tec = config['covered_trade_technologies']
message_regions = config['scenario']['regions']

# Get logger
log = get_logger(__name__)

# Load the scenario
mp = ixmp.Platform()

start_model = config.get("scenario", {}).get("start_model")
start_scen = config.get("scenario", {}).get("start_scen")

target_model = config.get("scenario", {}).get("target_model")
target_scen = "base_scenario"

base = message_ix.Scenario(mp, model=start_model, scenario=start_scen)
scen = base.clone(model=target_model, scenario=target_scen)
scen.set_as_default()

solver = "MESSAGE"
scen.solve(solver, solve_options=dict(lpmethod=4))

mp.close_db()