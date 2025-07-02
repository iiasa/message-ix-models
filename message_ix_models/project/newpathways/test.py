# -*- coding: utf-8 -*-
"""
Bilateralize trade flows
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

from pathlib import Path
from message_ix_models.util import package_data_path
from message_ix_models.tools.bilateralize.bilateralize import *
from message_ix_models.tools.bilateralize.historical_calibration import *

# Connect to ixmp
mp = ixmp.Platform()

# Get logger
log = get_logger(__name__)

# Generate bare sheets
generate_bare_sheets(log=log, mp=mp)

# Read and inflate sheets based on model horizon
trade_dict = build_parameter_sheets(log=log)

# Historical calibration
historical_activitydf = build_historical_activity('R12')

# Update scenario
clone_and_update(trade_dict=trade_dict,
                 log=log,
                 mp=mp, 
                 solve = True)
