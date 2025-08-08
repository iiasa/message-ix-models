# -*- coding: utf-8 -*-
"""
Run scenario(s)
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

# Import pickle of parameter definitions
tdf = os.path.join(os.path.dirname(config_path), 'scenario_parameters.pkl')
trade_parameters = pd.read_pickle(tdf)

# Update scenario
clone_and_update(trade_dict=trade_parameters,
                 project_name = 'newpathways',
                 config_name = 'config.yaml',
                 log=log,
                 to_gdx = False,
                 solve = True)


















