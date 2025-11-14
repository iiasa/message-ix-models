"""Combine bilateral trade to legacy reporting"""

import pandas as pd
from message_ix_models.util import package_data_path
from message_ix_models.tools.bilateralize.prepare_edit import load_config
import yaml
import matplotlib.pyplot as plt
import numpy as np
from message_ix_models.project.led_china.reporting.utils import load_configs, pull_legacy_data, pull_trade_data

config = load_configs()

#scenario_list = config['models_scenarios']
scenario_list = ['SSP2_Baseline']

# Bring in legacy reporting
legacy_df = pull_legacy_data(scenario_list)

# Bring in trade reporting
export_df = pull_trade_data(scenario_list, 'gross_exports')
import_df = pull_trade_data(scenario_list, 'gross_imports')

