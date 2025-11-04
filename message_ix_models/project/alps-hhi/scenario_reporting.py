# -*- coding: utf-8 -*-
"""
Report from scenarios
"""
from typing import List

import message_ix
import numpy as np
import pandas as pd
import pyam
from message_ix.report import Reporter
from message_ix_models.util import broadcast

from message_ix_models.tools.bilateralize.utils import load_config

config, config_path = load_config(project_name = 'alps-hhi', config_name = 'config.yaml')
models_scenarios = config['models_scenarios']


model_name = models_scenario[model_scen]['model']
scen_name = models_scenario[model_scen]['scenario']

mp = ixmp.Platform()
scen = message_ix.Scenario(mp, model=model_name, scenario=scen_name)

# Pull repoter
rep = Reporter.from_scenario(scen)


# Update base scenarios
for model_scen in models_scenarios.keys():
    base_model = models_scenarios[model_scen]['model']
    base_scen = models_scenarios[model_scen]['scenario']

    print(base_model)
    print(base_scen)

    