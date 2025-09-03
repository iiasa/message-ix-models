# -*- coding: utf-8 -*-
"""
Report using legacy reporting
"""
# Import packages
import os
import sys
import pandas as pd
import numpy as np
import logging
import yaml
import message_ix
import ixmp
import itertools
import plotly.graph_objects as go
from datetime import datetime
    
from message_ix_models.tools.bilateralize.bilateralize import *
from message_ix_models.util import package_data_path
import message_ix_models.report.legacy.iamc_report_hackathon as legacy_reporting

from ixmp import Platform

# Scenario dictionary
included_scenarios = {"base_scenario": "NP_SSP2_6.2",
                      "pipelines_LNG": "NP_SSP2_6.2",
                      "LNG_prod_penalty": "NP+SSP2_6.2"}
    
def run_legacy_reporting(scenarios_models):
    # Connect to ixmp
    mp = ixmp.Platform()
    
    # Report
    for s in scenarios_models.keys():
        scen = message_ix.Scenario(mp, model = scenarios_models[s], scenario = s)
        legacy_reporting.report(mp = mp, scen = scen)

run_legacy_reporting(scenarios_models = included_scenarios)

