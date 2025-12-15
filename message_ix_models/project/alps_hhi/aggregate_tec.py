# -*- coding: utf-8 -*-
"""
HHI with weighted sum
"""
# Import packages
from typing import Any

import numpy as np
import pandas as pd
import ixmp
#from ixmp import Platform
import message_ix

from message_ix_models.tools.bilateralize.utils import load_config, get_logger

def tec_aggregation(scenario: message_ix.Scenario,
                    tec_list_base: list,
                    output_commodity_base: str,
                    output_level_base: str,
                    output_commodity0: str,
                    output_level0: str,
                    output_technology: str,
                    output_commodity1: str,
                    output_level1: str,
                    log):
    base_output = scenario.par("output", filters = {"technology": tec_list_base,
                                                    "commodity": output_commodity_base,
                                                    "level": output_level_base})
    agg_output0 = base_output.copy()
    agg_output0["commodity"] = output_commodity0
    agg_output0["level"] = output_level0

    base_columns = ['node_loc', 'year_vtg', 'year_act', 'mode', 'node_dest',
                    'time', 'time_dest', 'unit'] # Everything but technology, commodity, level, value
    
    agg_input1 = agg_output0[base_columns].drop_duplicates()
    agg_input1 = agg_input1.rename(columns = {'node_dest': 'node_origin',
                                              'time_dest': 'time_origin'})
    agg_input1['technology'] = output_technology
    agg_input1['commodity'] = output_commodity0
    agg_input1['level'] = output_level0
    agg_input1['value'] = 1
    
    agg_output1 = agg_output0[base_columns].drop_duplicates()
    agg_output1['technology'] = output_technology
    agg_output1['commodity'] = output_commodity1
    agg_output1['level'] = output_level1
    agg_output1['value'] = 1

    log.info("Add required sets")
    with scenario.transact("Add levels and commodities"):
        scenario.add_set("level", [output_level0, output_level1])
        scenario.add_set("commodity", [output_commodity0, output_commodity1])
        scenario.add_set("technology", [output_technology])
        
    log.info(f"Remove base output for {tec_list_base}")
    with scenario.transact(f"Remove base output for {tec_list_base}"):
        scenario.remove_par("output", base_output)

    log.info(f"Replace with output to commodity {output_commodity0}")
    with scenario.transact(f"Add output to commodity {output_commodity0}"):
        scenario.add_par("output", agg_output0)
    
    log.info(f"Add input to aggregate technology {output_technology}")
    with scenario.transact(f"Add input to aggregate technology {output_technology}"):
        scenario.add_par("input", agg_input1) 

    log.info(f"Add output from aggregate technology {output_technology}")
    with scenario.transact(f"Add output from aggregate technology {output_technology}"):
        scenario.add_par("output", agg_output1) 