# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import ixmp
import message_ix as mix
from message_ix import Scenario

import message_data.model.material.data as dt
from message_data.model.material.plotting import Plots
import pyam


from message_data.tools import Context

from message_data.tools import (
    ScenarioInfo,
    make_df,
    broadcast,
    make_io,
    copy_column,
    make_matched_dfs,
    set_info,
)

from message_data.model.create import create_res
from message_data.model.material import build, get_spec

from message_data.model.material.util import read_config


#%% Main test run based on a MESSAGE scenario

from message_data.model.create import create_res

# Create Context obj
Context._instance = []
ctx = Context()

# Set default scenario/model names - Later coming from CLI
ctx.platform_info.setdefault('name', 'ixmp_dev')
ctx.scenario_info.setdefault('model', 'Material_test_MESSAGE_China')
ctx.scenario_info.setdefault('scenario', 'baseline')
# ctx['period_start'] = 2020
# ctx['regions'] = 'China'
# ctx['ssp'] = 'SSP2' # placeholder
ctx['scentype'] = 'C30-const_E414'
ctx['datafile'] = 'China_steel_cement_MESSAGE.xlsx'

# Use general code to create a Scenario with some stuff in it
scen = create_res(context = ctx)
                
# Use material-specific code to add certain stuff
a = build(scen)

# Solve the model
scen.solve()

p = Plots(scen, 'China', firstyear=2020)
p.plot_activity(baseyear=False, subset=['clinker_dry_cement', \
                                        'clinker_wet_cement'])
p.plot_activity(baseyear=False, subset=['grinding_ballmill_cement', \
                                        'grinding_vertmill_cement'])
# p.plot_capacity(baseyear=True, subset=['bf_steel', 'bof_steel', 'dri_steel', 'eaf_steel'])
# p.plot_new_capacity(baseyear=True, subset=['bf_steel', 'bof_steel', 'dri_steel', 'eaf_steel'])
p.plot_activity(baseyear=False, subset=['clinker_dry_cement', \
                                       'clinker_dry_ccs_cement', \
                                       'clinker_wet_cement', \
                                       'clinker_wet_ccs_cement'])

p.plot_activity(baseyear=False, subset=['dri_steel', \
                                   'bf_steel'])

#%% Auxiliary random test stuff

import pandas as pd

import message_data.model.material.data_util as du

# Test read_data_steel <- will be in create_res if working fine
df = dt.read_data_steel()
df = du.read_rel(ctx.datafile)

b = dt.read_data_generic()
b = dt.read_var_cost() 
bb = dt.read_sector_data()
c = pd.melt(b, id_vars=['technology', 'mode', 'units'], \
            value_vars=[2010, 2020, 2030, 2040, 2050, 2060, 2070, 2080, 2090, 2100], \
            var_name='year')
                    
df_gen = dt.gen_data_generic(scen) 
df_st = dt.gen_data_steel(scen) 
a = dt.get_data(scen, ctx)
dt.gen_mock_demand_cement(ScenarioInfo(scen))
dt.read_data_generic()
bare.add_data(scen)


info = ScenarioInfo(scen)
a = get_spec()

mp_samp = ixmp.Platform(name="ixmp_dev")
mp_samp.scenario_list()
sample = mix.Scenario(mp_samp, model="Material_test", scenario="baseline")
sample.set_list()
sample.set('year')
sample.cat('year', 'firstmodelyear')
mp_samp.close_db()

scen.to_excel("test.xlsx")
scen_rp = Scenario(scen)