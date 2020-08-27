# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import ixmp
import message_ix as mix
from message_ix import Scenario

import message_data.model.material.data as dt
import pyam

from message_data.model.material import bare

from message_data.tools.cli import Context

from message_data.tools import (
    ScenarioInfo,
    make_df,
    make_io,
    make_matched_dfs,
)

#%% Main test run

# Create Context obj
Context._instance = []
ctx = Context()

# Set default scenario/model names
ctx.scenario_info.setdefault('model', 'Material_test')
ctx.scenario_info.setdefault('scenario', 'baseline')

# Create bare model/scenario and solve it
scen = bare.create_res(context = ctx)
scen.solve()



#%% Auxiliary random test stuff

import pandas as pd

# Test read_data_steel <- will be in create_res if working fine
df = dt.read_data_steel()

b = dt.read_data_generic()
b = dt.read_var_cost() 
c = pd.melt(b, id_vars=['technology', 'mode', 'units'], \
            value_vars=[2010, 2020, 2030, 2040, 2050, 2060, 2070, 2080, 2090, 2100], \
            var_name='year')
                    
df_gen = dt.gen_data_generic(scen) 
df_st = dt.gen_data_steel(scen) 
a = dt.get_data(scen, ctx)

bare.add_data(scen)


info = ScenarioInfo(scen)
a = bare.get_spec(ctx)

mp_samp = ixmp.Platform(name="local")
mp_samp.scenario_list()
sample = mix.Scenario(mp_samp, model="Material_b", scenario="baseline", version="new")
sample.set_list()
sample.set('year')
sample.cat('year', 'firstmodelyear')
mp_samp.close_db()

scen.to_excel("test.xlsx")
scen_rp = Scenario(scen)