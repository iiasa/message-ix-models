# -*- coding: utf-8 -*-
"""
This script adds material intensity coefficients (prototype)
"""

# check Python and R environments (for debugging)
import rpy2.situation
for row in rpy2.situation.iter_info():
    print(row)

# load rpy2 modules
import rpy2.robjects as ro
#from rpy2.robjects.packages import importr 
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter

# paths to r code and lca data
rcode_path="C:/Users/krey/Documents/git/message_data/message_data/model/material/material_intensity/"
data_path = "C:/Users/krey/Documents/git/message_data/data/material"

# import MESSAGEix
import ixmp
import message_ix

# launch the IX modeling platform using the local default databases
mp = ixmp.Platform(name='default', jvmargs=['-Xmx12G'])

# model and scenario names of baseline scenario as basis for diagnostic analysis
model = "Material_Global"
scen = "NoPolicy"

# load existing scenario
scenario = message_ix.Scenario(mp, model, scen)

# read node, technology, commodity and level from existing scenario
node = scenario.set('node')
year = scenario.set('year')
technology = scenario.set('technology')
commodity = scenario.set('commodity')
level = scenario.set('level')
# read inv.cost data
inv_cost = scenario.par('inv_cost')

# check whether needed units are registered on ixmp and add if not the case
unit = mp.units()
#if (!('t/kW' %in% unit$.)) mp$add_unit('t/kW', 'tonnes (of commodity) per kW of capacity')

# source R code
r=ro.r
r.source(rcode_path+"ADVANCE_lca_coefficients_embedded.R")

# call R function with type conversion
with localconverter(ro.default_converter + pandas2ri.converter):
    p=r.read_material_intensities(data_path, node, year, technology, commodity, level, inv_cost)

