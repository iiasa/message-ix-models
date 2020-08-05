# This script makes the additions to run the MESSAGE-material model stand-alone.
# without a reference energy system.

import message_ix
import ixmp
import pandas as pd
from collections import defaultdict
from message_data.tools import broadcast, make_df, same_node

mp = ixmp.Platform()

# Adding a new unit to the library
mp.add_unit('Mt')

scenario = message_ix.Scenario(mp, model='MESSAGE_material',
                               scenario='baseline', version='new')

# Add model time steps

history = [2010]
model_horizon = [2020, 2030, 2040, 2050, 2060, 2070, 2080, 2090,2100]
scenario.add_horizon({'year': history + model_horizon,
                      'firstmodelyear': model_horizon[0]})

country = 'China'
scenario.add_spatial_sets({'country': country})

# Duration period

# Create duration period

val = [j-i for i, j in zip(model_horizon[:-1], model_horizon[1:])]
val.append(val[0])

duration_period = pd.DataFrame({
        'year': model_horizon,
        'value': val,
        'unit': "y",
    })

duration_period = duration_period["value"].values
scenario.add_par("duration_period", duration_period)

# Add exogenous demand
# The future projection of the demand: Increases by half of the GDP growth rate.

# TODO: Read this information from an excel file.
# Starting from 2020. Taken from global model input data.
gdp_growth = pd.Series([0.121448215899944, 0.0733079014579874, 0.0348154093342843, \
                        0.021827616787921,0.0134425983942219, 0.0108320197485592, \
                        0.00884341208063,0.00829374133206562, 0.00649794573935969],\
                        index=pd.Index(model_horizon, name='Time'))

i = 0
values = []
val = 36.27 * (1+ 0.147718884937996/2) ** duration_period[i]
values.append(val)

for element in gdp_growth:
    i = i + 1
    if i < len(model_horizon):
        print(i)
        val = val * (1+ element/2) ** duration_period[i]
        values.append(val)

aluminum_demand = pd.DataFrame({
        'node': country,
        'commodity': 'aluminum',
        'level': 'useful_material',
        'year': model_horizon,
        'time': 'year',
        'value': values ,
        'unit': 'Mt',
    })

scenario.add_par("demand", aluminum_demand)

# Interest rate
scenario.add_par("interestrate", model_horizon, value=0.05, unit='-')

# Representation of energy system:
# Unlimited supply of the commodities, with a fixed cost over the years.
# Represented via variable costs.
# Variable costs are taken from PRICE_COMMODIY baseline SSP2 scenario.

years_df = scenario.vintage_and_active_years()
vintage_years, act_years = years_df['year_vtg'], years_df['year_act']

# Retreive variable costs.
data_var_cost = pd.read_excel("variable_costs.xlsx",sheet_name="data")

# TODO: We need to add the technology and mode sets here to be able to add
# variable costs. Retrieve from config.yaml file.



# Add variable costs.
for row in data_var_cost.index:
    data = data_var_cost.iloc[row]

    values =[]
    for yr in act_years:
        values.append(data[yr])

    base_var_cost = pd.DataFrame({
        'node_loc': country,
        'year_vtg': vintage_years.values,
        'year_act': act_years.values,
        'mode': data["mode"],
        'time': 'year',
        'unit': 'USD/GWa',
        "technology": data["technology"],
        "value": values
    })

    scenario.par("var_cost",base_var_cost)
