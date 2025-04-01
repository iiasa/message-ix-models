# ----------------- DSL rule templates -----------------
# these templates are used to declare the supply transformation logic.
# dynamic values (e.g. years, node locations) are filled in within add_water_supply.

SLACK_TECHNOLOGY_RULES = [
    {
     "type" : "input",
     "technology": "return_flow",
     "value": 1,
     "unit" : "-",
     "level" : "water_avail_basin",
     "commodity" : "surfacewater_basin",
     "mode" : "M1",
     "year_vtg" : None,
     "year_act" : None,
    },
    {
        "type" : "input",
        "technology" : "gw_recharge",
        "value" : 1,
        "unit" : "-",
        "level" : "water_avail_basin",
        "commodity" : "groundwater_basin",
        "mode" : "M1",
        "year_vtg" : None,
        "year_act" : None,
    },
    {
        "type" : "input",
        "technology" : "basin_to_reg",
        "value" : 1,
        "unit" : "-",
        "level" : "water_supply_basin",
        "commodity" : "freshwater_basin",
        "mode" : "df_node[mode]",
        "node_origin" : "df_node[node]",
        "node_loc" : "df_node[region]",
    },
    {
        "type" : "input",
        "technology" : "salinewater_return",
        "value" : 1,
        "unit" : "-",
        "level" : "water_avail_basin",
        "commodity" : "salinewater_basin",
        "mode" : "M1",
        "time" : "year",
        "time_origin" : "year",
        "node_origin" : "df_node[node]",
        "node_loc" : "df_node[node]",
    }


]

# cooling branch (outputs only)
COOLING_SUPPLY_RULES = [
    {
        "type": "output",
        "technology": "extract_surfacewater",
        "value": 1,
        "unit": "km3",
        "year_vtg": None,
        "year_act": None,
        "level": "water_supply",
        "commodity": "freshwater",
        "mode": "M1",
        "time": "year",
        "time_dest": "year",
        "time_origin": "year",


    },
    {
        "type": "output",
        "technology": "extract_groundwater",
        "value": 1,
        "unit": "km3",
        "year_vtg": None,
        "year_act": None,
        "level": "water_supply",
        "commodity": "freshwater",
        "mode": "M1",
        "time": "year",
        "time_dest": "year",
        "time_origin": "year",
    },
    {
        "type": "output",
        "technology": "extract_salinewater",
        "value": 1,
        "unit": "km3",
        "year_vtg": None,
        "year_act": None,
        "level": "saline_supply",
        "commodity": "saline_ppl",
        "mode": "M1",
        "time": "year",
        "time_dest": "year",
        "time_origin": "year",
    },
]

# extraction input rules (inputs only)
EXTRACTION_INPUT_RULES = [
    {
        "type": "input",
        "technology": "extract_surfacewater",
        "value": 1,
        "unit": "-",
        "level": "water_avail_basin",
        "commodity": "surfacewater_basin",
        "mode": "M1",
        "node_origin": "df_node[node]",  # use df_node["node"]
        "node_loc": "df_node[node]",     # use df_node["node"]
        "broadcast": {"yv_ya": "yv_ya_sw", "time": "sub_time"},
    },
    {
        "type": "input",
        "technology": "extract_groundwater",
        "value": 1,
        "unit": "-",
        "level": "water_avail_basin",
        "commodity": "groundwater_basin",
        "mode": "M1",
        "node_origin": "df_node[node]",
        "node_loc": "df_node[node]",
        "broadcast": {"yv_ya": "yv_ya_gw", "time": "sub_time"},
    },
    {
        "type": "input",
        "technology": "extract_surfacewater",
        "value": 0.018835616,
        "unit": "-",
        "level": "final",
        "commodity": "electr",
        "mode": "M1",
        "time_origin": "year",
        "node_origin": "df_node[region]",  # use df_node["region"]
        "node_loc": "df_node[node]",
        "broadcast": {"yv_ya": "yv_ya_sw", "time": "sub_time"},
    },
    {
        "type": "input",
        "technology": "extract_groundwater",
        "value": "df_gwt['GW_per_km3_per_year'] + 0.043464579", # string expression
        "unit": "-",
        "level": "final",
        "commodity": "electr",
        "mode": "M1",
        "time_origin": "year",
        "node_origin": "df_node[region]",
        "node_loc": "df_node[node]",
        "broadcast": {"yv_ya": "yv_ya_gw", "time": "sub_time"},
    },
    {
        "type": "input",
        "technology": "extract_gw_fossil",
        "value": "2 * (df_gwt['GW_per_km3_per_year'] + 0.043464579)",
        "unit": "-",
        "level": "final",
        "commodity": "electr",
        "mode": "M1",
        "time_origin": "year",
        "node_origin": "df_node[region]",
        "node_loc": "df_node[node]",
        "broadcast": {"yv_ya": "yv_ya_gw", "time": "sub_time"},
        # adjustment for global regions can be applied after this dsl processing
    },
]

# new technical lifetime rules
TECHNICAL_LIFETIME_RULES = [
    {
        "type": "technical_lifetime",
        "technology": "extract_surfacewater",
        "value": 50,
        "unit": "y",
        "broadcast": {"year_vtg": None, "node_loc": None},
    },
    {
        "type": "technical_lifetime",
        "technology": "extract_groundwater",
        "value": 20,
        "unit": "y",
        "broadcast": {"year_vtg": None, "node_loc": None},
    },
    {
        "type": "technical_lifetime",
        "technology": "extract_gw_fossil",
        "value": 20,
        "unit": "y",
        "broadcast": {"year_vtg": None, "node_loc": None},
    },
]

# investment cost rules
INVESTMENT_COST_RULES = [
    {
        "type": "inv_cost",
        "technology": "extract_surfacewater",
        "value": 155.57,
        "unit": "USD/km3",
        "broadcast": {"year_vtg": None, "node_loc": None},
    },
    {
        "type": "inv_cost",
        "technology": "extract_groundwater",
        "value": 54.52,
        "unit": "USD/km3",
        "broadcast": {"year_vtg": None, "node_loc": None},
    },
    {
        "type": "inv_cost",
        "technology": "extract_gw_fossil",
        "value": 54.52 * 150,
        "unit": "USD/km3",
        "broadcast": {"year_vtg": None, "node_loc": None},
    },
]

# share mode rule to link basin and region water supply; uses mapped basin data from df_sw
SHARE_MODE_RULES = [
    {
        "type": "share_mode_up",
        "shares": "share_basin",
        "technology": "basin_to_reg",
        # dynamic fields; these string expressions will be evaluated in a context 
        # where df_sw is the output of map_basin_region_wat(context)
        "mode": "df_sw['mode']",
        "node_share": "df_sw['MSGREG']",
        "time": "df_sw['time']",
        "value": "df_sw['share']",
        "unit": "%",
        "year_act": "df_sw['year']",
    }
]

# historical new capacity rules - abstracts historical capacity data creation
HISTORICAL_NEW_CAPACITY_RULES = [
    {
        "type": "historical_new_capacity",
        "technology": "extract_surfacewater",
        "value": "df_hist['hist_cap_sw_km3_year'] / 5",
        "unit": "km3/year",
        "node_loc": "df_hist['BCU_name']",
        "year_vtg": 2015,
    },
    {
        "type": "historical_new_capacity",
        "technology": "extract_groundwater",
        "value": "df_hist['hist_cap_gw_km3_year'] / 5",
        "unit": "km3/year",
        "node_loc": "df_hist['BCU_name']",
        "year_vtg": 2015,
    },
]


EXTRACTION_OUTPUT_RULES = [
    {
        "type": "output",
        "technology": "extract_surfacewater",
        "value": 1,
        "unit": "-",
        "level": "water_supply_basin",
        "commodity": "freshwater_basin",
        "mode": "M1",
        "node_loc": "df_node['node']",
        "node_dest": "df_node['node']",
    },
    {
        "type": "output",
        "technology": "extract_groundwater",
        "value": 1,
        "unit": "-",
        "level": "water_supply_basin",
        "commodity": "freshwater_basin",
        "mode": "M1",
        "node_loc": "df_node['node']",
        "node_dest": "df_node['node']",
    },
    {
        "type": "output",
        "technology": "extract_gw_fossil",
        "value": 1,
        "unit": "-",
        "level": "water_supply_basin",
        "commodity": "freshwater_basin",
        "mode": "M1",
        "node_loc": "df_node['node']",
        "node_dest": "df_node['node']",
        "time_origin": "year",
    },
    {
        "type": "output",
        "technology": "extract_salinewater",
        "value": 1,
        "unit": "km3",
        "year_vtg": None,
        "year_act": None,
        "level": "saline_supply",
        "commodity": "saline_ppl",
        "mode": "M1",
        "time": "year",
        "time_dest": "year",
        "time_origin": "year",
    }
]

DUMMY_BASIN_TO_REG_OUTPUT_RULES = [
    {
        "type": "output",
        "technology": "basin_to_reg",
        "value": 1,
        "unit": "-",
        "level": "water_supply",
        "commodity": "freshwater",
        "time_dest": "year",
        "mode": "df_node['mode']",
        "node_loc": "df_node['region']",
        "node_dest": "df_node['region']",
    }
]

DUMMY_VARIABLE_COST_RULES = [
    {
        "type": "var_cost",
        "technology": "basin_to_reg",
        "mode": "df_node['mode']",
        "node_loc": "df_node['region']",
        "value": 20,
        "unit": "-",
    }, 
    {
        "type": "var_cost",
        "technology": "extract_surfacewater",
        "value": 0.0001,
        "unit": "USD/km3",
        "mode": "M1",
        "time": "year",
    },
    {
        "type": "var_cost",
        "technology": "extract_groundwater",
        "value": 0.001,
        "unit": "USD/km3",
        "mode": "M1",
        "time": "year",
    },
    
]

FIXED_COST_RULES = [
    {
        "type": "fix_cost",
        "technology": "extract_gw_fossil",
        "value": 300,
        "unit": "USD/km3",
    }
]

E_FLOW_RULES_DMD = [
    {
        "type": "demand",
        "node": "df_sw['Region'].astype(str)",
        "commodity": "surfacewater_basin",
        "level": "water_avail_basin",
        "year": "df_sw['year']",
        "time": "df_sw['time']",
        "value": "df_sw['value']",
        "unit": "km3/year"
    }
]

E_FLOW_RULES_BOUND = [
    {
        "type": "bound_activity_lo",
        "node": "df_env['Region'].astype(str)",
        "technology": "return_flow",
        "year_act": "df_env['year']",
        "mode": "M1",
        "time": "df_env['time']",
        "value": "df_env['value']",
        "unit": "km3/year",
    }
]
