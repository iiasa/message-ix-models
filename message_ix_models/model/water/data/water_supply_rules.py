import pandas as pd

from message_ix_models.model.water.data.infrastructure_utils import Rule

# ----------------- DSL rule templates -----------------
# these templates are used to declare the supply transformation logic.
# dynamic values (e.g. years, node locations) are filled in within add_water_supply.


SLACK_TECHNOLOGY_RULES = Rule(
    Base = {
        "type": "input",
        "condition": "Nexus",
        "unit": "-",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": False,
            "flag_time": True,
        }
    },
    Diff =[
    {
     "type" : "input",
     "condition": "Nexus",
     "technology": "return_flow",
     "value": 1,
     "unit" : "-",
     "level" : "water_avail_basin",
     "commodity" : "surfacewater_basin",
     "mode" : "M1",
     "year_vtg" : "runtime_vals[year_wat]",
     "year_act" : "runtime_vals[year_wat]",
     "pipe": {
        "flag_same_time": True,
        "flag_same_node": True,
        "flag_node_loc": True,
        },
    },
    {
        "type" : "input",
        "condition": "Nexus",
        "technology" : "gw_recharge",
        "value" : 1,
        "unit" : "-",
        "level" : "water_avail_basin",
        "commodity" : "groundwater_basin",
        "mode" : "M1",
        "year_vtg" : "runtime_vals[year_wat]",
        "year_act" : "runtime_vals[year_wat]",
        "pipe": {
            "flag_same_time": True,
            "flag_same_node": True,
            "flag_node_loc": True,
        }
    },
    {
        "type" : "input",
        "condition": "Nexus",
        "technology" : "basin_to_reg",
        "value" : 1,
        "unit" : "-",
        "level" : "water_supply_basin",
        "commodity" : "freshwater_basin",
        "mode" : "df_node[mode]",
        "node_origin" : "df_node[node]",
        "node_loc" : "df_node[region]",
        "pipe": {
            "flag_same_time": True,
            "flag_same_node": False,
            "flag_node_loc": False,
        },
    },
    {
        "type" : "input",
        "condition": "SKIP",
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
        "pipe": {
            "flag_same_time": True,
            "flag_same_node": True,
            "flag_node_loc": True,
        },
    }


]
)


COOLING_SUPPLY_RULES = Rule(
    Base={
    "type": "output",
    "value": 1,
    "unit": "km3",
    "year_vtg": None,
    "year_act": None,
    "mode": "M1",
    "time": "year",
    "time_dest": "year",
    "time_origin": "year",
    "pipe": {
        "flag_broadcast": True,
        "flag_map_yv_ya_lt": False,
        "flag_same_time": False,
        "flag_same_node": True,
        "flag_time": False,
        "flag_node_loc": True,
    },
}, Diff=[
    {
        "condition": None,
        "technology": "extract_surfacewater",
        "level": "water_supply",
        "commodity": "freshwater",
    },
    {
        "condition": None,
        "technology": "extract_groundwater",
        "level": "water_supply",
        "commodity": "freshwater",
    },
    {
        "condition": None,
        "technology": "extract_salinewater",
        "level": "saline_supply",
        "commodity": "saline_ppl",
    },
])

EXTRACTION_INPUT_RULES = Rule(
    Base={
        "type": "input",
        "unit": "-",
        "mode": "M1",

    },
    Diff=[
        {
            "condition": "default",
            "technology": "extract_surfacewater",
            "level": "water_avail_basin",
            "value": 1,
            "commodity": "surfacewater_basin",
            "mode": "M1",
            "node_origin": "df_node[node]",
            "node_loc": "df_node[node]",
            "pipe": {
                "flag_broadcast": True,
                "flag_map_yv_ya_lt": False,
                "flag_same_time": True,
                "flag_same_node": False,
                "flag_time": True,
                "flag_node_loc": False,
        }
        },
        {
            "condition": "default",
            "technology": "extract_groundwater",
            "level": "water_avail_basin",
            "value": 1,
            "commodity": "groundwater_basin",
            "mode": "M1",
            "node_origin": "df_node[node]",
            "node_loc": "df_node[node]",
            "pipe": {
                "flag_broadcast": True,
                "flag_map_yv_ya_lt": False,
                "flag_same_time": True,
                "flag_same_node": False,
                "flag_time": True,
                "flag_node_loc": False,
        }
        },
        {
            "condition": "default",
            "technology": "extract_surfacewater",
            "level": "final",
            "value": 0.018835616,
            "commodity": "electr",
            "mode": "M1",
            "time_origin": "year",
            "node_origin": "df_node[region]",
            "node_loc": "df_node[node]",
            "pipe": {
                "flag_broadcast": True,
                "flag_map_yv_ya_lt": False,
                "flag_same_time": False,
                "flag_same_node": False,
                "flag_time": True,
                "flag_node_loc": False,
        }
        },
        {
            "condition": "default",
            "technology": "extract_groundwater",
            "level": "final",
            "value": "df_gwt['GW_per_km3_per_year'] + 0.043464579",
            "commodity": "electr",
            "mode": "M1",
            "time_origin": "year",
            "node_origin": "df_node[region]",
            "node_loc": "df_node[node]",
            "pipe": {
                "flag_broadcast": True,
                "flag_map_yv_ya_lt": False,
                "flag_same_time": False,
                "flag_same_node": False,
                "flag_time": True,
                "flag_node_loc": False,
        }
        },
        {
            "condition": "default",
            "technology": "extract_gw_fossil",
            "level": "final",
            "value": "2 * (df_gwt['GW_per_km3_per_year'] + 0.043464579)",
            "commodity": "electr",
            "mode": "M1",
            "node_origin": "df_node[region]",
            "time_origin": "year",
            "node_loc": "df_node[node]",
            "pipe": {
                "flag_broadcast": True,
                "flag_map_yv_ya_lt": False,
                "flag_same_time": False,
                "flag_same_node": False,
                "flag_time": True,
                "flag_node_loc": False,
        },
        }
    ]
)


TECHNICAL_LIFETIME_RULES = Rule(
    Base={
    "type": "technical_lifetime",
    "unit": "y",
    "pipe": {
        "flag_broadcast": True,
        "flag_map_yv_ya_lt": False,
        "flag_same_time": False,
        "flag_same_node": True,
        "flag_time": False,
        "flag_node_loc": True,
    }
},
    Diff=[
    {"condition": None, "technology": "extract_surfacewater", "value": 50},
    {"condition": None, "technology": "extract_groundwater", "value": 20},
    {"condition": None, "technology": "extract_gw_fossil", "value": 20},
]
)


INVESTMENT_COST_RULES = Rule(
    Base={
    "type": "inv_cost",
    "unit": "USD/km3",
    "pipe": {
        "flag_broadcast": True,
        "flag_map_yv_ya_lt": False,
        "flag_same_time": False,
        "flag_same_node": False,
        "flag_time": False,
        "flag_node_loc": True,
    }
},

    Diff=[
    {"condition": "default", "technology": "extract_surfacewater", "value": 155.57},
    {"condition": "default", "technology": "extract_groundwater", "value": 54.52},
    {"condition": "default", "technology": "extract_gw_fossil", "value": 54.52 * 150},
])


SHARE_MODE_RULES = Rule(
    Base = {
        "type": "share_mode_up",
        "unit": "%",
        "pipe":{
            "flag_broadcast": False,
            "flag_map_yv_ya_lt": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": False ,
            "flag_node_loc": False,
        }
    },
    Diff = [
        {"condition": "default",
        "technology": "basin_to_reg",
        "shares": "share_basin",
        "mode": "df_sw['mode']",
        "node_share": "df_sw['MSGREG']",
        "time": "df_sw['time']",
        "value": "df_sw['share']",
        "year_act": "df_sw['year']",
        }
    ]
)


HISTORICAL_NEW_CAPACITY_RULES = Rule(
    Base={
    "type": "historical_new_capacity",
    "unit": "km3/year",
    "node_loc": "df_hist['BCU_name']",
    "year_vtg": 2015,
    "pipe": {
        "flag_broadcast": False,
        "flag_map_yv_ya_lt": False,
        "flag_same_time": False,
        "flag_same_node": False,
        "flag_time": False,
        "flag_node_loc": False,
    }
},

    Diff=[
    {
        "condition": "default",
        "technology": "extract_surfacewater",
        "value": "df_hist['hist_cap_sw_km3_year'] / 5",
    },
    {
        "condition": "default",
        "technology": "extract_groundwater",
        "value": "df_hist['hist_cap_gw_km3_year'] / 5",
    },
]
)


EXTRACTION_OUTPUT_RULES = Rule(
    Base = {
        "type": "output",
        "mode": "M1",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": False,
        }
    },
    Diff = [
    {   "condition": "default",
        "technology": "extract_surfacewater",
        "value": 1,
        "unit": "-",
        "level": "water_supply_basin",
        "commodity": "freshwater_basin",
        "node_loc": "df_node['node']",
        "node_dest": "df_node['node']",
        "pipe": {
            "flag_same_time": True,
            "flag_same_node": False,
            "flag_time": True,
            "flag_node_loc": False,
        }
    },
    {   "condition": "default",
        "technology": "extract_groundwater",
        "value": 1,
        "unit": "-",
        "level": "water_supply_basin",
        "commodity": "freshwater_basin",
        "node_loc": "df_node['node']",
        "node_dest": "df_node['node']",
        "pipe": {
            "flag_same_time": True,
            "flag_same_node": False,
            "flag_time": True,
            "flag_node_loc": False,
        }
    },
    {   "condition": "default",
        "technology": "extract_gw_fossil",
        "value": 1,
        "unit": "-",
        "level": "water_supply_basin",
        "commodity": "freshwater_basin",
        "node_loc": "df_node['node']",
        "node_dest": "df_node['node']",
        "time_origin": "year",
        "pipe": {
            "flag_same_time": True,
            "flag_same_node": False,
            "flag_time": True,
            "flag_node_loc": False,
        }
    },
    {   "condition": "default",
        "technology": "extract_salinewater",
        "value": 1,
        "unit": "km3",
        "year_vtg": "runtime_vals[year_wat]",
        "year_act": "runtime_vals[year_wat]",
        "level": "saline_supply",
        "commodity": "saline_ppl",
        "time": "year",
        "time_dest": "year",
        "time_origin": "year",
        "pipe": {
            "flag_same_time": False,
            "flag_same_node": True,
            "flag_time": False,
            "flag_node_loc": True,
        }
    }

    ]
)

DUMMY_BASIN_TO_REG_OUTPUT_RULES = Rule(
    Base={
    "type": "output",
    "unit": "-",
    "pipe": {
        "flag_broadcast": True,
        "flag_map_yv_ya_lt": False,
        "flag_same_time": False,
        "flag_same_node": False,
        "flag_time": True,
        "flag_node_loc": False,
    }
},
    Diff=[
    {"condition": "default",
    "technology": "basin_to_reg",
    "value": 1,
    "level": "water_supply",
    "commodity": "freshwater",
    "time_dest": "year",
    "mode": "df_node['mode']",
    "node_loc": "df_node['region']",
    "node_dest": "df_node['region']",}
])

DUMMY_VARIABLE_COST_RULES = Rule(
    Base = {
    "type": "var_cost",
    "unit": "-",

},
    Diff = [
    {"condition": "default",
    "technology": "basin_to_reg",
    "value": 20,
    "mode": "df_node['mode']",
    "node_loc": "df_node['region']",
    "pipe": {
        "flag_broadcast": True,
        "flag_map_yv_ya_lt": False,
        "flag_same_time": False,
        "flag_same_node": False,
        "flag_time": True,
        "flag_node_loc": False,

        },
    },
    {"condition": "SKIP",
    "technology": "extract_surfacewater",
    "value": 0.0001,
    "unit": "USD/km3",
    "mode": "M1",
    "time": "year",
    },
    {"condition": "SKIP",
    "technology": "extract_groundwater",
    "value": 0.001,
    "unit": "USD/km3",
    "mode": "M1",
    "time": "year",
    },

    ]
)



FIXED_COST_RULES = Rule(
    Base = {
        "type": "fix_cost",
        "unit": "USD/km3",
    },
    Diff = [
        {"condition": None,
        "technology": "extract_gw_fossil",
        "value": 300,
        "unit": "USD/km3",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": False,
            "flag_node_loc": True,
        }
    }
    ]
)

E_FLOW_RULES_DMD = Rule(
    Base = {
        "type": "demand",
        "unit": "km3/year",
        "pipe": {
            "flag_broadcast": False,
            "flag_map_yv_ya_lt": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": False,
            "flag_node_loc": False,
        }
    },
    Diff = [
        {"condition": "default",
        "node": "df_sw[Region]",
        "commodity": "surfacewater_basin",
        "level": "water_avail_basin",
        "year": "df_sw[year]",
        "time": "df_sw[time]",
        "value": "df_sw[value]",
        }
    ]
)

E_FLOW_RULES_BOUND = Rule(
    Base = {
        "type": "bound_activity_lo",
        "unit": "km3/year",
        "pipe": {
            "flag_broadcast": False,
            "flag_map_yv_ya_lt": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": False,
            "flag_node_loc": False,
        }
    },
    Diff = [
        {"condition": "default",
        "node_loc": "'B' + df_env[Region]",
        "technology": "return_flow",
        "year_act": "df_env[year]",
        "mode": "M1",
        "time": "df_env[time]",
        "value": "df_env[value]",
        }
    ]
)

