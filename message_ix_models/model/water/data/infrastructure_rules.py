
INPUT_DATAFRAME_STAGE1 = [
    {
        "type": "input",
        "condition": "default",
        "technology": "rows[tec]",
        "value": "rows[value_mid]",
        "unit": "-",
        "level": "rows[inlvl]",
        "commodity": "rows[incmd]",
        "mode": "M1",
        "node_loc": "df_node[node]",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": True,
            "flag_same_node": True,
            "flag_time": True,
            "flag_node_loc": False
        }
    },
    {
        "type": "input",
        "condition": "!baseline",
        "technology": "rows[tec]",
        "value": "rows[value_high]",
        "unit": "-",
        "level": "rows[inlvl]",
        "commodity": "rows[incmd]",
        "mode": "Mf",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": True,
            "flag_same_node": True,
            "flag_time": True,
            "flag_node_loc": True
        },
    },
    {
        "type": "input",
        "condition": "baseline_main",
        "technology": "rows[tec]",
        "value": "rows[value_mid]",
        "unit": "-",
        "level": "rows[inlvl]",
        "commodity": "rows[incmd]",
        "mode": "M1",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": True,
            "flag_same_node": True,
            "flag_time": True,
            "flag_node_loc": True
        },
    },
    {
        "type": "input",
        "condition": "baseline_additional",
        "technology": "rows[tec]",
        "value": "rows[value_high]",
        "unit": "-",
        "level": "rows[inlvl]",
        "commodity": "rows[incmd]",
        "mode": "Mf",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": True,
            "flag_same_node": True,
            "flag_time": True,
            "flag_node_loc": True
        },
    },
]

INPUT_DATAFRAME_STAGE2 = [
    {
        "type": "input",
        "condition": "!baseline",
        "technology": "rows[tec]",
        "value": "rows[value_high]",
        "unit": "-",
        "level": "final",
        "commodity": "electr",
        "mode": "Mf",
        "time_origin": "year",
        "node_loc": "df_node[node]",
        "node_origin": "df_node[region]",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": True,
            "flag_node_loc": False
        },
    },
    {
        "type": "input",
        "condition": "baseline_p1",
        "technology": "rows[tec]",
        "value": "rows[value_high]",
        "unit": "-",
        "level": "final",
        "commodity": "electr",
        "mode": "Mf",
        "time_origin": "year",
        "node_loc": "df_node[node]",
        "node_origin": "df_node[region]",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": True,
            "flag_node_loc": False
        },
    },
    {
        "type": "input",
        "condition": "baseline_p2",
        "technology": "rows[tec]",
        "value": "rows[value_mid]",
        "unit": "-",
        "level": "final",
        "commodity": "electr",
        "mode": "M1",
        "time_origin": "year",
        "node_loc": "df_node[node]",
        "node_origin": "df_node[region]",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": True,
            "flag_node_loc": False
        },
    },
    {
        "type": "input",
        "condition": "non_tech",
        "technology": "rows[tec]",
        "value": "rows[value_mid]",
        "unit": "-",
        "level": "final",
        "commodity": "electr",
        "mode": "M1",
        "time_origin": "year",
        "node_loc": "df_node[node]",
        "node_origin": "df_node[region]",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": True,
            "flag_node_loc": False
        },
    },
]


OUTPUT_RULES= [
    {
        "type": "output",
        "condition": "default",
        "technology": "rows[tec]",
        "value": "rows[out_value_mid]",
        "unit": "-",
        "level": "rows[outlvl]",
        "commodity": "rows[outcmd]",
        "mode": "M1",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_node": True,
            "flag_same_time": True,
            "flag_time": True
        }
    },
    {
        "type": "output",
        "condition": "!baseline",
        "technology": "df_out_dist[tec]",
        "value": "df_out_dist[out_value_mid]",
        "unit": "-",
        "level": "df_out_dist[outlvl]",
        "commodity": "df_out_dist[outcmd]",
        "mode": "Mf",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_node": True,
            "flag_same_time": True,
            "flag_time": True
        }
    },
    {
        "type": "output",
        "condition": "baseline_p1",
        "technology": "df_out_dist[tec]",
        "value": "df_out_dist[out_value_mid]",
        "unit": "-",
        "level": "df_out_dist[outlvl]",
        "commodity": "df_out_dist[outcmd]",
        "mode": "M1",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_node": True,
            "flag_same_time": True,
            "flag_time": True
        }
    },
    {
        "type": "output",
        "condition": "baseline_p2",
        "technology": "df_out_dist[tec]",
        "value": "df_out_dist[out_value_mid]",
        "unit": "-",
        "level": "df_out_dist[outlvl]",
        "commodity": "df_out_dist[outcmd]",
        "mode": "Mf",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_node": True,
            "flag_same_time": True,
            "flag_time": True
        }
    },
]


CAP_RULES = [
    {
        "type": "capacity_factor",
        "condition": "default",
        "technology": "rows[tec]",
        "value": "rows[capacity_factor_mid]",
        "unit": "%",
        "pipe":{
            "flag_broadcast":True,
            "flag_map_yv_ya_lt": True,
            "flag_time":True,
            "flag_same_time":False,
            "flag_same_node" : True
        }
    }
]

TL_RULES = [
    {
        "type": "technical_lifetime",
        "condition": "default",
        "technology": "rows[tec]",
        "value": "rows[technical_lifetime_mid]",
        "unit": "y",
        "pipe":{
                "flag_broadcast": True,
                "flag_map_yv_ya_lt": False,
                "flag_same_time": False,
                "flag_same_node": True,
                "flag_time": False
        }
    }
]

INV_COST_RULES = [
    {
        "type": "inv_cost",
        "condition": "default",
        "technology": "rows[tec]",
        "value": "rows[investment_mid]",
        "unit": "USD/km3",
        "pipe":{
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": False,
        }
    }
]

FIX_COST_RULES = [
    {
        "type": "fix_cost",
        "condition": "default",
        "technology": "rows[tec]",
        "value": "rows[fix_cost_mid]",
        "unit": "USD/km3",
        "pipe":{
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": False
    }
    }
]

VAR_COST_RULES = [
    {
        "type": "var_cost",
        "condition": "!baseline",
        "technology": "rows[tec]",
        "value": "rows[var_cost_mid]",
        "unit": "USD/km3",
        "mode": "M1",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": True
        }
    },
    {
        "type": "var_cost",
        "condition": "!baseline_dist",
        "technology": "rows[tec]",
        "value": "rows[var_cost_high]",
        "unit": "USD/km3",
        "mode": "Mf",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": True
        }
    },
    {
        "type": "var_cost",
        "condition": "baseline_main",
        "technology": "rows[tec]",
        "value": "df_var[var_cost_mid]",
        "unit": "USD/km3",
        "mode": "M1",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": True
        }
    },
    {
        "type": "var_cost",
        "condition": "baseline_dist_p1",
        "technology": "rows[tec]",
        "value": "rows[var_cost_mid]",
        "unit": "USD/km3",
        "mode": "M1",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": True
        }
    },
    {
        "type": "var_cost",
        "condition": "baseline_dist_p2",
        "technology": "rows[tec]",
        "value": "rows[var_cost_high]",
        "unit": "USD/km3",
        "mode": "Mf",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": True
        }
    },
]

DESALINATION_OUTPUT_RULES = [
    {
        "type": "output",
        "condition": "default",
        "technology": "extract_salinewater_basin",
        "value": 1,
        "unit": "km3/year",
        "level": "water_avail_basin",
        "commodity": "salinewater_basin",
        "mode": "M1",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_node": True,
            "flag_same_time": True,
            "flag_time": True,
        }
    }
]

TL_DESALINATION_RULES = [
    {
        "type": "technical_lifetime",
        "condition": "default",
        "technology": "extract_salinewater_basin",
        "value": 20,
        "unit": "y",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": False,
            "flag_same_time": False,
            "flag_same_node": True,
            "flag_time": False,
        }
    },
    {
        "type": "technical_lifetime",
        "condition": "default",
        "technology": "df_desal[tec]",
        "value": "df_desal[lifetime_mid]",
        "unit": "y",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": False,
            "flag_same_time": False,
            "flag_same_node": True,
            "flag_time": False,
        }
    }
]


DESALINATION_HISTORICAL_CAPACITY_RULES = [
    {
        "type": "historical_new_capacity",
        "condition": "default",
        "node_loc": "'B' + df_hist[BCU_name]",
        "technology": "df_hist[tec_type]",
        "year_vtg": "df_hist[year]",
        "value": "df_hist[cap_km3_year]",
        "unit": "km3/year",
        "pipe": {
            "flag_broadcast": False,
            "flag_map_yv_ya_lt": False,
            "flag_same_time": False,
            "flag_same_node": False,
        }
    }
]

DESALINATION_BOUND_TOTAL_CAPACITY_UP_RULES = [
    {
        "type": "bound_total_capacity_up",
        "condition": "default",
        "node_loc": "'B' + df_proj[BCU_name]",
        "technology": "extract_salinewater_basin",
        "year_act": "df_proj[year]",
        "value": "df_proj[cap_km3_year]",
        "unit": "km3/year",
        "pipe": {
            "flag_broadcast": False,
            "flag_map_yv_ya_lt": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": False,
        }
    }
]

DESALINATION_BOUND_LO_RULES = [
    {
        "type": "bound_activity_lo",
        "condition": "default",
        "node_loc": "'B' + df_bound[BCU_name]",
        "technology": "df_bound[tec_type]",
        "mode": "M1",
        "value": "df_bound[cap_km3_year]",
        "unit": "km3/year",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": True,
            "flag_node_loc": False
        }
    }
]

DESALINATION_INV_COST_RULES = [
    {
        "type": "inv_cost",
        "condition": "default",
        "technology": "df_desal[tec]",
        "value": "df_desal[inv_cost_mid]",
        "unit": "USD/km3",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": False
        }
    }
]

FIX_COST_DESALINATION_RULES = [
    {
        "type": "fix_cost",
        "condition": "default",
        "technology": "rows[tec]",
        "value": "rows[fix_cost_mid]",
        "unit": "USD/km3",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": False
        }
    }
]

VAR_COST_DESALINATION_RULES = [
    {
        "type": "var_cost",
        "condition": "default",
        "technology": "rows[tec]",
        "value": "rows[var_cost_mid]",
        "unit": "USD/km3",
        "mode": "M1",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": True
        }
    },
    {
        "type": "var_cost",
        "condition": "SKIP", # rule was commented out in the original code
        "technology": "extract_salinewater_basin",
        "value": 100,
        "unit": "USD/km3",
        "mode": "M1",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": False
        }
    }
]

DESALINATION_INPUT_RULES2 = [
    {
        "type": "input",
        "condition": "electricity",
        "technology": "rows[tec]",
        "value": "rows[electricity_input_mid]",
        "unit": "-",
        "level": "final",
        "commodity": "electr",
        "mode": "M1",
        "time_origin": "year",
        "node_loc": "df_node[node]",
        "node_origin": "df_node[region]",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": True,
            "flag_node_loc": False
        }
    },
    {
        "type": "input",
        "condition": "heat",
        "technology": "rows[tec]",
        "value": "rows[heat_input_mid]",
        "unit": "-",
        "level": "final",
        "commodity": "d_heat",
        "mode": "M1",
        "time_origin": "year",
        "node_loc": "df_node[node]",
        "node_origin": "df_node[region]",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": True,
            "flag_node_loc": False
        }
    },
    {
        "type": "input",
        "condition": "technology",
        "technology": "rows[tec]",
        "value": 1,
        "unit": "-",
        "level": "rows[inlvl]",
        "commodity": "rows[incmd]",
        "mode": "M1",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": True,
            "flag_same_node": True,
            "flag_time": True,
        }
    }

]

DESALINATION_OUTPUT_RULES2 = [
    {
        "type": "output",
        "condition": "default",
        "technology": "rows[tec]",
        "value": 1,
        "unit": "-",
        "level": "rows[outlvl]",
        "commodity": "rows[outcmd]",
        "mode": "M1",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": True,
            "flag_same_node": True,
            "flag_time": True,
        }
    }
]