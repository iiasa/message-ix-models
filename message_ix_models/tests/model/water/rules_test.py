# Testing the hardest rules in the DSL engine for each edge case
from message_ix_models.model.water.utils import Rule

# Constants used in the rules (copy-paste required for rule definitions)
WD_CONST = {
    "IDENTITY": 1,
    "NEGATIVE_MULTIPLIER": -1,
    "HIST_CAP_DIVISOR": 5,
    "SHARE_GW_MULT": 0.95,
    "UNIT_CONVERSION": 1e-3,
}
WS_CONST = {
    "IDENTITY": 1,
    "SW_ELEC_IN": 0.018835616,
    "GW_ADD_ELEC_IN": 0.043464579,
    "FOSSIL_GW_ELEC_MULT": 2,
    "SW_LIFETIME": 50,
    "GW_LIFETIME": 20,
    "SW_INV_COST": 155.57,
    "GW_INV_COST": 54.52,
    "FOSSIL_GW_INV_MULT": 150,
    "HIST_CAP_DIV": 5,
    "BASIN_REG_VAR_COST": 20,
    "SW_VAR_COST": 0.0001,
    "GW_VAR_COST": 0.001,
    "FOSSIL_GW_FIX_COST": 300,
}
WF_CONST = {
    "IDENTITY": 1,
    "DESALINATION_OUTPUT_VALUE": 1,
    "DESALINATION_TECH_LIFETIME": 20,
    "DESALINATION_VAR_COST": 100,
    "HIST_CAP_DIVISOR": 5,
}
CT_CONST = {
    "cool_tech_lifetime": 30,
    "cool_tech_output_default": 1,
}

# --- Selected Rules from demand_rules.py ---

INDUSTRIAL_DEMAND = Rule(
    Base={
        "type": "demand",
        "unit": "km3/year",
        "level": "final",
    },
    Diff=[
        {
            "condition": "default",
            "node": "'B' + manuf_mw[node]",
            "commodity": "industry_mw",
            "year": "manuf_mw[year]",
            "time": "manuf_mw[time]",
            "value": f"manuf_mw[value] * {WD_CONST['UNIT_CONVERSION']}",
        },
        {
            "condition": "default",
            "node": "'B' + manuf_uncollected_wst[node]",
            "commodity": "industry_uncollected_wst",
            "year": "manuf_uncollected_wst[year]",
            "time": "manuf_uncollected_wst[time]",
            "value": (
                f"manuf_uncollected_wst[value] * {WD_CONST['UNIT_CONVERSION']}"
                f" * {WD_CONST['NEGATIVE_MULTIPLIER']}"
            ),
        },
    ],
)

WATER_AVAILABILITY = Rule(
    Base={
        "type": "demand",
        "unit": "km3/year",
    },
    Diff=[
        {
            "condition": "sw",
            "node": "'B' + df_sw[Region].astype(str)",
            "commodity": "surfacewater_basin",
            "level": "water_avail_basin",
            "year": "df_sw[year]",
            "time": "df_sw[time]",
            "value": f"df_sw[value] * {WD_CONST['NEGATIVE_MULTIPLIER']}",
        },
        {
            "condition": "gw",
            "node": "'B' + df_gw[Region].astype(str)",
            "commodity": "groundwater_basin",
            "level": "water_avail_basin",
            "year": "df_gw[year]",
            "time": "df_gw[time]",
            "value": f"df_gw[value] * {WD_CONST['NEGATIVE_MULTIPLIER']}",
        },
    ],
)

SHARE_CONSTRAINTS_GW = Rule(
    Base={
        "type": "share_commodity_lo",
        "unit": "-",
    },
    Diff=[
        {
            "condition": "default",
            "shares": "share_low_lim_GWat",
            "node_share": "'B' + df_gw[Region].astype(str)",
            "year_act": "df_gw[year]",
            "time": "df_gw[time]",
            "value": (
                f"df_gw[value] / "
                f"(df_sw[value] + df_gw[value]) * "
                f"{WD_CONST['SHARE_GW_MULT']}"
            ),
            "unit": "-",
        }
    ],
)

# --- Selected Rules from water_supply_rules.py ---

SLACK_TECHNOLOGY_RULES = Rule(
    Base={
        "type": "input",
        "condition": "Nexus",
        "unit": "-",
        "pipe": {
            "flag_broadcast": True,
            "flag_time": True,
        },
    },
    Diff=[
        {
            "type": "input",
            "condition": "Nexus",
            "technology": "return_flow",
            "value": WS_CONST["IDENTITY"],
            "unit": "-",
            "level": "water_avail_basin",
            "commodity": "surfacewater_basin",
            "mode": "M1",
            "year_vtg": "runtime_vals[year_wat]",
            "year_act": "runtime_vals[year_wat]",
            "pipe": {
                "flag_same_time": True,
                "flag_same_node": True,
                "flag_node_loc": True,
            },
        },
        {
            "type": "input",
            "condition": "Nexus",
            "technology": "gw_recharge",
            "value": WS_CONST["IDENTITY"],
            "unit": "-",
            "level": "water_avail_basin",
            "commodity": "groundwater_basin",
            "mode": "M1",
            "year_vtg": "runtime_vals[year_wat]",
            "year_act": "runtime_vals[year_wat]",
            "pipe": {
                "flag_same_time": True,
                "flag_same_node": True,
                "flag_node_loc": True,
            },
        },
        {
            "type": "input",
            "condition": "Nexus",
            "technology": "basin_to_reg",
            "value": WS_CONST["IDENTITY"],
            "unit": "-",
            "level": "water_supply_basin",
            "commodity": "freshwater_basin",
            "mode": "df_node[mode]",
            "node_origin": "df_node[node]",
            "node_loc": "df_node[region]",
            "pipe": {
                "flag_same_time": True,
            },
        },
        {
            "type": "input",
            "condition": "SKIP",  # Example of a different condition
            "technology": "salinewater_return",
            "value": WS_CONST["IDENTITY"],
            "unit": "-",
            "level": "water_avail_basin",
            "commodity": "salinewater_basin",
            "mode": "M1",
            "time": "year",
            "time_origin": "year",
            "node_origin": "df_node[node]",
            "node_loc": "df_node[node]",
            "pipe": {
                "flag_same_time": True,
                "flag_same_node": True,
                "flag_node_loc": True,
            },
        },
    ],
)

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
            "value": WS_CONST["IDENTITY"],
            "commodity": "surfacewater_basin",
            "mode": "M1",
            "node_origin": "df_node[node]",
            "node_loc": "df_node[node]",
            "pipe": {
                "flag_broadcast": True,
                "flag_same_time": True,
                "flag_time": True,
            },
        },
        {
            "condition": "default",
            "technology": "extract_groundwater",
            "level": "water_avail_basin",
            "value": WS_CONST["IDENTITY"],
            "commodity": "groundwater_basin",
            "mode": "M1",
            "node_origin": "df_node[node]",
            "node_loc": "df_node[node]",
            "pipe": {
                "flag_broadcast": True,
                "flag_same_time": True,
                "flag_time": True,
            },
        },
        {
            "condition": "default",
            "technology": "extract_surfacewater",
            "level": "final",
            "value": WS_CONST["SW_ELEC_IN"],
            "commodity": "electr",
            "mode": "M1",
            "time_origin": "year",
            "node_origin": "df_node[region]",
            "node_loc": "df_node[node]",
            "pipe": {
                "flag_broadcast": True,
                "flag_time": True,
            },
        },
        {
            "condition": "default",
            "technology": "extract_groundwater",
            "level": "final",
            "value": f"df_gwt['GW_per_km3_per_year'] + {WS_CONST['GW_ADD_ELEC_IN']}",  # Complex value calc
            "commodity": "electr",
            "mode": "M1",
            "time_origin": "year",
            "node_origin": "df_node[region]",
            "node_loc": "df_node[node]",
            "pipe": {
                "flag_broadcast": True,
                "flag_time": True,
            },
        },
        {
            "condition": "default",
            "technology": "extract_gw_fossil",
            "level": "final",
            "value": f"{WS_CONST['FOSSIL_GW_ELEC_MULT']} * (df_gwt['GW_per_km3_per_year'] + {WS_CONST['GW_ADD_ELEC_IN']})",  # Complex value calc
            "commodity": "electr",
            "mode": "M1",
            "node_origin": "df_node[region]",
            "time_origin": "year",
            "node_loc": "df_node[node]",
            "pipe": {
                "flag_broadcast": True,
                "flag_time": True,
            },
        },
    ],
)


SHARE_MODE_RULES = Rule(
    Base={
        "type": "share_mode_up",  # Different type
        "unit": "%",
    },
    Diff=[
        {
            "condition": "default",
            "technology": "basin_to_reg",
            "shares": "share_basin",
            "mode": "df_sw['mode']",
            "node_share": "df_sw['MSGREG']",
            "time": "df_sw['time']",
            "value": "df_sw['share']",
            "year_act": "df_sw['year']",
        }
    ],
)

E_FLOW_RULES_BOUND = Rule(
    Base={
        "type": "bound_activity_lo",  # Different type
        "unit": "km3/year",
    },
    Diff=[
        {
            "condition": "default",
            "node_loc": "'B' + df_env[Region]",
            "technology": "return_flow",
            "year_act": "df_env[year]",
            "mode": "M1",
            "time": "df_env[time]",
            "value": "df_env[value]",
        }
    ],
)


VAR_COST_RULES = Rule(
    Base={
        "type": "var_cost",
        "unit": "USD/km3",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_time": True,
            "flag_node_loc": True,
        },
    },
    Diff=[
        {
            "condition": "!baseline",
            "technology": "rows[tec]",
            "value": "rows[var_cost_mid]",
            "mode": "M1",
        },
        {
            "condition": "!baseline_dist",  # Different condition
            "technology": "rows[tec]",
            "value": "rows[var_cost_high]",
            "mode": "Mf",
        },
        {
            "condition": "baseline_main",
            "technology": "rows[tec]",
            "value": "df_var[var_cost_mid]",  # Different dataframe source
            "mode": "M1",
        },
        {
            "condition": "baseline_dist_p1",
            "technology": "rows[tec]",
            "value": "rows[var_cost_mid]",
            "mode": "M1",
        },
        {
            "condition": "baseline_dist_p2",
            "technology": "rows[tec]",
            "value": "rows[var_cost_high]",
            "mode": "Mf",
        },
    ],
)

DESALINATION_INPUT_RULES2 = Rule(
    Base={
        "type": "input",
        "unit": "-",
        "mode": "M1",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_time": True,
        },
    },
    Diff=[
        {
            "condition": "electricity",  # Different condition
            "technology": "rows[tec]",
            "value": "rows[electricity_input_mid]",
            "level": "final",
            "commodity": "electr",
            "time_origin": "year",
            "node_loc": "df_node[node]",
            "node_origin": "df_node[region]",
        },
        {
            "condition": "heat",  # Different condition
            "technology": "rows[tec]",
            "value": "rows[heat_input_mid]",
            "level": "final",
            "commodity": "d_heat",
            "time_origin": "year",
            "node_loc": "df_node[node]",
            "node_origin": "df_node[region]",
        },
        {
            "condition": "technology",  # Different condition
            "technology": "rows[tec]",
            "value": WF_CONST["IDENTITY"],
            "level": "rows[inlvl]",
            "commodity": "rows[incmd]",
            "pipe": {  # Overriding pipe flags
                "flag_same_node": True,
                "flag_same_time": True,
                "flag_node_loc": True,
            },
        },
    ],
)


DESALINATION_OUTPUT_RULES = Rule(
    Base={
        "type": "output",
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
            "flag_node_loc": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "technology": "extract_salinewater_basin",
            "value": WF_CONST["IDENTITY"],
        },
    ],
)


COOL_TECH_OUTPUT_RULES = Rule(
    Base={
        "type": "output",
    },
    Diff=[
        {
            "condition": "default",
            "node_loc": "input_cool[node_loc]",
            "technology": "input_cool[technology_name]",
            "year_vtg": "input_cool[year_vtg]",
            "year_act": "input_cool[year_act]",
            "mode": "input_cool[mode]",
            "node_dest": "input_cool[node_origin]",
            "commodity": "input_cool['technology_name'].str.split('__').str.get(1)",  # Value from string manipulation
            "level": "share",
            "time": "year",
            "time_dest": "year",
            "value": CT_CONST["cool_tech_output_default"],
            "unit": "-",
        },
        {
            "condition": "nexus",  # Different condition
            "node_loc": "icfb_df[node_loc]",
            "technology": "icfb_df[technology_name]",
            "year_vtg": "icfb_df[year_vtg]",
            "year_act": "icfb_df[year_act]",
            "mode": "icfb_df[mode]",
            "commodity": "surfacewater_basin",
            "level": "water_avail_basin",
            "time": "year",
            "value": "icfb_df[value_return]",
            "unit": "MCM/GWa",
            "pipe": {
                "flag_broadcast": True,
            },
        },
    ],
)
