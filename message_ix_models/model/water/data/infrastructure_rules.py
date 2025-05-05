from message_ix_models.model.water.utils import Rule

# Constants for water infrastructure rules
WF_CONST = {
    "IDENTITY": 1,
    "DESALINATION_OUTPUT_VALUE": 1,
    "DESALINATION_TECH_LIFETIME": 20,  # used a lot so defined as constant
    "DESALINATION_VAR_COST": 100,  # same as above
    "HIST_CAP_DIVISOR": 5,
}

"""
These rules are used in start_creating_input_dataframe
they are grouped together because
they are all used in the same function
"""
INPUT_DATAFRAME_STAGE1 = Rule(
    Base={
        "type": "input",
        "technology": "rows[tec]",
        "unit": "-",
        "level": "rows[inlvl]",
        "commodity": "rows[incmd]",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": True,
            "flag_same_node": True,
            "flag_time": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "value": "rows[value_mid]",
            "mode": "M1",
            "node_loc": "df_node[node]",
        },
        {
            "condition": "!baseline",
            "value": "rows[value_high]",
            "mode": "Mf",
            "pipe": {"flag_node_loc": True},
        },
        {
            "condition": "baseline_main",
            "value": "rows[value_mid]",
            "mode": "M1",
            "pipe": {"flag_node_loc": True},
        },
        {
            "condition": "baseline_additional",
            "value": "rows[value_high]",
            "mode": "Mf",
            "pipe": {"flag_node_loc": True},
        },
    ],
)
"""
These rules are used in prepare_input_dataframe,
they are grouped together because
they are all used in the same function
"""
INPUT_DATAFRAME_STAGE2 = Rule(
    Base={
        "type": "input",
        "technology": "rows[tec]",
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
            "flag_time": True,
            "flag_node_loc": False,
        },
    },
    Diff=[
        {
            "condition": "!baseline",
            "value": "rows[value_high]",
        },
        {
            "condition": "baseline_p1",
            "value": "rows[value_high]",
        },
        {
            "condition": "baseline_p2",
            "value": "rows[value_mid]",
            "mode": "M1",
        },
        {
            "condition": "non_tech",
            "value": "rows[value_mid]",
            "mode": "M1",
        },
    ],
)

"""
These rules are used in calculate_infra_output,
default is always used,
SDG when SDG is run and baseline_main,
baseline_p1 and baseline_p2 when baseline is run via cli.
"""
OUTPUT_RULES = Rule(
    Base={
        "type": "output",
        "unit": "-",
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
            "technology": "rows[tec]",
            "value": "rows[out_value_mid]",
            "level": "rows[outlvl]",
            "commodity": "rows[outcmd]",
            "mode": "M1",
        },
        {
            "condition": "!baseline",
            "technology": "df_out_dist[tec]",
            "value": "df_out_dist[out_value_mid]",
            "level": "df_out_dist[outlvl]",
            "commodity": "df_out_dist[outcmd]",
            "mode": "Mf",
        },
        {
            "condition": "baseline_p1",
            "technology": "df_out_dist[tec]",
            "value": "df_out_dist[out_value_mid]",
            "level": "df_out_dist[outlvl]",
            "commodity": "df_out_dist[outcmd]",
            "mode": "M1",
        },
        {
            "condition": "baseline_p2",
            "technology": "df_out_dist[tec]",
            "value": "df_out_dist[out_value_mid]",
            "level": "df_out_dist[outlvl]",
            "commodity": "df_out_dist[outcmd]",
            "mode": "Mf",
        },
    ],
)

"""
Capacity factor rules for water infrastructure technologies.
"""
CAP_RULES = Rule(
    Base={
        "type": "capacity_factor",
        "technology": "rows[tec]",
        "unit": "%",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_time": True,
            "flag_same_node": True,
            "flag_node_loc": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "value": "rows[capacity_factor_mid]",
        },
    ],
)

"""
Technical lifetime rules for water infrastructure technologies.
"""
TL_RULES = Rule(
    Base={
        "type": "technical_lifetime",
        "technology": "rows[tec]",
        "unit": "y",
        "pipe": {
            "flag_broadcast": True,
            "flag_same_node": True,
            "flag_node_loc": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "value": "rows[technical_lifetime_mid]",
        },
    ],
)

"""
Investment cost rules for water infrastructure technologies.
"""
INV_COST_RULES = Rule(
    Base={
        "type": "inv_cost",
        "technology": "rows[tec]",
        "unit": "USD/km3",
        "pipe": {
            "flag_broadcast": True,
            "flag_node_loc": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "value": "rows[investment_mid]",
        },
    ],
)

"""
Fixed operating cost rules for water infrastructure technologies.
"""
FIX_COST_RULES = Rule(
    Base={
        "type": "fix_cost",
        "technology": "rows[tec]",
        "unit": "USD/km3",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_node_loc": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "value": "rows[fix_cost_mid]",
        },
    ],
)
"""
Variable operating cost rules for water infrastructure technologies under SDG
and baseline conditions, which includes baseline and baseline distributed.
"""
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
            "condition": "!baseline_dist",
            "technology": "rows[tec]",
            "value": "rows[var_cost_high]",
            "mode": "Mf",
        },
        {
            "condition": "baseline_main",
            "technology": "rows[tec]",
            "value": "df_var[var_cost_mid]",
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

"""
Output rule for desalination facilities at basin level.
"""
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

"""
Technical lifetime rules for desalination facilities.
"""
TL_DESALINATION_RULES = Rule(
    Base={
        "type": "technical_lifetime",
        "unit": "y",
        "pipe": {
            "flag_broadcast": True,
            "flag_same_node": True,
            "flag_node_loc": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "technology": "extract_salinewater_basin",
            "value": WF_CONST["DESALINATION_TECH_LIFETIME"],
        },
        {
            "condition": "default",
            "technology": "df_desal[tec]",
            "value": "df_desal[lifetime_mid]",
        },
    ],
)

"""
Historical new capacity rule for desalination facilities.
"""
DESALINATION_HISTORICAL_CAPACITY_RULES = Rule(
    Base={
        "type": "historical_new_capacity",
        "node_loc": "'B' + df_hist[BCU_name]",
        "technology": "df_hist[tec_type]",
        "year_vtg": "df_hist[year]",
        "unit": "km3/year",
        "pipe": {
            "flag_node_loc": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "value": "df_hist[cap_km3_year]",
        },
    ],
)

"""
Upper bound capacity rule for desalination facilities.
"""
DESALINATION_BOUND_TOTAL_CAPACITY_UP_RULES = Rule(
    Base={
        "type": "bound_total_capacity_up",
        "node_loc": "'B' + df_proj[BCU_name]",
        "technology": "extract_salinewater_basin",
        "year_act": "df_proj[year]",
        "unit": "km3/year",
        "pipe": {
            "flag_node_loc": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "value": "df_proj[cap_km3_year]",
        },
    ],
)

"""
Lower bound activity rule for desalination facilities.
"""
DESALINATION_BOUND_LO_RULES = Rule(
    Base={
        "type": "bound_activity_lo",
        "node_loc": "'B' + df_bound[BCU_name]",
        "technology": "df_bound[tec_type]",
        "mode": "M1",
        "unit": "km3/year",
        "pipe": {
            "flag_broadcast": True,
            "flag_time": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "value": "df_bound[cap_km3_year]",
        },
    ],
)

"""
Investment cost rules for desalination technologies.
"""
DESALINATION_INV_COST_RULES = Rule(
    Base={
        "type": "inv_cost",
        "technology": "df_desal[tec]",
        "unit": "USD/km3",
        "pipe": {
            "flag_broadcast": True,
            "flag_node_loc": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "value": "df_desal[inv_cost_mid]",
        },
    ],
)

"""
Fixed cost rules for desalination technologies.
"""
FIX_COST_DESALINATION_RULES = Rule(
    Base={
        "type": "fix_cost",
        "technology": "df_desal[tec]",
        "unit": "USD/km3",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_node_loc": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "value": "df_desal[fix_cost_mid]",
        },
    ],
)

"""
Variable operating cost rule for desalination technologies.
SKIP here was previously commented out, can be removed if not needed.
"""
VAR_COST_DESALINATION_RULES = Rule(
    Base={
        "type": "var_cost",
        "unit": "USD/km3",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_node_loc": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "technology": "rows[tec]",
            "value": "rows[var_cost_mid]",
            "mode": "M1",
            "pipe": {"flag_time": True},
        },
        {
            "condition": "SKIP",
            "technology": "extract_salinewater_basin",
            "value": WF_CONST["DESALINATION_VAR_COST"],
            "mode": "M1",
            "pipe": {"flag_time": False},
        },
    ],
)

"""
Input requirement rules for desalination technologies (electricity, heat, process).
"""
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
            "condition": "electricity",
            "technology": "rows[tec]",
            "value": "rows[electricity_input_mid]",
            "level": "final",
            "commodity": "electr",
            "time_origin": "year",
            "node_loc": "df_node[node]",
            "node_origin": "df_node[region]",
        },
        {
            "condition": "heat",
            "technology": "rows[tec]",
            "value": "rows[heat_input_mid]",
            "level": "final",
            "commodity": "d_heat",
            "time_origin": "year",
            "node_loc": "df_node[node]",
            "node_origin": "df_node[region]",
        },
        {
            "condition": "technology",
            "technology": "rows[tec]",
            "value": WF_CONST["IDENTITY"],
            "level": "rows[inlvl]",
            "commodity": "rows[incmd]",
            "pipe": {
                "flag_same_node": True,
                "flag_same_time": True,
                "flag_node_loc": True,
            },
        },
    ],
)

"""
Secondary output rules for desalination technologies mapping product flow.
"""
DESALINATION_OUTPUT_RULES2 = Rule(
    Base={
        "type": "output",
        "unit": "-",
        "mode": "M1",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": True,
            "flag_same_time": True,
            "flag_same_node": True,
            "flag_time": True,
            "flag_node_loc": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "technology": "rows[tec]",
            "value": WF_CONST["IDENTITY"],
            "level": "rows[outlvl]",
            "commodity": "rows[outcmd]",
        },
    ],
)
