from message_ix_models.model.water.utils import Rule

# ----------------- DSL rule templates -----------------
# these templates are used to declare the supply transformation logic.
# dynamic values (e.g. years, node locations) are filled in within j.

"""
Water Supply Constants
"""
WS_CONST = {
    # Input/Output Coefficients (Dimensionless)
    "IDENTITY": 1,
    "SW_ELEC_IN": 0.018835616,  # Surfacewater electricity input
    "GW_ADD_ELEC_IN": 0.043464579,  # Added to depth-dependent pumping cost
    "FOSSIL_GW_ELEC_MULT": 2,  # Multiplies (depth-dependent + additional) GW cost
    "SW_LIFETIME": 50,  # Technical Lifetimes (Years)
    "GW_LIFETIME": 20,  # Also applies to fossil GW
    # Investment Costs (USD/km3 annual capacity)
    "SW_INV_COST": 155.57,  # Surfacewater investment cost
    "GW_INV_COST": 54.52,  # Groundwater investment cost
    "FOSSIL_GW_INV_MULT": 150,  # Multiplies GW investment cost
    # Historical Capacity Calculation
    "HIST_CAP_DIV": 5,  # Divisor for annualizing historical capacity
    # Variable Costs (USD/km3)
    "BASIN_REG_VAR_COST": 20,  # Basin to region variable cost
    "SW_VAR_COST": 0.0001,  # Note: Rule marked as SKIP
    "GW_VAR_COST": 0.001,  # Note: Rule marked as SKIP
    # Fixed Costs (USD/km3 annual capacity)
    "FOSSIL_GW_FIX_COST": 300,  # Fossil groundwater fixed cost
}


"""
Rules for slack technologies
(return_flow, gw_recharge, basin_to_reg, salinewater_return).
Defines input parameters for these technologies.
Used in `_process_slack_rules` within `add_water_supply`.
"""
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
            "condition": "SKIP",
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


"""
Rules defining outputs for cooling-related water extraction technologies.
Specifies the output commodity and level for surface, ground,
and saline water extraction.
Used in `_process_cooling_supply` within `add_water_supply`.
"""
COOLING_SUPPLY_RULES = Rule(
    Base={
        "type": "output",
        "value": WS_CONST["IDENTITY"],
        "unit": "km3",
        "year_vtg": "runtime_vals[year_wat]",
        "year_act": "runtime_vals[year_wat]",
        "mode": "M1",
        "time": "year",
        "time_dest": "year",
        "time_origin": "year",
        "pipe": {
            "flag_broadcast": True,
            "flag_same_node": True,
            "flag_node_loc": True,
        },
    },
    Diff=[
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
    ],
)

"""
Rules defining inputs for water extraction technologies
(surface, ground, fossil ground). Includes inputs from
water sources (basin commodities) and electricity (final commodity).
Specifies electricity intensity, including depth-dependent costs for groundwater.
Used in `_process_extraction_input_rules` within `add_water_supply`.
"""
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
            "value": "df_gwt['GW_per_km3_per_year'] + {GW_ADD_ELEC_IN}".format(
                **WS_CONST
            ),  # .format notation for clarity
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
            "value": (
                "{FOSSIL_GW_ELEC_MULT} * "
                "(df_gwt['GW_per_km3_per_year'] + {GW_ADD_ELEC_IN})".format(**WS_CONST)
            ),
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


"""
Rules defining the technical lifetime for water extraction technologies.
Used in `_process_tl_rules` within `add_water_supply`.
"""
TECHNICAL_LIFETIME_RULES = Rule(
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
            "condition": None,
            "technology": "extract_surfacewater",
            "value": WS_CONST["SW_LIFETIME"],
        },
        {
            "condition": None,
            "technology": "extract_groundwater",
            "value": WS_CONST["GW_LIFETIME"],
        },
        {
            "condition": None,
            "technology": "extract_gw_fossil",
            "value": WS_CONST["GW_LIFETIME"],
        },
    ],
)


"""
Rules defining the investment costs for water extraction technologies.
Includes multipliers for fossil groundwater extraction.
Used in `_process_inv_cost_rules` within `add_water_supply`.
"""
INVESTMENT_COST_RULES = Rule(
    Base={
        "type": "inv_cost",
        "unit": "USD/km3",  # Unit likely USD/(km3/year)
        "pipe": {
            "flag_broadcast": True,
            "flag_node_loc": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "technology": "extract_surfacewater",
            "value": WS_CONST["SW_INV_COST"],
        },
        {
            "condition": "default",
            "technology": "extract_groundwater",
            "value": WS_CONST["GW_INV_COST"],
        },
        {
            "condition": "default",
            "technology": "extract_gw_fossil",
            "value": WS_CONST["GW_INV_COST"] * WS_CONST["FOSSIL_GW_INV_MULT"],
        },
    ],
)


"""
Rules defining upper bounds on the share of activity ('share_mode_up') for the
'basin_to_reg' technology, based on regional water availability shares.
Used in `_process_share_mode_rules` within `add_water_supply`.
"""
SHARE_MODE_RULES = Rule(
    Base={
        "type": "share_mode_up",
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


"""
Rules defining historical new capacity for surface and
groundwater extraction technologies.
Annualizes historical capacity data using HIST_CAP_DIV.
Used in `_process_hist_cap_rules` within `add_water_supply`.
"""
HISTORICAL_NEW_CAPACITY_RULES = Rule(
    Base={
        "type": "historical_new_capacity",
        "unit": "km3/year",
        "node_loc": "df_hist['BCU_name']",
        "year_vtg": 2015,
    },
    Diff=[
        {
            "condition": "default",
            "technology": "extract_surfacewater",
            "value": "df_hist['hist_cap_sw_km3_year'] / {HIST_CAP_DIV}".format(
                **WS_CONST
            ),
        },
        {
            "condition": "default",
            "technology": "extract_groundwater",
            "value": "df_hist['hist_cap_gw_km3_year'] / {HIST_CAP_DIV}".format(
                **WS_CONST
            ),
        },
    ],
)


"""
Rules defining outputs for water extraction technologies
(surface, ground, fossil ground, saline). Specifies the destination commodity and
level for each extraction type.
Used in `_process_extraction_output_rules` within `add_water_supply`.
"""
EXTRACTION_OUTPUT_RULES = Rule(
    Base={
        "type": "output",
        "mode": "M1",
        "pipe": {
            "flag_broadcast": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "technology": "extract_surfacewater",
            "value": WS_CONST["IDENTITY"],
            "unit": "-",
            "level": "water_supply_basin",
            "commodity": "freshwater_basin",
            "node_loc": "df_node['node']",
            "node_dest": "df_node['node']",
            "pipe": {
                "flag_same_time": True,
                "flag_time": True,
            },
        },
        {
            "condition": "default",
            "technology": "extract_groundwater",
            "value": WS_CONST["IDENTITY"],
            "unit": "-",
            "level": "water_supply_basin",
            "commodity": "freshwater_basin",
            "node_loc": "df_node['node']",
            "node_dest": "df_node['node']",
            "pipe": {
                "flag_same_time": True,
                "flag_time": True,
            },
        },
        {
            "condition": "default",
            "technology": "extract_gw_fossil",
            "value": WS_CONST["IDENTITY"],
            "unit": "-",
            "level": "water_supply_basin",
            "commodity": "freshwater_basin",
            "node_loc": "df_node['node']",
            "node_dest": "df_node['node']",
            "time_origin": "year",
            "pipe": {
                "flag_same_time": True,
                "flag_time": True,
            },
        },
        {
            "condition": "default",
            "technology": "extract_salinewater",
            "value": WS_CONST["IDENTITY"],
            "unit": "km3",
            "year_vtg": "runtime_vals[year_wat]",
            "year_act": "runtime_vals[year_wat]",
            "level": "saline_supply",
            "commodity": "saline_ppl",
            "time": "year",
            "time_dest": "year",
            "time_origin": "year",
            "pipe": {
                "flag_same_node": True,
                "flag_node_loc": True,
            },
        },
    ],
)

"""
Rules defining the output for the dummy 'basin_to_reg' technology.
This represents the transfer of freshwater from basin level to regional supply level.
Used in `_process_dummy_basin_output_rules` within `add_water_supply`.
"""
DUMMY_BASIN_TO_REG_OUTPUT_RULES = Rule(
    Base={
        "type": "output",
        "unit": "-",
        "pipe": {
            "flag_broadcast": True,
            "flag_time": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "technology": "basin_to_reg",
            "value": WS_CONST["IDENTITY"],
            "level": "water_supply",
            "commodity": "freshwater",
            "time_dest": "year",
            "mode": "df_node['mode']",
            "node_loc": "df_node['region']",
            "node_dest": "df_node['region']",
        }
    ],
)

"""
Rules defining variable costs for water supply technologies.
Includes costs for the 'basin_to_reg' transfer and skipped costs for extraction.
Used in `_process_var_cost_rules` within `add_water_supply`.
"""
DUMMY_VARIABLE_COST_RULES = Rule(
    Base={
        "type": "var_cost",
        "unit": "-",  # Unit likely USD/km3
    },
    Diff=[
        {
            "condition": "default",
            "technology": "basin_to_reg",
            "value": WS_CONST["BASIN_REG_VAR_COST"],
            "mode": "df_node['mode']",
            "node_loc": "df_node['region']",
            "pipe": {
                "flag_broadcast": True,
                "flag_time": True,
            },
        },
        {
            "condition": "SKIP",
            "technology": "extract_surfacewater",
            "value": WS_CONST["SW_VAR_COST"],
            "unit": "USD/km3",
            "mode": "M1",
            "time": "year",
        },
        {
            "condition": "SKIP",
            "technology": "extract_groundwater",
            "value": WS_CONST["GW_VAR_COST"],
            "unit": "USD/km3",
            "mode": "M1",
            "time": "year",
        },
    ],
)


"""
Rule defining the fixed O&M costs for fossil groundwater extraction.
Used in `_process_fix_cost_rules` within `add_water_supply`.
"""
FIXED_COST_RULES = Rule(
    Base={
        "type": "fix_cost",
        "unit": "USD/km3",
    },
    Diff=[
        {
            "condition": None,
            "technology": "extract_gw_fossil",
            "value": WS_CONST["FOSSIL_GW_FIX_COST"],
            "unit": "USD/km3",
            "pipe": {
                "flag_broadcast": True,
                "flag_node_loc": True,
            },
        }
    ],
)

"""
Rule defining environmental flow requirements as a
demand on surfacewater basin resources.
Used in `add_e_flow`.
"""
E_FLOW_RULES_DMD = Rule(
    Base={
        "type": "demand",
        "unit": "km3/year",
    },
    Diff=[
        {
            "condition": "default",
            "node": "df_sw[Region]",
            "commodity": "surfacewater_basin",
            "level": "water_avail_basin",
            "year": "df_sw[year]",
            "time": "df_sw[time]",
            "value": "df_sw[value]",
        }
    ],
)

"""
Rule defining environmental flow requirements as a lower bound on the activity
of the 'return_flow' technology.
Used in `add_e_flow`.
"""
E_FLOW_RULES_BOUND = Rule(
    Base={
        "type": "bound_activity_lo",
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
