
# ----------------- DSL rule templates -----------------
# these templates are used to declare the supply transformation logic.
# dynamic values (e.g. years, node locations) are filled in within add_water_supply.

# cooling branch (outputs only)
COOLING_SUPPLY_RULES = [
    {
        "type": "output",
        "technology": "extract_surfacewater",
        "value": 1,
        "unit": "km3",
        "level": "water_supply",
        "commodity": "freshwater",
        "mode": "M1",
        "time": "year",
        "time_dest": "year",
        "time_origin": "year",
        "broadcast": {"node_loc": None},  # to be set to node_region at runtime
    },
    {
        "type": "output",
        "technology": "extract_groundwater",
        "value": 1,
        "unit": "km3",
        "level": "water_supply",
        "commodity": "freshwater",
        "mode": "M1",
        "time": "year",
        "time_dest": "year",
        "time_origin": "year",
        "broadcast": {"node_loc": None},
    },
    {
        "type": "output",
        "technology": "extract_salinewater",
        "value": 1,
        "unit": "km3",
        "year_vtg": None,  # filled in dynamically
        "year_act": None,
        "level": "saline_supply",
        "commodity": "saline_ppl",
        "mode": "M1",
        "time": "year",
        "time_dest": "year",
        "time_origin": "year",
        "broadcast": {"node_loc": None},
    },
]

# nexus branch rules for constructing the input dataframe
NEXUS_INPUT_RULES = [
    {
        "type": "input",
        "technology": "return_flow",
        "value": 1,
        "unit": "-",
        "level": "water_avail_basin",
        "commodity": "surfacewater_basin",
        "mode": "M1",
        "broadcast": {"node_loc": None, "time": None},
    },
    {
        "type": "input",
        "technology": "gw_recharge",
        "value": 1,
        "unit": "-",
        "level": "water_avail_basin",
        "commodity": "groundwater_basin",
        "mode": "M1",
        "broadcast": {"node_loc": None, "time": None},
    },
    {
        "type": "input",
        "technology": "basin_to_reg",
        "value": 1,
        "unit": "-",
        "level": "water_supply_basin",
        "commodity": "freshwater_basin",
        "broadcast": {"node_origin": None, "node_loc": None, "time": None},
    },
    {
        "type": "input",
        "technology": "extract_surfacewater",
        "value": 1,
        "unit": "-",
        "level": "water_avail_basin",
        "commodity": "surfacewater_basin",
        "mode": "M1",
        "broadcast": {"node_origin": None, "node_loc": None, "year": None, "time": None},
    },
    {
        "type": "input",
        "technology": "extract_groundwater",
        "value": 1,
        "unit": "-",
        "level": "water_avail_basin",
        "commodity": "groundwater_basin",
        "mode": "M1",
        "broadcast": {"node_origin": None, "node_loc": None, "year": None, "time": None},
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
        "broadcast": {"node_origin": None, "node_loc": None, "year": None, "time": None},
    },
    {
        "type": "input",
        "technology": "extract_groundwater",
        "value": None,  # to be set to df_gwt["GW_per_km3_per_year"] + 0.043464579 at runtime
        "unit": "-",
        "level": "final",
        "commodity": "electr",
        "mode": "M1",
        "time_origin": "year",
        "broadcast": {"node_origin": None, "node_loc": None, "year": None, "time": None},
    },
    {
        "type": "input",
        "technology": "extract_gw_fossil",
        "value": None,  # to be set to (df_gwt["GW_per_km3_per_year"] + 0.043464579)*2
        "unit": "-",
        "level": "final",
        "commodity": "electr",
        "mode": "M1",
        "time_origin": "year",
        "broadcast": {"node_origin": None, "node_loc": None, "year": None, "time": None},
    },
]

# nexus branch rules for output dataframe
NEXUS_OUTPUT_RULES = [
    {
        "type": "output",
        "technology": "extract_surfacewater",
        "value": 1,
        "unit": "-",
        "level": "water_supply_basin",
        "commodity": "freshwater_basin",
        "mode": "M1",
        "broadcast": {"node_loc": None, "node_dest": None, "year": None, "time": None},
    },
    {
        "type": "output",
        "technology": "extract_groundwater",
        "value": 1,
        "unit": "-",
        "level": "water_supply_basin",
        "commodity": "freshwater_basin",
        "mode": "M1",
        "broadcast": {"node_loc": None, "node_dest": None, "year": None, "time": None},
    },
    {
        "type": "output",
        "technology": "extract_gw_fossil",
        "value": 1,
        "unit": "-",
        "level": "water_supply_basin",
        "commodity": "freshwater_basin",
        "mode": "M1",
        "time_origin": "year",
        "broadcast": {"node_loc": None, "node_dest": None, "year": None, "time": None},
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
        "broadcast": {"node_loc": None},
    },
    {
        "type": "output",
        "technology": "basin_to_reg",
        "value": 1,
        "unit": "-",
        "level": "water_supply",
        "commodity": "freshwater",
        "broadcast": {"node_loc": None, "node_dest": None, "year": None, "time": None},
    },
]

# nexus branch: historical new capacity rules
NEXUS_HIST_RULES = [
    {
        "type": "historical_new_capacity",
        "technology": "extract_surfacewater",
        "broadcast": {"node_loc": None},
        "value": None,  # to be set as df_hist["hist_cap_sw_km3_year"] / 5
        "unit": "km3/year",
        "year_vtg": 2015,
    },
    {
        "type": "historical_new_capacity",
        "technology": "extract_groundwater",
        "broadcast": {"node_loc": None},
        "value": None,  # to be set as df_hist["hist_cap_gw_km3_year"] / 5
        "unit": "km3/year",
        "year_vtg": 2015,
    },
]

# nexus branch: var cost
NEXUS_VAR_COST_RULES = [
    {
        "type": "var_cost",
        "technology": "basin_to_reg",
        "broadcast": {"node_loc": None, "time": None},
        "value": 20,
        "unit": "-",
        "year_vtg": None,
    },
]

# nexus branch: share constraint
NEXUS_SHARE_RULES = [
    {
        "type": "share_mode_up",
        "shares": "share_basin",
        "technology": "basin_to_reg",
        "broadcast": {"node_share": None, "time": None},
        "value": None,
        "unit": "%",
        "year_act": None,
    },
]

# nexus branch: technical lifetime rules
NEXUS_LIFETIME_RULES = [
    {
        "type": "technical_lifetime",
        "technology": "extract_surfacewater",
        "broadcast": {"year_vtg": None, "node_loc": None},
        "value": 50,
        "unit": "y",
    },
    {
        "type": "technical_lifetime",
        "technology": "extract_groundwater",
        "broadcast": {"year_vtg": None, "node_loc": None},
        "value": 20,
        "unit": "y",
    },
    {
        "type": "technical_lifetime",
        "technology": "extract_gw_fossil",
        "broadcast": {"year_vtg": None, "node_loc": None},
        "value": 20,
        "unit": "y",
    },
]

# nexus branch: investment cost rules
NEXUS_INV_COST_RULES = [
    {
        "type": "inv_cost",
        "technology": "extract_surfacewater",
        "broadcast": {"node_loc": None, "year_vtg": None},
        "value": 155.57,
        "unit": "USD/km3",
    },
    {
        "type": "inv_cost",
        "technology": "extract_groundwater",
        "broadcast": {"node_loc": None, "year_vtg": None},
        "value": 54.52,
        "unit": "USD/km3",
    },
    {
        "type": "inv_cost",
        "technology": "extract_gw_fossil",
        "broadcast": {"node_loc": None, "year_vtg": None},
        "value": 54.52 * 150,
        "unit": "USD/km3",
    },
]

# nexus branch: fixed cost for fossil groundwater extraction
NEXUS_FIX_COST_RULES = [
    {
        "type": "fix_cost",
        "technology": "extract_gw_fossil",
        "broadcast": {"node_loc": None, "year": None},
        "value": 300,
        "unit": "USD/km3",
    },
]

# ----------------- E-flow rule templates -----------------
E_FLOW_RULES = [
    {
        "type": "bound_activity_lo",
        "technology": "return_flow",
        "mode": "M1",
        "value": None,  # set dynamically based on environmental flow calculations
        "unit": "km3/year",
    }
]