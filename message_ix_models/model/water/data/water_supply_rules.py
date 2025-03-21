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
        "node_origin": "node",  # use df_node["node"]
        "node_loc": "node",     # use df_node["node"]
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
        "node_origin": "node",
        "node_loc": "node",
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
        "node_origin": "region",  # use df_node["region"]
        "node_loc": "node",
        "broadcast": {"yv_ya": "yv_ya_sw", "time": "sub_time"},
    },
    {
        "type": "input",
        "technology": "extract_groundwater",
        "value": "df_gwt['GW_per_km3_per_year'] + 0.043464579",
        "unit": "-",
        "level": "final",
        "commodity": "electr",
        "mode": "M1",
        "time_origin": "year",
        "node_origin": "region",
        "node_loc": "node",
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
        "node_origin": "region",
        "node_loc": "node",
        "broadcast": {"yv_ya": "yv_ya_gw", "time": "sub_time"},
        # adjustment for global regions can be applied after this dsl processing
    },
]