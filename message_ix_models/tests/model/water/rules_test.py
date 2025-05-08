import pytest

from message_ix_models.model.water.rules import (
    Constants,
    Rule,
    get_conversion_factor,
)

# Constants data for Constants manager
WD_CONST_DATA = [
    ("IDENTITY", 1, "-"),
    ("NEGATIVE_MULTIPLIER", -1, "-"),
    ("HIST_CAP_DIVISOR", 5, "-"),
    ("SHARE_GW_MULT", 0.95, "-"),
    ("UNIT_CONVERSION", 1e3, "-"),
]

WD_CONST = Constants(WD_CONST_DATA)

WS_CONST_DATA = [
    ("IDENTITY", 1, "-"),
    ("SW_ELEC_IN", 0.018835616, "-"),
    ("GW_ADD_ELEC_IN", 0.043464579, "-"),
    ("FOSSIL_GW_ELEC_MULT", 2, "-"),
    ("SW_LIFETIME", 50, "y"),
    ("GW_LIFETIME", 20, "y"),
    ("SW_INV_COST", 155.57, "USD/km3"),
    ("GW_INV_COST", 54.52, "USD/km3"),
    ("FOSSIL_GW_INV_MULT", 150, "-"),
    ("HIST_CAP_DIV", 5, "-"),
    ("BASIN_REG_VAR_COST", 20, "USD/km3"),
    ("SW_VAR_COST", 0.0001, "USD/km3"),
    ("GW_VAR_COST", 0.001, "USD/km3"),
    ("FOSSIL_GW_FIX_COST", 300, "USD/km3"),
]
WS_CONST = Constants(WS_CONST_DATA)

WF_CONST_DATA = [
    ("IDENTITY", 1, "-"),
    ("DESALINATION_OUTPUT_VALUE", 1, "-"),
    ("DESALINATION_TECH_LIFETIME", 20, "y"),
    ("DESALINATION_VAR_COST", 100, "USD/km3"),
    ("HIST_CAP_DIVISOR", 5, "-"),
]
WF_CONST = Constants(WF_CONST_DATA)

CT_CONST_DATA = [
    ("cool_tech_lifetime", 30, "y"),
    ("cool_tech_output_default", 1, "-"),
]
CT_CONST = Constants(CT_CONST_DATA)


# --- Selected Rules from demand_rules.py ---

INDUSTRIAL_DEMAND = Rule(
    Base={
        "type": "demand",
        "unit": "km3/year",
        "unit_in": "MCM/year",
        "level": "final",
    },
    Diff=[
        {
            "condition": "default",
            "node": "'B' + manuf_mw[node]",
            "commodity": "industry_mw",
            "year": "manuf_mw[year]",
            "time": "manuf_mw[time]",
            "value": "manuf_mw[value]",
        },
        {
            "condition": "default",
            "node": "'B' + manuf_uncollected_wst[node]",
            "commodity": "industry_uncollected_wst",
            "year": "manuf_uncollected_wst[year]",
            "time": "manuf_uncollected_wst[time]",
            "value": "manuf_uncollected_wst[value]* {NEGATIVE_MULTIPLIER}",
        },
    ],
    constants_manager=WD_CONST,
)

WATER_AVAILABILITY = Rule(
    Base={
        "type": "demand",
        "unit": "km3/year",
        "unit_in": "km3/year",
    },
    Diff=[
        {
            "condition": "sw",
            "node": "'B' + df_sw[Region].astype(str)",
            "commodity": "surfacewater_basin",
            "level": "water_avail_basin",
            "year": "df_sw[year]",
            "time": "df_sw[time]",
            "value": "df_sw[value] * {NEGATIVE_MULTIPLIER}",
        },
        {
            "condition": "gw",
            "node": "'B' + df_gw[Region].astype(str)",
            "commodity": "groundwater_basin",
            "level": "water_avail_basin",
            "year": "df_gw[year]",
            "time": "df_gw[time]",
            "value": "df_gw[value] * {NEGATIVE_MULTIPLIER}",
        },
    ],
    constants_manager=WD_CONST,
)

SHARE_CONSTRAINTS_GW = Rule(
    Base={
        "type": "share_commodity_lo",
        "unit": "-",
        "unit_in": "-",
    },
    Diff=[
        {
            "condition": "default",
            "shares": "share_low_lim_GWat",
            "node_share": "'B' + df_gw[Region].astype(str)",
            "year_act": "df_gw[year]",
            "time": "df_gw[time]",
            "value": "df_gw[value] / (df_sw[value] + df_gw[value]) * {SHARE_GW_MULT}",
        }
    ],
    constants_manager=WD_CONST,
)

# --- Selected Rules from water_supply_rules.py ---

SLACK_TECHNOLOGY_RULES = Rule(
    Base={
        "type": "input",
        "condition": "Nexus",
        "unit": "-",
        "unit_in": "-",
        "pipe": {
            "flag_broadcast": True,
            "flag_time": True,
        },
    },
    Diff=[
        {
            "type": "input",  # type can be in Diff
            "condition": "Nexus",
            "technology": "return_flow",
            "value": "{IDENTITY}",
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
            "value": "{IDENTITY}",
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
            "value": "{IDENTITY}",
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
            "value": "{IDENTITY}",
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
    constants_manager=WS_CONST,
)

EXTRACTION_INPUT_RULES = Rule(
    Base={
        "type": "input",
        "unit": "-",  # Base unit is dimensionless. Values are directly used.
        "unit_in": "-",
        "mode": "M1",
    },
    Diff=[
        {
            "condition": "default",
            "technology": "extract_surfacewater",
            "level": "water_avail_basin",
            "value": "{IDENTITY}",
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
            "value": "{IDENTITY}",
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
            "value": "{SW_ELEC_IN}",  # This constant is 0.0188... (unit "-")
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
            "value": "df_gwt['GW_per_km3_per_year'] + {GW_ADD_ELEC_IN}",
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
            "value": "{FOSSIL_GW_ELEC_MULT} *"
            "(df_gwt['GW_per_km3_per_year'] + {GW_ADD_ELEC_IN})",
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
    constants_manager=WS_CONST,
)


SHARE_MODE_RULES = Rule(
    Base={
        "type": "share_mode_up",
        "unit": "%",  # Base unit is percent
    },
    Diff=[
        {
            "condition": "default",
            "technology": "basin_to_reg",
            "shares": "share_basin",
            "mode": "df_sw['mode']",
            "node_share": "df_sw['MSGREG']",
            "time": "df_sw['time']",
            "value": "df_sw['share']",  # This comes directly from a dataframe
            "year_act": "df_sw['year']",
        }
    ],
    # No constants_manager needed if no {placeholders} are used in values
)

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
            "value": "df_env[value]",  # This comes directly from a dataframe
        }
    ],
    # No constants_manager needed
)


VAR_COST_RULES = Rule(
    Base={
        "type": "var_cost",
        "unit": "USD/km3",  # Target unit for costs
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
    constants_manager=WS_CONST,
)

DESALINATION_INPUT_RULES2 = Rule(
    Base={
        "type": "input",
        "unit": "-",  # Base unit is dimensionless
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
            "value": "{IDENTITY}",
            "level": "rows[inlvl]",
            "commodity": "rows[incmd]",
            "pipe": {
                "flag_same_node": True,
                "flag_same_time": True,
                "flag_node_loc": True,
            },
        },
    ],
    constants_manager=WF_CONST,
)


DESALINATION_OUTPUT_RULES = Rule(
    Base={
        "type": "output",
        "unit": "km3/year",  # Target unit
        "unit_in": "km3/year",
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
            "value": "{IDENTITY}",
        },
    ],
    constants_manager=WF_CONST,
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
            "commodity": "input_cool['technology_name'].str.split('__').str.get(1)",
            "level": "share",
            "time": "year",
            "time_dest": "year",
            "value": "{cool_tech_output_default}",  # from CT_CONST
            "unit": "-",  # Explicit unit for this diff
        },
        {
            "condition": "nexus",
            "node_loc": "icfb_df[node_loc]",
            "technology": "icfb_df[technology_name]",
            "year_vtg": "icfb_df[year_vtg]",
            "year_act": "icfb_df[year_act]",
            "mode": "icfb_df[mode]",
            "commodity": "surfacewater_basin",
            "level": "water_avail_basin",
            "time": "year",
            "value": "icfb_df[value_return]",  # Direct value
            "unit": "MCM/GWa",  # Explicit unit for this diff
            "pipe": {
                "flag_broadcast": True,
            },
        },
    ],
    constants_manager=CT_CONST,
)

# --- Unit Conversion Tests ---


def test_get_conversion_factor_identity():
    """Test conversion to the same unit."""
    assert get_conversion_factor("m3", "m3") == 1.0
    assert get_conversion_factor("km3/year", "km3/year") == 1.0
    assert get_conversion_factor("-", "-") == 1.0


@pytest.mark.parametrize(
    "unit_a, unit_b, value_a",
    [
        ("m3", "km3", 1e9),
        ("km3", "m3", 1),
        ("m3/year", "MCM/year", 1e6),
        ("MCM/year", "m3/year", 1),
        ("USD/m3", "USD/MCM", 1),
        ("USD/MCM", "USD/m3", 1e-6),
    ],
)
def test_get_conversion_factor_roundtrip(unit_a, unit_b, value_a):
    """Test A -> B -> A conversion recovers the original value."""
    factor_ab = get_conversion_factor(unit_a, unit_b)
    value_b = value_a * factor_ab
    factor_ba = get_conversion_factor(unit_b, unit_a)
    value_a_reconverted = value_b * factor_ba
    assert abs(value_a_reconverted - value_a) < 1e-9, (
        f"Roundtrip failed for {value_a} {unit_a} -> {unit_b} -> {unit_a}. "
        f"Intermediate: {value_b} {unit_b}, Final: {value_a_reconverted} {unit_a}"
    )


@pytest.mark.parametrize(
    "unit_a, unit_b, value_a, expected_value_b",
    [
        ("m3", "km3", 1e9, 1.0),
        ("m3", "MCM", 2e6, 2.0),
        ("km3", "m3", 0.5, 0.5e9),
        ("m3/year", "MCM/year", 5e6, 5.0),
        ("MCM/year", "km3/year", 2000.0, 2.0),
        ("km3/year", "m3/year", 0.001, 1e6),
        ("USD/m3", "USD/MCM", 100.0, 100.0 * 1e6),
        ("USD/MCM", "USD/km3", 50.0, 50000.0),
        ("GWh/km3", "GWh/MCM", 1.0, 0.001),
    ],
)
def test_get_conversion_factor_manual_check(unit_a, unit_b, value_a, expected_value_b):
    """Test A -> B conversion against manually calculated expected values."""
    converted_value = value_a * get_conversion_factor(unit_a, unit_b)
    assert abs(converted_value - expected_value_b) < 1e-9, (
        f"Manual check failed for {value_a} {unit_a} -> {unit_b}. "
        f"Got: {converted_value}, Expected: {expected_value_b}"
    )


def test_get_conversion_factor_errors():
    """Test error handling for invalid units or incompatible dimensions."""
    # Test unsupported unit (not in UNIT_FACTORS at all)
    with pytest.raises(ValueError, match="Unsupported unit: 'furlongs/fortnight'"):
        get_conversion_factor("m3", "furlongs/fortnight")

    # Test dimensional mismatch
    with pytest.raises(
        ValueError, match="Dimension mismatch: cannot convert from m3 .* to USD/MCM .*"
    ):
        get_conversion_factor("m3", "USD/MCM")
    with pytest.raises(
        ValueError, match="Dimension mismatch: cannot convert from km3/year .* to y .*"
    ):
        get_conversion_factor("km3/year", "y")  # Volume/Time to Time
    with pytest.raises(
        ValueError, match="Dimension mismatch: cannot convert from GWa .* to m3 .*"
    ):
        get_conversion_factor("GWa", "m3")  # Energy to Volume

