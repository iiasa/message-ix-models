from message_ix_models.model.water.rules import Constants, Rule

"""
Generic constants, no citations.
"""
WD_CONST_BASE_DATA = [
    ("IDENTITY", 1, "-"),
    ("NEGATIVE_MULTIPLIER", -1, "-"),
    ("HIST_CAP_DIVISOR", 5, "-"),
    ("SHARE_GW_MULT", 0.95, "-"),
]
WD_CONST = Constants(WD_CONST_BASE_DATA)

"""
Rules for urban water demand (municipal connected and disconnected).
Used in `add_sectoral_demands`.
"""
URBAN_DEMAND = Rule(
    Base={
        "type": "demand",
        "unit": "MCM/year",  # Target unit for the model
        "unit_in": "MCM/year",  # Unit of the input data (urban_mw, urban_dis)
        "level": "final",
    },
    Diff=[
        {
            "condition": "default",
            "node": "'B' + urban_mw[node]",
            "commodity": "urban_mw",
            "year": "urban_mw[year]",
            "time": "urban_mw[time]",
            "value": "urban_mw[value]",
        },
        {
            "condition": "default",
            "node": "'B' + urban_dis[node]",
            "commodity": "urban_disconnected",
            "year": "urban_dis[year]",
            "time": "urban_dis[time]",
            "value": "urban_dis[value]",
        },
    ],
)

"""
Rules for rural water demand (municipal connected and disconnected).
Used in `add_sectoral_demands`.
"""
RURAL_DEMAND = Rule(
    Base={
        "type": "demand",
        "unit": "MCM/year",
        "unit_in": "MCM/year",
        "level": "final",
    },
    Diff=[
        {
            "condition": "default",
            "node": "'B' + rural_mw[node]",
            "commodity": "rural_mw",
            "year": "rural_mw[year]",
            "time": "rural_mw[time]",
            "value": "rural_mw[value]",
        },
        {
            "condition": "default",
            "node": "'B' + rural_dis[node]",
            "commodity": "rural_disconnected",
            "year": "rural_dis[year]",
            "time": "rural_dis[time]",
            "value": "rural_dis[value]",
        },
    ],
)

"""
Rules for industrial water demand and
uncollected wastewater (treated as negative demand).
Used in `add_sectoral_demands`.
"""
INDUSTRIAL_DEMAND = Rule(
    Base={
        "type": "demand",
        "unit": "MCM/year",
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
            "value": "manuf_uncollected_wst[value] * {NEGATIVE_MULTIPLIER}",
        },
    ],
    constants_manager=WD_CONST,
)


"""
Rules for urban wastewater (collected and uncollected), represented as negative demands.
Used in `add_sectoral_demands`.
"""
URBAN_WST = Rule(
    Base={
        "type": "demand",
        "unit": "MCM/year",
        "unit_in": "MCM/year",
        "level": "final",
    },
    Diff=[
        {
            "condition": "default",
            "node": "'B' + urban_collected_wst[node]",
            "commodity": "urban_collected_wst",
            "year": "urban_collected_wst[year]",
            "time": "urban_collected_wst[time]",
            "value": ("urban_collected_wst[value] * {NEGATIVE_MULTIPLIER}"),
        },
        {
            "condition": "default",
            "node": "'B' + urban_uncollected_wst[node]",
            "commodity": "urban_uncollected_wst",
            "year": "urban_uncollected_wst[year]",
            "time": "urban_uncollected_wst[time]",
            "value": "urban_uncollected_wst[value] * {NEGATIVE_MULTIPLIER}",
        },
    ],
    constants_manager=WD_CONST,
)


"""
Rules for rural wastewater (collected and uncollected), represented as negative demands.
Used in `add_sectoral_demands`.
"""
RURAL_WST = Rule(
    Base={
        "type": "demand",
        "unit": "MCM/year",
        "unit_in": "MCM/year",
    },
    Diff=[
        {
            "condition": "default",
            "node": "'B' + rural_collected_wst[node]",
            "commodity": "rural_collected_wst",
            "level": "final",
            "year": "rural_collected_wst[year]",
            "time": "rural_collected_wst[time]",
            "value": "rural_collected_wst[value] * {NEGATIVE_MULTIPLIER}",
        },
        {
            "condition": "default",
            "node": "'B' + rural_uncollected_wst[node]",
            "commodity": "rural_uncollected_wst",
            "level": "final",
            "year": "rural_uncollected_wst[year]",
            "time": "rural_uncollected_wst[time]",
            "value": "rural_uncollected_wst[value] * {NEGATIVE_MULTIPLIER}",
        },
    ],
    constants_manager=WD_CONST,
)

"""
Rule for historical activity.
Used in `add_sectoral_demands`.
"""
HISTORICAL_ACTIVITY = Rule(
    Base={
        "type": "historical_activity",
        "unit": "MCM/year",
        "unit_in": "km3/year",
    },
    Diff=[
        {
            "condition": "default",
            "node_loc": "h_act[node]",
            "technology": "h_act[commodity]",
            "year_act": "h_act[year]",
            "mode": "M1",
            "time": "h_act[time]",
            "value": "h_act[value]",
        }
    ],
)

"""
Rule for historical new capacity, derived from historical activity.
Note the division by HIST_CAP_DIVISOR to annualize 5-year data.
Used in `add_sectoral_demands`.
"""
HISTORICAL_CAPACITY = Rule(
    Base={
        "type": "historical_new_capacity",
        "unit": "MCM/year",
        "unit_in": "km3/year",
    },
    Diff=[
        {
            "condition": "default",
            "node_loc": "h_cap[node]",
            "technology": "h_cap[commodity]",
            "year_vtg": "h_cap[year]",
            "value": "h_cap[value] / {HIST_CAP_DIVISOR}",
        }
    ],
    constants_manager=WD_CONST,
)


"""
Rule for setting lower bound share constraints on water recycling.
Used in `add_sectoral_demands`.
"""
SHARE_CONSTRAINTS_RECYCLING = Rule(
    Base={
        "type": "share_commodity_lo",
        "unit": "-",  # Dimensionless
        "unit_in": "-",  # Dimensionless
        "pipe": {
            "flag_broadcast": True,
            "flag_time": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "shares": "share_wat_recycle",
            "node_share": "'B' + df_recycling[node]",
            "year_act": "df_recycling[year]",
            "value": "df_recycling[value]",
        }
    ],
)

"""
Rules for water availability (surface and groundwater), represented as negative demands.
Used in `add_water_availability`.
"""
WATER_AVAILABILITY = Rule(
    Base={
        "type": "demand",
        "unit": "MCM/year",  # Target unit
        "unit_in": "km3/year",  # Original unit of df_sw, df_gw
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


"""
Rule for setting lower bound share constraints on groundwater withdrawal.
Includes a safety multiplier (SHARE_GW_MULT) to avoid infeasibility.
Used in `add_water_availability`.
"""
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
            # Value expression remains the same as it calculates a ratio
            "value": ("df_gw[value] / (df_sw[value] + df_gw[value]) * {SHARE_GW_MULT}"),
        }
    ],
    constants_manager=WD_CONST,
)
