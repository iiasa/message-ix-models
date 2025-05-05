from message_ix_models.model.water.utils import Rule

"""
Water Demand Constants
"""
WD_CONST = {
    "IDENTITY": 1,
    "NEGATIVE_MULTIPLIER": -1,
    "HIST_CAP_DIVISOR": 5,
    "SHARE_GW_MULT": 0.95,
    "UNIT_CONVERSION": 1e-3,
}


"""
Rules for urban water demand (municipal connected and disconnected).
Used in `add_sectoral_demands`.
"""
URBAN_DEMAND = Rule(
    Base={
        "type": "demand",
        "unit": "km3/year",
        "unit_in": "MCM/year",
        "level": "final",
    },
    Diff=[
        {
            "condition": "default",
            "node": "'B' + urban_mw[node]",
            "commodity": "urban_mw",
            "year": "urban_mw[year]",
            "time": "urban_mw[time]",
            "value": (f"urban_mw[value] * {WD_CONST['UNIT_CONVERSION']}"),
        },
        {
            "condition": "default",
            "node": "'B' + urban_dis[node]",
            "commodity": "urban_disconnected",
            "year": "urban_dis[year]",
            "time": "urban_dis[time]",
            "value": (f"urban_dis[value] * {WD_CONST['UNIT_CONVERSION']}"),
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
        "unit": "km3/year",
        "level": "final",
    },
    Diff=[
        {
            "condition": "default",
            "node": "'B' + rural_mw[node]",
            "commodity": "rural_mw",
            "year": "rural_mw[year]",
            "time": "rural_mw[time]",
            "value": (f"rural_mw[value] * {WD_CONST['UNIT_CONVERSION']}"),
        },
        {
            "condition": "default",
            "node": "'B' + rural_dis[node]",
            "commodity": "rural_disconnected",
            "year": "rural_dis[year]",
            "time": "rural_dis[time]",
            "value": (f"rural_dis[value] * {WD_CONST['UNIT_CONVERSION']}"),
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
            "value": "manuf_mw[value] * {UNIT_CONVERSION}".format(**WD_CONST),
        },  # .format notation for clarity
        {
            "condition": "default",
            "node": "'B' + manuf_uncollected_wst[node]",
            "commodity": "industry_uncollected_wst",
            "year": "manuf_uncollected_wst[year]",
            "time": "manuf_uncollected_wst[time]",
            "value": (
                "manuf_uncollected_wst[value] * "
                "{UNIT_CONVERSION} * "
                "{NEGATIVE_MULTIPLIER}".format(**WD_CONST)
            ),
        },
    ],
)


"""
Rules for urban wastewater (collected and uncollected), represented as negative demands.
Used in `add_sectoral_demands`.
"""
URBAN_WST = Rule(
    Base={
        "type": "demand",
        "unit": "km3/year",
        "level": "final",
    },
    Diff=[
        {
            "condition": "default",
            "node": "'B' + urban_collected_wst[node]",
            "commodity": "urban_collected_wst",
            "year": "urban_collected_wst[year]",
            "time": "urban_collected_wst[time]",
            "value": (
                "urban_collected_wst[value] * "
                "{UNIT_CONVERSION} * "
                "{NEGATIVE_MULTIPLIER}".format(**WD_CONST)
            ),
        },
        {
            "condition": "default",
            "node": "'B' + urban_uncollected_wst[node]",
            "commodity": "urban_uncollected_wst",
            "year": "urban_uncollected_wst[year]",
            "time": "urban_uncollected_wst[time]",
            "value": (
                "urban_uncollected_wst[value] * "
                "{UNIT_CONVERSION} * "
                "{NEGATIVE_MULTIPLIER}".format(**WD_CONST)
            ),
        },
    ],
)

"""
Rule for urban collected wastewater, represented as a negative demand.
Used in `add_sectoral_demands`.
"""
URBAN_COLLECTED_WST = Rule(
    Base={
        "type": "demand",
        "unit": "km3/year",
        "level": "final",
    },
    Diff=[
        {
            "condition": "default",
            "node": "'B' + urban_collected_wst[node]",
            "commodity": "urban_collected_wst",
            "year": "urban_collected_wst[year]",
            "time": "urban_collected_wst[time]",
            "value": (
                "urban_collected_wst[value] * "
                "{UNIT_CONVERSION} * "
                "{NEGATIVE_MULTIPLIER}".format(**WD_CONST)
            ),
        }
    ],
)


"""
Rule for urban uncollected wastewater, represented as a negative demand.
Used in `add_sectoral_demands`.
"""
URBAN_UNCOLLECTED_WST = Rule(
    Base={
        "type": "demand",
        "unit": "km3/year",
        "level": "final",
    },
    Diff=[
        {
            "condition": "default",
            "node": "'B' + urban_uncollected_wst[node]",
            "commodity": "urban_uncollected_wst",
            "year": "urban_uncollected_wst[year]",
            "time": "urban_uncollected_wst[time]",
            "value": (
                "urban_uncollected_wst[value] * "
                "{UNIT_CONVERSION} * "
                "{NEGATIVE_MULTIPLIER}".format(**WD_CONST)
            ),
        }
    ],
)

"""
Rules for rural wastewater (collected and uncollected), represented as negative demands.
Used in `add_sectoral_demands`.
"""
RURAL_WST = Rule(
    Base={
        "type": "demand",
        "unit": "km3/year",
    },
    Diff=[
        {
            "condition": "default",
            "node": "'B' + rural_collected_wst[node]",
            "commodity": "rural_collected_wst",
            "level": "final",
            "year": "rural_collected_wst[year]",
            "time": "rural_collected_wst[time]",
            "value": (
                "rural_collected_wst[value] * "
                "{UNIT_CONVERSION} * "
                "{NEGATIVE_MULTIPLIER}".format(**WD_CONST)
            ),
        },
        {
            "condition": "default",
            "node": "'B' + rural_uncollected_wst[node]",
            "commodity": "rural_uncollected_wst",
            "level": "final",
            "year": "rural_uncollected_wst[year]",
            "time": "rural_uncollected_wst[time]",
            "value": (
                "rural_uncollected_wst[value] * "
                "{UNIT_CONVERSION} * "
                "{NEGATIVE_MULTIPLIER}".format(**WD_CONST)
            ),
        },
    ],
)


"""
Rule for rural collected wastewater, represented as a negative demand.
Used in `add_sectoral_demands`.
"""
RURAL_COLLECTED_WST = Rule(
    Base={
        "type": "demand",
        "unit": "km3/year",
        "level": "final",
    },
    Diff=[
        {
            "condition": "default",
            "node": "'B' + rural_collected_wst[node]",
            "commodity": "rural_collected_wst",
            "year": "rural_collected_wst[year]",
            "time": "rural_collected_wst[time]",
            "value": (
                "rural_collected_wst[value] * "
                "{UNIT_CONVERSION} * "
                "{NEGATIVE_MULTIPLIER}".format(**WD_CONST)
            ),
        }
    ],
)
"""
Rule for rural uncollected wastewater, represented as a negative demand.
Used in `add_sectoral_demands`.
"""
RURAL_UNCOLLECTED_WST = Rule(
    Base={
        "type": "demand",
        "unit": "km3/year",
    },
    Diff=[
        {
            "condition": "default",
            "node": "'B' + rural_uncollected_wst[node]",
            "commodity": "rural_uncollected_wst",
            "level": "final",
            "year": "rural_uncollected_wst[year]",
            "time": "rural_uncollected_wst[time]",
            "value": (
                "rural_uncollected_wst[value] * "
                "{UNIT_CONVERSION} * "
                "{NEGATIVE_MULTIPLIER}".format(**WD_CONST)
            ),
        }
    ],
)

"""
Rule for historical activity.
Used in `add_sectoral_demands`.
"""
HISTORICAL_ACTIVITY = Rule(
    Base={
        "type": "historical_activity",
        "unit": "km3/year",
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
        "unit": "km3/year",
    },
    Diff=[
        {
            "condition": "default",
            "node_loc": "h_cap[node]",
            "technology": "h_cap[commodity]",
            "year_vtg": "h_cap[year]",
            "value": "h_cap[value] / {HIST_CAP_DIVISOR}".format(**WD_CONST),
        }
    ],
)


"""
Rule for setting lower bound share constraints on water recycling.
Used in `add_sectoral_demands`.
"""
SHARE_CONSTRAINTS_RECYCLING = Rule(
    Base={
        "type": "share_commodity_lo",
        "unit": "-",
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
            "unit": "-",
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
            "value": "df_sw[value] * {NEGATIVE_MULTIPLIER}".format(**WD_CONST),
        },
        {
            "condition": "gw",
            "node": "'B' + df_gw[Region].astype(str)",
            "commodity": "groundwater_basin",
            "level": "water_avail_basin",
            "year": "df_gw[year]",
            "time": "df_gw[time]",
            "value": "df_gw[value] * {NEGATIVE_MULTIPLIER}".format(**WD_CONST),
        },
    ],
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
    },
    Diff=[
        {
            "condition": "default",
            "shares": "share_low_lim_GWat",
            "node_share": "'B' + df_gw[Region].astype(str)",
            "year_act": "df_gw[year]",
            "time": "df_gw[time]",
            "value": (
                "df_gw[value] / (df_sw[value] + df_gw[value]) * {SHARE_GW_MULT}".format(
                    **WD_CONST
                )
            ),
            "unit": "-",
        }
    ],
)
