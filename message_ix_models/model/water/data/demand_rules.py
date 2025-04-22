from message_ix_models.model.water.utils import Rule

WD_CONST = {
    "IDENTITY": 1,
    "NEGATIVE_MULTIPLIER": -1,
    "HIST_CAP_DIVISOR": 5,
    "SHARE_GW_MULT": 0.95,
    "UNIT_CONVERSION": 1e-3,
}


URBAN_DEMAND = Rule(
    Base={
        "type": "demand",
        "unit": "km3/year",
        "level": "final",
    },
    Diff=[
        {
            "condition": "default",
            "node": "'B' + urban_mw[node]",
            "commodity": "urban_mw",
            "year": "urban_mw[year]",
            "time": "urban_mw[time]",
            "value": (f"urban_mw[value] * {WD_CONST['UNIT_CONVERSION']}")
            ,
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
            "value": (f"manuf_uncollected_wst[value] * {WD_CONST['UNIT_CONVERSION']}"
            f" * {WD_CONST['NEGATIVE_MULTIPLIER']}"),
        },
    ],
)




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
            "value":
            (f"urban_collected_wst[value] * "
            f"{WD_CONST['UNIT_CONVERSION']} * "
            f"{WD_CONST['NEGATIVE_MULTIPLIER']}"),
        },
        {
            "condition": "default",
            "node": "'B' + urban_uncollected_wst[node]",
            "commodity": "urban_uncollected_wst",
            "year": "urban_uncollected_wst[year]",
            "time": "urban_uncollected_wst[time]",
            "value": (f"urban_uncollected_wst[value] * "
            f"{WD_CONST['UNIT_CONVERSION']} * "
            f"{WD_CONST['NEGATIVE_MULTIPLIER']}"),
        }
    ],
)

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
            "value":
            (f"urban_collected_wst[value] * "
            f"{WD_CONST['UNIT_CONVERSION']} * "
            f"{WD_CONST['NEGATIVE_MULTIPLIER']}"),
        }
    ],
)


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
            "value": (f"urban_uncollected_wst[value] * "
            f"{WD_CONST['UNIT_CONVERSION']} * "
            f"{WD_CONST['NEGATIVE_MULTIPLIER']}"),
        }
    ],
)

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
            "value": (f"rural_collected_wst[value] * "
            f"{WD_CONST['UNIT_CONVERSION']} * "
            f"{WD_CONST['NEGATIVE_MULTIPLIER']}"),
        },
        {
            "condition": "default",
            "node": "'B' + rural_uncollected_wst[node]",
            "commodity": "rural_uncollected_wst",
            "level": "final",
            "year": "rural_uncollected_wst[year]",
            "time": "rural_uncollected_wst[time]",
            "value": (f"rural_uncollected_wst[value] * "
            f"{WD_CONST['UNIT_CONVERSION']} * "
            f"{WD_CONST['NEGATIVE_MULTIPLIER']}"),
        }

    ],
)


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
            "value":
            (f"rural_collected_wst[value] * "
            f"{WD_CONST['UNIT_CONVERSION']} * "
            f"{WD_CONST['NEGATIVE_MULTIPLIER']}"),
        }
    ],
)
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
            "value": (f"rural_uncollected_wst[value] * "
            f"{WD_CONST['UNIT_CONVERSION']} * "
            f"{WD_CONST['NEGATIVE_MULTIPLIER']}"),
        }
    ],
)

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
            "value": f"h_cap[value] / {WD_CONST['HIST_CAP_DIVISOR']}",
        }
    ],
)


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
