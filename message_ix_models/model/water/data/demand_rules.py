from message_ix_models.model.water.data.infrastructure_utils import Rule

URBAN_DEMAND = Rule(
    Base = {
        "pipe" : {
            "flag_broadcast": False,
            "flag_map_yv_ya_lt": False,
            "flag_time": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_node_loc": False,
        }
    },
    Diff = [
        {   "condition": "default",
            "type": "demand",
            "node": "'B' + urban_mw[node]",
            "commodity": "urban_mw",
            "level": "final",
            "year": "urban_mw[year]",
            "time": "urban_mw[time]",
            "value": "urban_mw[value] * 1",
            "unit": "km3/year",
        },
        {   "condition": "default",
            "type": "demand",
            "node": "'B' + urban_dis[node]",
            "commodity": "urban_disconnected",
            "level": "final",
            "year": "urban_dis[year]",
            "time": "urban_dis[time]",
            "value": "urban_dis[value] * 1",
            "unit": "km3/year",
        }
    ]
)

RURAL_DEMAND = Rule(
    Base = {
        "type": "demand",
        "unit": "km3/year",
        "pipe" : {
            "flag_broadcast": False,
            "flag_map_yv_ya_lt": False,
            "flag_time": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_node_loc": False,
        }
    },
    Diff = [
        {   "condition": "default",
            "node": "'B' + rural_mw[node]",
            "commodity": "rural_mw",
            "level": "final",
            "year": "rural_mw[year]",
            "time": "rural_mw[time]",
            "value": "rural_mw[value] * 1",
        },
        {   "condition": "default",
            "node": "'B' + rural_dis[node]",
            "commodity": "rural_disconnected",
            "level": "final",
            "year": "rural_dis[year]",
            "time": "rural_dis[time]",
            "value": "rural_dis[value] * 1",
        }
    ]
)

INDUSTRIAL_DEMAND = Rule(
    Base = {
        "type": "demand",
        "unit": "km3/year",
        "pipe" : {
            "flag_broadcast": False,
            "flag_map_yv_ya_lt": False,
            "flag_time": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_node_loc": False,
        }
    },
    Diff = [
        {   "condition": "default",
            "node": "'B' + manuf_mw[node]",
            "commodity": "industry_mw",
            "level": "final",
            "year": "manuf_mw[year]",
            "time": "manuf_mw[time]",
            "value": "manuf_mw[value] * 1",
        },
        {   "condition": "default",
            "node": "'B' + manuf_uncollected_wst[node]",
            "commodity": "industry_uncollected_wst",
            "level": "final",
            "year": "manuf_uncollected_wst[year]",
            "time": "manuf_uncollected_wst[time]",
            "value": "manuf_uncollected_wst[value] * -1",
        }
    ]
)

URBAN_COLLECTED_WST = Rule(
    Base = {
        "type": "demand",
        "unit": "km3/year",
        "pipe" : {
            "flag_broadcast": False,
            "flag_map_yv_ya_lt": False,
            "flag_time": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_node_loc": False,
        }
    },
    Diff = [
        {   "condition": "default",
            "node": "'B' + urban_collected_wst[node]",
            "commodity": "urban_collected_wst",
            "level": "final",
            "year": "urban_collected_wst[year]",
            "time": "urban_collected_wst[time]",
            "value": "urban_collected_wst[value] * -1",
        }
    ]
)

RURAL_COLLECTED_WST = Rule(
    Base = {
        "type": "demand",
        "unit": "km3/year",
        "pipe" : {
            "flag_broadcast": False,
            "flag_map_yv_ya_lt": False,
            "flag_time": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_node_loc": False,
        }
    },
    Diff = [
        {   "condition": "default",
            "node": "'B' + rural_collected_wst[node]",
            "commodity": "rural_collected_wst",
            "level": "final",
            "year": "rural_collected_wst[year]",
            "time": "rural_collected_wst[time]",
            "value": "rural_collected_wst[value] * -1",
        }
    ]
)

URBAN_UNCOLLECTED_WST = Rule(
    Base = {
        "type": "demand",
        "unit": "km3/year",
        "pipe" : {
            "flag_broadcast": False,
            "flag_map_yv_ya_lt": False,
            "flag_time": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_node_loc": False,
        }
    },
    Diff = [
        {   "condition": "default",
            "node": "'B' + urban_uncollected_wst[node]",
            "commodity": "urban_uncollected_wst",
            "level": "final",
            "year": "urban_uncollected_wst[year]",
            "time": "urban_uncollected_wst[time]",
            "value": "urban_uncollected_wst[value] * -1",
        }
    ]
)

RURAL_UNCOLLECTED_WST = Rule(
    Base = {
        "type": "demand",
        "unit": "km3/year",
        "pipe" : {
            "flag_broadcast": False,
            "flag_map_yv_ya_lt": False,
            "flag_time": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_node_loc": False,
        }
    },
    Diff = [
        {   "condition": "default",
            "node": "'B' + rural_uncollected_wst[node]",
            "commodity": "rural_uncollected_wst",
            "level": "final",
            "year": "rural_uncollected_wst[year]",
            "time": "rural_uncollected_wst[time]",
            "value": "rural_uncollected_wst[value] * -1",
        }
    ]
)

HISTORICAL_ACTIVITY = Rule(
    Base = {
        "type": "historical_activity",
        "unit": "km3/year",
        "pipe" : {
            "flag_broadcast": False,
            "flag_map_yv_ya_lt": False,
            "flag_time": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_node_loc": False,
        }
    },
    Diff = [
        {   "condition": "default",
            "node_loc": "h_act[node]",
            "technology": "h_act[commodity]",
            "year_act": "h_act[year]",
            "mode": "M1",
            "time": "h_act[time]",
            "value": "h_act[value]",
        }
    ]
)

HISTORICAL_CAPACITY = Rule(
    Base = {
        "type": "historical_new_capacity",
        "unit": "km3/year",
        "pipe" : {
            "flag_broadcast": False,
            "flag_map_yv_ya_lt": False,
            "flag_time": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_node_loc": False,
        }
    },
    Diff = [
        {   "condition": "default",
            "node_loc": "h_cap[node]",
            "technology": "h_cap[commodity]",
            "year_vtg": "h_cap[year]",
            "value": "h_cap[value] / 5",
        }
    ]
)


SHARE_CONSTRAINTS_RECYCLING = Rule(
    Base = {
        "type": "share_commodity_lo",
        "unit": "-",
        "pipe" : {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": False,
            "flag_time": True,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_node_loc": False,
        }
    },
    Diff = [
        {   "condition": "default",
            "shares": "share_wat_recycle",
            "node_share": "'B' + df_recycling[node]",
            "year_act": "df_recycling[year]",
            "value": "df_recycling[value]",
            "unit": "-",
        }
    ]
)

WATER_AVAILABILITY = Rule(
    Base = {
        "type": "demand",
        "unit": "km3/year",
        "pipe" : {
            "flag_broadcast": False,
            "flag_map_yv_ya_lt": False,
            "flag_time": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_node_loc": False,
        }
    },
    Diff = [
        {   "condition": "sw",
            "node": "'B' + df_sw[Region].astype(str)",
            "commodity": "surfacewater_basin",
            "level": "water_avail_basin",
            "year": "df_sw[year]",
            "time": "df_sw[time]",
            "value": "df_sw[value] * -1",
        },
        {   "condition": "gw",
            "node": "'B' + df_gw[Region].astype(str)",
            "commodity": "groundwater_basin",
            "level": "water_avail_basin",
            "year": "df_gw[year]",
            "time": "df_gw[time]",
            "value": "df_gw[value] * -1",
        }
    ]
)


SHARE_CONSTRAINTS_GW = Rule(
    Base = {
        "type": "share_commodity_lo",
        "unit": "-",
        "pipe" : {
            "flag_broadcast": False,
            "flag_map_yv_ya_lt": False,
            "flag_time": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_node_loc": False,
        }
    },
    Diff = [
        {   "condition": "default",
            "shares": "share_low_lim_GWat",
            "node_share": "'B' + df_gw[Region].astype(str)",
            "year_act": "df_gw[year]",
            "time": "df_gw[time]",
            "value": "df_gw[value] / (df_sw[value] + df_gw[value]) * 0.95",
            "unit": "-",
        }
    ]
)


SHARE_CONSTRAINTS_GW_old = [
    {   "type": "share_commodity_lo",
        "shares": "share_low_lim_GWat",
        "node": "df_gw[Region].astype(str)",
        "year": "df_gw[year]",
        "time": "df_gw[time]",
        "value": "df_gw[value] / (df_sw[value] + df_gw[value]) * 0.95",
        "unit": "-",
        "df_source1": "df_gw",
        "df_source2": "df_sw"
    }
]





