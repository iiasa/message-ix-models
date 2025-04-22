from message_ix_models.model.water.utils import Rule

CT_CONST = {
    "cool_tech_lifetime": 30,
    "cool_tech_output_default": 1,
}

COOL_TECH_INPUT_RULES = Rule(
    Base = {
        "type": "input",
        "time": "year",
        "time_origin": "year",
    },
    Diff = [
        {
            "condition": "default",
            "node_loc": "electr[node_loc]",
            "technology": "electr[technology_name]",
            "year_vtg": "electr[year_vtg]",
            "year_act": "electr[year_act]",
            "mode": "electr[mode]",
            "node_origin": "electr[node_origin]",
            "commodity": "electr",
            "level": "secondary",
            "value": "electr[value_cool]",
            "unit": "GWa",
        },
        {
            "condition": "default",
            "node_loc": "icmse_df[node_loc]",
            "technology": "icmse_df[technology_name]",
            "year_vtg": "icmse_df[year_vtg]",
            "year_act": "icmse_df[year_act]",
            "mode": "icmse_df[mode]",
            "node_origin": "icmse_df[node_origin]",
            "commodity": "freshwater",
            "level": "water_supply",
            "value": "icmse_df[value_cool]",
            "unit": "MCM/GWa",
        },
        {
            "condition": "default",
            "node_loc": "saline_df[node_loc]",
            "technology": "saline_df[technology_name]",
            "year_vtg": "saline_df[year_vtg]",
            "year_act": "saline_df[year_act]",
            "mode": "saline_df[mode]",
            "node_origin": "saline_df[node_origin]",
            "commodity": "saline_ppl",
            "level": "saline_supply",
            "value": "saline_df[value_cool]",
            "unit": "MCM/GWa",
        }
    ]
)

COOL_TECH_EMISSION_RULES = Rule(
    Base = {
        "type": "emission_factor",
        "unit": "MCM/GWa",
    },
    Diff = [{
        "condition": "default",
        "node_loc": "emiss_df[node_loc]",
        "technology": "emiss_df[technology_name]",
        "year_vtg": "emiss_df[year_vtg]",
        "year_act": "emiss_df[year_act]",
        "mode": "emiss_df[mode]",
        "emission": "fresh_return",
        "value": "emiss_df[value_return]",
    }])

COOL_TECH_OUTPUT_RULES = Rule(
    Base = {
        "type": "output",
    },
    Diff = [{
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
        "value": CT_CONST["cool_tech_output_default"],
        "unit": "-",
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
        "value": "icfb_df[value_return]",
        "unit": "MCM/GWa",
        "pipe":{
            "flag_broadcast": True,
        }
    }

    ])


COOL_TECH_ADDON_RULES = Rule(
    Base = {
        "type": "addon_conversion",
    },
    Diff = [{
        "condition": "default",
        "node": "adon_df[node_loc]",
        "technology": "adon_df[parent_tech]",
        "year_vtg": "adon_df[year_vtg]",
        "year_act": "adon_df[year_act]",
        "mode": "adon_df[mode]",
        "time": "year",
        "type_addon": "adon_df[tech]",
        "value": "adon_df[cooling_fraction]",
        "unit": "-",
    }]
)

COOL_TECH_LIFETIME_RULES = Rule(
    Base = {
        "type": "technical_lifetime",
        "pipe": {
            "flag_broadcast": True,
            "flag_same_node": True,
        }
    },
    Diff = [{
        "condition": "default",
        "technology": "inp['technology'].drop_duplicates()",
        "value": CT_CONST["cool_tech_lifetime"],
        "unit": "year",
    }]
)

NON_COOL_INPUT_RULES = Rule(
    Base={
        "type": "input",
        "unit": "MCM/GWa",
    },
    Diff=[
        {
            "condition": "default",
            "technology": "n_cool_df_merge['technology']",
            "value": "n_cool_df_merge['value_y']",
            "level": "water_supply",
            "commodity": "freshwater",
            "mode": "M1",
            "time": "year",
            "year_vtg": "n_cool_df_merge['year_vtg'].astype(int)",
            "year_act": "n_cool_df_merge['year_act'].astype(int)",
            "node_loc": "n_cool_df_merge['node_loc']",
            "node_origin": "n_cool_df_merge['node_dest']",
        }
    ]
)

COOL_SHARE_RULES = Rule(
    Base = {
        "type": "share_commodity_up",
        "pipe": {
            "flag_broadcast": True,
        }
    },
    Diff = [
        {
            "condition": "default",
            "shares": "df_share['shares']",
            "time": "df_share['time']",
            "value": "df_share['value']",
            "unit": "-",
        }
    ]
)

