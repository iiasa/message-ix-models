from message_ix_models.model.water.rules import Constants, Rule

# Cooling Technology Constants
CT_CONST_BASE_DATA = [
    ("CT_LIFETIME", 30, "y"),
    ("CT_OUTPUT_DEFAULT", 1, "-"),
]
CT_CONST = Constants(CT_CONST_BASE_DATA)


"""
Rules defining input parameters for cooling technologies.
Specifies inputs of electricity (secondary level), freshwater (water_supply level),
and saline water (saline_supply level) based on parent technology data.
Used in `cool_tech` function within `water_for_ppl.py`.
"""
COOL_TECH_INPUT_RULES = Rule(
    Base={
        "type": "input",
        "time": "year",
        "time_origin": "year",
    },
    Diff=[
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
            "unit_in": "GWa",
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
            "unit_in": "MCM/GWa",
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
            "unit_in": "MCM/GWa",
        },
    ],
)

"""
Rule defining emission factors for cooling technologies.
Specifically assigns 'fresh_return' emissions based on return flow values.
Used in `cool_tech` function within `water_for_ppl.py`.
"""
COOL_TECH_EMISSION_RULES = Rule(
    Base={
        "type": "emission_factor",
        "unit": "MCM/GWa",
        "unit_in": "MCM/GWa",
    },
    Diff=[
        {
            "condition": "default",
            "node_loc": "emiss_df[node_loc]",
            "technology": "emiss_df[technology_name]",
            "year_vtg": "emiss_df[year_vtg]",
            "year_act": "emiss_df[year_act]",
            "mode": "emiss_df[mode]",
            "emission": "fresh_return",
            "value": "emiss_df[value_return]",
        }
    ],
)

"""
Rules defining output parameters for cooling technologies.
Handles the main output commodity (derived from technology name) and,
conditionally ('nexus' setting), the return flow to surfacewater basins.
Used in `cool_tech` function within `water_for_ppl.py`.
"""
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
            "value": "CT_OUTPUT_DEFAULT",
            "unit": "-",
            "unit_in": "-",
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
            "unit_in": "MCM/GWa",
            "pipe": {
                "flag_broadcast": True,
            },
        },
    ],
    constants_manager=CT_CONST,
)

"""
Rule defining the addon conversion relationship between parent power plant
technologies and their associated cooling addons.
Specifies the cooling fraction as the conversion value.
Used in `cool_tech` function within `water_for_ppl.py`.
"""
COOL_TECH_ADDON_RULES = Rule(
    Base={
        "type": "addon_conversion",
    },
    Diff=[
        {
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
            "unit_in": "-",
        }
    ],
)

"""
Rule defining the technical lifetime for cooling technologies.
Applies a constant lifetime defined in `CT_CONST`.
Used in `cool_tech` function within `water_for_ppl.py`.
"""
COOL_TECH_LIFETIME_RULES = Rule(
    Base={
        "type": "technical_lifetime",
        "pipe": {
            "flag_broadcast": True,
            "flag_same_node": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "technology": "inp['technology'].drop_duplicates()",
            "value": "CT_LIFETIME",
            "unit": "year",
            "unit_in": "year",
        }
    ],
    constants_manager=CT_CONST,
)

"""
Rule defining water input for non-cooling power plant technologies.
Maps water withdrawal values to the 'input' parameter.
Used in `non_cooling_tec` function within `water_for_ppl.py`.
"""
NON_COOL_INPUT_RULES = Rule(
    Base={
        "type": "input",
        "unit": "MCM/GWa",
        "unit_in": "MCM/GWa",
    },
    Diff=[
        {
            "condition": "default",
            "technology": "n_cool_df_merge['technology']",
            "value": "n_cool_df_merge['value_y']",
            "level": "water_supply",
            "commodity": "freshwater",
            "time_origin": "year",
            "mode": "M1",
            "time": "year",
            "year_vtg": "n_cool_df_merge['year_vtg'].astype(int)",
            "year_act": "n_cool_df_merge['year_act'].astype(int)",
            "node_loc": "n_cool_df_merge['node_loc']",
            "node_origin": "n_cool_df_merge['node_dest']",
        }
    ],
)

"""
Rule defining upper bounds on commodity shares ('share_commodity_up').
Used in cooling_shares_SSP_from_yaml function.
"""
COOL_SHARE_RULES = Rule(
    Base={
        "type": "share_commodity_up",
        "pipe": {
            "flag_broadcast": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "shares": "df_share['shares']",
            "time": "df_share['time']",
            "value": "df_share['value']",
            "unit": "-",
            "unit_in": "-",
        }
    ],
)
