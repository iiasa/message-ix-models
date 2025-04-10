from message_ix_models.model.water.data.infrastructure_utils import Rule

    # Electricity values per unit of irrigation water supply
    # Reference: Evaluation of Water and Energy Use in
    # Pressurized Irrigation Networks in Southern Spain
    # Diaz et al. 2011 https://ascelibrary.org/
    # doi/10.1061/%28ASCE%29IR.1943-4774.0000338
    # Low Value :0.04690743
    # Average Value :0.101598174
    # High Value : 0.017123288


INPUT_IRRIGATION_RULES = Rule(
    Base={
        "type": "input",
        "unit": "-",
        "level": "water_supply",
        "commodity": "freshwater",
        "time": "year",
        "time_origin": "year",
        "node_origin": "df_node[region]",
        "node_loc": "df_node[region]",
        "mode": "M1",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": False,
            "flag_node_loc": False,
        },
    },

    Diff=[
        {
            "condition": "default",
            "technology": "irrigation_cereal",
            "value": 1,
        },
        {
            "condition": "default",
            "technology": "irrigation_oilcrops",
            "value": 1,
            "mode": "M1",
        },
        {
            "condition": "default",
            "technology": "irrigation_sugarcrops",
            "value": 1,
            "mode": "M1",
        },
        {
            "condition": "SKIP",
            "technology": "irrigation_sugarcrops",
            "value": 0.04690743,
            "mode": "M1",
        },
        {
            "condition": "SKIP",
            "technology": "irrigation_oilcrops",
            "value": 0.04690743,
            "mode": "M1",
        },
        {
            "condition": "SKIP",
            "technology": "irrigation_cereal",
            "value": 0.04690743,
            "mode": "M1",
        },


    ],
)


OUTPUT_IRRIGATION_RULES = Rule(
    Base={
        "type": "output",
        "unit": "km3/year",
        "commodity": "freshwater",
        "time": "year",
        "time_dest": "year",
        "node_loc": "df_node[region]",
        "node_dest": "df_node[region]",
        "pipe": {
            "flag_broadcast": True,
            "flag_map_yv_ya_lt": False,
            "flag_same_time": False,
            "flag_same_node": False,
            "flag_time": False,
            "flag_node_loc": False,
        },
    },
    Diff=[
        {
            "condition": "default",
            "technology": "irrigation_cereal",
            "value": 1,
            "level": "irr_cereal",
            "mode": "M1",
        },
        {
            "condition": "default",
            "technology": "irrigation_sugarcrops",
            "value": 1,
            "level": "irr_sugarcrops",
            "mode": "M1",
        },
        {
            "condition": "default",
            "technology": "irrigation_oilcrops",
            "value": 1,
            "level": "irr_oilcrops",
            "mode": "M1",
        },
    ],
)

