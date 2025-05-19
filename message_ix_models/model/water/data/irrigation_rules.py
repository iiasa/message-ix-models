from message_ix_models.model.water.rules import Constants, Rule

# Define the raw constant data: list of (name, value, unit)
IRRIGATION_CONST_VALUES = [
    ("IDENTITY", 1, "-"),
    ("ELECTRICITY_INPUT_LOW", 0.04690743, "-"),
    ("ELECTRICITY_INPUT_AVG", 0.101598174, "-"),
    ("ELECTRICITY_INPUT_HIGH", 0.017123288, "-"),
]

citations = [
    {
        "citation": "Diaz et al. 2011",
        "doi": "https://ascelibrary.org/doi/10.1061/%28ASCE%29IR.1943-4774.0000338",
        "description": (
            "Evaluation of Water and Energy Use in Pressurized Irrigation Networks "
            "in Southern Spain"
        ),
    },
]

# Instantiate Constants with data and citation metadata
IRRIGATION_CONST = Constants(
    data=IRRIGATION_CONST_VALUES,
    citations=citations,
)

"""
Rules defining inputs for irrigation technologies.
SKIP conditions were originally commented out, they can
be deleted if no longer needed. Currently they are
skipped.
Used in `add_irr_structure`.
"""
INPUT_IRRIGATION_RULES = Rule(
    Base={
        "type": "input",
        "unit": "-",
        "unit_in": "-",
        "level": "water_supply",
        "commodity": "freshwater",
        "time": "year",
        "time_origin": "year",
        "node_origin": "df_node[region]",
        "node_loc": "df_node[region]",
        "mode": "M1",
        "pipe": {
            "flag_broadcast": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "technology": "irrigation_cereal",
            "value": "IDENTITY",
        },
        {
            "condition": "default",
            "technology": "irrigation_oilcrops",
            "value": "IDENTITY",
            "mode": "M1",
        },
        {
            "condition": "default",
            "technology": "irrigation_sugarcrops",
            "value": "IDENTITY",
            "mode": "M1",
        },
        {
            "condition": "SKIP",
            "technology": "irrigation_sugarcrops",
            "value": "ELECTRICITY_INPUT_LOW",
            "mode": "M1",
        },
        {
            "condition": "SKIP",
            "technology": "irrigation_oilcrops",
            "value": "ELECTRICITY_INPUT_LOW",
            "mode": "M1",
        },
        {
            "condition": "SKIP",
            "technology": "irrigation_cereal",
            "value": "ELECTRICITY_INPUT_LOW",
            "mode": "M1",
        },
    ],
    constants_manager=IRRIGATION_CONST,
)

"""
Rules defining outputs for irrigation technologies.
Used in `add_irr_structure`.
"""
OUTPUT_IRRIGATION_RULES = Rule(
    Base={
        "type": "output",
        "unit": "MCM/year",
        "unit_in": "km3/year",
        "commodity": "freshwater",
        "time": "year",
        "time_dest": "year",
        "node_loc": "df_node[region]",
        "node_dest": "df_node[region]",
        "pipe": {
            "flag_broadcast": True,
        },
    },
    Diff=[
        {
            "condition": "default",
            "technology": "irrigation_cereal",
            "value": "IDENTITY",
            "level": "irr_cereal",
            "mode": "M1",
        },
        {
            "condition": "default",
            "technology": "irrigation_sugarcrops",
            "value": "IDENTITY",
            "level": "irr_sugarcrops",
            "mode": "M1",
        },
        {
            "condition": "default",
            "technology": "irrigation_oilcrops",
            "value": "IDENTITY",
            "level": "irr_oilcrops",
            "mode": "M1",
        },
    ],
    constants_manager=IRRIGATION_CONST,
)
