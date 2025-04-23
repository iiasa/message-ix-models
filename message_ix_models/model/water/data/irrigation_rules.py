from message_ix_models.model.water.utils import Rule
from message_ix_models.util.citation_wrapper import citation_wrapper

IRRIGATION_CONST = citation_wrapper(
    "Diaz et al. 2011",
    "https://ascelibrary.org/doi/10.1061/%28ASCE%29IR.1943-4774.0000338",
    description=(
        "Evaluation of Water and Energy Use in Pressurized Irrigation Networks "
        "in Southern Spain"
    ),
    metadata={"Values": "Irrigation Water Supply"},
)(
    {
        "IDENTITY": 1,
        "ELECTRICITY_INPUT_LOW": 0.04690743,  # Low value from reference
        "ELECTRICITY_INPUT_AVG": 0.101598174,  # Average value from reference
        "ELECTRICITY_INPUT_HIGH": 0.017123288,  # High value from reference
    }
)

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
        },
    },
    Diff=[
        {
            "condition": "default",
            "technology": "irrigation_cereal",
            "value": IRRIGATION_CONST["IDENTITY"],
        },
        {
            "condition": "default",
            "technology": "irrigation_oilcrops",
            "value": IRRIGATION_CONST["IDENTITY"],
            "mode": "M1",
        },
        {
            "condition": "default",
            "technology": "irrigation_sugarcrops",
            "value": IRRIGATION_CONST["IDENTITY"],
            "mode": "M1",
        },
        {
            "condition": "SKIP",
            "technology": "irrigation_sugarcrops",
            "value": IRRIGATION_CONST["ELECTRICITY_INPUT_LOW"],
            "mode": "M1",
        },
        {
            "condition": "SKIP",
            "technology": "irrigation_oilcrops",
            "value": IRRIGATION_CONST["ELECTRICITY_INPUT_LOW"],
            "mode": "M1",
        },
        {
            "condition": "SKIP",
            "technology": "irrigation_cereal",
            "value": IRRIGATION_CONST["ELECTRICITY_INPUT_LOW"],
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
        },
    },
    Diff=[
        {
            "condition": "default",
            "technology": "irrigation_cereal",
            "value": IRRIGATION_CONST["IDENTITY"],
            "level": "irr_cereal",
            "mode": "M1",
        },
        {
            "condition": "default",
            "technology": "irrigation_sugarcrops",
            "value": IRRIGATION_CONST["IDENTITY"],
            "level": "irr_sugarcrops",
            "mode": "M1",
        },
        {
            "condition": "default",
            "technology": "irrigation_oilcrops",
            "value": IRRIGATION_CONST["IDENTITY"],
            "level": "irr_oilcrops",
            "mode": "M1",
        },
    ],
)
