import copy


# Helper functions for dynamically generating rules
def merge_dicts(base: dict, overrides: dict) -> dict:
    """Merge two dictionaries with special handling for nested dictionaries like 'pipe'."""
    merged = copy.deepcopy(base)
    for k, v in overrides.items():
        if isinstance(v, dict) and k in merged and isinstance(merged[k], dict):
            # Special handling for nested dictionaries like 'pipe'
            for nested_k, nested_v in v.items():
                merged[k][nested_k] = nested_v
        else:
            merged[k] = v
    return merged


def generate_rule(base: dict, diffs: list[dict]) -> list[dict]:
    """Generate a list of rules by applying each diff to the base template."""
    ruleset = []
    for diff in diffs:
        combined_rule = merge_dicts(base, diff)
        ruleset.append(combined_rule)
    return ruleset


# Base templates and diffs for INPUT_DATAFRAME_STAGE1
INPUT_STAGE1_BASE = {
    "type": "input",
    "unit": "-",
    "pipe": {
        "flag_broadcast": True,
        "flag_map_yv_ya_lt": True,
        "flag_same_time": True,
        "flag_same_node": True,
        "flag_time": True,
    },
}

INPUT_STAGE1_DIFFS = [
    {
        "condition": "default",
        "technology": "rows[tec]",
        "value": "rows[value_mid]",
        "level": "rows[inlvl]",
        "commodity": "rows[incmd]",
        "mode": "M1",
        "node_loc": "df_node[node]",
        "pipe": {"flag_node_loc": False},
    },
    {
        "condition": "!baseline",
        "technology": "rows[tec]",
        "value": "rows[value_high]",
        "level": "rows[inlvl]",
        "commodity": "rows[incmd]",
        "mode": "Mf",
        "pipe": {"flag_node_loc": True},
    },
    {
        "condition": "baseline_main",
        "technology": "rows[tec]",
        "value": "rows[value_mid]",
        "level": "rows[inlvl]",
        "commodity": "rows[incmd]",
        "mode": "M1",
        "pipe": {"flag_node_loc": True},
    },
    {
        "condition": "baseline_additional",
        "technology": "rows[tec]",
        "value": "rows[value_high]",
        "level": "rows[inlvl]",
        "commodity": "rows[incmd]",
        "mode": "Mf",
        "pipe": {"flag_node_loc": True},
    },
]

# Base templates and diffs for INPUT_DATAFRAME_STAGE2
INPUT_STAGE2_BASE = {
    "type": "input",
    "unit": "-",
    "level": "final",
    "commodity": "electr",
    "time_origin": "year",
    "node_loc": "df_node[node]",
    "node_origin": "df_node[region]",
    "pipe": {
        "flag_broadcast": True,
        "flag_map_yv_ya_lt": True,
        "flag_same_time": False,
        "flag_same_node": False,
        "flag_time": True,
        "flag_node_loc": False,
    },
}

INPUT_STAGE2_DIFFS = [
    {
        "condition": "!baseline",
        "technology": "rows[tec]",
        "value": "rows[value_high]",
        "mode": "Mf",
    },
    {
        "condition": "baseline_p1",
        "technology": "rows[tec]",
        "value": "rows[value_high]",
        "mode": "Mf",
    },
    {
        "condition": "baseline_p2",
        "technology": "rows[tec]",
        "value": "rows[value_mid]",
        "mode": "M1",
    },
    {
        "condition": "non_tech",
        "technology": "rows[tec]",
        "value": "rows[value_mid]",
        "mode": "M1",
    },
]

# Base templates and diffs for OUTPUT_RULES
OUTPUT_BASE = {
    "type": "output",
    "unit": "-",
    "pipe": {
        "flag_broadcast": True,
        "flag_map_yv_ya_lt": True,
        "flag_same_node": True,
        "flag_same_time": True,
        "flag_time": True,
        "flag_node_loc": True,
    },
}

OUTPUT_DIFFS = [
    {
        "condition": "default",
        "technology": "rows[tec]",
        "value": "rows[out_value_mid]",
        "level": "rows[outlvl]",
        "commodity": "rows[outcmd]",
        "mode": "M1",
    },
    {
        "condition": "!baseline",
        "technology": "df_out_dist[tec]",
        "value": "df_out_dist[out_value_mid]",
        "level": "df_out_dist[outlvl]",
        "commodity": "df_out_dist[outcmd]",
        "mode": "Mf",
    },
    {
        "condition": "baseline_p1",
        "technology": "df_out_dist[tec]",
        "value": "df_out_dist[out_value_mid]",
        "level": "df_out_dist[outlvl]",
        "commodity": "df_out_dist[outcmd]",
        "mode": "M1",
    },
    {
        "condition": "baseline_p2",
        "technology": "df_out_dist[tec]",
        "value": "df_out_dist[out_value_mid]",
        "level": "df_out_dist[outlvl]",
        "commodity": "df_out_dist[outcmd]",
        "mode": "Mf",
    },
]

# Base templates and diffs for VAR_COST_RULES
VAR_COST_BASE = {
    "type": "var_cost",
    "unit": "USD/km3",
    "pipe": {
        "flag_broadcast": True,
        "flag_map_yv_ya_lt": True,
        "flag_same_time": False,
        "flag_same_node": False,
        "flag_time": True,
        "flag_node_loc": True,
    },
}

VAR_COST_DIFFS = [
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
]

# Base templates and diffs for VAR_COST_DESALINATION_RULES
VAR_COST_DESAL_BASE = {
    "type": "var_cost",
    "unit": "USD/km3",
    "pipe": {
        "flag_broadcast": True,
        "flag_map_yv_ya_lt": True,
        "flag_same_time": False,
        "flag_same_node": False,
        "flag_node_loc": True,
    },
}



# Base templates and diffs for DESALINATION_INPUT_RULES2
DESAL_INPUT_BASE = {
    "type": "input",
    "unit": "-",
    "pipe": {
        "flag_broadcast": True,
        "flag_map_yv_ya_lt": True,
        "flag_same_time": False,
        "flag_same_node": False,
    },
}

DESAL_INPUT_DIFFS = [
    {
        "condition": "electricity",
        "technology": "rows[tec]",
        "value": "rows[electricity_input_mid]",
        "level": "final",
        "commodity": "electr",
        "mode": "M1",
        "time_origin": "year",
        "node_loc": "df_node[node]",
        "node_origin": "df_node[region]",
        "pipe": {
            "flag_time": True,
            "flag_node_loc": False,
        },
    },
    {
        "condition": "heat",
        "technology": "rows[tec]",
        "value": "rows[heat_input_mid]",
        "level": "final",
        "commodity": "d_heat",
        "mode": "M1",
        "time_origin": "year",
        "node_loc": "df_node[node]",
        "node_origin": "df_node[region]",
        "pipe": {
            "flag_time": True,
            "flag_node_loc": False,
        },
    },
    {
        "condition": "technology",
        "technology": "rows[tec]",
        "value": 1,
        "level": "rows[inlvl]",
        "commodity": "rows[incmd]",
        "mode": "M1",
        "pipe": {
            "flag_same_time": True,
            "flag_same_node": True,
            "flag_time": True,
            "flag_node_loc": True,
        },
    },
]



# Generate rules dynamically
INPUT_DATAFRAME_STAGE1 = generate_rule(INPUT_STAGE1_BASE, INPUT_STAGE1_DIFFS)
INPUT_DATAFRAME_STAGE2 = generate_rule(INPUT_STAGE2_BASE, INPUT_STAGE2_DIFFS)
OUTPUT_RULES = generate_rule(OUTPUT_BASE, OUTPUT_DIFFS)
VAR_COST_RULES = generate_rule(VAR_COST_BASE, VAR_COST_DIFFS)
DESALINATION_INPUT_RULES2= generate_rule(DESAL_INPUT_BASE, DESAL_INPUT_DIFFS)



