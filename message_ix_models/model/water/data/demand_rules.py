import pandas as pd

URBAN_DEMAND = [
    {   "type": "demand",
        "node": "urban_mw[node]",
        "commodity": "urban_mw",
        "level": "final",
        "year": "urban_mw[year]",
        "time": "urban_mw[time]",
        "value": "urban_mw[value]",
        "unit": "km3/year",
        "rate": "urban_mw[rate]",
        "sign": 1,
    },
    {   "type": "demand",
        "node": "urban_dis[node]",
        "commodity": "urban_disconnected",
        "level": "final",
        "year": "urban_dis[year]",
        "time": "urban_dis[time]",
        "value": "urban_dis[value]",
        "unit": "km3/year",
        "rate": "urban_dis[rate]",
        "sign": 1,
    }
]

RURAL_DEMAND = [
    {   "type": "demand",
        "node": "rural_mw[node]",
        "commodity": "rural_mw",
        "level": "final",
        "year": "rural_mw[year]",
        "time": "rural_mw[time]",
        "value": "rural_mw[value]",
        "unit": "km3/year",
        "sign": 1,
    },
    {   "type": "demand",
        "node": "rural_dis[node]",
        "commodity": "rural_disconnected",
        "level": "final",
        "year": "rural_dis[year]",
        "time": "rural_dis[time]",
        "value": "rural_dis[value]",
        "unit": "km3/year",
        "sign": 1,
    }
]

INDUSTRIAL_DEMAND = [
    {   "type": "demand",
        "node": "manuf_mw[node]",
        "commodity": "industry_mw",
        "level": "final",
        "year": "manuf_mw[year]",
        "time": "manuf_mw[time]",
        "value": "manuf_mw[value]",
        "unit": "km3/year",
        "sign": 1,
    },
    {   "type": "demand",
        "node": "manuf_uncollected_wst[node]",
        "commodity": "industry_uncollected_wst",
        "level": "final",
        "year": "manuf_uncollected_wst[year]",
        "time": "manuf_uncollected_wst[time]",
        "value": "manuf_uncollected_wst[value]",
        "unit": "km3/year",
        "sign": -1,
    }
]

URBAN_COLLECTED_WST = [
    {   "type": "demand",
        "node": "urban_collected_wst[node]",
        "commodity": "urban_collected_wst",
        "level": "final",
        "year": "urban_collected_wst[year]",
        "time": "urban_collected_wst[time]",
        "value": "urban_collected_wst[value]",
        "unit": "km3/year",
        "sign": -1,
    }
]

RURAL_COLLECTED_WST = [
    {   "type": "demand",
        "node": "rural_collected_wst[node]",
        "commodity": "rural_collected_wst",
        "level": "final",
        "year": "rural_collected_wst[year]",
        "time": "rural_collected_wst[time]",
        "value": "rural_collected_wst[value]",
        "unit": "km3/year",
        "sign": -1,
    }
]

URBAN_UNCOLLECTED_WST = [
    {   "type": "demand",
        "node": "urban_uncollected_wst[node]",
        "commodity": "urban_uncollected_wst",
        "level": "final",
        "year": "urban_uncollected_wst[year]",
        "time": "urban_uncollected_wst[time]",
        "value": "urban_uncollected_wst[value]",
        "unit": "km3/year",
        "sign": -1,
    }
]

RURAL_UNCOLLECTED_WST = [
    {   "type": "demand",
        "node": "rural_uncollected_wst[node]",
        "commodity": "rural_uncollected_wst",
        "level": "final",
        "year": "rural_uncollected_wst[year]",
        "time": "rural_uncollected_wst[time]",
        "value": "rural_uncollected_wst[value]",
        "unit": "km3/year",
        "sign": -1,
    }
]



HISTORICAL_ACTIVITY = [
    {   "type": "historical_activity",
        "node": "h_act[node]",
        "technology": "h_act[commodity]",  # Will be mapped using HISTORICAL_ACTIVITY_MAPPING
        "year": "h_act[year]",
        "mode": "M1",
        "time": "h_act[time]",
        "value": "h_act[value].abs()",
        "unit": "km3/year",
    }
]

HISTORICAL_CAPACITY = [
    {   "type": "historical_new_capacity",
        "node": "h_cap[node]",
        "technology": "h_cap[commodity]",  # Will be mapped using HISTORICAL_ACTIVITY_MAPPING
        "year": "h_cap[year]",
        "value": "h_cap[value] / 5",  # Division by 5 as per original code
        "unit": "km3/year",
    }
]

SHARE_CONSTRAINTS = [
    {   "type": "share_commodity_lo",
        "shares": "share_wat_recycle",
        "node": "df_recycling[node]",
        "year": "df_recycling[year]",
        "value": "df_recycling[value]",
        "unit": "-",
    }
]

def key_check(rules: list[list[dict]]) -> None:
    """Loop through rules and check if all rules have in common the keys in the DataFrame, each rule is a list of dictionaries. Prove that all the rules have the same keys"""
    
    set_of_keys = set(rules[0][0].keys())
    for rule in rules:
        set_of_keys = set_of_keys.intersection(set(rule[0].keys()))
        if set_of_keys != set(rules[0][0].keys()):
            raise ValueError(f"Missing required columns: {set_of_keys - set(rules[0][0].keys())}")
    return set_of_keys


def eval_field(expr: str, df_processed: pd.DataFrame):
    # Split the expression into field access and arithmetic parts
    if '[' in expr:
        # Extract the variable name and the rest of the expression
        var_name, rest = expr.split('[', 1)
        
        # Find the closing bracket and separate field access from arithmetic
        bracket_end = rest.find(']')
        if bracket_end == -1:
            raise ValueError(f"Invalid expression: missing closing bracket in {expr}")
            
        key_with_bracket = rest[:bracket_end]
        arithmetic = rest[bracket_end+1:].strip()
        
        # Process the key
        key = key_with_bracket
        if not (key.startswith('"') or key.startswith("'")):
            key = f"'{key}'"
            
        # Reconstruct the base expression
        base_expr = f"{var_name}[{key}]"
        
        # Combine with arithmetic if present
        if arithmetic:
            full_expr = f"({base_expr}){arithmetic}"
        else:
            full_expr = base_expr
            
        local_context = {var_name: df_processed}
        return eval(full_expr, {}, local_context)
    else:
        # Handle case where there's no field access, just arithmetic
        return eval(expr, {}, {})

def pre_rule_processing(df_processed: pd.DataFrame) -> pd.DataFrame:
    """
    Pre-process the DataFrame to prepare it for the rule evaluation.
    """
    # Reset the index of the DataFrame
    df_processed = df_processed.reset_index(drop=True)
    # Drop time, rename rate to rate_
    df_processed = df_processed.merge()
    # Drop the index column
    df_processed = df_processed.drop(columns=["index"])
    return df_processed

def load_rules(rule: dict, df_processed: pd.DataFrame = None) -> pd.DataFrame:
    """
    Load a demand rule into a DataFrame. If a processed DataFrame is provided,
    return it directly. Otherwise, construct the DataFrame using the rule's
    string templates and the legacy make_df routine.
    """
    r = rule.copy()
    df_rule = make_df(
        r["type"],
        node="B" + eval_field(r["node"], df_processed),
        commodity=r["commodity"],
        level=r["level"],
        year=eval_field(r["year"], df_processed),
        time=eval_field(r["time"], df_processed),
        value=eval_field(r["value"], df_processed) * r["sign"],
        unit=r["unit"]
    )
    return df_rule



if __name__ == "__main__":
    #iterate through all the rules and check if they have the same keys
    rules_set = {
        "URBAN_DEMAND" : URBAN_DEMAND,
        "RURAL_DEMAND" : RURAL_DEMAND,
        "INDUSTRIAL_DEMAND" : INDUSTRIAL_DEMAND,
        "URBAN_COLLECTED_WST" : URBAN_COLLECTED_WST,
        "RURAL_COLLECTED_WST" : RURAL_COLLECTED_WST,
        "URBAN_UNCOLLECTED_WST" : URBAN_UNCOLLECTED_WST,
        "RURAL_UNCOLLECTED_WST" : RURAL_UNCOLLECTED_WST
    }

    # how to access an item in the list of rules?
    # more like item in rules_set, faster than iterating through the list??
    for item in rules_set:
        match item:
            case "URBAN_DEMAND":
                print("URBAN_DEMAND")
            case "RURAL_DEMAND":
                print("RURAL_DEMAND")
            case "INDUSTRIAL_DEMAND":
                print("INDUSTRIAL_DEMAND")
