from __future__ import annotations
import pandas as pd
from message_ix import make_df

# helper: apply supply rule
def apply_supply_rule(rule: dict, year_vtg=None, year_act=None) -> pd.DataFrame:
    # allowed keys for make_df; only include those with non-none value.
    allowed_keys = [
        "technology", "value", "unit", "level", "commodity", "mode",
        "node_loc", "node_origin", "node_dest", "year_vtg", "year_act",
        "time", "time_origin", "time_dest", "shares", "node_share"
    ]
    args = {}
    for key in allowed_keys:
        if key in rule and rule[key] is not None:
            args[key] = rule[key]
    if year_vtg and "year_vtg" not in args:
        args["year_vtg"] = year_vtg
    if year_act and "year_act" not in args:
        args["year_act"] = year_act
    df = make_df(rule["type"], **args)
    if "broadcast" in rule:
        df = df.pipe(broadcast, **rule["broadcast"])
    return df

# helper: apply dsl transformations with pattern matching on rules list
def apply_supply_dsl_transformations(rules: list[dict], year_vtg=None, year_act=None) -> pd.DataFrame:
    match rules:
        case []:
            return pd.DataFrame()
        case _:
            dfs = [apply_supply_rule(rule, year_vtg, year_act) for rule in rules]
            return pd.concat(dfs, ignore_index=True)

# helper: update rule with extra keys and broadcast changes using pattern matching
def update_rule(rule: dict, extra: dict, broadcast_updates: dict,
                tech_year_mapping: dict | None = None, update_value_fn=None) -> dict:
    r = rule.copy()
    match extra:
        case {}:
            pass
        case _:
            for key, value in extra.items():
                r[key] = value
    if "broadcast" in r:
        match r["broadcast"]:
            case {}:
                pass
            case _:
                for bkey, bval in broadcast_updates.items():
                    if bkey in r["broadcast"]:
                        r["broadcast"][bkey] = bval
    if tech_year_mapping and "broadcast" in r and "year" in r["broadcast"]:
        tech = r.get("technology")
        if tech in tech_year_mapping:
            r["broadcast"]["year"] = tech_year_mapping[tech]
    if update_value_fn is not None:
        r["value"] = update_value_fn(r)
    return r