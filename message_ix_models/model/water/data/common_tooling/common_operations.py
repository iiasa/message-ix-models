"""
helper operations to streamline dataframe creation using pattern matching.
"""

from message_ix import make_df
from message_ix_models.util import broadcast, same_node, same_time
from message_ix_models.model.water.utils import map_yv_ya_lt


def create_param_df(param_type: str, row, context, df_node, year_wat, first_year, sub_time=None):
    """
    create a param dataframe based on param type using pattern matching.
    """
    match param_type:
        case "input":
            match context.SDG:
                case "baseline":
                    df = make_df(
                        "input",
                        technology=row["tec"],
                        value=row["value_mid"],
                        unit="-",
                        level=row["inlvl"],
                        commodity=row["incmd"],
                        mode="M1",
                        node_loc=df_node["node"],
                    )
                case _:
                    df = make_df(
                        "input",
                        technology=row["tec"],
                        value=row.get("value_high", row["value_mid"]),
                        unit="-",
                        level=row["inlvl"],
                        commodity=row["incmd"],
                        mode="Mf",
                        node_loc=df_node["node"],
                    )
            return (
                df.pipe(broadcast, map_yv_ya_lt(year_wat, row.get("technical_lifetime_mid", 1), first_year), time=sub_time)
                  .pipe(same_node)
                  .pipe(same_time)
            )
        case "output":
            match context.SDG:
                case "baseline":
                    df = make_df(
                        "output",
                        technology=row["tec"],
                        value=row["out_value_mid"],
                        unit="-",
                        level=row["outlvl"],
                        commodity=row["outcmd"],
                        mode="M1",
                    )
                case _:
                    df = make_df(
                        "output",
                        technology=row["tec"],
                        value=row["out_value_mid"],
                        unit="-",
                        level=row["outlvl"],
                        commodity=row["outcmd"],
                        mode="Mf",
                    )
            return (
                df.pipe(broadcast, map_yv_ya_lt(year_wat, row.get("technical_lifetime_mid", 1), first_year),
                         node_loc=df_node["node"], time=sub_time)
                  .pipe(same_node)
                  .pipe(same_time)
            )
        case "fix_cost":
            df = make_df(
                "fix_cost",
                technology=row["tec"],
                value=row["fix_cost_mid"],
                unit="USD/km3",
            )
            return df.pipe(broadcast, map_yv_ya_lt(year_wat, row.get("technical_lifetime_mid"), first_year),
                            node_loc=df_node["node"])
        case "var_cost":
            match context.SDG:
                case "baseline":
                    df = make_df(
                        "var_cost",
                        technology=row["tec"],
                        value=row["var_cost_mid"],
                        unit="USD/km3",
                        mode="M1",
                    )
                case _:
                    df = make_df(
                        "var_cost",
                        technology=row["tec"],
                        value=row.get("var_cost_high", row["var_cost_mid"]),
                        unit="USD/km3",
                        mode="Mf",
                    )
            return df.pipe(broadcast, map_yv_ya_lt(year_wat, row.get("technical_lifetime_mid"), first_year),
                            node_loc=df_node["node"], time=sub_time)
        case "capacity_factor":
            df = make_df(
                "capacity_factor",
                technology=row["tec"],
                value=row["capacity_factor_mid"],
                unit="%",
            )
            return (
                df.pipe(broadcast, map_yv_ya_lt(year_wat, row.get("technical_lifetime_mid"), first_year),
                        node_loc=df_node["node"], time=sub_time)
                  .pipe(same_node)
            )
        case "technical_lifetime":
            df = make_df(
                "technical_lifetime",
                technology=row["tec"],
                value=row["technical_lifetime_mid"],
                unit="y",
            )
            return df.pipe(broadcast, year_vtg=year_wat, node_loc=df_node["node"]).pipe(same_node)
        case _:
            raise ValueError(f"unknown param type: {param_type}")


def create_emission_factor_df(row):
    """
    create emission factor df using pattern matching on the row.
    """
    match row:
        case {
            "technology_name": tech,
            "node_loc": node,
            "year_vtg": ytv,
            "year_act": ya,
            "mode": mode,
            "return_rate": rr
        }:
            return make_df(
                "emission_factor",
                node_loc=node,
                technology=tech,
                year_vtg=ytv,
                year_act=ya,
                mode=mode,
                emission="fresh_return",
                value=rr,
                unit="MCM/GWa",
            )
        case _:
            raise ValueError("row does not contain required keys for emission factor")