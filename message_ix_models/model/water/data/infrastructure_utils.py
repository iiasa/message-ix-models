import re
from typing import Any, Union

import pandas as pd
from message_ix import make_df

from message_ix_models.model.water.utils import eval_field, map_yv_ya_lt
from message_ix_models.util import broadcast, same_node, same_time


def feed_make_df_sl(rule: dict, df: any, skip_kwargs: list[str] = []) -> pd.DataFrame:
    """
    Feed the make_df function with the rule parameters.
    For each value call the eval_field function to evaluate the value.
    """
    # create dummy rows dict to make it work wit the rules
    kwargs = {k: v for k, v in rule.items() if k != "type" and k not in skip_kwargs}
    for k, v in kwargs.items():
        if isinstance(v, str):
            kwargs[k] = eval_field(v, df)

    return kwargs


def feed_make_df_pl(rule: dict, dfs: dict, skip_kwargs: list[str] = [], default_df_key: str = "rows") -> dict:
    """
    Feed the make_df function with the rule parameters.
    For each value call the eval_field function to evaluate the value.

    Args:
        rule: Dictionary containing rule parameters
        dfs: Dictionary of dataframes where keys are dataframe names used in rules
        skip_kwargs: List of keys to skip from rule
        default_df_key: Default dataframe key to use if no specific df is referenced

    Returns:
        Dictionary of evaluated parameters
    """
    kwargs = {k: v for k, v in rule.items() if k != "type" and k not in skip_kwargs}

    for k, v in kwargs.items():
        if isinstance(v, str):
            # Extract dataframe name from string if present (e.g., "df_out_dist[tec]")
            df_key = default_df_key
            for possible_df_key in dfs.keys():
                if v.startswith(f"{possible_df_key}["):
                    df_key = possible_df_key
                    break

            # Use the appropriate dataframe for evaluation
            kwargs[k] = eval_field(v, dfs[df_key])

    return kwargs


def feed_make_df(rule: dict, rule_dfs: pd.Series | pd.DataFrame | dict, skip_kwargs: list[str] = []) -> dict:
    match rule_dfs:
        case dict():
            return feed_make_df_pl(rule, rule_dfs, skip_kwargs)
        case pd.Series() | pd.DataFrame():
            return feed_make_df_sl(rule, rule_dfs, skip_kwargs)
        case _:
            raise ValueError(f"Invalid input type for rule_dfs: {type(rule_dfs)}")



def standard_operation(
    *,
    rule: dict,
    rule_dfs: pd.Series | pd.DataFrame | dict,
    lt: pd.Series | pd.DataFrame | int | None = None,
    lt_arg: str | None = None,
    skip_kwargs: list,
    df_node: pd.DataFrame | None = None,
    df_node_arg: str | None = None,
    year_wat: tuple,
    first_year: int,
    sub_time: tuple = None,
    flag_same_time: bool = False,
    flag_same_node: bool = False,
    flag_broadcast: bool = True,
    flag_map_yv_ya_lt: bool = True,
    flag_node_loc: bool = True,
    flag_time: bool = True,
    extra_args: dict | None = None,
):
    """
    Creates a DataFrame using `make_df` and applies a series of pipe operations based on various flags.

    Operations include:
      - Broadcasting with `broadcast` if flag_broadcast is True.
      - Mapping year variables via `map_yv_ya_lt` if flag_map_yv_ya_lt is True and lt is of the appropriate type.
      - Adding additional keyword arguments such as node location and time based on provided flags.
      - Optionally applying `same_time` and `same_node` functions.
    """
    # Create the initial DataFrame.
    out = make_df(
        rule["type"],
        **feed_make_df(rule, rule_dfs, skip_kwargs),
    )

    # Build pipe arguments and keyword arguments if either broadcast or mapping is enabled.
    if flag_broadcast or flag_map_yv_ya_lt:
        pipe_arguments = []
        if flag_broadcast:
            pipe_arguments.append(broadcast)

        # Use pattern matching to determine the mapping function argument.
        match (flag_map_yv_ya_lt, lt):
            case (True, pd.Series() | pd.DataFrame()):
                # Ensure lt_arg is provided to extract the proper column.
                if lt_arg is not None:
                    pipe_arguments.append(
                        map_yv_ya_lt(periods=year_wat, lt=lt[lt_arg], ya=first_year)
                    )
            case (True, int()):
                pipe_arguments.append(
                    map_yv_ya_lt(periods=year_wat, lt=lt, ya=first_year)
                )
            case _:
                pass

        # Build keyword arguments (kw) based on the flags.
        if flag_broadcast and (not flag_map_yv_ya_lt) and (not flag_time):
            kw = dict(extra_args) if extra_args is not None else {}
            if flag_node_loc:
                kw["node_loc"] = df_node[df_node_arg]
        else:
            kw = {}
            if extra_args is not None:
                kw.update(extra_args)
            if flag_node_loc and df_node is not None and df_node_arg is not None:
                kw["node_loc"] = df_node[df_node_arg]
            if flag_time:
                kw["time"] = sub_time

        # Apply the pipe with the constructed positional and keyword arguments.
        out = out.pipe(*pipe_arguments, **kw)

    # Apply additional pipes if their corresponding flags are set.
    if flag_same_time:
        out = out.pipe(same_time)
    if flag_same_node:
        out = out.pipe(same_node)

    return out
