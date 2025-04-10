import copy
import warnings
from functools import wraps

import numpy as np
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


def feed_make_df_pl(
    rule: dict, dfs: dict, skip_kwargs: list[str] = [], default_df_key: str = "rows"
) -> dict:
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

    df_search_terms = {df_key: f"{df_key}[" for df_key in dfs}  # precompute once
    for k, v in kwargs.items():
        if isinstance(v, str):
            # Extract dataframe name from string if present (e.g., "df_out_dist[tec]")
            df_key = default_df_key
            for possible_df_key, search_term in df_search_terms.items():
                if search_term in v:
                    df_key = possible_df_key
                    break

            # Use the appropriate dataframe for evaluation
            kwargs[k] = eval_field(v, dfs[df_key])

    return kwargs


def feed_make_df(
    rule: dict,
    rule_dfs: pd.Series | pd.DataFrame | dict,
    skip_kwargs: list[str] = [],
    default_df_key: str | None = None,
) -> dict:
    match rule_dfs:
        case dict():
            return feed_make_df_pl(rule, rule_dfs, skip_kwargs, default_df_key)
        case pd.Series() | pd.DataFrame():
            return feed_make_df_sl(rule, rule_dfs, skip_kwargs)
        case _:
            raise ValueError(f"Invalid input type for rule_dfs: {type(rule_dfs)}")


def run_standard(
    r: dict, base_args: dict, extra_args: dict | None = None, broadcast_year: any = None
) -> pd.DataFrame:
    """Merge base args and rule-specific pipe arguments then call standard_operation."""

    match base_args.get("skip_kwargs", None):
        case None:
            base_args["skip_kwargs"] = ["condition", "pipe"]
        case ["condition", "pipe"]:
            warnings.warn("skip_kwargs is no longer required", UserWarning)
        case _:
            raise ValueError(f"Invalid skip_kwargs: {base_args['skip_kwargs']}")

    args = {**base_args, **r.get("pipe", {})}
    kwargs = {}
    if extra_args is not None:
        kwargs["extra_args"] = extra_args
    if broadcast_year is not None:
        kwargs["broadcast_year"] = broadcast_year
    return standard_operation(rule=r, suppress_warning=True, **args, **kwargs)


# Define helper to build the list of pipe functions/arguments.
def _build_pipe_arguments(
    flag_broadcast: bool,
    broadcast_year: int | None,
    flag_map_yv_ya_lt: bool,
    lt: pd.Series | pd.DataFrame | int | None,
    lt_arg: str | None,
    year_wat: tuple | None,
    first_year: int | None,
) -> list:
    pipe_args = []
    if flag_broadcast:
        pipe_args.append(broadcast)
    if broadcast_year is not None:
        pipe_args.append(broadcast_year)

        # Use structural pattern matching on lt's type.
    match (flag_map_yv_ya_lt, lt, lt_arg):
        case False, _, _:
            pass
        case True, pd.Series() | pd.DataFrame(), str():
            pipe_args.append(
                map_yv_ya_lt(periods=year_wat, lt=lt[lt_arg], ya=first_year)
            )
        case True, int(), None:
            pipe_args.append(map_yv_ya_lt(periods=year_wat, lt=lt, ya=first_year))
        case True, None, None:
            raise ValueError("lt must be provided when flag_map_yv_ya_lt is True")

    return pipe_args

    # Define helper to build additional keyword arguments.


def _build_kw_args(
    flag_node_loc: bool,
    node_loc: pd.DataFrame | np.ndarray | None,
    node_loc_arg: str | None,
    flag_time: bool,
    sub_time: pd.Series | str | None,
    extra_args: dict | None,
) -> dict:
    kw = dict(extra_args) if extra_args is not None else {}
    match (flag_node_loc, node_loc, node_loc_arg):
        case (False, _, _):
            pass
        case (True, pd.DataFrame(), str()):
            kw["node_loc"] = node_loc[node_loc_arg]
        case (True, np.ndarray(), None):
            kw["node_loc"] = node_loc
        case (True, None, None):
            raise ValueError(
                "node_loc and node_loc_arg must be provided if flag_node_loc is True"
            )
        case (True, _, None):
            raise ValueError(f"unexpected data type for node_loc : {type(node_loc)}")


    match (flag_time, sub_time):
        case (False, _):
            pass
        case (True, pd.Series() | str()):
            kw["time"] = sub_time
        case (True, None):
            raise ValueError("sub_time must be provided if flag_time is True")
        case (True, _):
            raise ValueError(f"unexpected data type for sub_time : {type(sub_time)}")

    return kw


def warn_run_standard(func):
    """Decorator to warn users to prefer run_standard()
    over direct standard_operation() calls."""

    @wraps(func)
    def wrapper(*args, suppress_warning: bool = False, **kwargs):
        if not suppress_warning:
            warnings.warn(
                f"{func.__name__} is a lower-level API that requires more manual "
                "parameter handling. Only use {func.__name__} if you need access to its"
                "full functionality.",
                UserWarning,
                stacklevel=3,
            )
        return func(*args, **kwargs)

    return wrapper


@warn_run_standard  # Apply the decorator to standard_operation
def standard_operation(
    *,
    rule: dict,
    rule_dfs: pd.Series | pd.DataFrame | dict,
    default_df_key: str | None = None,
    lt: pd.Series | pd.DataFrame | int | None = None,
    lt_arg: str | None = None,
    skip_kwargs: list,
    node_loc: pd.DataFrame | None = None,
    node_loc_arg: str | None = None,
    year_wat: tuple | None = None,
    first_year: int | None = None,
    sub_time: pd.Series | str | None = None,
    flag_same_time: bool = False,
    flag_same_node: bool = False,
    flag_broadcast: bool = True,
    flag_map_yv_ya_lt: bool = True,
    flag_node_loc: bool = True,
    flag_time: bool = True,
    broadcast_year: int | None = None,
    extra_args: dict | None = None,
):
    """
    Creates a DataFrame using `make_df` and applies a series of
    pipe operations based on various flags.

    Operations include:
      - Broadcasting with `broadcast` if flag_broadcast is True.
      - Mapping year variables via `map_yv_ya_lt` if flag_map_yv_ya_lt is True
        and lt is of the appropriate type.
      - Adding additional keyword arguments such as node location and time
        based on provided flags.
      - Optionally applying `same_time` and `same_node` functions.
    """

    match (rule_dfs, default_df_key):
        case (pd.Series() | pd.DataFrame(), None):
            out = make_df(
                rule["type"],
                **feed_make_df(rule, rule_dfs, skip_kwargs),
            )
        case (dict(), str()):
            out = make_df(
                rule["type"],
                **feed_make_df(rule, rule_dfs, skip_kwargs, default_df_key),
            )
        case (pd.Series() | pd.DataFrame(), str()):
            raise ValueError(
                f"Default key provided defaults to rows, but rule_dfs is a {type(rule_dfs)}"
            )
        case _:
            raise ValueError(f"Invalid input type for rule_dfs: {type(rule_dfs)}")

    # If broadcast or mapping is enabled, construct pipe arguments and keyword args.
    if flag_broadcast or flag_map_yv_ya_lt:
        pipe_arguments = _build_pipe_arguments(
            flag_broadcast,
            broadcast_year,
            flag_map_yv_ya_lt,
            lt,
            lt_arg,
            year_wat,
            first_year,
        )
        kw = _build_kw_args(
            flag_node_loc,
            node_loc,
            node_loc_arg,
            flag_time,
            sub_time,
            extra_args,
        )
        out = out.pipe(*pipe_arguments, **kw)
    # Optionally apply same_time and same_node pipes.
    if flag_same_time:
        out = out.pipe(same_time)
    if flag_same_node:
        out = out.pipe(same_node)

    return out


# Helper function for deep merging
def deep_merge(base, diff):
    """Recursively merges diff dict into a deep copy of base dict."""
    merged = copy.deepcopy(base)  # Start with a deep copy of base
    for key, value in diff.items():
        # Check if the key exists in merged and both values are dictionaries
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            # Overwrite or add the key-value pair from diff
            merged[key] = value
    return merged


class Rule:
    def return_base(self):
        return self.base()

    def return_diff(self):
        return self.diff()

    def get_rule(self):
        result = []
        base_dict = self.base()  # Get the base dictionary once
        for diff_item in self.Diff:
            if diff_item["condition"] == "SKIP":
                continue
            if diff_item:  # Only process non-empty dictionaries
                # Perform a deep merge instead of shallow update
                merged = deep_merge(base_dict, diff_item)
                result.append(merged)
        return result

    def change_unit(self, conversion_factor: float, new_unit: str):
        current_unit = self.base().get("unit")
        # replace current unit with new unit without magic methods
        self.base()["unit"] = new_unit
        self.base().value *= conversion_factor

    def __init__(self, Base=None, Diff=None):
        self.Base = Base or {}
        self.Diff = Diff or [{}, {}]

    def base(self):
        return self.Base

    def diff(self):
        return self.Diff
