import re
import warnings
from functools import wraps
from typing import Any

import numpy as np
import pandas as pd
from message_ix import make_df

from message_ix_models.model.water.utils import map_yv_ya_lt
from message_ix_models.util import (
    broadcast,
    minimum_version,
    same_node,
    same_time,
)


@minimum_version("python 3.10")
def eval_field(expr: str, dfs: dict[str, pd.DataFrame]):
    """Augments the eval function to handle dataframe references.

    Handles expressions referencing keys from the `dfs` dictionary.
    """
    if not isinstance(expr, str):
        return expr

    # Find all dataframe prefixes that occur before a '[' character.
    prefixes = re.findall(r"(\w+)\s*\[", expr)
    unique_prefixes = []
    for p in prefixes:
        if p not in unique_prefixes:
            unique_prefixes.append(p)

    # No dataframe reference found; return the expression string as-is.
    if not unique_prefixes:
        return expr

    # Build the local context for eval()
    local_context = {}
    num_prefixes = len(unique_prefixes)
    num_dfs = len(dfs)

    # Special case: 1 prefix in expr, 1 DataFrame provided.
    # Map the prefix found in the expr to the single available DataFrame,
    # regardless of the key used in the `dfs` dict.
    if num_prefixes == 1 and num_dfs == 1:
        expression_prefix = unique_prefixes[0]
        # Get the single DataFrame value from the dfs dict
        single_df = next(iter(dfs.values()))
        local_context[expression_prefix] = single_df
    # General case: multiple prefixes or multiple dfs.
    # Match prefixes found in the expr to keys in the `dfs` dict.
    else:
        for prefix in unique_prefixes:
            if prefix in dfs:
                local_context[prefix] = dfs[prefix]
            else:
                # Raise error if a prefix in the expression is not found in dfs
                raise KeyError(
                    f"DataFrame prefix '{prefix}' from expression '{expr}' "
                    f"not found in provided dfs keys: {list(dfs.keys())}"
                )

    # Replace instances of field-access that are missing quotes around the key.
    def repl(match):
        prefix = match.group(1)
        key = match.group(2).strip()
        # If the key is already quoted, return as is.
        if (key.startswith("'") and key.endswith("'")) or (
            key.startswith('"') and key.endswith('"')
        ):
            return f"{prefix}[{key}]"
        else:
            return f"{prefix}['{key}']"

    # Process the expression (e.g. convert df_gw[value] to df_gw['value']).
    new_expr = re.sub(r"(\w+)\[\s*([^\]]+?)\s*\]", repl, expr)

    # Evaluate the new expression using an empty globals dict and the local context.
    return eval(new_expr, {}, local_context)


def feed_make_df_sl(rule: dict, df: pd.DataFrame, skip_kwargs: list[str] = []) -> dict:
    """
    Feed the make_df function with the rule parameters.

    For each value call the eval_field function to evaluate the value. Assumes
    expressions in the rule refer to the single input dataframe as 'df'.
    Parameters
    ----------
    rule: dict
        The rule to feed the make_df function with.
    df: pd.DataFrame
        The dataframe to feed the make_df function with.
    skip_kwargs: list[str]
        List of keys to skip from the rule.
    """
    kwargs = {k: v for k, v in rule.items() if k != "type" and k not in skip_kwargs}
    dfs_context = {"df": df}  # Wrap the single df in a dict with key 'df'
    for k, v in kwargs.items():
        if isinstance(v, str):
            # Pass the context dictionary to eval_field
            kwargs[k] = eval_field(v, dfs_context)

    return kwargs


def feed_make_df_pl(rule: dict, dfs: dict, skip_kwargs: list[str] = []) -> dict:
    """Process rule parameters, evaluating DataFrame references and keeping literals.

    Args:
        rule: Rule parameters dict
        dfs: DataFrames available for reference (dict mapping name to DataFrame)
        skip_kwargs: Keys to exclude from processing
    Returns:
        Dict of processed parameters
    """
    kwargs = {k: v for k, v in rule.items() if k != "type" and k not in skip_kwargs}
    # df_search_terms = {df_key: f"{df_key}[" for df_key in dfs} # No longer needed

    for k, v in kwargs.items():
        if isinstance(v, str):
            # Directly call eval_field, passing the full dfs dictionary.
            # eval_field will now resolve the correct
            # dataframes based on prefixes found in v.
            try:
                kwargs[k] = eval_field(v, dfs)
            except KeyError as e:
                # Re-raise KeyError if eval_field couldn't find a required df prefix
                raise KeyError(f"Error processing rule key '{k}' with value '{v}': {e}")
            except Exception as e:
                # Catch other potential evaluation errors from eval_field
                raise ValueError(
                    f"Error evaluating rule key '{k}' with value '{v}': {e}"
                )
        # else: # No change needed for non-string values
        #    kwargs[k] = v

    return kwargs


@minimum_version("python 3.10")
def feed_make_df(
    rule: dict,
    rule_dfs: pd.Series | pd.DataFrame | dict,
    skip_kwargs: list[str] = [],
) -> dict:
    """Feed the make_df function with the rule parameters.
    For each value call the eval_field function to evaluate the value.
    Parameters
    ----------
    rule: dict
        The rule to feed the make_df function with.
    rule_dfs: pd.Series | pd.DataFrame | dict
        The dataframes to feed the make_df function with.
    skip_kwargs: list[str]
        List of keys to skip from the rule.
    """
    match rule_dfs:
        case dict():
            return feed_make_df_pl(rule, rule_dfs, skip_kwargs)
        case pd.Series() | pd.DataFrame():
            return feed_make_df_sl(rule, rule_dfs, skip_kwargs)
        case _:
            raise ValueError(f"Invalid input type for rule_dfs: {type(rule_dfs)}")


@minimum_version("python 3.10")
def build_standard(
    r: dict, base_args: dict, extra_args: dict | None = None, broadcast_year: Any = None
) -> pd.DataFrame:
    """Merge base args and rule-specific pipe arguments then call standard_operation.
    Parameters
    ----------
    r: dict
        The rule to feed the make_df function with.
    base_args: dict
        The base arguments to feed the make_df function with.
    extra_args: dict | None
    """

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
@minimum_version("python 3.10")
def _build_pipe_arguments(
    flag_broadcast: bool,
    broadcast_year: int | None,
    flag_map_yv_ya_lt: bool,
    lt: pd.Series | pd.DataFrame | int | float | None,
    year_wat: tuple[int, ...] | None,
    first_year: int | None,
) -> list[Any]:
    """Build the list of pipe functions/arguments with type guards.

    Parameters
    ----------
    flag_broadcast : bool
        Whether to broadcast the data.
    broadcast_year : int | None
        The year to broadcast the data to.
    flag_map_yv_ya_lt : bool
        Whether to map the year variables.
    lt : pd.Series | pd.DataFrame | int | float | None
        The lifetime of the data.
    year_wat : tuple[int, ...] | None
        The years to map the data to.
    first_year : int | None
        The first year to map the data to.
    """
    pipe_args: list[Any] = []
    if flag_broadcast:
        pipe_args.append(broadcast)
    if broadcast_year is not None:
        pipe_args.append(broadcast_year)

    # Use structural pattern matching on lt's type.
    match (flag_map_yv_ya_lt, lt, year_wat):
        case False, _, _:
            pass
        case True, int() | float(), tuple():
            # Convert lt to int since map_yv_ya_lt expects an integer.
            pipe_args.append(map_yv_ya_lt(periods=year_wat, lt=int(lt), ya=first_year))
        case True, None, _:
            raise ValueError("lt must be provided when flag_map_yv_ya_lt is True")
        case True, _, _:
            raise TypeError(
                f"Expected a numeric type for 'lt' when flag_map_yv_ya_lt"
                f" is True, got {type(lt)}"
            )
        case True, _, None:
            raise ValueError("year_wat must be provided when flag_map_yv_ya_lt is True")

    return pipe_args

    # Define helper to build additional keyword arguments.


@minimum_version("python 3.10")
def _build_kw_args(
    flag_node_loc: bool,
    node_loc: pd.DataFrame | np.ndarray | None,
    flag_time: bool,
    sub_time: pd.Series | str | None,
    extra_args: dict | None,
) -> dict:
    """Build additional keyword arguments.
    to be passed to broadcast or map_yv_ya_lt

    Parameters
    ----------
    flag_node_loc: bool
        Whether to broadcast the data.
    node_loc: pd.DataFrame | np.ndarray | None
        The node location of the data.
    node_loc_arg: str | None
        The argument to pass to the node location if it is a dataframe/series.
    flag_time: bool
        Whether to broadcast the data.
    sub_time: pd.Series | str | None
        The time of the data.
    extra_args: dict | None
        The extra arguments to pass to the function for broadcast or map_yv_ya_lt.
    """
    kw = dict(extra_args) if extra_args is not None else {}
    match (flag_node_loc, node_loc):
        case (False, _):
            pass
        case (True, np.ndarray() | pd.Series()):
            kw["node_loc"] = node_loc
        case (True, None):
            raise ValueError("node_loc if flag_node_loc is True")
        case (True, _):
            raise ValueError(f"unexpected data type for node_loc : {type(node_loc)}")

    match (flag_time, sub_time):
        case (False, _):
            pass
        case (True, str()) if len(sub_time) == 1:
            raise ValueError("sub_time cannot be a single character{sub_time!r}")
            # Found cases where subtime was iterating over 'y', e.g. 'y' in 'year'
        case (True, pd.Series() | str()):
            kw["time"] = sub_time
        case (True, None):
            raise ValueError("sub_time must be provided if flag_time is True")
        case (True, _):
            raise ValueError(f"unexpected data type for sub_time : {type(sub_time)}")

    return kw


def warn_build_standard(func):
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


@minimum_version("python 3.10")
@warn_build_standard  # Apply the decorator to standard_operation
def standard_operation(
    *,
    rule: dict,
    rule_dfs: pd.Series | pd.DataFrame | dict,
    lt: pd.Series | pd.DataFrame | int | None = None,
    skip_kwargs: list,
    node_loc: pd.DataFrame | None = None,
    node_loc_arg: str | None = None,
    year_wat: tuple | None = None,
    first_year: int | None = None,
    sub_time: pd.Series | str | None = None,
    broadcast_year: int | None = None,
    extra_args: dict | None = None,
    flag_same_time: bool = False,
    flag_same_node: bool = False,
    flag_broadcast: bool = False,
    flag_map_yv_ya_lt: bool = False,
    flag_node_loc: bool = False,
    flag_time: bool = False,
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
    out = make_df(rule["type"], **feed_make_df(rule, rule_dfs, skip_kwargs))

    # If broadcast or mapping is enabled, construct pipe arguments and keyword args.
    if flag_broadcast or flag_map_yv_ya_lt:
        pipe_arguments = _build_pipe_arguments(
            flag_broadcast,
            broadcast_year,
            flag_map_yv_ya_lt,
            lt,
            year_wat,
            first_year,
        )
        kw = _build_kw_args(
            flag_node_loc,
            node_loc,
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
