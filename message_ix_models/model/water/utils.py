import logging
from collections import defaultdict
from functools import lru_cache
from itertools import product
from typing import Optional

import numpy as np
import pandas as pd
import xarray as xr
from deprecated import deprecated
from message_ix import make_df
from sdmx.model.v21 import Code

from message_ix_models import Context
from message_ix_models.model.structure import get_codes
from message_ix_models.util import load_package_data

log = logging.getLogger(__name__)

# Configuration files
METADATA = [
    # Information about MESSAGE-water
    ("water", "config"),
    ("water", "set"),
    ("water", "technology"),
]


def read_config(context: Optional[Context] = None):
    """Read the water model configuration / metadata from file.

    Numerical values are converted to computation-ready data structures.

    Returns
    -------
    .Context
        The current Context, with the loaded configuration.
    """

    context = context or Context.get_instance(-1)

    # if context.nexus_set == 'nexus':
    if "water set" in context:
        # Already loaded
        return context

    # Load water configuration
    for parts in METADATA:
        # Key for storing in the context
        key = " ".join(parts)

        # Actual filename parts; ends with YAML
        _parts = list(parts)
        _parts[-1] += ".yaml"

        context[key] = load_package_data(*_parts)

    return context


@lru_cache()
def map_add_on(rtype=Code):
    """Map addon & type_addon in ``sets.yaml``."""
    dims = ["add_on", "type_addon"]

    # Retrieve configuration
    context = read_config()

    # Assemble group information
    result = defaultdict(list)

    for indices in product(*[context["water set"][d]["add"] for d in dims]):
        # Create a new code by combining two
        result["code"].append(
            Code(
                id="".join(str(c.id) for c in indices),
                name=", ".join(str(c.name) for c in indices),
            )
        )

        # Tuple of the values along each dimension
        result["index"].append(tuple(c.id for c in indices))

    if rtype == "indexers":
        # Three tuples of members along each dimension
        indexers = zip(*result["index"])
        indexers = {
            d: xr.DataArray(list(i), dims="consumer_group")
            for d, i in zip(dims, indexers)
        }
        indexers["consumer_group"] = xr.DataArray(
            [c.id for c in result["code"]],
            dims="consumer_group",
        )
        return indexers
    elif rtype is Code:
        return sorted(result["code"], key=str)
    else:
        raise ValueError(rtype)


def add_commodity_and_level(df: pd.DataFrame, default_level=None):
    # Add input commodity and level
    t_info: list = Context.get_instance()["water set"]["technology"]["add"]
    c_info: list = get_codes("commodity")

    @lru_cache()
    def t_cl(t):
        input = t_info[t_info.index(t)].annotations["input"]
        # Commodity must be specified
        commodity = input["commodity"]
        # Use the default level for the commodity in the RES (per
        # commodity.yaml)
        level = (
            input.get("level", "water_supply")
            or c_info[c_info.index(commodity)].annotations.get("level", None)
            or default_level
        )

        return commodity, level

    def func(row: pd.Series):
        row[["commodity", "level"]] = t_cl(row["technology"])
        return row

    return df.apply(func, axis=1)


def map_yv_ya_lt(
    periods: tuple[int, ...], lt: int, ya: Optional[int] = None
) -> pd.DataFrame:
    """All meaningful combinations of (vintage year, active year) given `periods`.

    Parameters
    ----------
    periods : tuple[int, ...]
        A sequence of years.
    lt : int, lifetime
    ya : int, active year
        The first active year.
    Returns
    -------
    pd.DataFrame
        A DataFrame with columns 'year_vtg' and 'year_act'.
    """
    if not ya:
        ya = periods[0]
        log.info(f"First active year set as {ya!r}")
    if not lt:
        raise ValueError("Add a fixed lifetime parameter 'lt'")

    # The following lines are the same as
    # message_ix.tests.test_feature_vintage_and_active_years._generate_yv_ya

    # - Create a mesh grid using numpy built-ins
    # - Take the upper-triangular portion (setting the rest to 0)
    # - Reshape
    data = np.triu(np.meshgrid(periods, periods, indexing="ij")).reshape((2, -1))
    # Filter only non-zero pairs
    df = pd.DataFrame(
        filter(sum, zip(data[0, :], data[1, :])),
        columns=["year_vtg", "year_act"],
        dtype=np.int64,
    )

    # Select values using the `ya` and `lt` parameters
    return df.loc[(ya <= df.year_act) & (df.year_act - df.year_vtg <= lt)].reset_index(
        drop=True
    )


def key_check(rules: list[list[dict]]) -> None:
    """Loop through rules and check if all rules have in common the keys in the DataFrame,
    each rule is a list of dictionaries. Prove that all the rules have the same keys"""

    set_of_keys = set(rules[0][0].keys())
    for rule in rules:
        set_of_keys = set_of_keys.intersection(set(rule[0].keys()))
        if set_of_keys != set(rules[0][0].keys()):
            raise ValueError(
                f"Missing required columns: {set_of_keys - set(rules[0][0].keys())}"
            )
    return set_of_keys


def eval_field(
    expr: str, df_processed: pd.DataFrame, df_processed2: pd.DataFrame = None
):
    # If the expression is already a numeric literal (or any non-string type),
    # return it directly.
    if not isinstance(expr, str):
        return expr

    import re  # Ensure regex is available.

    # Find all dataframe prefixes (e.g., "df_gw", "df_sw") that occur before a '[' character.
    prefixes = re.findall(r"(\w+)\s*\[", expr)
    unique_prefixes = []
    for p in prefixes:
        if p not in unique_prefixes:
            unique_prefixes.append(p)
    # No dataframe reference found; evaluate the expression as-is.
    match len(unique_prefixes):
        case 0:
            return expr  # eval(expr, {}, {})
        case 1:
            local_context = {unique_prefixes[0]: df_processed}
        case 2 if df_processed2 is not None:
            local_context = {
                unique_prefixes[0]: df_processed,
                unique_prefixes[1]: df_processed2,
            }
        case _:
            raise ValueError(
                f"Expression '{expr}' uses more than two different dataframes: {unique_prefixes}"
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

    # Evaluate the new expression using an empty globals dict and the constructed local context.
    return eval(new_expr, {}, local_context)


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


@deprecated(reason="Use the new standard operation, this is a legacy function")
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
        unit=r["unit"],
    )
    return df_rule


def safe_concat(input_df: list[pd.DataFrame] | pd.DataFrame) -> pd.DataFrame:
    """Optimized concatenation that avoids unnecessary operations.
    For lists with single DataFrame, returns it directly. For multiple DataFrames,
    uses pd.concat with copy=False to avoid duplicating data. Handles single
    DataFrame inputs by returning them unchanged.
    """
    if isinstance(input_df, list):
        return input_df[0] if len(input_df) == 1 else pd.concat(input_df, copy=False)
    return input_df
