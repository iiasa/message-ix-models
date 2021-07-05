"""Utilities for nodes."""
from functools import singledispatch
from typing import Dict, Union

import pandas as pd
from message_ix.reporting import Quantity

# FIXME to be robust to changes in message_ix, read these names from the package
#: Names of dimensions indexed by 'node'.
NODE_DIMS = [
    "n",
    "node",
    "node_loc",
    "node_origin",
    "node_dest",
    "node_rel",
    "node_share",
]


@singledispatch
def adapt_R11_R14(data: Dict[str, Union[pd.DataFrame, Quantity]]):
    """Adapt `data` from R11 to R14 node list.

    The data is adapted by copying the data for R11_FSU to R14_CAS, R14_RUS, R14_SCS,
    and R14_UBM.
    """
    # Dispatch to the methods for the value types
    return {par: adapt_R11_R14(value) for par, value in data.items()}


@adapt_R11_R14.register
def _0(df: pd.DataFrame) -> pd.DataFrame:
    """Adapt a :class:`pandas.DataFrame`."""
    # New values for columns indexed by node
    new_values = {}
    for dim in filter(lambda d: d in NODE_DIMS, df.columns):
        # NB need astype() here in case the column contains Code objects; these must be
        # first converted to str before pd.Series.str accessor can work
        new_values[dim] = (
            df[dim]
            .astype(str)
            .str.replace("R11_", "R14_")
            # Map FSU to RUS directly
            .str.replace("R14_FSU", "R14_RUS")
        )

    # List of data frames to be concatenated
    result = [df.assign(**new_values)]

    # True for rows where R14_RUS appears in any column
    mask = (result[0][list(new_values.keys())] == "R14_RUS").any(axis=1)

    # Copy R11_FSU data
    result.extend(
        [
            result[0][mask].replace("R14_RUS", "R14_CAS"),
            result[0][mask].replace("R14_RUS", "R14_SCS"),
            result[0][mask].replace("R14_RUS", "R14_UBM"),
        ]
    )

    # Concatenate and return
    return pd.concat(result, ignore_index=True)


@adapt_R11_R14.register
def _1(qty: Quantity) -> Quantity:
    """Adapt a :class:`genno.Quantity`."""
    s = qty.to_series()
    result = Quantity.from_series(
        adapt_R11_R14(s.reset_index()).set_index(s.index.names)
    )

    try:
        # Copy units
        result.attrs["_unit"] = qty.attrs["_unit"]  # type: ignore [attr-defined]
    except KeyError:
        pass

    return result


def adapt_R11_R12(
    data: Dict[str, Union[pd.DataFrame, Quantity]]
) -> Dict[str, Union[pd.DataFrame, Quantity]]:
    raise NotImplementedError
