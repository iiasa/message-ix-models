"""Utilities for nodes."""
import logging
from functools import singledispatch
from typing import Dict, Union

import pandas as pd
from message_ix import Scenario
from message_ix.reporting import Quantity

log = logging.getLogger(__name__)

#: Names of dimensions indexed by 'node'.
#:
#: .. todo:: to be robust to changes in :mod:`message_ix`, read these names from that
#:    package.
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

    The data is adapted by:

    - Renaming regions such as R11_NAM to R14_NAM.
    - Copying the data for R11_FSU to R14_CAS, R14_RUS, R14_SCS, and R14_UBM.

    …wherever these appear in a column/dimension named ‘node’, ‘node_*’, or ‘n’.

    The function may be called with:

    - :class:`pandas.DataFrame`,
    - :class:`genno.Quantity`, or
    - :class:`dict` mapping :class:`str` parameter names to values (either of the above
      types).
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
    except KeyError:  # pragma: no cover
        pass

    return result


def adapt_R11_R12(
    data: Dict[str, Union[pd.DataFrame, Quantity]]
) -> Dict[str, Union[pd.DataFrame, Quantity]]:  # pragma: no cover
    raise NotImplementedError


def identify_nodes(scenario: Scenario) -> str:
    """Return the ID of a node codelist given the contents of `scenario`.

    Returns
    -------
    str
        The ID of the :doc:`/pkg-data/node` containing the regions of `scenario`.

    Raises
    ------
    ValueError
        if no codelist can be identified, or the nodes in the scenario do not match the
        children of the “World” node in the codelist.
    """
    from message_ix_models.model.structure import get_codes

    nodes = sorted(scenario.set("node"))

    # Candidate ID: split e.g. "R14_AFR" to "R14"
    id = nodes[0].split("_")[0]

    try:
        # Get the corresponding codelist
        codes = get_codes(f"node/{id}")
    except FileNotFoundError:
        raise ValueError(f"Couldn't identify node codelist from {repr(nodes)}")

    # Expected list of nodes
    world = codes[codes.index("World")]  # type: ignore [arg-type]
    codes = [world] + world.child

    try:
        assert set(nodes) == set(map(str, codes))
    except AssertionError:
        raise ValueError(
            "\n".join(
                [
                    f"Node IDs suggest codelist {repr(id)}, values do not match:",
                    repr(nodes),
                    repr(codes),
                ]
            )
        )
    else:
        log.info(f"Identified node codelist {repr(id)}")
        return id
