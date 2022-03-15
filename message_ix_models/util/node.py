"""Utilities for nodes."""
import logging
from abc import abstractmethod
from collections.abc import Mapping
from typing import Sequence, Tuple

import pandas as pd
from genno.computations import concat
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


class Adapter:
    """Adapt `data`.

    Adapter is an abstract base class for tools that adapt data in any way, e.g.
    between different code lists for certain dimensions. An instance of an Adapter can
    be called with any of the following as `data`:

    - :class:`genno.Quantity`,
    - :class:`pandas.DataFrame`, or
    - :class:`dict` mapping :class:`str` parameter names to values (either of the above
      types).

    …and will return data of the same type.
    """

    def __call__(self, data):
        if isinstance(data, Quantity):
            return self._adapt(data)
        elif isinstance(data, pd.DataFrame):
            # Convert to Quantity
            qty = Quantity.from_series(
                data.set_index(
                    list(filter(lambda c: c not in ("value", "unit"), data.columns))
                )["value"],
            )

            # Store units
            if "unit" in data.columns:
                units = data["unit"].unique()
                assert 1 == len(units), f"Non-unique units {units}"
                unit = units[0]
            else:
                unit = ""  # dimensionless

            # Adapt, convert back to pd.DataFrame, return
            return self._adapt(qty).to_dataframe().assign(unit=unit).reset_index()
        elif isinstance(data, Mapping):
            return {par: self(value) for par, value in data.items()}
        else:
            raise TypeError(type(data))

    @abstractmethod
    def _adapt(self, qty: Quantity) -> Quantity:
        """Adapt data."""
        pass


class MappingAdapter(Adapter):
    """Adapt data using mappings for 1 or more dimension(s).

    Parameters
    ----------
    maps : dict of sequence of (str, str)
        Keys are names of dimensions. Values are sequences of 2-tuples; each tuple
        consists of an original label and a target label.

    Examples
    --------
    >>> a = MappingAdapter({"foo": [("a", "x"), ("a", "y"), ("b", "z")]})
    >>> df = pd.DataFrame(
    ...     [["a", "m", 1], ["b", "n", 2]], columns=["foo", "bar", "value"]
    ... )
    >>> a(df)
      foo  bar  value
    0   x    m      1
    1   y    m      1
    2   z    n      2
    """

    maps: Mapping

    def __init__(self, maps: Mapping[str, Sequence[Tuple[str, str]]]):
        self.maps = maps

    def _adapt(self, qty: Quantity) -> Quantity:
        result = qty

        for dim, labels in self.maps.items():
            if dim not in qty.dims:  # type: ignore [attr-defined]
                continue
            result = concat(
                *[
                    qty.sel(
                        {dim: label[0]}, drop=True
                    ).expand_dims(  # type: ignore [attr-defined]
                        {dim: [label[1]]}
                    )
                    for label in labels
                ]
            )

        return result


#: Mapping from R11 to R12 node IDs.
R11_R12 = (
    ("R11_AFR", "R12_AFR"),
    ("R11_CPA", "R12_CHN"),
    ("R11_EEU", "R12_EEU"),
    ("R11_FSU", "R12_FSU"),
    ("R11_LAM", "R12_LAM"),
    ("R11_MEA", "R12_MEA"),
    ("R11_NAM", "R12_NAM"),
    ("R11_PAO", "R12_PAO"),
    ("R11_PAS", "R12_PAS"),
    ("R11_CPA", "R12_RCPA"),
    ("R11_SAS", "R12_SAS"),
    ("R11_WEU", "R12_WEU"),
)

#: Mapping from R11 to R14 node IDs.
R11_R14 = (
    ("R11_AFR", "R14_AFR"),
    ("R11_FSU", "R14_CAS"),
    ("R11_CPA", "R14_CPA"),
    ("R11_EEU", "R14_EEU"),
    ("R11_LAM", "R14_LAM"),
    ("R11_MEA", "R14_MEA"),
    ("R11_NAM", "R14_NAM"),
    ("R11_PAO", "R14_PAO"),
    ("R11_PAS", "R14_PAS"),
    ("R11_FSU", "R14_RUS"),
    ("R11_SAS", "R14_SAS"),
    ("R11_FSU", "R14_SCS"),
    ("R11_FSU", "R14_UBM"),
    ("R11_WEU", "R14_WEU"),
)

#: Adapt data from the R11 to the R14 node list.
#:
#: The data is adapted using the mappings in :data:`R11_R12` for each of the dimensions
#: in :data:`NODE_DIMS`.
adapt_R11_R12 = MappingAdapter({d: R11_R12 for d in NODE_DIMS})

#: Adapt data from the R11 to the R14 node list.
#:
#: The data is adapted using the mappings in :data:`R11_R14` for each of the dimensions
#: in :data:`NODE_DIMS`.
adapt_R11_R14 = MappingAdapter({d: R11_R14 for d in NODE_DIMS})


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

    glb_node = [n.endswith("_GLB") for n in nodes]
    if any(glb_node):
        omit = nodes.pop(glb_node.index(True))
        log.info(f"Omit known, non-standard node '{omit}' from set to match")

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
