"""Utilities for nodes."""
import logging
from typing import List, Sequence, Union

from message_ix import Scenario
from sdmx.model.v21 import Code

from .common import Adapter, MappingAdapter  # noqa: F401

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


def nodes_ex_world(nodes: Sequence[Union[str, Code]]) -> List[Union[str, Code]]:
    """Exclude "World" and anything containing "GLB" from `nodes`.

    May also be used as a reporting computation.

    .. todo:: Make available from :mod:`message_ix_models.report`.
    """
    return list(filter(lambda n_: "GLB" not in n_ and n_ != "World", nodes))
