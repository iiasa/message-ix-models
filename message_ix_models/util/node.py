"""Utilities for nodes."""

import logging
from collections.abc import Sequence
from itertools import product

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

#: Mapping from R12 to R11 node IDs.
R12_R11 = (
    ("R12_AFR", "R11_AFR"),
    ("R12_CPA", "R11_CHN"),
    ("R12_EEU", "R11_EEU"),
    ("R12_FSU", "R11_FSU"),
    ("R12_LAM", "R11_LAM"),
    ("R12_MEA", "R11_MEA"),
    ("R12_NAM", "R11_NAM"),
    ("R12_PAO", "R11_PAO"),
    ("R12_PAS", "R11_PAS"),
    ("R12_RCPA", "R11_CPA"),
    ("R12_SAS", "R11_SAS"),
    ("R12_WEU", "R11_WEU"),
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


#: Adapt data from the R12 to the R11 node list.
#:
#: The data is adapted using the mappings in :data:`R12_R11` for each of the dimensions
#: in :data:`NODE_DIMS`.
adapt_R12_R11 = MappingAdapter({d: R12_R11 for d in NODE_DIMS})


def create_maps(
    id_a: str, id_b: str, *, debug: bool = False
) -> tuple[dict[str, str], dict[str, str]]:
    """Create mappings between two node code lists with IDs `id_a` and `id_b`.

    Two code lists, A and B, are loaded using :func:`.get_codelist`. Mappings are
    determined by counting the common children of every pair of codes in A and B. For
    each code in A, the code in B with the *most* common children is mapped.

    For example:

    - In code list A, code 'A1' has children 'AFG', 'BRA', 'CAN', and 'DEN'.
    - In code list B:

      - Code 'B1' has children 'AFG' and 'EGY'.
      - Code 'B2' has children 'BRA', 'CAN', and 'FRA'.

    - A1 and B1 have 1 common child; A1 and B2 have 2 common children.
    - Thus A1 maps to B1.

    The inverse mapping is computed using the same counts.

    Parameters
    ----------
    id_a :
        The IDs of a code list in :doc:`/pkg-data/node`, for instance "R12". The code
        list must have a code "World" whose first-level children are regions or nodes.
    id_b :
        The ID of a second code list, different from `id_a`. It must also have a code
        "World", and its second-level children must be a set with some overlap of the
        second-level children of A.
    debug :
        If :any:`True`, log the full matrix of counts.

    Returns
    -------
    tuple of (dict, dict)
        The first :class:`dict` maps the ID of every region in code list A (keys) to one
        in B (values). The second mapping does the opposite, mapping the ID of every
        region in code list B to one in A.

        As a consequence of the algorithm:

        - Not every code of B  necessarily appears as a :class:`dict` value.
        - If two possible mappings have the same count, the result is undefined.
    """
    import pandas as pd

    from message_ix_models.model.structure import get_codelist

    # Retrieve the code lists
    cl_a = get_codelist(f"node/{id_a}")
    cl_b = get_codelist(f"node/{id_b}")

    # Count the common children of each pair of items from cl_a and cl_b
    data = []
    for a, b in product(cl_a["World"].child, cl_b["World"].child):
        data.append((a.id, b.id, len(set(a.child) & set(b.child))))

    # Convert to data frame
    df = pd.DataFrame(data, columns=["a", "b", "count"])

    # Create mappings
    result = []
    for orient in "a", "b":
        # - Sort by "a", for instance, then by count, and group by "a".
        # - The last row in each group contains the highest count and corresponding "b".
        # - Reassamble a data frame. The grouping column appears first.
        df_map = df.sort_values([orient, "count"]).groupby(orient).last().reset_index()
        # Convert to dict
        result.append({row.iloc[0]: row.iloc[1] for _, row in df_map.iterrows()})

    if debug:
        # Show debug output
        log.debug(
            "\n"
            + df.pivot(columns="a", index="b", values="count")
            .replace(0, pd.NA)
            .to_string()
            .replace("<NA>", "   -")
        )

    return result[0], result[1]


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


def nodes_ex_world(nodes: Sequence[str | Code]) -> list[str | Code]:
    """Exclude "World" and anything containing "GLB" from `nodes`.

    May also be used as a genno (reporting) operator.
    """
    return list(filter(lambda n_: "GLB" not in n_ and n_ != "World", nodes))
