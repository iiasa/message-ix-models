"""Tools for World Bank data."""

import logging
from collections import defaultdict
from collections.abc import MutableMapping
from functools import lru_cache
from typing import TYPE_CHECKING, Optional, cast

import pandas as pd

if TYPE_CHECKING:
    import sdmx.model.common
    from sdmx.message import StructureMessage

log = logging.getLogger(__name__)


# FIXME Reduce complexity from 12 → ≤11
def assign_income_groups(  # noqa: C901
    cl_node: "sdmx.model.common.Codelist",
    cl_income_group: "sdmx.model.common.Codelist",
    method: str = "population",
    replace: Optional[dict[str, str]] = None,
) -> None:
    """Annotate `cl_node` with income groups.

    Each node is assigned an :class:`.Annotation` with :py:`id="wb-income-group"`,
    according to the income groups of its children (countries), as reflected in
    `cl_income_group` (see :func:`.get_income_group_codelist`).

    Parameters
    ----------
    method : "population" or "count"
        Method for aggregation:

        - :py:`"population"` (default): the WB World Development Indicators (WDI) 2020
          population for each country is used as a weight, so that the node's income
          group is the income group of the plurality of the population of its children.
        - :py:`"count"`: each country is weighted equally, so that the node's income
          group is the mode (most frequently occurring value) of its childrens'.
    replace : dict
        Mapping from wb-income-group annotation text appearing in `cl_income_group` to
        texts to be attached to `cl_node`. Mapping two keys to the same value
        effectively combines or aggregates those groups. See :func:`.make_map`.

    Example
    -------
    Annotate the R12 node list with income group information, mapping high income
    countries (HIC) and upper-middle income countries (UMC) into one group and
    aggregating by population.

    >>> cl_node = get_codelist(f"node/R12")
    >>> cl_ig = get_income_group_codelist()
    >>> replace = make_map({"HIC": "HMIC", "UMC": "HMIC"})
    >>> assign_income_groups(cl_node, cl_ig, replace=replace)
    >>> cl_node["R12_NAM"].get_annotation(id="wb-income-group").text
    HMIC
    """

    import sdmx
    from sdmx.model import v21

    replace = replace or dict()

    if method == "count":

        def get_weight(code: "sdmx.model.common.Code") -> float:
            """Weight of the country `code` in aggregation."""
            return 1.0

    elif method == "population":
        # Work around khaeru/sdmx#191: ensure the HTTPS URL is used
        client = sdmx.Client("WB_WDI")
        client.source.url = client.source.url.replace("http://", "https://")

        # Retrieve WB_WDI data for SERIES=SP_POP_TOTAL (Population, total)
        dm = client.data(
            "WDI", key="A.SP_POP_TOTL.", params=dict(startPeriod=2020, endPeriod=2020)
        )

        # Convert to pd.Series with multi-index with levels: REF_AREA, SERIES, FREQ,
        # TIME_PERIOD. Because of the query, there is only 1 value for each unique
        # REF_AREA.
        df = sdmx.to_pandas(dm.data[0])

        def get_weight(code: "sdmx.model.common.Code") -> float:
            """Return a weight for the country `code`: its total population."""
            try:
                return df[code.id].item()
            except KeyError:
                # log.debug(f"No population data for {code!r}; omitted")
                return 0
    else:  # pragma: no cover
        raise ValueError(f"method={method!r}")

    weight_info = {}  # For debugging

    # Iterate over nodes
    for node in cl_node:
        if not len(node.child):
            continue  # Country → skip

        # Total weight of different income groups among `node`'s countries
        weight: MutableMapping[Optional[str], float] = defaultdict(lambda: 0.0)

        # Iterate over countries
        for country in node.child:
            # Identify the income group of `country` from an annotation
            try:
                ig = str(
                    cl_income_group[country.id]
                    .get_annotation(id="wb-income-group")
                    .text
                )
                # Apply replacement to `ig`
                ig = replace.get(ig, ig)
            except KeyError:
                # country.id is not in cl_income_group, or no such annotation
                ig = None

            weight[ig] += get_weight(country)

        if {None} == set(weight):
            continue  # World node → no direct children that are countries

        # Sort weights and group IDs from largest/first alphabetically to smallest/last
        weight_sorted = sorted([(-v, k) for k, v in weight.items()])
        weight_info[node.id] = pd.Series({k: -v for v, k in weight_sorted})

        # Identify the income group with the largest weight; not None
        _, ig = next(filter(lambda item: item[1] is not None, weight_sorted))

        try:
            # Remove any existing annotation
            node.pop_annotation(id="wb-income-group")
        except KeyError:
            pass

        # Annotate the node
        node.annotations.append(v21.Annotation(id="wb-income-group", text=ig))

    log.debug(
        "(node, group) weights:\n"
        + pd.concat(weight_info, axis=1).fillna(0).to_string()
    )


def fetch_codelist(id: str) -> "sdmx.model.common.Codelist":
    """Retrieve code lists related to the WB World Development Indicators.

    In principle this could be done with :py:`sdmx.Client("WB_WDI").codelist(id)`, but
    the World Bank SDMX REST API does not support queries for a specific code list. See
    https://datahelpdesk.worldbank.org/knowledgebase/articles/1886701-sdmx-api-queries.

    :func:`fetch_codelist` retrieves http://api.worldbank.org/v2/sdmx/rest/codelist/WB/,
    the structure message containing *all* code lists; and extracts and returns the one
    with the given `id`.
    """
    import pooch
    import sdmx

    file = pooch.retrieve(
        url="https://api.worldbank.org/v2/sdmx/rest/codelist/WB/", known_hash=None
    )
    # Read the retrieved SDMX StructureMessage and extract the code list
    sm = cast("StructureMessage", sdmx.read_sdmx(file))

    return sm.codelist[id]


@lru_cache()
def get_income_group_codelist() -> "sdmx.model.common.Codelist":
    """Return a :class:`.Codelist` with World Bank income group information.

    The returned code list is a modified version of the one with URN
    ``…Codelist=WB:CL_REF_AREA_WDI(1.0)``, via :func:`.fetch_codelist`.

    This is augmented with information about the income group and lending category
    concepts as described at
    https://datahelpdesk.worldbank.org/knowledgebase/articles/906519

    The information is stored two ways:

    - Existing codes in the list like "HIC: High income" that designate groups of
      countries are associated with child codes that are designated as members of that
      country. These can be accessed at :attr:`Code.child
      <sdmx.model.common.Item.child>`.
    - Existing codes in the list like "ABW: Aruba" are annotated with:

      - :py:`id="wb-income-group"`: the URN of the income group code, for instance
        "urn:sdmx:org.sdmx.infomodel.codelist.Code=WB:CL_REF_AREA_WDI(1.0).HIC". This is
        an unambiguous reference to a code in the same list.
      - :py:`id="wb-lending-category"`: the name of the lending category, if any.

      These can be accessed using :attr:`Code.annotations
      <sdmx.model.common.AnnotableArtefact.annotations>`, :attr:`Code.get_annotation
      <sdmx.model.common.AnnotableArtefact.get_annotation>`, and other methods.
    """
    import pooch
    from sdmx.model import v21

    cl = fetch_codelist("CL_REF_AREA_WDI")

    @lru_cache()
    def urn_for(name: str) -> str:
        """Return the URN of a code in `cl`, given its `name`."""
        for code in cl:
            if str(code.name) == name:
                return code.urn
        raise ValueError(name)  # pragma: no cover

    # Fetch the file containing the classification
    file = pooch.retrieve(
        url="https://datacatalogfiles.worldbank.org/ddh-published/0037712/DR0090755/"
        "CLASS.xlsx",
        known_hash="sha256:"
        "1418a4fd6badb7c26ae2bc3a9bfef4903f3d9c54c1679f856e1dece3c729e935",
    )

    # Open the retrieved file
    ef = pd.ExcelFile(file)

    # Read the "List of economies" sheet → store wb-{income-group,lending-category}
    tmp = (
        pd.read_excel(ef, sheet_name="List of economies")
        .drop(["Economy", "Region"], axis=1)
        .dropna(subset=["Income group"], axis=0)
        .set_index("Code")
    )
    for code in cl:
        try:
            row = tmp.loc[code.id, :]
        except KeyError:
            # log.debug(f"Not in 'List of economies' sheet: {code!r}")
            continue

        # Annotate wb-income-group; map a value like "Low income" to a URN
        code.annotations.append(
            v21.Annotation(id="wb-income-group", text=urn_for(row["Income group"]))
        )

        try:
            code.annotations.append(
                v21.Annotation(id="wb-lending-category", text=row["Lending category"])
            )
        except ValueError:
            pass  # text was None → no value

    # Read the "Groups" sheet → assign hierarchy
    for group_id, group_df in (
        pd.read_excel(ef, sheet_name="Groups")
        .drop(["GroupName", "CountryName", "Unnamed: 4"], axis=1)
        .groupby("GroupCode")
    ):
        try:
            # Identify the Code for this group ID
            group = cl[group_id]
        except KeyError:
            # log.debug(f"Group {group_id!r} is not in {cl}")
            continue

        for child_id in sorted(group_df["CountryCode"]):
            try:
                group.append_child(cl[child_id])
            except KeyError:
                # log.debug(f"No code for child {child_id!r}")
                continue

        # log.debug(f"{cl[group_id]}: {len(cl[group_id].child)} children")

    # Read "Notes" sheet → append to description of `cl`
    tmp = "\n\n".join(pd.read_excel(ef, sheet_name="Notes").dropna()["Notes"])

    # Ensure the "en" localization exists
    cl.description.localizations.setdefault("en", "")
    cl.description.localizations["en"] += (
        "\n\nThis code list has been modified from the official version by the "
        "'message-ix-models' Python package to add annotations and hierarchy parsed "
        "from the World Bank income groups and lending categories as described at "
        "https://datahelpdesk.worldbank.org/knowledgebase/articles/906519. The original"
        f" Excel file parsed includes the following descriptive text:\n\n{tmp}"
    )

    return cl


def make_map(
    source: dict[str, str], expand_key_urn: bool = True, expand_value_urn: bool = False
) -> dict[str, str]:
    """Prepare the :py:`replace` parameter of :func:`assign_income_groups`.

    The result has one (`key`, `value`) for each in `source`.

    Parameters
    ----------
    expand_key_urn : bool
        If :obj:`True` (the default), replace each `key` from `source` with the URN for
        the code in ``CL_REF_AREA_WDI`` with :py:`id=key`.
    expand_value_urn : bool
        If :obj:`True`, replace each `value` from `source` with the URN for the code in
        ``CL_REF_AREA_WDI`` with :py:`id=value`.
    """
    # Retrieve the code list
    cl = fetch_codelist("CL_REF_AREA_WDI")

    result = dict()
    for key, value in source.items():
        key = cl[key].urn if expand_key_urn else key
        value = cl[value].urn if expand_value_urn else value
        result[key] = value

    return result
