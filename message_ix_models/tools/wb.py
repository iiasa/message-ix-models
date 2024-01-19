"""Tools for World Bank data."""
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, MutableMapping, Optional

if TYPE_CHECKING:
    import sdmx.model.common

log = logging.getLogger(__name__)


def assign_income_groups(
    cl_node: "sdmx.model.common.Codelist",
    cl_income_group: "sdmx.model.common.Codelist",
    method: str = "population",
) -> None:
    """Annotate `cl_node` with income groups. .

    Each node is assigned an |Annotation| with :py:`id="wb-income-group"`, according to
    the income groups of its children (countries), as reflected in `cl_income_group`
    (see :func:`.get_income_group_codelist`).

    Parameters
    ----------
    method : "population" or "count"
        Method for aggregating

        - :py:`"population"` (default): the WB World Development Indicators (WDI) 2020
          population for each country is used as a weight, so that the node's income
          group is the income group of the plurality of the population of its children.
        - :py:`"count"`: each country is weighted equally, so that the node's income
          group is the mode (most frequently occurring value) of its childrens'.
    """
    import sdmx
    import sdmx.model.v21 as m

    if method == "count":

        def weight(code: "sdmx.model.common.Code") -> float:
            """Weight of the country `code` in aggregation."""
            return 1.0

    elif method == "population":
        # Retrieve WB_WDI data for SERIES=SP_POP_TOTAL (Population, total)
        dm = sdmx.Client("WB_WDI").data(
            "WDI", key="A.SP_POP_TOTL.", params=dict(startperiod=2020, endperiod=2020)
        )

        # Convert to pd.Series with multi-index with levels: REF_AREA, SERIES, FREQ,
        # TIME_PERIOD. Because of the query, there is only 1 value for each unique
        # REF_AREA.
        df = sdmx.to_pandas(dm.data[0])

        def weight(code: "sdmx.model.common.Code") -> float:
            """Return a weight for the country `code`: its total population."""
            try:
                return df[code.id].item()
            except KeyError:
                log.warning(f"No population data for {code!r}")
                return 0
    else:
        raise ValueError(f"method={method!r}")

    # Iterate over nodes
    for node in cl_node:
        if not len(node.child):
            continue  # Country → skip

        # Count of appearances of different income groups among `node`'s countries
        count: MutableMapping[Optional[str], float] = defaultdict(lambda: 0.0)

        # Iterate over countries
        for country in node.child:
            # Identify the income group of `country` from an annotation
            try:
                ig = str(
                    cl_income_group[country.id]
                    .get_annotation(id="wb-income-group")
                    .text
                )
            except KeyError:
                # country.id is not in cl_income_group *or* no such annotation
                ig = None

            # TODO apply a mapping to `ig`

            count[ig] += weight(country)

        if {None} == set(count):
            continue  # World node → no direct children that are countries

        # Sort counts from highest to lowest
        count_sorted = sorted([(v, k) for k, v in count.items()], reverse=True)
        log.debug(f"{node}: {count_sorted}")

        # Identify the income group with the highest count; not None
        for N, ig in count_sorted:
            if ig is not None:
                break

        # Annotate the node
        node.annotations.append(m.Annotation(id="wb-income-group", text=ig))


def get_income_group_codelist() -> "sdmx.model.common.Codelist":
    """Return a |Codelist| with World Bank income group information.

    The returned code list is a modified version of the one with URN
    ``…Codelist=WB:CL_REF_AREA_WDI(1.0)`` as described at
    https://datahelpdesk.worldbank.org/knowledgebase/articles/1886701-sdmx-api-queries
    and available from http://api.worldbank.org/v2/sdmx/rest/codelist/WB/.

    This is augmented with information about the income group and lending category
    concepts as described at
    https://datahelpdesk.worldbank.org/knowledgebase/articles/906519

    The information is stored two ways:

    - Existing codes in the list like "HIC: High income" that designate groups of
      countries are associated with child codes that are designated as members of that
      country. These can be accessed at :attr:`Code.child
      <sdmx.model.common.Item.child>`.
    - Existing codes in the list like "ABW: Aruba" are annotated with:

      - :py:`id="wb-income-group"`: the name of the income group, for instance
        "High income".
      - :py:`id="wb-lending-category"`: the name of the lending category, if any.

      These can be accessed using :attr:`Code.annotations
      <sdmx.model.common.AnnotableArtefact.annotations>`, :attr:`Code.get_annotation
      <sdmx.model.common.AnnotableArtefact.get_annotation>`, and other methods.
    """
    import pandas as pd
    import pooch
    import sdmx
    import sdmx.model.v21 as m

    # Retrieve the WB WDI related code lists
    # NB Would prefer to use sdmx.Client("WB_WDI").codelist("CL_REF_AREA_WDI"), but the
    #    World Bank SDMX REST API does not support queries for a specific code list.
    file = pooch.retrieve(
        url="http://api.worldbank.org/v2/sdmx/rest/codelist/WB/",
        known_hash=None,
    )
    # Read the retrieved SDMX StructureMessage and extract the code list
    sm = sdmx.read_sdmx(file)
    cl = sm.codelist["CL_REF_AREA_WDI"]

    # Retrieve the file containing the classification
    file = pooch.retrieve(
        url="https://datacatalogfiles.worldbank.org/ddh-published/0037712/DR0090755/"
        "CLASS.xlsx",
        known_hash="sha256:"
        "9b8452db52e391602c9e9e4d4ef4d254f505ce210ce6464497cf3e40002a3545",
    )

    # Open the retrieved file
    ef = pd.ExcelFile(file)

    # Read the "List of economies" sheet
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
            log.debug(f"Not in 'List of economies' sheet: {code!r}")
            continue

        code.annotations.append(
            m.Annotation(id="wb-income-group", text=row["Income group"])
        )

        try:
            code.annotations.append(
                m.Annotation(id="wb-lending-category", text=row["Lending category"])
            )
        except ValueError:
            pass

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
            log.info(f"No code for group {group_id!r}")
            continue

        for child_id in sorted(group_df["CountryCode"]):
            try:
                group.append_child(cl[child_id])
            except KeyError:
                log.debug(f"No code for child {child_id!r}")
                continue

        log.info(f"{cl[group_id]}: {len(cl[group_id].child)} children")

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
