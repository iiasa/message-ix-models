"""Tools for World Bank data."""
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sdmx.model.common

log = logging.getLogger(__name__)


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
