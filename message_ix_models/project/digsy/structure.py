"""Structural metadata for the DIGSY project."""

import sdmx.urn
from sdmx.model import common

from message_ix_models.util.sdmx import ItemSchemeEnumType, URNLookupEnum, read


def get_cl_scenario() -> "common.Codelist":
    """Return a code list with the identifiers of DIGSY scenarios."""
    cl: "common.Codelist" = common.Codelist(
        id="DIGSY_SCENARIO",
        maintainer=read("IIASA_ECE:AGENCIES")["IIASA_ECE"],
        version="0.2",
        is_final=True,
        is_external_reference=False,
    )

    for id_, name in (
        ("BASE", "Base scenario"),
        ("BEST-C", "Best case, conservative"),
        ("BEST-S", "Best case, stretch"),
        ("WORST-C", "Worst case, conservative"),
        ("WORST-S", "Worst case, stretch"),
        ("_Z", "Not applicable"),
    ):
        c = cl.setdefault(id=id_)
        c.urn = sdmx.urn.make(c)

    return cl


class SCENARIO(URNLookupEnum, metaclass=ItemSchemeEnumType):
    """Enumeration of DIGSY scenario IDs."""

    def _get_item_scheme(self) -> "common.Codelist":
        return get_cl_scenario()
