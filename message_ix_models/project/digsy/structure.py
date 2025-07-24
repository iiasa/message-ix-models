import sdmx.urn
from sdmx.model import common

from message_ix_models.util.sdmx import ItemSchemeEnumType, URNLookupEnum, read


def get_cl_scenario() -> "common.Codelist":
    """Return a code list with the identifiers of DIGSY scenarios."""
    cl: "common.Codelist" = common.Codelist(
        id="DIGSY_SCENARIO",
        maintainer=read("IIASA_ECE:AGENCIES")["IIASA_ECE"],
        version="0.1",
        is_final=True,
        is_external_reference=False,
    )

    for id_, name in (
        ("BASE", "Base scenario"),
        ("BEST", "Best case"),
        ("WORST", "Worst case"),
        ("_Z", "Not applicable"),
    ):
        c = cl.setdefault(id=id_)
        c.urn = sdmx.urn.make(c)

    return cl


class SCENARIO(URNLookupEnum, metaclass=ItemSchemeEnumType):
    def _get_item_scheme(self) -> "common.Codelist":
        return get_cl_scenario()
