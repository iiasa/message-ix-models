"""Structural metadata for the DIGSY project."""

from sdmx.model.common import Codelist

from message_ix_models.util.sdmx import (
    ItemSchemeEnumType,
    StructureFactory,
    URNLookupEnum,
)


class CL_SCENARIO_DIGSY(StructureFactory[Codelist]):
    """List of codes for DIGSY scenarios."""

    urn = "IIASA_ECE:CL_SCENARIO_DIGSY"
    version = "0.2.0"

    @classmethod
    def create(cls) -> Codelist:
        cl = cls.maintainable(Codelist)

        for id_, name in (
            ("BASE", "Base scenario"),
            ("BEST-C", "Best case, conservative"),
            ("BEST-S", "Best case, stretch"),
            ("WORST-C", "Worst case, conservative"),
            ("WORST-S", "Worst case, stretch"),
            ("_Z", "Not applicable"),
        ):
            cl.setdefault(id=id_)

        return cl


class SCENARIO(URNLookupEnum, metaclass=ItemSchemeEnumType):
    """Enumeration of DIGSY scenario IDs."""

    def _get_item_scheme(self) -> Codelist:
        return CL_SCENARIO_DIGSY.get()
