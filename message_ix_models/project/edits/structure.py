"""Structural metadata for the EDITS project."""

from sdmx.model.common import Codelist

from message_ix_models.util.sdmx import (
    ItemSchemeEnumType,
    StructureFactory,
    URNLookupEnum,
)


class CL_SCENARIO_EDITS_MCE(StructureFactory[Codelist]):
    """List of codes for EDITS Model Complementarity Exercise (MCE) scenarios."""

    urn = "IIASA_ECE:CL_SCENARIO_EDITS_MCE"
    version = "0.2.0"

    @classmethod
    def create(cls) -> Codelist:
        cl = cls.maintainable(Codelist)

        for id_, name in (
            ("CA", "Current ambition"),
            ("HA", "High ambition"),
            ("_Z", "Not applicable"),
        ):
            cl.setdefault(id=id_)

        return cl


class SCENARIO(URNLookupEnum, metaclass=ItemSchemeEnumType):
    """Enumeration of EDITS MCE scenario IDs."""

    def _get_item_scheme(self) -> Codelist:
        return CL_SCENARIO_EDITS_MCE.get()
