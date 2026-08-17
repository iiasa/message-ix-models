"""Transport Futures project."""

from sdmx.model.common import Codelist

from message_ix_models.util.sdmx import (
    ItemSchemeEnumType,
    StructureFactory,
    URNLookupEnum,
)


class CL_SCENARIO_FUTURES(StructureFactory[Codelist]):
    """Code lists with identifiers of Transport Futures scenarios."""

    urn = "IIASA_ECE:CL_SCENARIO_FUTURES"
    version = "1.0"

    @classmethod
    def create(cls) -> Codelist:
        cl = cls.maintainable(Codelist)

        cl.setdefault(id="BASE", name="Base scenario")
        cl.setdefault(id="A___", name="Activity")
        cl.setdefault(id="AS__", name="Activity, structure")
        cl.setdefault(id="ASI_", name="Activity, structure, intensity")
        cl.setdefault(id="ASIF", name="Activity, structure, intensity, fuels")
        cl.setdefault(id="DEBUG", name="Additional scenario for debugging")

        return cl


class SCENARIO(URNLookupEnum, metaclass=ItemSchemeEnumType):
    """Enumeration of Transport Futures scenario IDs."""

    def _get_item_scheme(self) -> Codelist:
        return CL_SCENARIO_FUTURES.get()
