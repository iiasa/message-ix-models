from copy import deepcopy
from typing import TYPE_CHECKING

from message_ix_models.util.sdmx import StructureFactory

if TYPE_CHECKING:
    from sdmx.model import common


class CL_SCENARIO(StructureFactory["common.Codelist"]):
    """List of codes for CircEUlar integrated (‘BMT’) scenarios."""

    urn = "IIASA_ECE:CL_SCENARIO_CIRCEULAR"
    version = "1.0.0"

    @classmethod
    def create(cls) -> "common.Codelist":
        from sdmx.model import common

        from message_ix_models.util.sdmx import read

        # Other data structures
        IIASA_ECE = read("IIASA_ECE:AGENCIES")["IIASA_ECE"]

        cl: "common.Codelist" = common.Codelist(
            id=cls.urn.partition(":")[-1],
            name="CircEUlar integrative scenarios for MESSAGEix-GLOBIOM",
            maintainer=IIASA_ECE,
            version=cls.version,
            is_external_reference=False,
            is_final=True,
        )

        cl.setdefault(
            id="R",
            name="CircEUlar ‘reference’ scenario",
            description="This scenario is based on SSP2.",
        )
        cl.setdefault(
            id="C",
            name="CircEUlar ‘close’ scenario",
            description="This scenario is based on ‘reference’.",
        )
        cl.setdefault(
            id="N",
            name="CircEUlar ‘narrow’ scenario",
            description="This scenario is based on ‘reference’.",
        )
        cl.setdefault(
            id="S",
            name="CircEUlar ‘slow’ scenario",
            description="This scenario is based on ‘reference’.",
        )
        cl.setdefault(
            id="A",
            name="CircEUlar ‘all-in’ scenario",
            description="This scenario includes all of the close, narrow, and slow "
            "narrative elements. It is based on ‘reference’.",
        )
        cl.setdefault(
            id="E",
            name="CircEUlar ‘efficiency’ scenario",
            description="This is a variant of the ‘all-in’ scenario in which energy "
            "efficiency also improves. It is based on SSP2",
        )

        # TODO Add 1 or more policy variant of some or all of the above, with distinct
        #      IDs
        # TODO Add association to elements from CL_TRANSPORT_SCENARIO below.

        return cl


class CL_SCENARIO_TRANSPORT(StructureFactory["common.Codelist"]):
    """List of unique IDs for CircEUlar transport scenarios.

    These scenarios provide the realizations of :class:`CL_SCENARIO` in
    :mod:`.model.transport`.
    """

    urn = "IIASA_ECE:CL_SCENARIO_CIRCEULAR_TRANSPORT"
    version = "1.1.0"

    @classmethod
    def create(cls) -> "common.Codelist":
        from sdmx.model import common

        from message_ix_models.model.transport.config import CL_SCENARIO
        from message_ix_models.util.sdmx import read

        # Other data structures
        IIASA_ECE = read("IIASA_ECE:AGENCIES")["IIASA_ECE"]

        # Retrieve the code "M SSP2" from IIASA_ECE:CL_TRANSPORT_SCENARIO.
        # The annotations on this code control .model.transport.build().
        transport_ssp2 = CL_SCENARIO.get()["M SSP2"]

        cl: "common.Codelist" = common.Codelist(
            id=cls.urn.partition(":")[-1],
            maintainer=IIASA_ECE,
            version=cls.version,
            is_external_reference=False,
            is_final=True,
        )

        for id_, market, fuel_economy in (
            # 'Narrow' is one of the following 2
            ("CC-C-D-D", "Compact car", "default"),
            ("CC-C-I-D", "Compact car", "improvement"),
            # 'Slow', 'Close', and 'SSP' are one of the following 2
            ("CT-C-D-D", "Continuing trends", "default"),
            ("CT-C-I-D", "Continuing trends", "improvement"),
            # Sensitivity cases
            ("ES-C-D-D", "Extreme SUVs", "default"),
            ("ES-C-I-D", "Extreme SUVs", "improvement"),
            ("NoS-C-D-D", "No SUVs", "default"),
            ("NoS-C-I-D", "No SUVs", "improvement"),
        ):
            cl.append(
                common.Code(
                    id=id_,
                    name=f"{market}, {fuel_economy}",
                    description="regional=convergence, material=default",
                    annotations=deepcopy(transport_ssp2.annotations),
                )
            )

        return cl
