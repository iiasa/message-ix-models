from copy import deepcopy
from typing import TYPE_CHECKING

from message_ix_models.util.sdmx import StructureFactory

if TYPE_CHECKING:
    from sdmx.model import common


class CL_TRANSPORT_SCENARIO(StructureFactory["common.Codelist"]):
    """SDMX code list ``IIASA_ECE:CL_CIRCEULAR_SCENARIO``.

    This code lists contains unique IDs for CircEUlar transport scenarios.
    """

    urn = "IIASA_ECE:CL_CIRCEULAR_SCENARIO"
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
