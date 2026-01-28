from typing import TYPE_CHECKING

from message_ix_models.util.sdmx import StructureFactory

if TYPE_CHECKING:
    from sdmx.model import common


class CL_TRANSPORT_SCENARIO(StructureFactory["common.Codelist"]):
    """SDMX code list ``IIASA_ECE:CL_CIRCEULAR_TRANSPORT_SCENARIO``.

    This code lists contains unique IDs for CircEUlar transport scenarios.
    """

    urn = "IIASA_ECE:CL_CIRCEULAR_TRANSPORT_SCENARIO"
    version = "1.0.0"

    @classmethod
    def create(cls) -> "common.Codelist":
        from sdmx.model import common

        from message_ix_models.util.sdmx import read

        # Other data structures
        IIASA_ECE = read("IIASA_ECE:AGENCIES")["IIASA_ECE"]

        cl: "common.Codelist" = common.Codelist(
            id=cls.urn.partition(":")[-1],
            maintainer=IIASA_ECE,
            version=cls.version,
            is_external_reference=False,
            is_final=True,
        )

        for id_, market, fuel_economy in (
            # 'Narrow' is one of the following 2
            ("_CC_C_D_D", "Compact car", "default"),
            ("_CC_C_I_D", "Compact car", "improvement"),
            # 'Slow', 'Close', and 'SSP' are one of the following 2
            ("_CT_C_D_D", "Continuing trends", "default"),
            ("_CT_C_I_D", "Continuing trends", "improvement"),
            # Sensitivity cases
            ("_ES_C_D_D", "Extreme SUVs", "default"),
            ("_ES_C_I_D", "Extreme SUVs", "improvement"),
            ("_NoS_C_D_D", "No SUVs", "default"),
            ("_NoS_C_I_D", "No SUVs", "improvement"),
        ):
            cl.append(
                common.Code(
                    id=id_,
                    name=f"{market}, {fuel_economy}",
                    description="regional=convergence, material=default",
                )
            )

        return cl
