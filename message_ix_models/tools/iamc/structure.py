"""IAMC structural metadata."""

from dataclasses import dataclass
from enum import Enum, auto
from textwrap import dedent
from typing import TYPE_CHECKING, TypedDict

from message_ix_models.util.sdmx import (
    AnnotationsMixIn,
    StructureFactory,
    read,
    register_agency,
)

if TYPE_CHECKING:
    from sdmx.model import common

    class Static(TypedDict):
        id: str
        characterization: list["CHARACTERIZATION"]
        description: str
        tier: "TIER"


__all__ = [
    "Annotations",
    "CHARACTERIZATION",
    "CL_SCENARIO_DIAGNOSTIC",
    "TIER",
]


class CHARACTERIZATION(Enum):
    """Labels appearing in column ‘Characterization’ of Table 1.

    The protocol does not give definitions for these labels.

    The label ‘Any’, appearing in the row for “*-SSPX”, is omitted. See the
    documentation of :class:`CL_SCENARIO_DIAGNOSTIC`.
    """

    Current_policies = auto()
    Exponential_pricing = auto()
    Land_protection = auto()
    Limiting_CCS = auto()
    Limiting_CDR = auto()
    Limiting_animal_products = auto()
    Limiting_biomass = auto()
    Linear_pricing = auto()
    No_Policy_baseline = auto()
    Shock_in_price = auto()
    additional_mitigation_policies = auto()
    reference = auto()


class TIER(Enum):
    """Labels appearing in column ‘Tier’ of Table 1.

    The label “Optional (strongly encouraged)”, appearing in the row for “*-SSPX”, is
    omitted.
    """

    Mandatory = auto()
    Optional = auto()


@dataclass
class Annotations(AnnotationsMixIn):
    """Set of annotations appearing on each Code in :class:`CL_SCENARIO_DIAGNOSTIC`."""

    #: Value from the ‘Tier’ column of Table 1.
    iamc_diagnostic_tier: TIER

    #: Value from the ‘Characterization’ column of Table 1.
    iamc_diagnostic_characterization: list[CHARACTERIZATION]

    #: The URN of a code identifying the SSP scenario to be used for sociodemographic
    #: data, for instance
    #: "urn:sdmx:org.sdmx.infomodel.codelist.Code=ICONICS:SSP(2024).1".
    SSP_URN: str = ""


C = CHARACTERIZATION
STATIC: tuple["Static", ...] = (
    dict(
        id="CP",
        tier=TIER.Mandatory,
        characterization=[C.Current_policies, C.reference],
        description="""
            The default current climate policy reference scenario (CP). Only implemented
            policies should be included, not targets that are not supported by policies
            (e.g. NDC). Climate policies can be explicitly modelled (preferably) or
            represented by a carbon price. If neither option is available, model teams
            can use their default No-Policy baseline. If a scenario deviates from CP, a
            description of the scenario assumptions should be provided.
            """,
    ),
    dict(
        id="C400-lin",
        tier=TIER.Mandatory,
        characterization=[C.Linear_pricing],
        description="""
            For t < 2030: Fix to CP. For t in [2030, 2100]:
            Price(t) = 35 USD + 18.25 USD * (t-2030) (USD400 reached in 2050). After
            2030, take C-price from CP if higher than Price(t), until Price(t) becomes
            higher. Then take Price(t).
            """,
    ),
    dict(
        id="C160-gr5",
        tier=TIER.Mandatory,
        characterization=[C.Exponential_pricing],
        description="""
            For t < 2030: Fix to CP. For t in [2030, 2100]:
            Price(t) = 160 USD * 1.05(t-2050) (USD160 reached in 2050). After 2030, take
            C-price from CP if higher than Price(t), until Price(t) becomes higher. Then
            take Price(t).
            """,
    ),
    dict(
        id="C80-gr5",
        tier=TIER.Mandatory,
        characterization=[C.Exponential_pricing],
        description="""
            For t < 2030: Fix to CP. For t in [2030, 2100]:
            Price(t) = 80 USD * 1.05(t-2050) (USD 80 reached in 2050). After 2030, take
            C-price from CP if higher than Price(t), until Price(t) becomes higher. Then
            take Price(t).
            """,
    ),
    dict(
        id="C0to400-lin",
        tier=TIER.Mandatory,
        characterization=[C.Linear_pricing, C.Shock_in_price],
        description="""
            Follow CP in the short run, but converge to Price(t) = 0 USD right before
            2050. For t in [2050, 2100]: price as C400-lin.
            """,
    ),
    dict(
        id="C400-lin-LimBio",
        tier=TIER.Optional,
        characterization=[C.Linear_pricing, C.Limiting_biomass],
        description="""
            Emission prices follow C400-lin. Global primary modern bioenergy supply
            (from any primary resource) is limited to 100 EJ. No constraints on
            traditional biomass use are applied.
            """,
    ),
    dict(
        id="C400-lin-LimCCS",
        tier=TIER.Optional,
        characterization=[C.Linear_pricing, C.Limiting_CCS],
        description="""
            Emission prices follow C400-lin. Global application of geological Carbon
            Capture and Storage (including fossil CCS, DACCS and BECCS) is limited to a
            maximum of 2 Gt CO2/yr. The total non-CCS carbon dioxide removal (CDR)
            should not exceed the total non-CCS CDR level of C400-lin. A margin of 0.5
            Gt CO2/yr in 2060 is allowed.
            """,
    ),
    dict(
        id="C400-lin-LimCDR",
        tier=TIER.Optional,
        characterization=[C.Linear_pricing, C.Limiting_CDR],
        description="""
            Limited carbon dioxide removal (CDR). Emission prices follow C400-lin.
            Includes limits from C400-lin-LimBio and C400-lin-LimCCS. In addition: this
            applies the following limits: 350 Mha for afforestation and reforestation
            (combined), 0.5 Gt CO₂/yr for biochar, 0 Gt CO₂/yr for ocean algae, 0.5 Gt
            CO₂/yr for enhanced weathering.
            """,
    ),
    dict(
        id="C400-lin-Diet",
        tier=TIER.Optional,
        characterization=[C.Linear_pricing, C.Limiting_animal_products],
        description="""
            Emission prices follow C400-lin. EAT-Lancet 2 diet is implemented from 2025
            to 2070. Only suitable for models that represent agriculture, land-use and
            natural environments.
            """,
    ),
    dict(
        id="C400-lin-BiodivProt",
        tier=TIER.Optional,
        characterization=[C.Linear_pricing, C.Land_protection],
        description="""
            Emission prices follow C400-lin. Implementing land use protection for
            biodiversity of 30% from 2030 to 2100 in line with the GBF (Kunming-Montreal
            Global Biodiversity Framework). Only suitable for models that represent
            agriculture, land-use and natural environments.
            """,
    ),
    dict(
        id="C400-lin-Policies",
        tier=TIER.Optional,
        characterization=[C.Linear_pricing, C.additional_mitigation_policies],
        description="""
            Emission prices follow C400-lin; plus all non-carbon price-related policies
            / modeling switches that a modeling team usually uses in “lowest
            stabilization” (e.g., 1.5°C) scenarios (teams need to provide the details of
            the original scenario). This would not mean harmonizing those policies, but
            just take the current “default” from that team. Comparing this run to the
            C400-lin would give an idea of how important additional policies are for a
            specific model/team.
            """,
    ),
    dict(
        id="No-Policy",
        tier=TIER.Optional,
        characterization=[C.No_Policy_baseline],
        description="""Counterfactual scenario with no climate policies and no
            carbon prices.""",
    ),
)


class CL_SCENARIO_DIAGNOSTIC(StructureFactory):
    """List of identifiers for IAMC diagnostic scenarios.

    This list transcribes Table 1 from the document “IAM community diagnostic assessment
    protocol”, `doi: 10.5281/zenodo.19554965
    <https://doi.org/10.5281/zenodo.19554965>`_ and adapts to the SDMX information
    model as follows:

    - :attr:`Code.id`: Text from the ‘Scenario name’ column, for instance :py:`"CP"`.
    - :attr:`Code.name`: Table 1 does not give a short, human-readable name. These are
      constructed as, for instance, "CP with SSP2".
    - :attr:`Code.description`: Text from the ‘Details’ column. Line breaks are
      discarded. Periods are inserted at the ends of some lines to avoid ambiguity.
      Missing spaces between magnitudes and units are inserted: for example, "100EJ"
      becomes "100 EJ".
    - :attr:`Code.annotations`: 3 annotations given by :class:`Annotations`.
    - “*-SSPX”: Table 1 describes these codes but does not give an explicit list. This
      class constructs the complete list. The description is:

         For any of the scenarios in the above (notably the mandatory ones), teams are
         encouraged to submit alternative SSP variations (other than SSP2), named
         *-SSPX, where * is the normal scenario name, and X is the SSP identifier. Note
         that in that case, teams need to submit the reference scenario and one or more
         of the pricing scenarios. For the default (SSP2-or similar-based) scenarios,
         no -SSPX suffix should be added.
    """

    urn = "IAMC:CL_SCENARIO_DIAGNOSTIC"
    version = "1.0.0"

    @classmethod
    def create(cls) -> "common.Codelist":
        from sdmx.model import common

        as_ = register_agency(common.Agency(id="IAMC"))
        IAMC = as_["IAMC"]

        cl: "common.Codelist" = common.Codelist(
            id=cls.urn.partition(":")[-1],
            maintainer=IAMC,
            version=cls.version,
            is_external_reference=False,
            is_final=True,
        )

        for ssp in read("ICONICS:SSP(2024)"):
            for kwargs in STATIC:
                a = Annotations(
                    iamc_diagnostic_characterization=kwargs["characterization"],
                    iamc_diagnostic_tier=kwargs["tier"],
                    SSP_URN=ssp.urn,
                )
                id_ = kwargs["id"] + (f"-SSP{ssp.id}" if ssp.id != "2" else "")
                name = kwargs["id"] + f" with SSP{ssp.id}"
                description = dedent(kwargs["description"]).strip()
                cl.append(
                    common.Code(
                        id=id_,
                        name=name,
                        description=description,
                        annotations=a.get_annotations(list),
                    )
                )

        return cl
