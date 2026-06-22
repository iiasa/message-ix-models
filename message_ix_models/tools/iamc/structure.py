"""IAMC structural metadata."""

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto
from textwrap import dedent
from typing import TYPE_CHECKING, Optional, TypedDict

from message_ix_models.util.sdmx import (
    AnnotationsMixIn,
    StructureFactory,
    read,
    register_agency,
)

if TYPE_CHECKING:
    from sdmx.model import common

    class Static(TypedDict, total=False):
        id: str
        characterization: list["CHARACTERIZATION"]
        description: str
        tier: "TIER"
        #: Carbon price path in 2010 USD/tCO₂ as a function of year.
        #: Returns 0.0 for years before PRICE_START_YR.
        #: None means price is model-derived (CP) or zero (No-Policy).
        price_schedule: Optional[Callable[[int], float]]


__all__ = [
    "Annotations",
    "CHARACTERIZATION",
    "CL_SCENARIO_DIAGNOSTIC",
    "PRICE_START_YR",
    "PROTOCOL_CONV",
    "TIER",
    "protocol_price_co2",
]

# ---------------------------------------------------------------------------
# Unit conversion constants
# ---------------------------------------------------------------------------
# Protocol prices are in 2010 USD/tCO₂.
# MESSAGEix tax_emission is in 2005 USD/tC.
# PROTOCOL_CONV converts: price_model = price_protocol * PROTOCOL_CONV
# ---------------------------------------------------------------------------
_CO2_TO_C: float = 12.0 / 44.0           # tCO₂ → tC
_USD2010_TO_USD2005: float = 1.0 / 1.10774  # 2010 USD → 2005 USD
PROTOCOL_CONV: float = (1.0 / _CO2_TO_C) * _USD2010_TO_USD2005

#: First year in which protocol carbon price paths are active (Table 2).
PRICE_START_YR: int = 2030


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
        id=”CP”,
        tier=TIER.Mandatory,
        characterization=[C.Current_policies, C.reference],
        price_schedule=None,  # price is model-derived (current policies)
        description=”””
            The default current climate policy reference scenario (CP). Only implemented
            policies should be included, not targets that are not supported by policies
            (e.g. NDC). Climate policies can be explicitly modelled (preferably) or
            represented by a carbon price. If neither option is available, model teams
            can use their default No-Policy baseline. If a scenario deviates from CP, a
            description of the scenario assumptions should be provided.
            “””,
    ),
    dict(
        id=”C400-lin”,
        tier=TIER.Mandatory,
        characterization=[C.Linear_pricing],
        # Table 2: Price(t) = 35 + 18.25*(t-2030) USD2010/tCO₂; USD 400 reached in 2050
        price_schedule=lambda t: max(0.0, 35.0 + 18.25 * (t - 2030)),
        description=”””
            For t < 2030: Fix to CP. For t in [2030, 2100]:
            Price(t) = 35 USD + 18.25 USD * (t-2030) (USD400 reached in 2050). After
            2030, take C-price from CP if higher than Price(t), until Price(t) becomes
            higher. Then take Price(t).
            “””,
    ),
    dict(
        id=”C160-gr5”,
        tier=TIER.Mandatory,
        characterization=[C.Exponential_pricing],
        # Table 2: Price(t) = 160 * 1.05^(t-2050) USD2010/tCO₂; USD 160 reached in 2050
        price_schedule=lambda t: 160.0 * (1.05 ** (t - 2050)),
        description=”””
            For t < 2030: Fix to CP. For t in [2030, 2100]:
            Price(t) = 160 USD * 1.05(t-2050) (USD160 reached in 2050). After 2030, take
            C-price from CP if higher than Price(t), until Price(t) becomes higher. Then
            take Price(t).
            “””,
    ),
    dict(
        id=”C80-gr5”,
        tier=TIER.Mandatory,
        characterization=[C.Exponential_pricing],
        # Table 2: Price(t) = 80 * 1.05^(t-2050) USD2010/tCO₂; USD 80 reached in 2050
        price_schedule=lambda t: 80.0 * (1.05 ** (t - 2050)),
        description=”””
            For t < 2030: Fix to CP. For t in [2030, 2100]:
            Price(t) = 80 USD * 1.05(t-2050) (USD 80 reached in 2050). After 2030, take
            C-price from CP if higher than Price(t), until Price(t) becomes higher. Then
            take Price(t).
            “””,
    ),
    dict(
        id=”C0to400-lin”,
        tier=TIER.Mandatory,
        characterization=[C.Linear_pricing, C.Shock_in_price],
        # Table 2: Zero before 2050; C400-lin formula from 2050 onward
        price_schedule=lambda t: max(0.0, 35.0 + 18.25 * (t - 2030)) if t >= 2050 else 0.0,
        description=”””
            Follow CP in the short run, but converge to Price(t) = 0 USD right before
            2050. For t in [2050, 2100]: price as C400-lin.
            “””,
    ),
    dict(
        id=”C400-lin-LimBio”,
        tier=TIER.Optional,
        characterization=[C.Linear_pricing, C.Limiting_biomass],
        # Same carbon price path as C400-lin; additional bioenergy limit applied separately
        price_schedule=lambda t: max(0.0, 35.0 + 18.25 * (t - 2030)),
        description=”””
            Emission prices follow C400-lin. Global primary modern bioenergy supply
            (from any primary resource) is limited to 100 EJ. No constraints on
            traditional biomass use are applied.
            “””,
    ),
    dict(
        id=”C400-lin-LimCCS”,
        tier=TIER.Optional,
        characterization=[C.Linear_pricing, C.Limiting_CCS],
        # Same carbon price path as C400-lin; additional CCS limit applied separately
        price_schedule=lambda t: max(0.0, 35.0 + 18.25 * (t - 2030)),
        description=”””
            Emission prices follow C400-lin. Global application of geological Carbon
            Capture and Storage (including fossil CCS, DACCS and BECCS) is limited to a
            maximum of 2 Gt CO2/yr. The total non-CCS carbon dioxide removal (CDR)
            should not exceed the total non-CCS CDR level of C400-lin. A margin of 0.5
            Gt CO2/yr in 2060 is allowed.
            “””,
    ),
    dict(
        id=”C400-lin-LimCDR”,
        tier=TIER.Optional,
        characterization=[C.Linear_pricing, C.Limiting_CDR],
        # Same carbon price path as C400-lin; LimBio + LimCCS + additional CDR limits
        price_schedule=lambda t: max(0.0, 35.0 + 18.25 * (t - 2030)),
        description=”””
            Limited carbon dioxide removal (CDR). Emission prices follow C400-lin.
            Includes limits from C400-lin-LimBio and C400-lin-LimCCS. In addition: this
            applies the following limits: 350 Mha for afforestation and reforestation
            (combined), 0.5 Gt CO₂/yr for biochar, 0 Gt CO₂/yr for ocean algae, 0.5 Gt
            CO₂/yr for enhanced weathering.
            “””,
    ),
    dict(
        id=”C400-lin-Diet”,
        tier=TIER.Optional,
        characterization=[C.Linear_pricing, C.Limiting_animal_products],
        # Same carbon price path as C400-lin; EAT-Lancet 2 diet constraint is land-use specific
        price_schedule=lambda t: max(0.0, 35.0 + 18.25 * (t - 2030)),
        description=”””
            Emission prices follow C400-lin. EAT-Lancet 2 diet is implemented from 2025
            to 2070. Only suitable for models that represent agriculture, land-use and
            natural environments.
            “””,
    ),
    dict(
        id=”C400-lin-BiodivProt”,
        tier=TIER.Optional,
        characterization=[C.Linear_pricing, C.Land_protection],
        # Same carbon price path as C400-lin; 30% land protection is land-use specific
        price_schedule=lambda t: max(0.0, 35.0 + 18.25 * (t - 2030)),
        description=”””
            Emission prices follow C400-lin. Implementing land use protection for
            biodiversity of 30% from 2030 to 2100 in line with the GBF (Kunming-Montreal
            Global Biodiversity Framework). Only suitable for models that represent
            agriculture, land-use and natural environments.
            “””,
    ),
    dict(
        id=”C400-lin-Policies”,
        tier=TIER.Optional,
        characterization=[C.Linear_pricing, C.additional_mitigation_policies],
        # Same carbon price path as C400-lin; additional non-price policies are team-specific
        price_schedule=lambda t: max(0.0, 35.0 + 18.25 * (t - 2030)),
        description=”””
            Emission prices follow C400-lin; plus all non-carbon price-related policies
            / modeling switches that a modeling team usually uses in “lowest
            stabilization” (e.g., 1.5°C) scenarios (teams need to provide the details of
            the original scenario). This would not mean harmonizing those policies, but
            just take the current “default” from that team. Comparing this run to the
            C400-lin would give an idea of how important additional policies are for a
            specific model/team.
            “””,
    ),
    dict(
        id=”No-Policy”,
        tier=TIER.Optional,
        characterization=[C.No_Policy_baseline],
        price_schedule=None,  # no climate policies, no carbon price
        description=”””Counterfactual scenario with no climate policies and no
            carbon prices.”””,
    ),
)


def protocol_price_co2(scenario_id: str, year: int) -> float:
    """Return protocol carbon price in 2010 USD/tCO₂ for *scenario_id* at *year*.

    Looks up the ``price_schedule`` callable registered in :data:`STATIC` for
    *scenario_id* (stripping any ``-SSPX`` suffix). Returns 0.0 for years
    before :data:`PRICE_START_YR` or for scenarios with no price schedule
    (CP, No-Policy).

    Parameters
    ----------
    scenario_id :
        Scenario identifier, e.g. ``"C400-lin"`` or ``"C400-lin-SSP1"``.
    year :
        Model year.

    Returns
    -------
    float
        Carbon price in 2010 USD/tCO₂.

    Raises
    ------
    ValueError
        If *scenario_id* (after stripping SSP suffix) is not in :data:`STATIC`.
    """
    if year < PRICE_START_YR:
        return 0.0
    base_id = scenario_id.split("-SSP")[0]
    for entry in STATIC:
        if entry["id"] == base_id:
            fn = entry.get("price_schedule")
            return fn(year) if fn is not None else 0.0
    raise ValueError(
        f"Unknown IAMC diagnostic scenario {scenario_id!r}. "
        f"Valid ids: {[e['id'] for e in STATIC]}"
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
