"""Input data flows for MESSAGEix-Transport, read from CSV files.

See :ref:`transport-data-files` for documentation of the data flows in :data:`FILES`.
"""

import logging
from collections.abc import Iterator
from functools import cache
from typing import TYPE_CHECKING, cast

from message_ix_models.util import package_data_path
from message_ix_models.util.sdmx import DATAFLOW, STORE, Dataflow

if TYPE_CHECKING:
    import sdmx.message
    import sdmx.model.common


log = logging.getLogger(__name__)


@cache
def common_structures() -> "sdmx.model.common.ConceptScheme":
    """Return common structures for use in the current module."""
    import sdmx.urn
    from sdmx.model.common import ConceptScheme

    from message_ix_models.util.sdmx import get_version, read

    # Create a shared concept scheme with…
    # - Same maintainer "IIASA_ECE" as in "IIASA_ECE:AGENCIES".
    # - Version based on the current version of message_ix_models.
    # - Final and not an external reference
    cs = ConceptScheme(
        id="CS_MESSAGE_TRANSPORT",
        maintainer=read("IIASA_ECE:AGENCIES")["IIASA_ECE"],
        version=get_version(),
        is_final=False,
        is_external_reference=False,
    )
    cs.urn = sdmx.urn.make(cs)

    STORE.add(cs)

    return cs


def collect_structures() -> "sdmx.message.StructureMessage":
    """Collect all SDMX data structures from :data:`FILES` and store.

    The structural metadata are written to :file:`transport-in.xml`.
    """
    from sdmx.message import StructureMessage

    from message_ix_models.util import sdmx

    sm = StructureMessage()

    for df in DATAFLOW.values():
        # Add both the DFD and DSD
        sdmx.collect_structures(sm, df.df)

    sdmx.write(sm, basename="transport-in")

    return sm


def iter_files() -> Iterator[tuple[str, "Dataflow"]]:
    other: set[Dataflow] = set(DATAFLOW.values())

    for name, df in globals().items():
        try:
            other.remove(df)
        except (KeyError, TypeError):
            pass
        else:
            yield (name, df)

    for df in other:
        yield ("", df)


def _make_dataflow(**kwargs) -> "Dataflow":
    """Shorthand for data flows from this module."""
    common_structures()  # Ensure CS_MESSAGE_TRANSPORT exists

    kwargs.setdefault("module", __name__)
    desc = kwargs.setdefault("description", "")
    kwargs["description"] = f"{desc.strip()}\n\nInput data for MESSAGEix-Transport."
    kwargs.setdefault("i_o", Dataflow.FLAG.IN)
    kwargs.setdefault("cs_urn", ("ConceptScheme=IIASA_ECE:CS_MESSAGE_TRANSPORT",))

    return Dataflow(**kwargs)


def read_structures() -> "sdmx.message.StructureMessage":
    """Read structural metadata from :file:`transport-in.xml`."""
    import sdmx

    with open(package_data_path("sdmx", "transport-in.xml"), "rb") as f:
        return cast("sdmx.message.StructureMessage", sdmx.read_sdmx(f))


act_non_ldv = _make_dataflow(
    path="act-non_ldv.csv",
    key="activity:n-t-y:non-ldv+exo",
    name="Fixed activity of non-LDV technologies.",
    units="dimensionless",
)


activity_freight = _make_dataflow(
    key="freight activity:n:exo",
    name="Freight transport activity",
    units="Gt / km",
)

activity_ldv = _make_dataflow(
    key="ldv activity:n:exo",
    name="Activity (driving distance) per light duty vehicle",
    units="km / year",
)

age_ldv = _make_dataflow(
    path="ldv-age",
    key="age:n-t-y:ldv+exo",
    name="Mean age of LDVs as of the model base period",
    units="years",
)

cap_new_ldv = _make_dataflow(
    path="ldv-new-capacity",
    key="cap_new:nl-t-yv:ldv+exo",
    name="New capacity values for LDVs",
    description="""Applied as historical_new_capacity and bound_new_capacity_{lo,up}
values for LDVs""",
    units="MVehicle",
    required=False,
)

class_ldv = _make_dataflow(
    path="ldv-class",
    dims=("n", "vehicle_class"),
    name="Share of light-duty vehicles by class",
    units="dimensionless",
    required=False,
)

disutility = _make_dataflow(
    key="disutility:n-cg-t-y:per vehicle",
    name="Disutility cost of LDV usage",
    units="kUSD / vehicle",
)

demand_scale = _make_dataflow(
    key="demand-scale:n-y",
    name="Scaling of total demand relative to base year levels",
    units="dimensionless",
)

# NB This differs from fuel_emi_intensity in including (a) a 't[echnology]' dimension
#    and (b) more and non-GHG species.
emi_intensity = _make_dataflow(
    key="emissions intensity:t-c-e:transport",
    path="emi-intensity",
    name="Emissions intensity of fuel use",
    units="g / EJ",
)

energy_other = _make_dataflow(
    key="energy:c-n:transport other",
    path="energy-other",
    name="2020 demand for other transport energy",
    units="TJ",
    required=False,
)

# NB This differs from emi_intensity in (a) having no 't[echnology]' dimension and (b)
#    including only CO₂.
fuel_emi_intensity = _make_dataflow(
    key="fuel-emi-intensity:c-e",
    name="GHG emissions intensity of fuel use",
    description="""Values are in GWP-equivalent mass of carbon, not in mass of the
emissions species.""",
    units="tonne / kWa",
)

ikarus_availability = _make_dataflow(
    path=("ikarus", "availability"),
    dims=("source", "t", "c", "y"),
    name="Availability of non-LDV transport technologies",
    description="""- 'source' is either "IKARUS" or "Krey/Linßen".""",
    units="km / a",
)

ikarus_fix_cost = _make_dataflow(
    path=("ikarus", "fix_cost"),
    dims=("source", "t", "c", "y"),
    name="Fixed cost of non-LDV transport technologies",
    description="Costs are per vehicle.",
    units="kEUR_2000",
)

ikarus_input = _make_dataflow(
    path=("ikarus", "input"),
    dims=("source", "t", "c", "y"),
    name="Input energy intensity of non-LDV transport technologies",
    units="GJ / (100 vehicle km)",
)

ikarus_inv_cost = _make_dataflow(
    path=("ikarus", "inv_cost"),
    dims=("source", "t", "c", "y"),
    name="Investment/capital cost of non-LDV transport technologies",
    units="MEUR_2000",
)

ikarus_technical_lifetime = _make_dataflow(
    path=("ikarus", "technical_lifetime"),
    dims=("source", "t", "c", "y"),
    name="Technical lifetime of non-LDV transport technologies",
    units="year",
)

ikarus_var_cost = _make_dataflow(
    path=("ikarus", "var_cost"),
    dims=("source", "t", "c", "y"),
    name="Variable cost of non-LDV transport technologies",
    units="EUR_2000 / (100 vehicle km)",
)

input_adj_ldv = _make_dataflow(
    key="ldv input adj:n-scenario:exo",
    name="Calibration factor for LDV fuel economy",
    units="dimensionless",
)

input_base = _make_dataflow(
    path="input-base",
    key="input:t-c-h:base",
    name="Base model input efficiency",
    units="GWa",
)

input_ref_ldv = _make_dataflow(
    path="ldv-input-ref",
    key="fuel economy:nl-m:ldv+ref",
    name="Reference fuel economy for LDVs",
    units="GWa / (Gvehicle km)",
    required=False,
)

input_share = _make_dataflow(
    key="input-share:t-c-y:exo",
    name="Share of input of LDV technologies from each commodity",
    units="dimensionless",
)

lifetime_ldv = _make_dataflow(
    key="lifetime:nl-yv:ldv+exo",
    path="lifetime-ldv",
    name="Technical lifetime (maximum age) of LDVs",
    description="""Values are filled forwards. In MESSAGE(V)-Transport, this quantity
had the additional dimension of driver_type, and values were 20 years for
driver_type='average', 15 y for 'moderate', and 10 y for 'frequent'.""",
    units="year",
)

load_factor_ldv = _make_dataflow(
    key="load factor ldv:scenario-n-y:exo",
    name="Load factor (occupancy) of LDVs",
    description="""Units are implicitly passengers per vehicle.""",
    units="dimensionless",
)

load_factor_nonldv = _make_dataflow(
    key="load factor nonldv:t:exo",
    name="Load factor (occupancy) of non-LDV passenger vehicles",
    units="passenger / vehicle",
)

mer_to_ppp = _make_dataflow(
    key="mer to ppp:n-y",
    name="Conversion from market exchange rate (MER) to PPP",
    units="dimensionless",
    required=False,
)

mode_share_freight = _make_dataflow(
    key="freight mode share:n-t:exo",
    path="freight-mode-share-ref",
    name="Mode shares of freight activity in the model base period",
    units="dimensionless",
)

pdt_cap_proj = _make_dataflow(
    key="P activity:scenario-n-t-y:exo",
    path="pdt-cap",
    name="Projected passenger-distance travelled (PDT) per capita",
    units="km / year",
    required=False,
)

pdt_cap_ref = _make_dataflow(
    key="pdt:n:capita+ref",
    path="pdt-cap-ref",
    name="Reference (historical) passenger-distance travelled (PDT) per capita",
    units="km / year",
)

pdt_elasticity = _make_dataflow(
    key="pdt elasticity:scenario-n-y:exo",
    name="“Elasticity” of PDT-capita with respect to GDP",
    units="dimensionless",
)

pop_share_attitude = _make_dataflow(
    path=("ma3t", "attitude"),
    dims=("attitude",),
    name="Share of population by technology propensity/attitude",
    units="dimensionless",
)

pop_share_cd_at = _make_dataflow(
    path=("ma3t", "population"),
    dims=("census_division", "area_type"),
    name="Share of population by census division and area type",
    description="Values sum to roughly 1 across 'area_type' for each census_division.",
    units="dimensionless",
)

pop_share_driver = _make_dataflow(
    path=("ma3t", "driver"),
    dims=("census_division", "area_type", "driver_type"),
    name="Share of population by driver type, census_division, and area_type",
    description="""Values sum to roughly 1 across 'area_type' for each combination of
other dimensions.""",
    units="dimensionless",
)

population_suburb_share = _make_dataflow(
    key="population suburb share:n-y:exo",
    name="Share of MSA population that is suburban",
    units="dimensionless",
    required=False,
)

speed = _make_dataflow(
    key="speed:scenario-n-t-y:exo",
    name="Vehicle speed",
    description="""
- This is the mean value for all vehicles of all technologies for the given mode.
- The code that handles this file interpolates on the ‘year’ dimension.""",
    units="km / hour",
)

t_share_ldv = _make_dataflow(
    path="ldv-t-share",
    key="tech share:n-t:ldv+exo",
    name="Share of total stock for LDV technologies",
    description="""
- Values must sum to 1 across the 't' dimension.
- Technology codes annotated "historical-only: True" (e.g. ICE_L_ptrp) must be omitted
  or have zero values. If not, incompatible/infeasible constraint values are created.
""",
    units="dimensionless",
)
