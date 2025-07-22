"""Input and output data flows for MESSAGEix-Transport.

See :ref:`transport-data-files` for documentation of the input data flows.
"""

import logging
from collections import defaultdict
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from functools import cache, partial
from itertools import chain
from operator import le
from typing import TYPE_CHECKING, Optional, cast

import genno
import pandas as pd
from genno import Computer, Key
from genno.core.key import single_key
from ixmp.report.common import RENAME_DIMS
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.tools.exo_data import BaseOptions, ExoDataSource
from message_ix_models.util import (
    adapt_R11_R12,
    adapt_R11_R14,
    broadcast,
    make_io,
    make_matched_dfs,
    make_source_tech,
    merge_data,
    package_data_path,
    same_node,
)
from message_ix_models.util.sdmx import DATAFLOW, STORE, Dataflow

if TYPE_CHECKING:
    import sdmx.message
    import sdmx.model.common
    from genno.types import AnyQuantity
    from sdmx.model.v21 import Code

log = logging.getLogger(__name__)

IEA_EWEB_FLOW = [
    "AVBUNK",
    "DOMESAIR",
    "DOMESNAV",
    "PIPELINE",
    "RAIL",
    "ROAD",
    "TOTTRANS",
    "TRNONSPE",
    "WORLDAV",
    "WORLDMAR",
]


class IEA_Future_of_Trucks(ExoDataSource):
    """Retrieve IEA “Future of Trucks” data.

    Parameters
    ----------
    measure : int
        One of the keys of ;attr:`name_unit`.
    """

    @dataclass
    class Options(BaseOptions):
        measure: str = "0"
        convert_units: Optional[str] = None

    options: Options

    #: Mapping from :attr:`Options.measure` to name and unit.
    name_unit = {
        1: ("energy intensity of VDT", "GWa / (Gv km)"),
        2: ("load factor", None),
        3: ("energy intensity of FV", None),
    }

    def __init__(self, *args, **kwargs) -> None:
        self.options = self.Options.from_args(self, *args, **kwargs)

        self.options.name, self._unit = self.name_unit[int(self.options.measure)]
        self.path = package_data_path(
            "transport", f"iea-2017-t4-{self.options.measure}.csv"
        )
        super().__init__()

    def get(self) -> "AnyQuantity":
        from genno.operator import load_file

        return load_file(self.path, dims=RENAME_DIMS)

    def transform(self, c: "Computer", base_key: Key) -> Key:
        import xarray as xr

        # Broadcast to regions. map_as_qty() expects columns in from/to order.
        map_node = pd.DataFrame(
            [
                ("R12_CHN", "CHN"),
                ("R12_EEU", "EU28"),
                ("R12_NAM", "USA"),
                ("R12_SAS", "IND"),
                ("R12_WEU", "EU28"),
                # Assumed similarity
                ("R12_AFR", "IND"),
                ("R12_FSU", "CHN"),
                ("R12_LAM", "USA"),
                ("R12_MEA", "CHN"),
                ("R12_PAO", "CHN"),
                ("R12_PAS", "CHN"),
                ("R12_RCPA", "CHN"),
            ],
            columns=["n2", "n"],
        )[["n", "n2"]]

        # Share of freight activity; transcribed from figure 18, page 38
        share = genno.Quantity(
            xr.DataArray([0.1, 0.3, 0.6], coords=[("t", ["LCV", "MFT", "HFT"])])
        )

        # Add tasks
        k = base_key
        # Map from IEA source nodes to target nodes
        c.add(k[1], "map_as_qty", map_node, [])
        c.add(k[2], "broadcast_map", base_key, k[1], rename={"n2": "n"})
        # Weight by share of freight activity
        result = c.add(k[3], "sum", k[2], weights=share, dimensions=["t"])

        if self.options.convert_units:
            result = c.add(
                k[4], "convert_units", k[3], units=self.options.convert_units
            )

        return single_key(result)


class MaybeAdaptR11Source(ExoDataSource):
    """Source of transport data, possibly adapted from R11 for other node code lists.

    Parameters
    ----------
    source_kw :
       Must include exactly the keys "measure", "nodes", and "scenario".
    """

    @dataclass
    class Options(BaseOptions):
        #: ID of the node code list.
        nodes: str = ""

        #: Scenario identifier.
        scenario: str = ""

    options: Options

    #: Set of measures recognized by a subclass.
    measures: set[str] = set()

    #: Mapping from :attr:`.measures` entries to file names.
    filename: Mapping[str, str] = dict()

    _adapter: Optional[Callable] = None

    def __init__(self, *args, **kwargs) -> None:
        from .util import region_path_fallback

        opt = self.options = self.Options.from_args(self, *args, **kwargs)
        super().__init__()  # Create .key

        # Check that the given measure is supported by the current class
        if opt.measure not in self.measures:
            raise ValueError(opt.measure)

        # Dimensions for loaded data
        self.dims = RENAME_DIMS | dict(scenario="scenario")

        filename = self.filename[opt.measure]
        try:
            self.path = region_path_fallback(opt.nodes, filename)
            self._repr = f"Load {self.path}"
        except FileNotFoundError:
            log.info(f"Fall back to R11 data for {self.options.measure}")
            self.path = region_path_fallback("R11", filename)
            self._repr = f"Load {self.path} and adapt R11 → {opt.nodes}"

            # Identify an adapter that can convert data from R11 to `nodes`
            self._adapter = {"R12": adapt_R11_R12, "R14": adapt_R11_R14}.get(opt.nodes)
            if self._adapter is None:
                msg = (
                    f"transform {type(self).__name__} data from 'R11' to {opt.nodes!r}"
                )
                log.warning(f"Not implemented: {msg}")
                raise NotImplementedError(msg)

    def get(self) -> "AnyQuantity":
        from genno.operator import load_file

        return load_file(self.path, dims=self.dims, name=self.options.measure)

    def __repr__(self) -> str:
        return self._repr

    def transform(self, c: "Computer", base_key: Key) -> Key:
        # Apply self.adapt, if any
        if self._adapter:
            k0 = base_key[0]
            c.add(base_key[0], self._adapter, base_key)
        else:
            k0 = base_key

        # Select on the 'scenario' dimension, if any
        k1 = (k0 / "scenario")[1]
        c.add(k1, "maybe_select", k0, indexers={"scenario": self.options.scenario})

        return k1


class MERtoPPP(MaybeAdaptR11Source):
    """Provider of exogenous MERtoPPP data.

    Parameters
    ----------
    source_kw :
       Must include exactly the keys "measure" (must be "MERtoPPP") and "nodes" (the ID
       of the node code list).
    """

    id = "transport MERtoPPP"
    measures = {"MERtoPPP"}
    filename = {"MERtoPPP": "mer-to-ppp.csv"}


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


def conversion(
    nodes: list[str], years: list[int], config: dict
) -> dict[str, pd.DataFrame]:
    """Input and output data for conversion technologies:

    The technologies are named 'transport {service} load factor'.
    """
    common = dict(
        year_vtg=years,
        year_act=years,
        mode="all",
        # No subannual detail
        time="year",
        time_origin="year",
        time_dest="year",
    )

    service_info = [
        # ("freight", config["transport"].load_factor["freight"], "Gt km"),
        ("pax", 1.0, "Gp km / a"),
    ]

    data0: Mapping[str, list] = defaultdict(list)
    for service, factor, output_unit in service_info:
        i_o = make_io(
            (f"transport {service} vehicle", "useful", "Gv km"),
            (f"transport {service}", "useful", output_unit),
            factor,
            on="output",
            technology=f"transport {service} load factor",
            **common,
        )
        for par, df in i_o.items():
            data0[par].append(df.pipe(broadcast, node_loc=nodes).pipe(same_node))

    data1 = {par: pd.concat(dfs) for par, dfs in data0.items()}

    data1.update(
        make_matched_dfs(
            base=data1["input"],
            capacity_factor=1,
            technical_lifetime=10,
        )
    )

    return data1


def dummy_supply(technologies: list["Code"], info, config) -> dict[str, pd.DataFrame]:
    """Dummy fuel supply for the bare RES."""
    if not config["transport"].dummy_supply:
        return dict()

    # Identify (level, commodity) from `technologies`
    level_commodity = set()
    for input_info in map(lambda t: t.eval_annotation(id="input"), technologies):
        if input_info is None or input_info.get("level", None) == "useful":
            continue  # No `input` annotation, or an LDV usage pseudo-technology
        commodity = input_info["commodity"]
        if isinstance(commodity, str):
            level_commodity.add(("final", commodity))
        elif isinstance(commodity, list):
            level_commodity.update(("final", c) for c in commodity)
        else:
            raise TypeError(type(commodity))

    result: dict[str, pd.DataFrame] = dict()
    common = dict(mode="all", time="year", time_dest="year", unit="GWa")
    values = dict(output=1.0, var_cost=1.0)

    # Make one source technology for each (level, commodity)
    for level, c in sorted(level_commodity):
        t = f"DUMMY supply of {c}"
        merge_data(
            result,
            make_source_tech(
                info, dict(commodity=c, level=level, technology=t) | common, **values
            ),
        )

    return result


_DEFAULT_MODULE = __name__.rpartition(".")[0]


def iter_files(module: str = _DEFAULT_MODULE) -> Iterator[tuple[str, "Dataflow"]]:
    """Iterate over all :class:`Dataflows <.Dataflow>` defined in :mod:`.transport`."""
    # Dataflows defined in the current module
    df_local: set[Dataflow] = set(
        [v for v in globals().values() if isinstance(v, Dataflow)]
    )
    # Other Dataflow instances
    df_other: set[Dataflow] = set(DATAFLOW.values()) - df_local

    yield from filter(
        lambda x: isinstance(x[1], Dataflow) and x[1].module.startswith(module),
        chain(globals().items(), map(lambda y: ("", y), df_other)),
    )


def misc(info: ScenarioInfo, nodes: list[str], y: list[int]):
    """Miscellaneous bounds for calibration/vetting."""

    # Limit activity of methanol LDVs in the model base year
    # TODO investigate the cause of the underlying behaviour; then remove this
    name = "bound_activity_up"
    data = {
        name: make_df(
            name,
            technology="ICAm_ptrp",
            year_act=y[0],
            mode="all",
            time="year",
            value=0.0,
            # unit=info.units_for("technology", "ICAm_ptrp"),
            unit="Gv km",
        ).pipe(broadcast, node_loc=nodes)
    }

    log.info("Miscellaneous bounds for calibration/vetting")
    return data


def navigate_ele(
    nodes: list[str], techs: list["Code"], t_groups, years: list[int], config
) -> dict[str, pd.DataFrame]:
    """Return constraint data for :attr:`ScenarioFlags.ELE`.

    The text reads as follows as of 2023-02-15:

    1. Land-based transport: Fuel/technology mandates ensure full electrification (BEV
       and/or FCEV) of passenger vehicles and light-duty trucks by 2040.
    2. Because there are much larger hurdles for full electrification of heavy-duty
       vehicles (Gray et al., 2021; Mulholland et al., 2018), we only assume a phase-out
       of diesel engines in the fleet of heavy-duty vehicles (HDV) by 2040.
    3. We assume that electric short-haul planes become available after 2050 (based on
       Barzkar & Ghassemi, 2020).
    4. Further, we assume full electrification of ports (and a reduction of auxiliary
       engines needed in ships) by 2030. In alignment with this, vessels are adapted to
       zero-emission berth standards by 2040. This timeline for port electrification is
       loosely based on Gillingham & Huang (2020) and the Global EV Outlook (IEA,
       2020a). Assuming that ships spend approximately 15% of the time at berth and that
       15% of their total fuel consumption is related to the auxiliary engine, we assume
       that 2.3% of the total fuel consumption can be saved by cold ironing (in line
       with Bauman et al., 2017).
    5. Fuels standards/mandates, infrastructure development and removing blending
       restrictions increase the use of alternative fuels (biofuels/electrofuels).
       Following the Sustainable Development Scenario (IEA, 2020b) the share of hydrogen
       in final energy demand grows to 40% in the aviation sector and to 50% in the
       shipping sector by 2070. The share of biofuels increases to 15% for both the
       aviation and shipping sector.

    Currently only items (1) and (2) are implemented.
    """
    from message_ix_models.project.navigate import T35_POLICY

    if not (T35_POLICY.ELE & config["transport"].project["navigate"]):
        return dict()

    # Technologies to constrain for items (1) and (2)
    to_constrain = []

    # Item (1): identify LDV technologies with inputs other than electricity or hydrogen
    for code in map(lambda t: techs[techs.index(t)], t_groups["t"]["LDV"]):
        input_info = code.eval_annotation("input")

        if input_info.get("commodity") not in ("electr", "hydrogen"):
            to_constrain.append(code.id)

    # Item (2): identify diesel-fueled freight truck technologies
    for code in map(lambda t: techs[techs.index(t)], t_groups["t"]["F ROAD"]):
        if "diesel" in str(code.description):
            to_constrain.append(code.id)

    # Create data
    name = "bound_new_capacity_up"
    data = make_df(name, value=0, unit="Gv km").pipe(
        broadcast,
        node_loc=nodes,
        technology=to_constrain,
        year_vtg=list(filter(partial(le, 2040), years)),
    )

    return {name: data}


def prepare_computer(c: Computer):
    """Add miscellaneous transport data."""
    # Data-generating calculations
    n, y = "n::ex world", "y::model"
    for comp in (
        (conversion, n, y, "config"),
        (misc, "info", n, y),
        (dummy_supply, "t::transport", "info", "config"),
        (navigate_ele, n, "t::transport", "t::transport agg", y, "config"),
    ):
        # Add 2 computations: one to generate the data
        name = getattr(comp[0], "__name__")
        k1 = c.add(f"{name}::ixmp", *comp)
        # …one to add it to `scenario`
        c.add("transport_data", f"transport {name}", key=k1)


def read_structures() -> "sdmx.message.StructureMessage":
    """Read structural metadata from :file:`transport-in.xml`."""
    import sdmx

    with open(package_data_path("sdmx", "transport-in.xml"), "rb") as f:
        return cast("sdmx.message.StructureMessage", sdmx.read_sdmx(f))


# Input data flows


def _input_dataflow(**kwargs) -> "Dataflow":
    """Shorthand for input data flows."""
    common_structures()  # Ensure CS_MESSAGE_TRANSPORT exists

    kwargs.setdefault("module", __name__)
    desc = kwargs.setdefault("description", "")
    kwargs["description"] = f"{desc.strip()}\n\nInput data for MESSAGEix-Transport."
    kwargs.setdefault("i_o", Dataflow.FLAG.IN)
    kwargs.setdefault("cs_urn", ("ConceptScheme=IIASA_ECE:CS_MESSAGE_TRANSPORT",))
    kwargs.setdefault("version", "2025.3.11")

    return Dataflow(**kwargs)


act_non_ldv = _input_dataflow(
    path="act-non_ldv.csv",
    key="activity:n-t-y:non-ldv+exo",
    name="Fixed activity of non-LDV technologies.",
    units="dimensionless",
)


activity_freight = _input_dataflow(
    key="freight activity:n:exo",
    name="Freight transport activity",
    units="Gt / km",
)

activity_ldv = _input_dataflow(
    key="ldv activity:scenario-n-y:exo",
    name="Activity (driving distance) per light duty vehicle",
    units="km / year",
)

age_ldv = _input_dataflow(
    path="ldv-age",
    key="age:n-t-y:ldv+exo",
    name="Mean age of LDVs as of the model base period",
    units="years",
)

cap_new_ldv = _input_dataflow(
    path="ldv-new-capacity",
    key="cap_new:nl-t-yv:ldv+exo",
    name="New capacity values for LDVs",
    description="""Applied as historical_new_capacity and bound_new_capacity_{lo,up}
values for LDVs.

In particular, values up to 2015 (the final period before |y0|) are used for
:py:`historical_new_capacity`. Values from |y0| onwards are used for
:py:`bound_new_capacity_lo` and :py:`bound_new_capacity_up`.
""",
    units="Mvehicle",
    required=False,
)

class_ldv = _input_dataflow(
    path="ldv-class",
    dims=("n", "vehicle_class"),
    name="Share of light-duty vehicles by class",
    description="Empty or missing values are treated as zero.",
    units="dimensionless",
    required=False,
)

constraint_dynamic = _input_dataflow(
    key="constraint-dynamic:t-c-name",
    name="Values for dynamic constraints",
    units="dimensionless",
)

disutility = _input_dataflow(
    key="disutility:n-cg-t-y:per vehicle",
    name="Disutility cost of LDV usage",
    units="kUSD / vehicle",
)

demand_scale = _input_dataflow(
    key="demand-scale:n-y",
    name="Scaling of total demand relative to base year levels",
    units="dimensionless",
)

elasticity_f = _input_dataflow(
    key="elasticity:scenario-n-y:F+exo",
    id="elasticity_f",
    path="elasticity-f",
    name="‘Elasticity’ of freight activity with respect to GDP(PPP)",
    units="dimensionless",
)

elasticity_p = _input_dataflow(
    key="elasticity:scenario-n-y:P+exo",
    id="elasticity_p",
    path="elasticity-p",
    name="‘Elasticity’ of PDT per capita with respect to GDP(PPP) per capita",
    description="The code that handles this input data flow interpolates on the ‘year’ "
    "dimension. The ‘node’ dimension is optional; if not provided, values are broadcast"
    " across all existing nodes.",
    units="dimensionless",
)

# NB This differs from fuel_emi_intensity in including (a) a 't[echnology]' dimension
#    and (b) more and non-GHG species.
emi_intensity = _input_dataflow(
    key="emissions intensity:t-c-e:transport",
    path="emi-intensity",
    name="Emissions intensity of fuel use",
    description="In particular, the units are mass of emissions species 'e' per MJ of "
    "input energy commodity 'c'",
    units="g / EJ",
)

energy_other = _input_dataflow(
    key="energy:c-n:transport other",
    path="energy-other",
    name="2020 demand for OTHER transport energy",
    units="TJ",
    required=False,
)

# NB This differs from emi_intensity in (a) having no 't[echnology]' dimension and (b)
#    including only CO₂.
fuel_emi_intensity = _input_dataflow(
    key="fuel-emi-intensity:c-e",
    name="GHG emissions intensity of fuel use",
    description="""Values are in GWP-equivalent mass of carbon, not in mass of the
emissions species.""",
    units="tonne / kWa",
)

ikarus_availability = _input_dataflow(
    path=("ikarus", "availability"),
    dims=("source", "t", "c", "y"),
    name="Availability of non-LDV transport technologies",
    description="""- 'source' is either "IKARUS" or "Krey/Linßen".""",
    units="km / a",
)

ikarus_fix_cost = _input_dataflow(
    path=("ikarus", "fix_cost"),
    dims=("source", "t", "c", "y"),
    name="Fixed cost of non-LDV transport technologies",
    description="Costs are per vehicle.",
    units="kEUR_2000",
)

ikarus_input = _input_dataflow(
    path=("ikarus", "input"),
    dims=("source", "t", "c", "y"),
    name="Input energy intensity of non-LDV transport technologies",
    units="GJ / (hectovehicle km)",
)

ikarus_inv_cost = _input_dataflow(
    path=("ikarus", "inv_cost"),
    dims=("source", "t", "c", "y"),
    name="Investment/capital cost of non-LDV transport technologies",
    units="MEUR_2000",
)

ikarus_technical_lifetime = _input_dataflow(
    path=("ikarus", "technical_lifetime"),
    dims=("source", "t", "c", "y"),
    name="Technical lifetime of non-LDV transport technologies",
    units="year",
)

ikarus_var_cost = _input_dataflow(
    path=("ikarus", "var_cost"),
    dims=("source", "t", "c", "y"),
    name="Variable cost of non-LDV transport technologies",
    units="EUR_2000 / (hectovehicle km)",
)

input_adj_ldv = _input_dataflow(
    key="ldv input adj:n-scenario:exo",
    name="Calibration factor for LDV fuel economy",
    units="dimensionless",
)

input_base = _input_dataflow(
    path="input-base",
    key="input:t-c-h:base",
    name="Base model input efficiency",
    units="GWa",
)

input_ref_ldv = _input_dataflow(
    path="ldv-input-ref",
    key="fuel economy:nl-m:ldv+ref",
    name="Reference fuel economy for LDVs",
    units="GWa / (Gvehicle km)",
    required=False,
)

input_share = _input_dataflow(
    key="input-share:t-c-y:exo",
    name="Share of input of LDV technologies from each commodity",
    units="dimensionless",
)

lifetime_ldv = _input_dataflow(
    key="lifetime:scenario-nl-t-yv:ldv+exo",
    path="lifetime-ldv",
    name="Technical lifetime (maximum age) of LDVs",
    description="""Values are interpolated across the model horizon. In MESSAGE(V)-
Transport, this quantity had the additional dimension of driver_type, and values were 20
years for driver_type='average', 15 y for 'moderate', and 10 y for 'frequent'.""",
    units="year",
)

load_factor_ldv = _input_dataflow(
    key="load factor ldv:scenario-n-y:exo",
    name="Load factor (occupancy) of LDVs",
    description="""Units are implicitly passengers per vehicle.""",
    units="dimensionless",
)

load_factor_nonldv = _input_dataflow(
    key="load factor nonldv:t:exo",
    name="Load factor (occupancy) of non-LDV passenger vehicles",
    units="passenger / vehicle",
)

mer_to_ppp = _input_dataflow(
    key="mer to ppp:n-y",
    name="Conversion from market exchange rate (MER) to PPP",
    units="dimensionless",
    required=False,
)

mode_share_freight = _input_dataflow(
    key="freight mode share:n-t:exo",
    path="freight-mode-share-ref",
    name="Mode shares of freight activity in the model base period",
    units="dimensionless",
)

pdt_cap_proj = _input_dataflow(
    key="P activity:scenario-n-t-y:exo",
    path="pdt-cap",
    name="Projected passenger-distance travelled (PDT) per capita",
    units="km / year",
    required=False,
)

pdt_cap_ref = _input_dataflow(
    key="pdt:n:capita+ref",
    path="pdt-cap-ref",
    name="Reference (historical) passenger-distance travelled (PDT) per capita",
    description="In particular, this is the PDT per capita in the model base year, "
    "currently 2020",
    units="km / year",
)

pop_share_attitude = _input_dataflow(
    path=("ma3t", "attitude"),
    dims=("attitude",),
    name="Share of population by technology propensity/attitude",
    units="dimensionless",
)

pop_share_cd_at = _input_dataflow(
    path=("ma3t", "population"),
    dims=("census_division", "area_type"),
    name="Share of population by census division and area type",
    description="Values sum to roughly 1 across 'area_type' for each census_division.",
    units="dimensionless",
)

pop_share_driver = _input_dataflow(
    path=("ma3t", "driver"),
    dims=("census_division", "area_type", "driver_type"),
    name="Share of population by driver type, census_division, and area_type",
    description="""Values sum to roughly 1 across 'area_type' for each combination of
other dimensions.""",
    units="dimensionless",
)

population_suburb_share = _input_dataflow(
    key="population suburb share:n-y:exo",
    name="Share of MSA population that is suburban",
    units="dimensionless",
    required=False,
)

speed = _input_dataflow(
    key="speed:scenario-n-t-y:exo",
    name="Vehicle speed",
    description="""
- This is the mean value for all vehicles of all technologies for the given mode.
- The code that handles this file interpolates on the ‘year’ dimension.""",
    units="km / hour",
)

t_share_ldv = _input_dataflow(
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

# Output data flows (for reporting / model integration)


def _output_dataflow(**kwargs) -> "Dataflow":
    """Shorthand for output data flows."""
    common_structures()  # Ensure CS_MESSAGE_TRANSPORT exists

    kwargs.setdefault("module", __name__)
    kwargs.setdefault("units", "dimensionless")  # FIXME Look up the correct units
    kwargs.setdefault("i_o", Dataflow.FLAG.OUT)
    desc = kwargs.setdefault("description", "")
    kwargs["description"] = f"{desc.strip()}\n\nOutput data from MESSAGEix-Transport."
    kwargs.setdefault("cs_urn", ("ConceptScheme=IIASA_ECE:CS_MESSAGE_TRANSPORT",))
    kwargs.setdefault("version", "2025.3.11")

    return Dataflow(**kwargs)


activity_passenger = _output_dataflow(
    id="ACTIVITY_PASSENGER",
    name="Passenger activity",
    key="pdt:n-y-t",
    units="dimensionless",
)
activity_vehicle = _output_dataflow(
    id="ACTIVITY_VEHICLE",
    name="Vehicle activity",
    description='Same as the IAMC ‘variable’ code "Energy Service|Transportation".',
    key="out:nl-t-ya-c:transport+units",
)
fe_transport = _output_dataflow(
    id="FE_TRANSPORT",
    name="Final energy",
    description='Same as the IAMC ‘variable’ code "Final Energy|Transportation".',
    key="in:nl-t-ya-c:transport+units",
)
gdp_in = _output_dataflow(
    id="GDP_IN",
    name="GDP",
    description="Pass-through of MESSAGEix-Transport input data to aid processing and "
    "interpretation of other outputs.",
    key="gdp:n-y",
)
population_in = _output_dataflow(
    id="POPULATION_IN",
    name="Population",
    description="Pass-through of MESSAGEix-Transport input data to aid processing and "
    "interpretation of other outputs.",
    key="pop:n-y",
)
