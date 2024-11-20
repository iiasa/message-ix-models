"""Compute MESSAGEix-compatible input data for MESSAGEix-Transport."""

import logging
from collections import defaultdict
from collections.abc import Callable, Mapping
from copy import deepcopy
from functools import partial
from operator import le
from typing import TYPE_CHECKING, Optional

import pandas as pd
from genno import Computer, Key, Quantity
from genno.core.key import single_key
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.tools.exo_data import ExoDataSource, register_source
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
from message_ix_models.util.ixmp import rename_dims

if TYPE_CHECKING:
    from sdmx.model.v21 import Code

log = logging.getLogger(__name__)


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


class IEA_Future_of_Trucks(ExoDataSource):
    """Retrieve IEA “Future of Trucks” data.

    Parameters
    ----------
    measure : int
        One of:

        1. energy intensity of vehicle distance travelled
        2. load
        3. energy intensity of freight service (mass × distance)
    """

    id = "iea-future-of-trucks"

    convert_units: Optional[str] = None

    _name_unit = {
        1: ("energy intensity of VDT", "GWa / (Gv km)"),
        2: ("load factor", None),
        3: ("energy intensity of FV", None),
    }

    def __init__(self, source, source_kw):
        if not source == "IEA Future of Trucks":
            raise ValueError

        self.measure = source_kw.pop("measure")
        self.name, self._unit = self._name_unit[self.measure]
        self.path = package_data_path("transport", f"iea-2017-t4-{self.measure}.csv")

    def __call__(self):
        from genno.operator import load_file

        return load_file(self.path, dims=rename_dims())

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
        share = Quantity(
            xr.DataArray([0.1, 0.3, 0.6], coords=[("t", ["LCV", "MFT", "HFT"])])
        )

        # Add tasks
        k = base_key
        # Map from IEA source nodes to target nodes
        c.add(k + "1", "map_as_qty", map_node, [])
        c.add(k + "2", "broadcast_map", base_key, k + "1", rename={"n2": "n"})
        # Weight by share of freight activity
        result = c.add(k + "3", "sum", k + "2", weights=share, dimensions=["t"])

        if self.convert_units:
            result = c.add(k + "4", "convert_units", k + "3", units=self.convert_units)

        return single_key(result)


class MaybeAdaptR11Source(ExoDataSource):
    """Source of transport data, possibly adapted from R11 for other node code lists.

    Parameters
    ----------
    source_kw :
       Must include exactly the keys "measure", "nodes", and "scenario".
    """

    #: Set of measures recognized by a subclass.
    measures: set[str] = set()

    #: Mapping from :attr:`.measures` entries to file names.
    filename: Mapping[str, str] = dict()

    _adapter: Optional[Callable] = None

    def __init__(self, source, source_kw):
        from .util import path_fallback

        # Check that the given measure is supported by the current class
        if not source == self.id:
            raise ValueError(source)
        measure = source_kw.pop("measure", None)
        if measure not in self.measures:
            raise ValueError(measure)
        else:
            self.measure = measure

        # ID of the node code list
        nodes = source_kw.pop("nodes")

        # Scenario identifier
        self.scenario = source_kw.pop("scenario", None)

        # Dimensions for loaded data
        self.dims = deepcopy(rename_dims())
        self.dims["scenario"] = "scenario"

        self.raise_on_extra_kw(source_kw)

        filename = self.filename[measure]
        try:
            self.path = path_fallback(nodes, filename)
            self._repr = f"Load {self.path}"
        except FileNotFoundError:
            log.info(f"Fall back to R11 data for {self.measure}")
            self.path = path_fallback("R11", filename)

            # Identify an adapter that can convert data from R11 to `nodes`
            self._adapter = {"R12": adapt_R11_R12, "R14": adapt_R11_R14}.get(nodes)
            self._repr = f"Load {self.path} and adapt R11 → {nodes}"

            if self._adapter is None:
                log.warning(
                    f"Not implemented: transform {self.id} data from 'R11' to {nodes!r}"
                )
                raise NotImplementedError

    def __call__(self):
        from genno.operator import load_file

        return load_file(self.path, dims=self.dims, name=self.measure)

    def __repr__(self) -> str:
        return self._repr

    def get_keys(self) -> tuple[Key, Key]:
        """Return the target keys for the (1) raw and (2) transformed data."""
        k = self.key or Key(
            self.name or self.measure.lower(), ("n", "y") + self.extra_dims
        )
        return (k * "scenario" + self.id, k)

    def transform(self, c: "Computer", base_key: Key) -> Key:
        # Apply self.adapt, if any
        if self._adapter:
            k0 = base_key + "0"
            c.add(base_key + "0", self._adapter, base_key)
        else:
            k0 = base_key

        # Select on the 'scenario' dimension, if any
        k1 = k0 / "scenario" + "1"
        c.add(k1, "maybe_select", k0, indexers={"scenario": self.scenario})

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


# Attempt to register each source; tolerate exceptions if the model is re-imported
# FIXME Should not be necessary; improve register_source upstream
for cls in IEA_Future_of_Trucks, MERtoPPP:
    try:
        register_source(cls)  # type: ignore [type-abstract]
    except ValueError as e:
        log.info(str(e))
