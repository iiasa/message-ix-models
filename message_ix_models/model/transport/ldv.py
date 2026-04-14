"""Data for light-duty vehicles (LDVs) for passenger transport."""

import logging
from collections.abc import Mapping
from operator import itemgetter
from typing import TYPE_CHECKING, cast

import genno
from genno import Computer, Key, Keys
from message_ix import make_df
from sdmx.model.common import Code

from message_ix_models.model import disutility
from message_ix_models.util import (
    ScenarioInfo,
    broadcast,
    convert_units,
    make_matched_dfs,
    merge_data,
    same_node,
)
from message_ix_models.util.genno import Collector

from . import key as K
from . import util
from .data import MaybeAdaptR11Source
from .emission import ef_for_input
from .util import COMMON, wildcard

if TYPE_CHECKING:
    from genno.types import AnyQuantity

    from message_ix_models.types import ParameterData

    from .config import Config

log = logging.getLogger(__name__)

#: Shorthand for tags on keys.
Li = "::LDV+ixmp"

#: Mapping from :mod:`message_ix` parameter dimensions to source dimensions in some
#: quantities.
DIMS = util.DIMS | dict(node_dest="n", node_loc="n", node_origin="n")

#: Target key that collects all data generated in this module.
TARGET = f"transport{Li}"


class LDV(MaybeAdaptR11Source):
    """Provider of exogenous data on LDVs.

    Parameters
    ----------
    source_kw :
       Must include exactly the keys "measure" (must be one of "fuel economy",
       "fix_cost", or "inv_cost"), "nodes", and "scenario".
    """

    measures = {"inv_cost", "fuel economy", "fix_cost"}

    #: Names of expected files given :attr:`measure`.
    filename = {
        "inv_cost": "ldv-inv_cost.csv",
        "fuel economy": "ldv-fuel-economy.csv",
        "fix_cost": "ldv-fix_cost.csv",
    }

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Use "exo" tag on the target key, to align with existing code in this module
        self.key = Key(f"{self.options.measure}:n-t-y:LDV+exo")


collect = Collector(TARGET, "{}::LDV+ixmp".format)


def prepare_computer(c: Computer):
    """Set up `c` to compute parameter data for light-duty-vehicle technologies.

    Results in a key :data:`TARGET` that triggers computation of :mod:`ixmp`-ready
    parameter data for LDV technologies. These computations respond to, *inter alia*,
    :attr:`.transport.Config.dummy_LDV`:

    - :any:`True`: :func:`get_dummy` is used.
    - :any:`False`: :func:`prepare_tech_econ` is used.

    In both cases, :func:`constraint_data` is used to generate constraint data.
    """
    from genno import Key

    from . import factor

    # Collect data in `TARGET` and connect to the "add transport data" key
    collect.computer = c
    c.add("transport_data", __name__, key=TARGET)

    context = c.graph["context"]
    config: "Config" = context.transport

    # Some keys/shorthand
    k = Keys(
        fe="fuel economy:n-t-y:LDV",
        eff="efficiency:t-y-n:LDV",
        factor_input="input:t-y:LDV+factor",
    )

    # Use .tools.exo_data.prepare_computer() to add tasks that load, adapt, and select
    # the appropriate data
    kw0 = dict(nodes=context.model.regions, scenario=config.ssp.urn.partition("=")[2])
    for kw0["measure"] in LDV.measures:
        LDV.add_tasks(c, context=context, **kw0, strict=False)

    # Insert a scaling factor that varies according to SSP
    c.apply(
        factor.insert, k.fe["exo"], name="ldv fuel economy", target=k.fe, dims="nty"
    )

    # Reciprocal value, i.e. from  Gv km / GW a → GW a / Gv km
    c.add(k.eff[0], "div", genno.Quantity(1.0), k.fe)

    # Compute the input efficiency adjustment factor for the NAVIGATE project
    # TODO Move this to project-specific code
    c.add(k.factor_input, "factor_input", "y", K.t, K.agg.t, "config")

    # Product of NAVIGATE input efficiency factor and LDV efficiency
    c.add(k.eff[1], "mul", k.factor_input, k.eff[0])

    # Multiply by values from ldv-input-adj.csv. See file comment. Drop the 'scenario'
    # dimension; there is only one value in the file per 'n'.
    c.add("input:n:LDV+adj", "sum", K.exo.input_adj_ldv, dimensions=["scenario"])
    c.add(k.eff[2], "mul", k.eff[1], "input:n:LDV+adj")

    # Apply the function usage_data() for further processing
    collect("usage", usage_data, K.exo.load_factor_ldv, "cg", K.n, K.t["LDV"], K.y)

    # Add further keys for MESSAGE-structured data
    # Techno-economic attributes
    # Select a task for the final step that computes "tech::LDV+ixmp"
    if config.dummy_LDV:
        collect("tech", get_dummy, "context")
    else:
        c.apply(
            prepare_tech_econ,
            efficiency=k.eff[2],
            inv_cost=Key("inv_cost:n-t-y:LDV+exo"),
            fix_cost=Key("fix_cost:n-t-y:LDV+exo"),
        )

    # Calculate base-period CAP_NEW and historical_new_capacity (‘sales’)
    if config.ldv_stock_method == "A":
        # Data from file ldv-new-capacity.csv
        try:
            k.stock = Key(c.full_key("cap_new::ldv+exo"))
        except KeyError:
            k.stock = Key("")  # No such file in this configuration
    elif config.ldv_stock_method == "B":
        # Now handled in .vehicle
        pass


def prepare_tech_econ(
    c: Computer, *, efficiency: Key, inv_cost: Key, fix_cost: Key
) -> None:
    """Prepare `c` to calculate techno-economic parameters for LDVs.

    This prepares `k_target` to return a data structure with MESSAGE-ready data for the
    parameters ``input``, ``ouput``, ``fix_cost``, and ``inv_cost``.
    """
    # Identify periods to include
    # FIXME Avoid hard-coding this period
    c.add("y::LDV", lambda y: list(filter(lambda x: 1995 <= x, y)), "y")

    # Create base quantity for "output" parameter
    k = output_base = Key("output:n-t-y:LDV+base")
    c.add(k[0], wildcard(1.0, "Gv km", k.dims))

    # Broadcast over (n, t, y) dimensions
    c.add(k[1], "broadcast_wildcard", k[0], K.n, K.t["LDV"], K.y, dim=k.dims)

    # Broadcast `exo.input_share` over (c, t) dimensions. This produces a large Quantity
    # with 1.0 everywhere except explicit entries in the input data file.
    # NB Order matters here
    k = K.exo.input_share
    coords = [K.t["LDV"], "c::transport+base", "y"]  # NB include historical periods
    c.add(k[0], "broadcast_wildcard", k, *coords, dim=k.dims)

    # Multiply by `bcast_tcl.input` to keep only the entries that correspond to actual
    # input commodities of particular technologies.
    input_bcast = c.add("input broadcast::LDV", "mul", k[0], K.bcast_tcl.input)

    ### Convert input and output to MESSAGE data structure
    for par_name, base, bcast in (
        ("input", efficiency, input_bcast),
        ("output", output_base[1], K.bcast_tcl.output),
    ):
        k = Key(par_name, base.dims, "LDV")

        # Extend data over missing periods in the model horizon
        c.add(k[0], "extend_y", base, "y::LDV")

        # Broadcast from (y) to (yv, ya) dims to produce the full quantity for
        # input/output efficiency
        prev = c.add(k[1], "mul", k[0], bcast, K.bcast_y.all)

        # Convert to ixmp/MESSAGEix-structured pd.DataFrame
        c.add(k[2], "as_message_df", prev, name=par_name, dims=DIMS, common=COMMON)

        # Convert to target units and append to `TARGET`
        collect(par_name, convert_units, k[2], "transport info")

    ### Transform costs
    kw = dict(fill_value="extrapolate")
    for name, base in (("fix_cost", fix_cost), ("inv_cost", inv_cost)):
        prev = c.add(f"{name}::LDV+0", "interpolate", base, "y::coords", kwargs=kw)
        prev = c.add(f"{name}::LDV+1", "mul", prev, K.bcast_y.all)
        collect(name, "as_message_df", prev, name=name, dims=DIMS, common=COMMON)

    ### Compute CO₂ emissions factors
    # Extract the 'input' data frame
    other = Key("other::LDV")
    c.add(other[0], itemgetter("input"), f"input{Li}")

    # Apply ef_for_input; append to `TARGET`
    collect("emission_factor", ef_for_input, "context", other[0], species="CO2")


def get_dummy(context) -> "ParameterData":
    """Generate dummy, equal-cost output for each LDV technology."""
    # Information about the target structure
    config: "Config" = context.transport
    info = config.base_model_info

    # List of years to include
    years = list(filter(lambda y: y >= 2010, info.set["year"]))

    # List of LDV technologies
    all_techs = config.spec.add.set["technology"]
    ldv_techs = list(map(str, all_techs[all_techs.index("LDV")].child))

    # 'output' parameter values: all 1.0 (ACT units == output units)
    # - Broadcast across nodes.
    # - Broadcast across LDV technologies.
    # - Add commodity ID based on technology ID.
    output = (
        make_df(
            "output",
            value=1.0,
            year_act=years,
            year_vtg=years,
            unit="Gv km",
            level="useful",
            **COMMON,
        )
        .pipe(broadcast, node_loc=info.N[1:], technology=ldv_techs)
        .assign(commodity=lambda df: "transport vehicle " + df["technology"])
        .pipe(same_node)
    )

    # Discard rows for the historical LDV technology beyond 2010
    output = output[~output.eval("technology == 'ICE_L_ptrp' and year_vtg > 2010")]

    # Add matching data for 'capacity_factor' and 'var_cost'
    data = make_matched_dfs(output, capacity_factor=1.0, var_cost=1.0)
    data["output"] = output

    return data


def usage_data(
    load_factor: "AnyQuantity",
    cg: list["Code"],
    nodes: list[str],
    technologies: list["Code"],
    years: list,
) -> "ParameterData":
    """Generate data for LDV “usage pseudo-technologies”.

    These technologies convert commodities like "transport ELC_100 vehicle" (that is,
    vehicle-distance traveled) into "transport pax RUEAM" (that is, passenger-distance
    traveled). These data incorporate:

    1. Load factor, in the ``output`` efficiency.
    2. Required consumption of a "disutility" commodity, in ``input``.
    """
    from .structure import TEMPLATE

    info = ScenarioInfo(set={"node": nodes, "year": years})

    # Regenerate the Spec for the disutility formulation
    spec = disutility.get_spec(groups=cg, technologies=technologies, template=TEMPLATE)

    data = disutility.data_conversion(info, spec)

    # Apply load factor
    cols = list(data["output"].columns[:-2])
    unit = data["output"]["unit"].unique()[0]
    rename = cast(Mapping, {"n": "node_loc", "y": "year_act"})
    data["output"] = (
        (
            genno.Quantity(data["output"].set_index(cols)["value"])
            * load_factor.rename(rename)
        )
        .to_dataframe()
        .reset_index()
        .assign(unit=unit)
    )

    # Add a source that produces the "disutility" commodity
    merge_data(data, disutility.data_source(info, spec))

    return data
