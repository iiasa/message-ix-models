"""Data for light-duty vehicles (LDVs) for passenger transport."""

import logging
from collections.abc import Mapping
from operator import itemgetter
from typing import TYPE_CHECKING, Any, cast

import genno
from genno import Computer, Key, Keys
from genno.core.key import single_key
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

from . import util
from .data import MaybeAdaptR11Source
from .emission import ef_for_input
from .key import activity_ldv_full, bcast_tcl, bcast_y, exo
from .util import COMMON, EXTRAPOLATE, wildcard

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

    collect.computer = c

    context = c.graph["context"]
    config: "Config" = context.transport
    info = config.base_model_info

    # Some keys/shorthand
    k = Keys(
        fe=Key("fuel economy:n-t-y:LDV"),
        eff=Key("efficiency:t-y-n:LDV"),
        factor_input=Key("input:t-y:LDV+factor"),
    )
    t_ldv = "t::transport LDV"

    # Use .tools.exo_data.prepare_computer() to add tasks that load, adapt, and select
    # the appropriate data
    kw0 = dict(nodes=context.model.regions, scenario=config.ssp.urn.partition("=")[2])
    for kw0["measure"] in LDV.measures:
        LDV.add_tasks(c, context=context, **kw0, strict=False)

    # Insert a scaling factor that varies according to SSP
    c.apply(
        factor.insert, k.fe + "exo", name="ldv fuel economy", target=k.fe, dims="nty"
    )

    # Reciprocal value, i.e. from  Gv km / GW a → GW a / Gv km
    c.add(k.eff[0], "div", genno.Quantity(1.0), k.fe)

    # Compute the input efficiency adjustment factor for the NAVIGATE project
    # TODO Move this to project-specific code
    c.add(
        k.factor_input,
        "factor_input",
        "y",
        "t::transport",
        "t::transport agg",
        "config",
    )

    # Product of NAVIGATE input efficiency factor and LDV efficiency
    c.add(k.eff[1], "mul", k.factor_input, k.eff[0])

    # Multiply by values from ldv-input-adj.csv. See file comment. Drop the 'scenario'
    # dimension; there is only one value in the file per 'n'.
    c.add("input:n:LDV+adj", "sum", exo.input_adj_ldv, dimensions=["scenario"])
    c.add(k.eff[2], "mul", k.eff[1], "input:n:LDV+adj")

    ### Load factor
    # Interpolate on "y" dimension
    k.lf_nsy = Key(exo.load_factor_ldv)
    c.add(k.lf_nsy[0], "interpolate", k.lf_nsy, "y::coords", **EXTRAPOLATE)

    # Select load factor
    k.lf_ny = k.lf_nsy / "scenario"
    c.add(k.lf_ny[0], "select", k.lf_nsy[0], "indexers:scenario:LED")

    # Insert a scaling factor that varies according to SSP
    c.apply(factor.insert, k.lf_ny[0], name="ldv load factor", target=k.lf_ny)

    # Apply the function usage_data() for further processing
    collect("usage", usage_data, k.lf_ny, "cg", "n::ex world", t_ldv, "y::model")

    ### Technical lifetime
    tl, k_tl = "technical_lifetime", exo.lifetime_ldv

    # Interpolate on "yv" dimension
    c.add(k_tl[0], "interpolate", k_tl, "yv::coords", **EXTRAPOLATE)

    # Broadcast to all nodes, scenarios, and LDV technologies
    coords = ["scenario::all", "n::ex world", "t::LDV"]
    c.add(k_tl[1], "broadcast_wildcard", k_tl[0], *coords, dim=("scenario", "nl", "t"))

    # Select values for the current scenario
    c.add(k_tl[2] / "scenario", "select", k_tl[1], "indexers:scenario:LED")

    # Convert to integer
    # NB This is required because the MESSAGEix GAMS implementation cannot handle non-
    #    integer values
    c.add(k_tl[3] / "scenario", lambda qty: qty.astype(int), k_tl[2] / "scenario")

    # Convert to MESSAGE data structure
    dims = dict(node_loc="nl", technology="t", year_vtg="yv")
    collect(tl, "as_message_df", k_tl[3] / "scenario", name=tl, dims=dims, common={})

    ### Capacity factor
    cf, k_cf_s = "capacity_factor", exo.activity_ldv
    k_cf = k_cf_s / "scenario"
    # Convert units
    c.add(k_cf_s[0], "convert_units", k_cf_s, units="Mm/year")
    # Broadcast to all scenarios
    c.add(k_cf_s[1], "broadcast_wildcard", k_cf_s[0], "scenario::all", dim="scenario")
    # Select values for the current scenario
    c.add(k_cf[2], "select", k_cf_s[1], "indexers:scenario:LED")
    # Interpolate on "y" dimension
    c.add(k_cf["full"], "interpolate", k_cf[2], "y::coords", **EXTRAPOLATE)
    assert k_cf["full"] == activity_ldv_full
    # Add dimension "t" indexing all LDV technologies
    prev = c.add(k_cf[4] * "t", "expand_dims", k_cf["full"], "t::transport LDV")
    # Broadcast y → (yV, yA)
    prev = c.add(k_cf[5], "mul", prev, bcast_y.all)
    # Convert to MESSAGE data structure
    collect(cf, "as_message_df", prev, name=cf, dims=DIMS, common=COMMON)

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
        k.stock = single_key(c.apply(stock))

    if k.stock:
        # Convert units
        c.add(k.stock[0], "convert_units", k.stock, units="million * vehicle / year")

        # historical_new_capacity: select only data prior to y₀
        kw1: dict[str, Any] = dict(
            common={},
            dims=dict(node_loc="nl", technology="t", year_vtg="yv"),
            name="historical_new_capacity",
        )
        y_historical = list(filter(lambda y: y < info.y0, info.set["year"]))
        c.add(k.stock[1], "select", k.stock[0], indexers=dict(yv=y_historical))
        collect(kw1["name"], "as_message_df", k.stock[1], **kw1)

        # CAP_NEW/bound_new_capacity_{lo,up}
        # - Select only data from y₀ and later.
        # - Discard values for ICE_conv.
        #   TODO Do not hard code this label; instead, identify the technology with the
        #   largest share and avoid setting constraints on it.
        # - Add both upper and lower constraints to ensure the solution contains exactly
        #   the given value.
        c.add(k.stock[2], "select", k.stock[0], indexers=dict(yv=info.Y))
        indexers = dict(t=["ICE_conv"])
        c.add(k.stock[3], "select", k.stock[2], indexers=indexers, inverse=True)
        for kw1["name"] in map("bound_new_capacity_{}".format, ("lo", "up")):
            collect(kw1["name"], "as_message_df", k.stock[3], **kw1)

    # Add the data to the target scenario
    c.add("transport_data", __name__, key=TARGET)


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
    coords = ["n::ex world", "t::LDV", "y::model"]
    c.add(k[1], "broadcast_wildcard", k[0], *coords, dim=k.dims)

    # Broadcast `exo.input_share` over (c, t) dimensions. This produces a large Quantity
    # with 1.0 everywhere except explicit entries in the input data file.
    # NB Order matters here
    k = exo.input_share
    coords = ["t::LDV", "c::transport+base", "y"]  # NB include historical periods
    c.add(k[0], "broadcast_wildcard", k, *coords, dim=k.dims)

    # Multiply by `bcast_tcl.input` to keep only the entries that correspond to actual
    # input commodities of particular technologies.
    input_bcast = c.add("input broadcast::LDV", "mul", k[0], bcast_tcl.input)

    ### Convert input and output to MESSAGE data structure
    for par_name, base, bcast in (
        ("input", efficiency, input_bcast),
        ("output", output_base[1], bcast_tcl.output),
    ):
        k = Key(par_name, base.dims, "LDV")

        # Extend data over missing periods in the model horizon
        c.add(k[0], "extend_y", base, "y::LDV")

        # Broadcast from (y) to (yv, ya) dims to produce the full quantity for
        # input/output efficiency
        prev = c.add(k[1], "mul", k[0], bcast, bcast_y.all)

        # Convert to ixmp/MESSAGEix-structured pd.DataFrame
        c.add(k[2], "as_message_df", prev, name=par_name, dims=DIMS, common=COMMON)

        # Convert to target units and append to `TARGET`
        collect(par_name, convert_units, k[2], "transport info")

    ### Transform costs
    kw = dict(fill_value="extrapolate")
    for name, base in (("fix_cost", fix_cost), ("inv_cost", inv_cost)):
        prev = c.add(f"{name}::LDV+0", "interpolate", base, "y::coords", kwargs=kw)
        prev = c.add(f"{name}::LDV+1", "mul", prev, bcast_y.all)
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


def stock(c: Computer, *, margin: float = 0.2) -> Key:
    """Prepare `c` to compute base-period stock and historical sales.

    Parameters
    ----------
    margin :
        Fractional margin by which to increase the resulting sales values. Because these
        values are used to compute ``historical_new_capacity`` and
        ``bound_new_capacity_{lo,up}``, this relaxes the resulting constraints on LDV
        technologies in the first model period.
    """
    from .key import ldv_ny

    k = Keys(stock="stock:n-y:LDV", sales="sales:n-t-y:LDV", result="sales:nl-t-yv:LDV")

    # - Divide total LDV activity by (1) annual driving distance per vehicle and (2)
    #   load factor (occupancy) to obtain implied stock.
    # - Correct units: "load factor ldv:n-y" is dimensionless, should be
    #   passenger/vehicle
    # - Select only the base-period value.
    c.add(k.stock[0], "div", ldv_ny + "total", activity_ldv_full)
    c.add(k.stock[1], "div", k.stock[0], exo.load_factor_ldv / "scenario")
    c.add(k.stock[2], "div", k.stock[1], genno.Quantity(1.0, units="passenger/vehicle"))
    c.add(k.stock[3] / "y", "select", k.stock[2], "y0::coord")

    # Multiply by exogenous technology shares to obtain stock with (n, t) dimensions
    c.add(k.stock, "mul", k.stock[3] / "y", exo.t_share_ldv)

    # TODO Move the following 4 calls to .build.add_structure() or similar
    # Identify the subset of periods up to and including y0
    c.add(
        "y::to y0",
        lambda periods, y0: dict(y=list(filter(lambda y: y <= y0, periods))),
        "y",
        "y0",
    )
    # Convert duration_period to Quantity
    c.add("duration_period:y", "duration_period", "info")
    # Duration_period up to and including y0
    c.add("duration_period:y:to y0", "select", "duration_period:y", "y::to y0")
    # Groups for aggregating annual to period data
    c.add("y::annual agg", "groups_y_annual", "duration_period:y")

    # Fraction of sales in preceding years (annual, not MESSAGE 'year' referring to
    # multi-year periods)
    c.add(k.sales["fraction"], "sales_fraction_annual", exo.age_ldv)
    # Absolute sales in preceding years
    c.add(k.sales["annual"], "mul", k.stock, k.sales["fraction"], 1.0 + margin)
    # Aggregate to model periods; total sales across the period
    c.add(k.sales["total"], "aggregate", k.sales["annual"], "y::annual agg", keep=False)
    # Divide by duration_period for the equivalent of CAP_NEW/historical_new_capacity
    c.add(k.sales, "div", k.sales["total"], "duration_period:y")

    # Rename dimensions to match those expected in prepare_computer(), above
    c.add(k.result, "rename_dims", k.sales, name_dict={"n": "nl", "y": "yv"})

    return k.result


def usage_data(
    load_factor: "AnyQuantity",
    cg: list["Code"],
    nodes: list[str],
    t_ldv: Mapping[str, list],
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
    spec = disutility.get_spec(groups=cg, technologies=t_ldv["t"], template=TEMPLATE)

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
