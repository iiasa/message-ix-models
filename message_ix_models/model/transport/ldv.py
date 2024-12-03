"""Data for light-duty vehicles (LDVs) for passenger transport."""

import logging
from collections.abc import Mapping
from operator import itemgetter
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import genno
import pandas as pd
from genno import Computer, Key, KeySeq
from message_ix import make_df
from sdmx.model.common import Code

from message_ix_models.model import disutility
from message_ix_models.tools import exo_data
from message_ix_models.util import (
    ScenarioInfo,
    broadcast,
    convert_units,
    make_matched_dfs,
    merge_data,
    minimum_version,
    same_node,
)

from . import files as exo
from .data import MaybeAdaptR11Source
from .emission import ef_for_input
from .util import wildcard

if TYPE_CHECKING:
    from genno import KeyLike
    from genno.types import AnyQuantity

    from message_ix_models.types import ParameterData

    from .config import Config

log = logging.getLogger(__name__)

#: Shorthand for tags on keys
Li = "::LDV+ixmp"

#: Target key that collects all data generated in this module.
TARGET = f"transport{Li}"


@exo_data.register_source
class LDV(MaybeAdaptR11Source):
    """Provider of exogenous data on LDVs.

    Parameters
    ----------
    source_kw :
       Must include exactly the keys "measure" (must be one of "fuel economy",
       "fix_cost", or "inv_cost"), "nodes", and "scenario".
    """

    id = __name__
    measures = {"inv_cost", "fuel economy", "fix_cost"}

    #: Names of expected files given :attr:`measure`.
    filename = {
        "inv_cost": "ldv-inv_cost.csv",
        "fuel economy": "ldv-fuel-economy.csv",
        "fix_cost": "ldv-fix_cost.csv",
    }

    def __init__(self, source, source_kw) -> None:
        super().__init__(source, source_kw)
        # Use "exo" tag on the target key, to align with existing code in this module
        self.key = Key(f"{self.measure}:n-t-y:LDV+exo")


def _add(c: "Computer", _target_name: str, *args, **kwargs):
    """Update `c` to merge ``_target_name::LDV+ixmp`` into :data:`TARGET`.

    The `args` and `kwargs` are passed to :meth:`.Computer.add`.
    """
    key = f"{_target_name}{Li}"
    c.add(key, *args, **kwargs)
    c.graph[TARGET] = c.graph[TARGET] + (key,)


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
    from genno.core.attrseries import AttrSeries

    from . import factor

    context = c.graph["context"]
    config: "Config" = context.transport
    info = config.base_model_info

    # Some keys/shorthand
    k = SimpleNamespace(
        fe=Key("fuel economy:n-t-y:LDV"),
        eff=KeySeq("efficiency:t-y-n:LDV"),
        factor_input=Key("input:t-y:LDV+factor"),
    )
    t_ldv = "t::transport LDV"

    # Add a placeholder task to merge together all of the data prepared by this module.
    # The helper function _add() above extends this with keys for the different pieces.
    c.add(TARGET, "merge_data")

    # Use .tools.exo_data.prepare_computer() to add tasks that load, adapt, and select
    # the appropriate data
    kw = dict(nodes=context.model.regions, scenario=str(config.ssp))
    for kw["measure"] in LDV.measures:
        exo_data.prepare_computer(
            context, c, source=__name__, source_kw=kw, strict=False
        )

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

    # Interpolate load factor
    k.lf_nsy = KeySeq(exo.load_factor_ldv)
    c.add(
        k.lf_nsy[0],
        "interpolate",
        k.lf_nsy.base,
        "y::coords",
        kwargs=dict(fill_value="extrapolate"),
    )

    # Select load factor
    k.lf_ny = k.lf_nsy / "scenario"
    c.add(k.lf_ny[0], "select", k.lf_nsy[0], "indexers:scenario")

    # Insert a scaling factor that varies according to SSP
    c.apply(factor.insert, k.lf_ny[0], name="ldv load factor", target=k.lf_ny.base)

    # Extend (forward fill) lifetime to cover all periods
    name = "technical_lifetime"
    c.add(exo.lifetime_ldv + "0", "extend_y", exo.lifetime_ldv, "y", dim="yv")
    # Broadcast to all nodes
    c.add(
        f"{name}:nl-yv:LDV",
        "broadcast_wildcard",
        exo.lifetime_ldv + "0",
        "n::ex world",
        dim="nl",
    )
    # Broadcast to all LDV technologies
    # TODO Use a named operator like genno.operator.expand_dims, instead of the method
    #      of the AttrSeries class
    c.add(f"{name}:nl-t-yv:LDV", AttrSeries.expand_dims, f"{name}:nl-yv:LDV", t_ldv)
    # Convert to MESSAGE data structure
    _add(
        c,
        name,
        "as_message_df",
        f"{name}:nl-t-yv:LDV",
        name=name,
        dims=dict(node_loc="nl", technology="t", year_vtg="yv"),
        common={},
    )

    # Add further keys for MESSAGE-structured data
    # Techno-economic attributes
    # Select a task for the final step that computes "tech::LDV+ixmp"
    if config.dummy_LDV:
        _add(c, "tech", get_dummy, "context")
    else:
        c.apply(
            prepare_tech_econ,
            k_efficiency=k.eff[2],
            k_inv_cost="inv_cost:n-t-y:LDV+exo",
            k_fix_cost="fix_cost:n-t-y:LDV+exo",
        )

    # Usage
    _add(c, "usage", usage_data, k.lf_ny.base, "cg", "n::ex world", t_ldv, "y::model")
    # Constraints
    _add(c, "constraints", constraint_data, "context")
    # Capacity factor
    _add(
        c,
        "capacity_factor",
        capacity_factor,
        exo.activity_ldv,
        t_ldv,
        "y",
        "broadcast:y-yv-ya:all",
    )

    # Calculate base-period CAP_NEW and historical_new_capacity (‘sales’)
    if config.ldv_stock_method == "A":
        # Data from file ldv-new-capacity.csv
        try:
            k.stock = KeySeq(c.full_key("cap_new::ldv+exo"))
        except KeyError:
            k.stock = None  # No such file in this configuration
    elif config.ldv_stock_method == "B":
        k.stock = KeySeq(c.apply(stock))

    if k.stock:
        # historical_new_capacity: select only data prior to y₀
        kw: dict[str, Any] = dict(
            common={},
            dims=dict(node_loc="nl", technology="t", year_vtg="yv"),
            name="historical_new_capacity",
        )
        y_historical = list(filter(lambda y: y < info.y0, info.set["year"]))
        c.add(k.stock[1], "select", k.stock.base, indexers=dict(yv=y_historical))
        _add(c, kw["name"], "as_message_df", k.stock[1], **kw)

        # CAP_NEW/bound_new_capacity_{lo,up}
        # - Select only data from y₀ and later.
        # - Discard values for ICE_conv.
        #   TODO Do not hard code this label; instead, identify the technology with the
        #   largest share and avoid setting constraints on it.
        # - Add both upper and lower constraints to ensure the solution contains exactly
        #   the given value.
        c.add(k.stock[2], "select", k.stock.base, indexers=dict(yv=info.Y))
        c.add(
            k.stock[3],
            "select",
            k.stock[2],
            indexers=dict(t=["ICE_conv"]),
            inverse=True,
        )
        for kw["name"] in map("bound_new_capacity_{}".format, ("lo", "up")):
            _add(c, kw["name"], "as_message_df", k.stock[3], **kw)

    # Add the data to the target scenario
    c.add("transport_data", __name__, key=TARGET)


DIMS = dict(
    commodity="c",
    level="l",
    node_dest="n",
    node_loc="n",
    node_origin="n",
    technology="t",
    year_act="ya",
    year_vtg="yv",
)
COMMON = dict(mode="all", time="year", time_dest="year", time_origin="year")


def prepare_tech_econ(
    c: Computer,
    *,
    k_efficiency: "KeyLike",
    k_inv_cost: "KeyLike",
    k_fix_cost: "KeyLike",
) -> None:
    """Prepare `c` to calculate techno-economic parameters for LDVs.

    This prepares `k_target` to return a data structure with MESSAGE-ready data for the
    parameters ``input``, ``ouput``, ``fix_cost``, and ``inv_cost``.
    """
    # Collection of KeySeq for starting-points
    k = SimpleNamespace(input=KeySeq("input::LDV"), output=KeySeq("output::LDV"))

    # Identify periods to include
    # FIXME Avoid hard-coding this period
    c.add("y::LDV", lambda y: list(filter(lambda x: 1995 <= x, y)), "y")

    # Create base quantity for "output" parameter
    nty = tuple("nty")
    c.add(k.output[0] * nty, wildcard(1.0, "gigavehicle km", nty))
    for i, coords in enumerate(["n::ex world", "t::LDV", "y::model"]):
        c.add(
            k.output[i + 1] * nty,
            "broadcast_wildcard",
            k.output[i] * nty,
            coords,
            dim=coords[0],
        )

    ### Convert input, output to MESSAGE data structure
    for par_name, base, ks, i in (
        ("input", k_efficiency, k.input, 0),
        ("output", k.output[3] * nty, k.output, 4),
    ):
        # Extend data over missing periods in the model horizon
        c.add(ks[i], "extend_y", base, "y::LDV")

        # Produce the full quantity for input/output efficiency
        prev = c.add(
            ks[i + 1],
            "mul",
            ks[i],
            f"broadcast:t-c-l:transport+{par_name}",
            "broadcast:y-yv-ya:all",
        )

        # Convert to ixmp/MESSAGEix-structured pd.DataFrame
        # NB quote() is necessary with dask 2024.11.0, not with earlier versions
        c.add(ks[i + 2], "as_message_df", prev, name=par_name, dims=DIMS, common=COMMON)

        # Convert to target units
        _add(c, par_name, convert_units, ks[i + 2], "transport info")

    ### Transform costs
    for par_name, base in (("fix_cost", k_fix_cost), ("inv_cost", k_inv_cost)):
        prev = c.add(
            f"{par_name}::LDV+0",
            "interpolate",
            base,
            "y::coords",
            kwargs=dict(fill_value="extrapolate"),
        )
        prev = c.add(f"{par_name}::LDV+1", "mul", prev, "broadcast:y-yv-ya:all")
        _add(
            c, par_name, "as_message_df", prev, name=par_name, dims=DIMS, common=COMMON
        )

    ### Compute CO₂ emissions factors

    # Extract the 'input' data frame
    k.other = KeySeq("other::LDV")
    c.add(k.other[0], itemgetter("input"), f"input{Li}")

    # Use ef_for_input
    _add(c, "emission_factor", ef_for_input, "context", k.other[0], species="CO2")


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


@minimum_version("message_ix 3.6")
def capacity_factor(
    qty: "AnyQuantity", t_ldv: dict, y, y_broadcast: "AnyQuantity"
) -> "ParameterData":
    """Return capacity factor data for LDVs.

    The data are:

    - Broadcast across all |yV|, |yA| (`broadcast_y`), and LDV technologies (`t_ldv`).
    - Converted to :mod:`message_ix` parameter format using :func:`.as_message_df`.

    Parameters
    ----------
    qty
        Input data, for instance from file :`ldv-activity.csv`, with dimension |n|.
    y_broadcast
        The structure :py:`"broadcast:y-yv-va"`.
    t_ldv
        The structure :py:`"t::transport LDV"`, mapping the key "t" to the list of LDV
        technologies.
    y
        All periods, including pre-model periods.
    """
    from genno.operator import convert_units

    try:
        from message_ix.report.operator import as_message_df
    except ImportError:
        from message_ix.reporting.computations import as_message_df

    # TODO determine units from technology annotations
    data = convert_units(qty.expand_dims(y=y) * y_broadcast, "Mm / year")

    name = "capacity_factor"
    dims = dict(node_loc="n", year_vtg="yv", year_act="ya")
    # TODO Remove typing exclusion once message_ix is updated for genno 1.25
    result = as_message_df(data, name, dims, dict(time="year"))  # type: ignore [arg-type]

    result[name] = result[name].pipe(broadcast, technology=t_ldv["t"])

    return result


def constraint_data(context) -> "ParameterData":
    """Return constraints on light-duty vehicle technology activity and usage.

    Responds to the :attr:`.Config.constraint` key :py:`"LDV growth_activity"`; see
    description there.
    """
    config: "Config" = context.transport

    # Information about the target structure
    info = config.base_model_info
    years = info.Y[1:]

    # Technologies as a hierarchical code list
    techs = config.spec.add.set["technology"]
    ldv_techs = techs[techs.index("LDV")].child

    # All technologies in the spec, as strings
    all_techs = list(map(str, techs))

    # List of technologies to constrain, including the LDV technologies, plus the
    # corresponding "X usage by CG" pseudo-technologies
    constrained: list[Code] = []
    for t in map(str, ldv_techs):
        constrained.extend(filter(lambda _t: t in _t, all_techs))  # type: ignore

    data: dict[str, pd.DataFrame] = dict()
    for bound in "lo", "up":
        name = f"growth_activity_{bound}"

        # Retrieve the constraint value from configuration
        value = config.constraint[f"LDV {name}"]

        # Assemble the data
        data[name] = make_df(
            name, value=value, year_act=years, time="year", unit="-"
        ).pipe(broadcast, node_loc=info.N[1:], technology=constrained)

        if bound == "lo":
            continue

        # Add initial_activity_up values allowing usage to begin in any period
        name = f"initial_activity_{bound}"
        data[name] = make_df(
            name, value=1e6, year_act=years, time="year", unit="-"
        ).pipe(broadcast, node_loc=info.N[1:], technology=constrained)

    # Prevent new capacity from being constructed for techs annotated
    # "historical-only: True"
    historical_only_techs = list(
        filter(lambda t: t.eval_annotation("historical-only") is True, techs)
    )
    name = "bound_new_capacity_up"
    data[name] = make_df(name, year_vtg=info.Y, value=0.0, unit="-").pipe(
        broadcast, node_loc=info.N[1:], technology=historical_only_techs
    )

    return data


def stock(c: Computer) -> Key:
    """Prepare `c` to compute base-period stock and historical sales."""
    from .key import ldv_ny

    k = KeySeq("stock:n-y:LDV")

    # - Divide total LDV activity by (1) annual driving distance per vehicle and (2)
    #   load factor (occupancy) to obtain implied stock.
    # - Correct units: "load factor ldv:n-y" is dimensionless, should be
    #   passenger/vehicle
    # - Select only the base-period value.
    c.add(k[0], "div", ldv_ny + "total", exo.activity_ldv)
    c.add(k[1], "div", k[0], "load factor ldv:n-y:exo")
    c.add(k[2], "div", k[1], genno.Quantity(1.0, units="passenger / vehicle"))
    c.add(k[3] / "y", "select", k[2], "y0::coord")

    # Multiply by exogenous technology shares to obtain stock with (n, t) dimensions
    c.add("stock:n-t:LDV", "mul", k[3] / "y", exo.t_share_ldv)

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
    c.add("sales fraction:n-t-y:LDV", "sales_fraction_annual", exo.age_ldv)
    # Absolute sales in preceding years
    c.add("sales:n-t-y:LDV+annual", "mul", "stock:n-t:LDV", "sales fraction:n-t-y:LDV")
    # Aggregate to model periods; total sales across the period
    c.add(
        "sales:n-t-y:LDV+total",
        "aggregate",
        "sales:n-t-y:LDV+annual",
        "y::annual agg",
        keep=False,
    )
    # Divide by duration_period for the equivalent of CAP_NEW/historical_new_capacity
    c.add("sales:n-t-y:LDV", "div", "sales:n-t-y:LDV+total", "duration_period:y")

    # Rename dimensions to match those expected in prepare_computer(), above
    k = Key("sales:nl-t-yv:LDV")
    c.add(k, "rename_dims", "sales:n-t-y:LDV", name_dict={"n": "nl", "y": "yv"})

    return k


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
