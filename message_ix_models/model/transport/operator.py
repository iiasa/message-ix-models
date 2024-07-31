""":mod:`genno` operators for MESSAGEix-Transport."""

import logging
import re
from functools import partial, reduce
from itertools import product
from operator import gt, le, lt
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Hashable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    cast,
)

import genno
import numpy as np
import pandas as pd
import xarray as xr
from genno import Computer, KeySeq, Operator, quote
from genno.operator import apply_units, rename_dims
from genno.testing import assert_qty_allclose, assert_units
from sdmx.model.v21 import Code

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codelist, get_codes
from message_ix_models.project.navigate import T35_POLICY
from message_ix_models.report.operator import compound_growth
from message_ix_models.report.util import as_quantity
from message_ix_models.util import (
    MappingAdapter,
    broadcast,
    datetime_now_with_tz,
    nodes_ex_world,
    show_versions,
)

from .config import Config

if TYPE_CHECKING:
    from genno.types import AnyQuantity
    from message_ix import Scenario
    from xarray.core.types import Dims

    import message_ix_models.model.transport.factor
    from message_ix_models import Context

log = logging.getLogger(__name__)

__all__ = [
    "base_model_data_header",
    "base_shares",
    "broadcast_advance",
    "broadcast_y_yv_ya",
    "cost",
    "distance_ldv",
    "distance_nonldv",
    "dummy_prices",
    "extend_y",
    "factor_fv",
    "factor_input",
    "factor_pdt",
    "groups_iea_eweb",
    "iea_eei_fv",
    "indexers_n_cd",
    "indexers_usage",
    "input_commodity_level",
    "logit",
    "max",
    "min",
    "merge_data",
    "nodes_ex_world",  # Re-export from message_ix_models.util TODO do this upstream
    "nodes_world_agg",
    "price_units",
    "quantity_from_config",
    "relabel2",
    "share_weight",
    "smooth",
    "transport_check",
    "transport_data",
    "votm",
]


def base_model_data_header(scenario: "Scenario", *, name: str) -> Dict[str, str]:
    """Return a header comment for writing out base model data."""
    versions = "\n\n".join(show_versions().split("\n\n")[:2])

    return dict(
        header_comment=f"""`{name}` parameter data for MESSAGEix-GLOBIOM.

Generated: {datetime_now_with_tz().isoformat()}
from: ixmp://{scenario.platform.name}/{scenario.url}
using:
{versions}
"""
    )


def base_shares(
    base: "AnyQuantity", nodes: List[str], techs: List[str], y: List[int]
) -> "AnyQuantity":
    """Return base mode shares.

    The mode shares are read from a file at
    :file:`data/transport/{regions}/mode-shares/{name}.csv`, where `name` is from the
    configuration key ``mode-share:``, and `region` uses :func:`.path_fallback`.

    Labels on the t (technology) dimension must match the ``demand modes:`` from the
    configuration.

    If the data lack the n (node, spatial) and/or y (time) dimensions, they are
    broadcast over these.
    """
    from genno.operator import aggregate, sum
    from numpy import allclose

    # Check: ensure values sum to 1
    tmp = sum(base, dimensions=["t"])
    check = allclose(tmp.to_series().values, 1.0)
    if not check:
        log.warning(
            "Sum across modes ≠ 1.0; will rescale:\n" + (tmp[tmp != 1.0].to_string())
        )
        result = base / tmp
    else:
        result = base

    assert allclose(sum(result, dimensions=["t"]).to_series().values, 1.0)

    # Aggregate extra modes that do not appear in the data
    extra_modes = set(result.coords["t"].data) - set(techs)
    if extra_modes == {"OTHER ROAD"}:
        # Add "OTHER ROAD" to "LDV"
        groups = {t: [t] for t in techs}
        groups["LDV"].append("OTHER ROAD")
        result = aggregate(result, groups=dict(t=groups), keep=False)
    elif len(extra_modes):
        raise NotImplementedError(f"Extra mode(s) t={extra_modes}")

    missing = cast(Set[Hashable], set("nty")) - set(result.dims)
    if len(missing):
        log.info(f"Broadcast base mode shares with dims {base.dims} over {missing}")

        coords = [("n", nodes), ("t", techs), ("y", y)]
        result = base * genno.Quantity(xr.DataArray(1.0, coords=coords), units="")

    return result


def broadcast_advance(data: "AnyQuantity", y0: int, config: dict) -> "AnyQuantity":
    """Broadcast ADVANCE `data` from native `n` coords to :py:`config["regions"]`."""
    from genno.operator import sum

    assert "R12" == config["regions"], "ADVANCE data mapping only for R12 regions"

    # Create a quantity for broadcasting
    df = pd.DataFrame(
        [
            ["ASIA", 0.1, "R12_RCPA"],
            ["ASIA", 0.5 - 0.1, "R12_PAS"],
            ["CHN", 1.0, "R12_CHN"],
            ["EU", 0.1, "R12_EEU"],
            ["EU", 0.9, "R12_WEU"],
            ["IND", 1.0, "R12_SAS"],
            ["LAM", 1.0, "R12_LAM"],
            ["MAF", 0.5, "R12_AFR"],
            ["MAF", 0.5, "R12_MEA"],
            ["OECD90", 0.08, "R12_PAO"],
            ["REF", 1.0, "R12_FSU"],
            ["USA", 1.0, "R12_NAM"],
        ],
        columns=["n", "value", "n_new"],
    )
    bcast = genno.Quantity(df.set_index(["n", "n_new"])["value"])

    check = data.sel(n="World", y=y0, drop=True)

    # - Multiply by `bcast`, adding a new dimension "n_new".
    # - Sum on "n" and drop that dimension.
    # - Rename "n_new" to "n".
    result = (
        (data.sel(y=y0, drop=True) * bcast)
        .pipe(sum, dimensions=["n"])
        .pipe(rename_dims, {"n_new": "n"})
    )

    # Ensure the total across the new `n` coords still matches the world total
    assert_qty_allclose(check, sum(result, dimensions=["n"]), rtol=5e-2)

    return result


def broadcast_y_yv_ya(y: List[int], y_model: List[int]) -> "AnyQuantity":
    """Return a quantity for broadcasting y to (yv, ya).

    This is distinct from :attr:`.ScenarioInfo.ya_ya`, because it omits all
    :math:`y^V < y_0`.
    """
    dims = ["y", "yv", "ya"]
    series = (
        pd.DataFrame(product(y, y_model), columns=dims[1:])
        .query("ya >= yv")
        .assign(value=1.0, y=lambda df: df["yv"])
        .set_index(dims)["value"]
    )
    return genno.Quantity(series)


def cost(
    price: "AnyQuantity",
    gdp_ppp_cap: "AnyQuantity",
    whours: "AnyQuantity",
    speeds: "AnyQuantity",
    votm: "AnyQuantity",
    y: List[int],
) -> "AnyQuantity":
    """Calculate cost of transport [money / distance].

    Calculated from two components:

    1. The inherent price of the mode.
    2. Value of time, in turn from:

       1. a value of time multiplier (`votm`),
       2. the wage rate per hour (`gdp_ppp_cap` / `whours`), and
       3. the travel time per unit distance (1 / `speeds`).
    """
    from genno.operator import add

    # NB for some reason, the 'y' dimension of result becomes `float`, rather than
    # `int`, in this step
    # FIXME genno does not handle units here correctly using "+" instead of add()
    return add(price, (gdp_ppp_cap * votm) / (speeds * whours)).sel(y=y)


def distance_ldv(config: dict) -> "AnyQuantity":
    """Return annual driving distance per LDV.

    - Regions other than R11_NAM have M/F values in same proportion to their A value as
      in NAM
    """
    # Load from config.yaml
    result = as_quantity(config["transport"].ldv_activity) * as_quantity(
        config["transport"].factor["activity"]["ldv"]
    )

    result.name = "ldv distance"

    return result


#: Mapping from technology names appearing in the IEA EEI data to those in
#: MESSAGEix-Transport.
EEI_TECH_MAP = {
    "Buses": "BUS",
    "Cars/light trucks": "LDV",
    "Freight trains": "freight rail",
    "Freight trucks": "freight truck",
    "Motorcycles": "2W",
    "Passenger trains": "RAIL",
}


def distance_nonldv(context: "Context") -> "AnyQuantity":
    """Return annual travel distance per vehicle for non-LDV transport modes."""
    # FIXME Remove this type exclusion; added only to merge #549
    import message_ix_models.tools.iea.eei  # type: ignore  # noqa: F401
    from message_ix_models.tools import exo_data

    log.warning(
        "distance_nonldv() currently returns a sum, rather than weighted average. Use"
        "with caution."
    )

    c = Computer()
    source_kw = dict(measure="Vehicle use", aggregate=True)
    keys = exo_data.prepare_computer(context, c, "IEA EEI", source_kw)

    ks = KeySeq(keys[0])

    c.add(ks[0], "select", ks.base, indexers={"SECTOR": "transport"}, drop=True)
    c.add(ks[1], "rename", ks[0], quote({"Mode/vehicle type": "t"}))
    # Replace IEA EEI technology codes with MESSAGEix-Transport ones
    c.add(ks[2], "relabel", ks[1], labels=dict(t=EEI_TECH_MAP))
    # Ensure compatible dimensionality and convert units
    c.add(ks[3], "convert_units", ks[2], units="Mm / vehicle / year")
    c.add(ks[4], "rename_dims", ks[3], quote({"n": "nl"}))

    # Execute the calculation
    result = c.get(ks[4])

    # Select the latest year.
    # TODO check whether coverage varies by year; if so, then fill-forward or
    #      extrapolate
    y_m1 = result.coords["y"].data[-1]
    log.info(f"Return data for y={y_m1}")
    return result.sel(y=y_m1, drop=True)


def dummy_prices(gdp: "AnyQuantity") -> "AnyQuantity":
    # Commodity prices: all equal to 0.1

    # Same coords/shape as `gdp`, but with c="transport"
    coords = [(dim, item.data) for dim, item in gdp.coords.items()]
    coords.append(("c", ["transport"]))
    shape = list(len(c[1]) for c in coords)

    return genno.Quantity(
        xr.DataArray(np.full(shape, 0.1), coords=coords), units="USD / km"
    )


def extend_y(qty: "AnyQuantity", y: List[int]) -> "AnyQuantity":
    """Extend `qty` along the "y" dimension to cover `y`."""
    y_ = set(y)

    # Subset of `y` appearing in `qty`
    y_qty = sorted(set(qty.to_series().reset_index()["y"].unique()) & y_)
    # Subset of `target_years` to fill forward from the last period in `qty`
    y_to_fill = sorted(filter(partial(lt, y_qty[-1]), y_))

    log.info(f"{qty.name}: extend from {y_qty[-1]} → {y_to_fill}")

    # Map existing labels to themselves, and missing labels to the last existing one
    y_map = [(y, y) for y in y_qty] + [(y_qty[-1], y) for y in y_to_fill]
    # - Forward-fill *within* `qty` existing values.
    # - Use message_ix_models MappingAdapter to do the rest.
    return MappingAdapter({"y": y_map})(qty.ffill("y"))  # type: ignore [attr-defined]


def factor_fv(n: List[str], y: List[int], config: dict) -> "AnyQuantity":
    """Scaling factor for freight activity.

    If :attr:`.Config.project` is :data:`ScenarioFlags.ACT`, the value declines from
    1.0 at the first `y` to 0.865 (reduction of 13.5%) at y=2050, then constant
    thereafter.

    Otherwise, the value is 1.0 for every (`n`, `y`).
    """
    # Empty data frame
    df = pd.DataFrame(columns=["value"], index=pd.Index(y, name="y"))

    # Default value
    df.iloc[0, :] = 1.0

    # NAVIGATE T3.5 "act" demand-side scenario
    if T35_POLICY.ACT & config["transport"].project["navigate"]:
        years = list(filter(lambda y: y <= 2050, y))
        df.loc[years, "value"] = np.interp(years, [y[0], 2050], [1.0, 0.865])

    # - Fill all values forward from the latest.
    # - Convert to long format.
    # - Broadcast over all nodes `n`.
    # - Set dimensions as index.
    return genno.Quantity(
        df.infer_objects()
        .ffill()
        .reset_index()
        .assign(n=None)
        .pipe(broadcast, n=n)
        .set_index(["n", "y"])["value"],
        units="",
    )


def factor_input(
    y: List[int], t: List[Code], t_agg: Dict, config: dict
) -> "AnyQuantity":
    """Scaling factor for ``input`` (energy intensity of activity).

    If :attr:`.Config.project` is :data:`ScenarioFlags.TEC`, the value declines from 1.0
    at the first `y` to 0.865 (reduction of 13.5%) at y=2050, then constant thereafter.

    Otherwise, the value is 1.0 for every (`t`, `y`).

    The return value includes ``y`` from 2010 onwards.
    """

    def _not_disutility(tech):
        return tech.eval_annotation("is-disutility") is None

    techs = list(filter(_not_disutility, t))

    # Empty data frame
    df = pd.DataFrame(
        columns=pd.Index(map(str, techs), name="t"),
        index=pd.Index(filter(partial(le, 2010), y), name="y"),
    )

    # Default value
    df.iloc[0, :] = 1.0

    # NAVIGATE T3.5 "tec" demand-side scenario
    if T35_POLICY.TEC & config["transport"].project["navigate"]:
        years = list(filter(partial(gt, 2050), df.index))

        # Prepare a dictionary mapping technologies to their respective EI improvement
        # rates
        t_groups = t_agg["t"]
        value = {}
        for group, v in {
            "2W": 1.5,
            "BUS": 1.5,
            "LDV": 1.5,
            "freight truck": 2.0,
            "AIR": 1.3,
        }.items():
            value.update({t: 1 - (v / 100.0) for t in t_groups[group]})

        # Apply the rates, or 1.0 if none set for a particular technology
        for t_ in map(str, techs):
            df.loc[years, t_] = value.get(t_, 1.0)

    qty = genno.Quantity(
        df.infer_objects().fillna(1.0).reset_index().set_index("y").stack()
    )

    return compound_growth(qty, "y")


def factor_pdt(n: List[str], y: List[int], t: List[str], config: dict) -> "AnyQuantity":
    """Scaling factor for passenger activity.

    When :attr:`.Config.scenarios` includes :attr:`ScenarioFlags.ACT` (i.e. NAVIGATE
    Task 3.5, demand-side scenario "act"), the value of 0.8 is specified for LDV, 2050,
    and all regions. This function implements this as a linear decrease between the
    first model period (currently 2020) and that point.

    Otherwise, the value is 1.0 for every (`n`, `t`, `y`).
    """
    # Empty data frame
    df = pd.DataFrame(columns=t, index=pd.Index(y, name="y"))

    # Set 1.0 (no scaling) for first period
    df.iloc[0, :] = 1.0

    # Handle particular scenarios
    if T35_POLICY.ACT & config["transport"].project["navigate"]:
        # NAVIGATE T3.5 "act" demand-side scenario
        years = list(filter(lambda y: y <= 2050, y))
        df.loc[years, "LDV"] = np.interp(years, [y[0], 2050], [1.0, 0.8])

    # - Fill all values forward from the latest.
    # - Convert to long format.
    # - Broadcast over all nodes `n`.
    # - Set dimensions as index.
    return genno.Quantity(
        df.infer_objects()
        .ffill()
        .reset_index()
        .melt(id_vars="y", var_name="t")
        .assign(n=None)
        .pipe(broadcast, n=n)
        .set_index(["n", "y", "t"])["value"],
        units="",
    )


def factor_ssp(
    config: dict,
    nodes: List[str],
    years: List[int],
    *others: List,
    info: "message_ix_models.model.transport.factor.Factor",
    extra_dims: Optional[Sequence[str]] = None,
) -> "AnyQuantity":
    """Return a scaling factor for an SSP realization."""
    kw = dict(n=nodes, y=years, scenario=config["transport"].ssp)
    for dim, labels in zip(extra_dims or (), others):
        kw[dim] = labels
    return info.quantify(**kw)


Groups = Dict[str, Dict[str, List[str]]]


def groups_iea_eweb(technologies: List[Code]) -> Tuple[Groups, Groups, Dict]:
    """Structure for calibration to IEA Extended World Energy Balances (EWEB).

    Returns 3 sets of groups:

    1. Groups for aggregating the EWEB data. In particular:

       - Labels for IEA ``product`` are aggregated to labels for MESSAGEix-Transport
         ``commodity``.
       - Labels for IEA ``flow`` are selected 1:1 *and* aggregated to a flow named
         "transport".

    2. Groups for aggregating MESSAGEix-Transport data. In particular:

       - Labels for MESSAGEix-Transport transport modes (|t| dimension) are aggregated
         to labels for IEA ``flow``.

    3. Indexers for *dis* aggregating computed scaling factors; that is, reversing (2).
    """
    g0: Groups = dict(flow={}, product={})
    g1: Groups = dict(t={})
    g2: Dict = dict(t=[], t_new=[])

    # Add groups from base model commodity code list:
    # - IEA product list → MESSAGE commodity (e.g. "lightoil")
    # - IEA flow list → MESSAGE technology group (e.g. "transport")
    for c in get_codelist("commodity"):
        if products := c.eval_annotation(id="iea-eweb-product"):
            g0["product"][c.id] = products
        if flows := c.eval_annotation(id="iea-eweb-flow"):
            g0["flow"][c.id] = flows

    # Add groups from MESSAGEix-Transport technology code list
    for t in technologies:
        if flows := t.eval_annotation(id="iea-eweb-flow"):
            target = flows[0] if len(flows) == 1 else t.id

            g0["flow"][target] = flows

            # Append the mode name, for instance "AIR"
            g1["t"].setdefault(target, []).append(t.id)
            # # Append the name of individual technologies for this mode
            # g1["t"][target].extend(map(lambda c: c.id, t.child))

            g2["t"].append(target)
            g2["t_new"].append(t.id)

    g2["t"] = xr.DataArray(g2.pop("t"), coords=[("t_new", g2.pop("t_new"))])

    return g0, g1, g2


def input_commodity_level(t: List[Code], default_level=None) -> "AnyQuantity":
    """Return a Quantity for broadcasting dimension (t) to (c, l) for ``input``.

    .. todo:: This essentially replaces :func:`.transport.util.input_commodity_level`,
       and is much faster. Replace usage of the other function with this one, then
       remove the other.
    """

    c_info = get_codes("commodity")

    # Map each `tech` to a `commodity` and `level`
    data = []
    for tech in t:
        # Retrieve the "input" annotation for this technology
        input_ = tech.eval_annotation("input")

        # Retrieve the code for this commodity
        try:
            # Commodity ID
            commodity = input_["commodity"]
            c_code = c_info[c_info.index(commodity)]
        except (KeyError, ValueError, TypeError):
            # TypeError: input_ is None
            # KeyError: "commodity" not in the annotation
            # ValueError: `commodity` not in c_info
            continue

        # Level, in order of precedence:
        # 1. Technology-specific input level from `t_code`.
        # 2. Default level for the commodity from `c_code`.
        # 3. `default_level` argument to this function.
        level = input_.get("level") or c_code.eval_annotation("level") or default_level

        data.append((tech.id, commodity, level))

    idx = pd.MultiIndex.from_frame(pd.DataFrame(data, columns=["t", "c", "l"]))
    s = pd.Series(1.0, index=idx)
    return genno.Quantity(s)


def logit(
    x: "AnyQuantity", k: "AnyQuantity", lamda: "AnyQuantity", y: List[int], dim: str
) -> "AnyQuantity":
    r"""Compute probabilities for a logit random utility model.

    The choice probabilities have the form:

    .. math::

       Pr(i) = \frac{k_j x_j ^{\lambda_j}}
                    {\sum_{\forall i \in D} k_i x_i ^{\lambda_i}}
               \forall j \in D

    …where :math:`D` is the dimension named by the `dim` argument. All other dimensions
    are broadcast automatically.
    """
    # Systematic utility
    u = (k * x**lamda).sel(y=y)

    # commented: for debugging
    # u.to_csv("u.csv")

    # Logit probability
    return u / u.sum(dim)


def max(
    qty: "AnyQuantity",
    dim: "Dims" = None,
    *,
    skipna: Optional[bool] = None,
    keep_attrs: Optional[bool] = None,
    **kwargs: Any,
) -> "AnyQuantity":
    """Like :meth:`xarray.DataArray.max`."""
    assert skipna is keep_attrs is None and 0 == len(kwargs), NotImplementedError

    # FIXME This is AttrSeries only
    return qty.groupby(level=dim).max()  # type: ignore


def min(
    qty: "AnyQuantity",
    dim: "Dims" = None,
    *,
    skipna: Optional[bool] = None,
    keep_attrs: Optional[bool] = None,
    **kwargs: Any,
) -> "AnyQuantity":
    """Like :meth:`xarray.DataArray.min`."""
    assert skipna is keep_attrs is None and 0 == len(kwargs), NotImplementedError

    # FIXME This is AttrSeries only
    return qty.groupby(level=dim).min()  # type: ignore


def merge_data(
    *others: Mapping[Hashable, pd.DataFrame],
) -> Dict[Hashable, pd.DataFrame]:
    """Slightly modified from message_ix_models.util.

    .. todo: move upstream or merge functionality with
       :func:`message_ix_models.util.merge_data`.
    """
    keys: Set[Hashable] = reduce(lambda x, y: x | y.keys(), others, set())
    return {k: pd.concat([o.get(k, None) for o in others]) for k in keys}


def iea_eei_fv(name: str, config: Dict) -> "AnyQuantity":
    """Returns base-year demand for freight from IEA EEI, with dimensions n-c-y."""
    from message_ix_models.tools.iea import eei

    result = eei.as_quantity(name, config["regions"])  # type: ignore [attr-defined]
    ym1 = result.coords["y"].data[-1]

    log.info(f"Use y={ym1} data for base-year freight transport activity")

    assert set("nyt") == set(result.dims)
    return result.sel(y=ym1, t="Total freight transport", drop=True)


def indexers_n_cd(config: Dict) -> Dict[str, xr.DataArray]:
    """Indexers for selecting (`n`, `census_division`) → `n`.

    Based on :attr:`.Config.node_to_census_division`.
    """
    n_cd_map = config["transport"].node_to_census_division
    n, cd = zip(*n_cd_map.items())
    return dict(
        n=xr.DataArray(list(n), dims="n"),
        census_division=xr.DataArray(list(cd), dims="n"),
    )


def indexers_usage(technologies: List[Code]) -> Dict:
    """Indexers for replacing LDV `t` and `cg` with `t_new` for usage technologies."""
    labels: Dict[str, List[str]] = dict(cg=[], t=[], t_new=[])
    for t in technologies:
        if not t.eval_annotation("is-disutility"):
            continue
        t_base, *_, cg = t.id.split()
        labels["t"].append(t_base)
        labels["cg"].append(cg)
        labels["t_new"].append(t.id)

    return {
        "cg": xr.DataArray(labels["cg"], coords=[("t_new", labels["t_new"])]),
        "t": xr.DataArray(labels["t"], coords=[("t_new", labels["t_new"])]),
    }


def nodes_world_agg(config, dim: Hashable = "nl") -> Dict[Hashable, Mapping]:
    """Mapping to aggregate e.g. nl="World" from values for child nodes of "World".

    This mapping should be used with :func:`.genno.operator.aggregate`, giving the
    argument ``keep=False``. It includes 1:1 mapping from each region name to itself.

    .. todo:: move upstream, to :mod:`message_ix_models`.
    """
    from message_ix_models.model.structure import get_codes

    result = {}
    for n in get_codes(f"node/{config['regions']}"):
        # "World" node should have no parent and some children. Countries (from
        # pycountry) that are omitted from a mapping have neither parent nor children.
        if len(n.child) and n.parent is None:
            name = str(n)

            # FIXME Remove. This is a hack to suit the legacy reporting, which expects
            #       global aggregates at *_GLB rather than "World".
            new_name = f"{config['regions']}_GLB"
            log.info(f"Aggregates for {n!r} will be labelled {new_name!r}")
            name = new_name

            # Global total as aggregate of child nodes
            result = {name: list(map(str, n.child))}

            # Also add "no-op" aggregates e.g. "R12_AFR" is the sum of ["R12_AFR"]
            result.update({c: [c] for c in map(str, n.child)})

            return {dim: result}

    raise RuntimeError("Failed to identify the World node")


def price_units(qty: "AnyQuantity") -> "AnyQuantity":
    """Forcibly adjust price units, if necessary."""
    target = "USD_2010 / km"
    if not qty.units.is_compatible_with(target):
        log.warning(f"Adjust price units from {qty.units} to {target}")
    return apply_units(qty, target)


def quantity_from_config(
    config: dict, name: str, dimensionality: Optional[Dict] = None
) -> "AnyQuantity":
    if dimensionality:
        raise NotImplementedError
    result = getattr(config["transport"], name)
    if not isinstance(result, genno.Quantity):
        result = as_quantity(result)
    return result


def relabel2(qty: "AnyQuantity", new_dims: dict):
    """Replace dimensions with new ones using label templates.

    .. todo:: Choose a more descriptive name.
    """
    from collections import defaultdict

    from genno.operator import select

    result = qty

    # Iterate over new dimensions for which templates are provided
    for new_dim, template in new_dims.items():
        if new_dim in qty.dims:  # pragma: no cover
            print(qty.coords)
            raise NotImplementedError(
                f"Replace existing dimension {new_dim} in {qty.dims}"
            )

        # Identify 1 or more source dimension(s) in the template expression
        # TODO improve the regex or use another method that can handle e.g. "{func(t)}"
        source_dims = re.findall(r"{([^\.}]*)", template)

        # Resulting labels
        labels = defaultdict(list)

        # Iterate over the Cartesian product of coords on the `source_dims`
        for values in product(*[qty.coords[d].data for d in source_dims]):
            _locals = dict()  # Locals for eval()
            for d, v in zip(source_dims, values):
                _locals[d] = v
                labels[d].append(v)

            # Construct the label for the `new_dim` by formatting the template string
            labels[new_dim].append(eval(f"f'{template}'", None, _locals))

        # Convert matched lists of labels to xarray selectors
        selectors = {
            d: xr.DataArray(labels[d], coords=[(new_dim, labels[new_dim])])
            for d in source_dims
        }

        result = select(result, selectors)

    return result


def share_weight(
    share: "AnyQuantity",
    gdp: "AnyQuantity",
    cost: "AnyQuantity",
    lamda: "AnyQuantity",
    t_modes: List[str],
    y: List[int],
    config: dict,
) -> "AnyQuantity":
    """Calculate mode share weights.

    - In the base year (:py:`y[0]`), the weights for each |n| are as given in `share`.
    - In the convergence year (:attr:`.Config.year_convergence`,
      via :py:`config["transport"]`), the weights are between the same-node base year
      mode shares and the mean of the base-year mode shares in 0 or more reference nodes
      given by the mapping :attr:`Config.share_weight_convergence`.

      - The interpolation between these points is given by the ratio :math:`k` between
        the same-node convergence-year GDP PPP per capita (`gdp`) and the reference
        node(s mean) base-year GDP PPP per capita.
      - If no reference nodes are given, the values converge towards equal weights for
        each of `t_modes`, with a fixed parameter :math:`k = 1/3`.

    - Values for the years between the base year and the convergence year are
      interpolated.

    Parameters
    ----------
    gdp :
       GDP per capita in purchasing power parity.

    Returns
    -------
    Quantity
        With dimensions :math:`(n, t, y)`: |n| matching `gdp_ppp_cap; :math:`t` per
        `t_modes`, and |y| per `y`.
    """
    from builtins import min

    # Extract info from arguments
    cfg: Config = config["transport"]
    nodes = sorted(gdp.coords["n"].data)
    years = list(filter(lambda year: year <= cfg.year_convergence, y))

    # Empty container for share weights
    weight = xr.DataArray(np.nan, coords=[("n", nodes), ("y", years), ("t", t_modes)])

    # Selectors
    # A scalar induces xarray but not genno <= 1.21 to drop
    y0: Dict[Any, Any] = dict(y=y[0])
    y0_ = dict(y=[y[0]])  # Do not drop
    yC: Dict[Any, Any] = dict(y=cfg.year_convergence)

    # Weights in y0 for all modes and nodes
    # NB here and below, with Python 3.9 one could do: dict(t=modes, n=nodes) | y0
    idx = dict(t=t_modes, n=nodes, **y0)
    w0 = share.sel(idx) / (cost.sel(idx).sel(c="transport", drop=True) ** lamda)

    # Normalize to 1 across modes
    w0 = w0 / w0.sum("t")

    # Insert into `weight`
    *_, weight.loc[y0_] = xr.align(weight, xr.DataArray.from_series(w0.to_series()))

    # Weights at the convergence year, yC
    for node in nodes:
        # Retrieve reference nodes: a set of 0+ nodes to converge towards
        ref_nodes = cfg.share_weight_convergence[node]

        # Indexers
        _1 = dict(n=node, **yC)  # Same node, convergence year
        _2 = dict(n=ref_nodes, **y0)  # Reference node(s), base year

        if ref_nodes:
            # Ratio between this node's GDP in yC and the mean of the reference nodes'
            # GDP values in y0. Maximum 1.0.
            k = min(
                (gdp.sel(_1) / (gdp.sel(_2).sum() / float(len(ref_nodes)))).item(), 1.0
            )
            # As k tends to 1, converge towards the mean of the reference nodes' share
            # weights in y0/base shares.
            target = weight.sel(_2).mean("n")
        else:
            # `node` without `ref_nodes`
            # Arbitrary value
            k = 1 / 3.0
            # As k tends to 1, converge towards equal weights
            target = xr.DataArray(1.0 / len(t_modes))

        # Scale weights in convergence year
        # - As k tends to 0, converge towards the same node's base shares.
        weight.loc[_1] = k * target + (1 - k) * weight.sel(n=node, **y0)

    # Interpolate linearly between y0 and yC
    # NB this will not work if yC is before the final period; it will leave NaN after yC
    weight = weight.interpolate_na(dim="y")

    return genno.Quantity(weight)


def smooth(qty: "AnyQuantity") -> "AnyQuantity":
    """Smooth `qty` (e.g. PRICE_COMMODITY) in the ``y`` dimension."""
    from genno.operator import add, concat

    # General smoothing
    result = add(0.25 * qty.shift(y=-1), 0.5 * qty, 0.25 * qty.shift(y=1))

    y = qty.coords["y"].values

    # Shorthand for weights
    def _w(values, years):
        return genno.Quantity(values, coords={"y": years}, units="")

    # First period
    r0 = (qty * _w([0.4, 0.4, 0.2], y[:3])).sum("y").expand_dims(dict(y=y[:1]))

    # Final period. “closer to the trend line”
    # NB the inherited R file used a formula equivalent to weights like [-⅛, 0, ⅜, ¾];
    # didn't make much sense.
    r_m1 = (qty * _w([0.2, 0.2, 0.6], y[-3:])).sum("y").expand_dims(dict(y=y[-1:]))

    # apply_units() is to work around khaeru/genno#64
    # TODO remove when fixed upstream
    return apply_units(concat(r0, result.sel(y=y[1:-1]), r_m1), qty.units)


def _add_transport_data(func, c: "Computer", name: str, *, key) -> None:
    """Add data from `key` to the target scenario.

    Adds one task to `c` that uses :func:`.add_par_data` to store the data from `key` on
    "scenario". Also updates the "add transport data" computation by appending the new
    task.
    """
    c.add(f"add {name}", "add_par_data", "scenario", key, "dry_run", strict=True)
    c.graph["add transport data"].append(f"add {name}")


@Operator.define(helper=_add_transport_data)
def transport_data(*args):
    """No action.

    This exists to connect :func:`._add_transport_data` to
    :meth:`genno.Computer.add`.
    """
    pass  # pragma: no cover


def transport_check(scenario: "Scenario", ACT: "AnyQuantity") -> pd.Series:
    """Reporting operator for :func:`.check`."""
    info = ScenarioInfo(scenario)

    # Mapping from check name → bool
    checks = {}

    # Correct number of outputs
    ACT_lf = ACT.sel(t=["transport freight load factor", "transport pax load factor"])
    checks["'transport * load factor' technologies are active"] = len(
        ACT_lf
    ) == 2 * len(info.Y) * (len(info.N) - 1)

    # # Force the check to fail
    # checks['(fail for debugging)'] = False

    return pd.Series(checks)


def votm(gdp_ppp_cap: "AnyQuantity") -> "AnyQuantity":
    """Calculate value of time multiplier.

    A value of 1 means the VoT is equal to the wage rate per hour.

    Parameters
    ----------
    gdp_ppp_cap
        PPP GDP per capita.
    """
    from genno.operator import assign_units

    u = gdp_ppp_cap.units
    assert_units(gdp_ppp_cap, "kUSD / passenger / year")
    n = gdp_ppp_cap.coords["n"].data

    result = 1 / (
        1
        + assign_units(
            np.exp(
                (genno.Quantity(30, units=u).expand_dims({"n": n}) - gdp_ppp_cap) / 20
            ),
            units="",
        )
    )
    assert_units(result, "")
    return result
