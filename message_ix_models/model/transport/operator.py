""":mod:`genno` operators for MESSAGEix-Transport."""

import logging
import re
from collections.abc import Hashable, Sequence
from functools import partial
from itertools import product
from operator import gt, le, lt
from typing import TYPE_CHECKING, Any, Literal, Optional, Union, cast

import genno
import numpy as np
import pandas as pd
import xarray as xr
from genno import Computer, KeySeq, Operator, quote
from genno.operator import apply_units, as_quantity, rename_dims
from genno.testing import assert_qty_allclose, assert_units
from scipy import integrate
from sdmx.model.common import Code, Codelist

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codelist
from message_ix_models.project.navigate import T35_POLICY
from message_ix_models.report.operator import compound_growth
from message_ix_models.util import (
    MappingAdapter,
    datetime_now_with_tz,
    minimum_version,
    show_versions,
)

from .config import Config

if TYPE_CHECKING:
    from pathlib import Path

    import sdmx.message
    from genno.types import AnyQuantity, TQuantity
    from message_ix import Scenario
    from xarray.core.types import Dims

    import message_ix_models.model.transport.factor
    from message_ix_models import Context

log = logging.getLogger(__name__)

__all__ = [
    "base_model_data_header",
    "base_shares",
    "broadcast_advance",
    "broadcast_t_c_l",
    "broadcast_y_yv_ya",
    "cost",
    "distance_ldv",
    "distance_nonldv",
    "dummy_prices",
    "duration_period",
    "extend_y",
    "factor_fv",
    "factor_input",
    "factor_pdt",
    "groups_iea_eweb",
    "groups_y_annual",
    "iea_eei_fv",
    "indexer_scenario",
    "indexers_n_cd",
    "indexers_usage",
    "logit",
    "max",
    "maybe_select",
    "min",
    "price_units",
    "quantity_from_config",
    "relabel2",
    "sales_fraction_annual",
    "share_weight",
    "smooth",
    "transport_check",
    "transport_data",
    "votm",
]


def base_model_data_header(scenario: "Scenario", *, name: str) -> dict[str, str]:
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
    base: "AnyQuantity", nodes: list[str], techs: list[str], y: list[int]
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

    missing = cast(set[Hashable], set("nty")) - set(result.dims)
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


def broadcast(q1: "AnyQuantity", q2: "AnyQuantity") -> "AnyQuantity":
    import numpy as np

    # Squeeze dimensions of q1 that are (a) in q2 and (b) contain only NaN or None
    # labels
    squeezed = q1
    for d in q2.dims:
        if set(q1.coords[d].data) <= {np.nan, None}:
            squeezed = squeezed.squeeze(dim=d)

    # TODO Use the following once supported by genno
    # squeezed = q1.squeeze(
    #     dim=[d for d in q2.dims if set(q1.coords[d].data) <= {np.nan, None}]
    # )

    return squeezed * q2


def broadcast_t_c_l(
    technologies: list[Code],
    commodities: list[Union[Code, str]],
    kind: str,
    default_level: Optional[str] = None,
) -> "AnyQuantity":
    """Return a Quantity for broadcasting dimension (t) to (c, l) for `kind`.

    Parameter
    ---------
    kind :
       Either "input" or "output".
    """
    assert kind in ("input", "output")

    # Convert list[Union[Code, str]] into an SDMX Codelist for simpler usage
    cl_commodity: "Codelist" = Codelist()
    for c0 in commodities:
        cl_commodity.setdefault(id=c0.id if isinstance(c0, Code) else c0)

    # Map each `tech` to a `commodity` and `level`
    data = []
    for tech in technologies:
        # Retrieve the "input" or "output" annotation for this technology
        input_ = tech.eval_annotation(kind)
        if input_ is None:
            continue  # No I/O commodity for this technology → skip

        # Retrieve the "commodity" key: either the ID of one commodity, or a sequence
        commodity = input_.get("commodity", ())

        # Iterate over 0 or more commodity IDs
        for c_id in (commodity,) if isinstance(commodity, str) else commodity:
            try:
                # Retrieve the Code object for this commodity
                c1 = cl_commodity[c_id]
            except KeyError:
                continue  # Unknown commodity

            # Level, in order of precedence:
            # 1. Technology-specific input level from `t_code`.
            # 2. Default level for the commodity from `c_code`.
            # 3. `default_level` argument to this function.
            try:
                level_anno = str(c1.get_annotation(id="level").text)
            except (AttributeError, KeyError):
                level_anno = None
            level = input_.get("level", level_anno or default_level)

            data.append((tech.id, c1, level))

    idx = pd.MultiIndex.from_frame(pd.DataFrame(data, columns=["t", "c", "l"]))
    s = pd.Series(1.0, index=idx)
    return genno.Quantity(s)


def broadcast_y_yv_ya(
    y: list[int], y_include: list[int], *, method: str = "product"
) -> "AnyQuantity":
    r"""Return a quantity for broadcasting y to (yv, ya).

    This omits all :math:`y^V \notin y^{include}`.

    If :py:`"y::model"` is passed as `y_include`, this is equivalent to
    :attr:`.ScenarioInfo.yv_ya`.

    Parameters
    ----------
    method :
        Either "product" or "zip".
    """
    dims = ["y", "yv", "ya"]
    func = {"product": product, "zip": zip}[method]
    series = (
        pd.DataFrame(func(y, y_include), columns=dims[1:])
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
    y: list[int],
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
    "Freight trains": "F RAIL",
    "Freight trucks": "F ROAD",
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


def duration_period(info: "ScenarioInfo") -> "AnyQuantity":
    """Convert ``duration_period`` parameter data to :class:`.Quantity`.

    .. todo:: Move to a more general module/location.
    """
    from genno.operator import unique_units_from_dim

    return genno.Quantity(
        info.par["duration_period"]
        .replace("y", "year")
        .rename(columns={"year": "y"})
        .set_index(["y", "unit"])["value"]
    ).pipe(unique_units_from_dim, "unit")


def extend_y(qty: "AnyQuantity", y: list[int], *, dim: str = "y") -> "AnyQuantity":
    """Extend `qty` along the dimension `dim` to cover all of `y`.

    - Values are first filled forward, then backwards, within existing `dim` labels in
      `qty`.
    - Labels in `y` that are *not* in `qty` are filled using the first or last existing
      value.
    """
    y_ = set(y)

    # Subset of `y` appearing in `qty`
    existing = sorted(set(qty.coords[dim].data) & y_)
    # y-coords to fill backward from the earliest appearing in `qty`
    to_bfill = sorted(filter(partial(gt, existing[0]), y_))
    # y-coords to fill forward from the latest appearing in `qty`
    to_ffill = sorted(filter(partial(lt, existing[-1]), y_))

    log.info(
        f"{qty.name}: extend {to_bfill} ← {dim}={existing[0]}; "
        f"{dim}={existing[-1]} → {to_ffill}"
    )

    # Map existing labels to themselves, and missing labels to the first or last in
    # `existing`
    y_map = (
        [(existing[0], y) for y in to_bfill]
        + [(y, y) for y in existing]
        + [(existing[-1], y) for y in to_ffill]
    )
    # - Forward-fill within existing y-coords of `qty`.
    # - Backward-fill within existing y-coords of `qty`
    # - Use MappingAdapter to do the rest.
    return MappingAdapter({dim: y_map})(qty.ffill(dim).bfill(dim))  # type: ignore [attr-defined]


def factor_fv(n: list[str], y: list[int], config: dict) -> "AnyQuantity":
    """Scaling factor for freight activity.

    If :attr:`.Config.project` is :data:`ScenarioFlags.ACT`, the value declines from
    1.0 at the first `y` to 0.865 (reduction of 13.5%) at y=2050, then constant
    thereafter.

    Otherwise, the value is 1.0 for every (`n`, `y`).
    """
    from message_ix_models.util import broadcast

    # Empty data frame
    df = pd.DataFrame(columns=["value"], index=pd.Index(y, name="y"))

    # Default value
    df.iloc[0, :] = 1.0

    # NAVIGATE T3.5 "act" demand-side scenario
    if T35_POLICY.ACT & config["transport"].project.get("navigate", T35_POLICY.REF):
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
    y: list[int], t: list[Code], t_agg: dict, config: dict
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
    if T35_POLICY.TEC & config["transport"].project.get("navigate", T35_POLICY.REF):
        years = list(filter(partial(gt, 2050), df.index))

        # Prepare a dictionary mapping technologies to their respective EI improvement
        # rates
        t_groups = t_agg["t"]
        value = {}
        for group, v in {
            "2W": 1.5,
            "BUS": 1.5,
            "LDV": 1.5,
            "F ROAD": 2.0,
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


def factor_pdt(n: list[str], y: list[int], t: list[str], config: dict) -> "AnyQuantity":
    """Scaling factor for passenger activity.

    When :attr:`.Config.scenarios` includes :attr:`ScenarioFlags.ACT` (i.e. NAVIGATE
    Task 3.5, demand-side scenario "act"), the value of 0.8 is specified for LDV, 2050,
    and all regions. This function implements this as a linear decrease between the
    first model period (currently 2020) and that point.

    Otherwise, the value is 1.0 for every (`n`, `t`, `y`).
    """
    from message_ix_models.util import broadcast

    # Empty data frame
    df = pd.DataFrame(columns=t, index=pd.Index(y, name="y"))

    # Set 1.0 (no scaling) for first period
    df.iloc[0, :] = 1.0

    # Handle particular scenarios
    if T35_POLICY.ACT & config["transport"].project.get("navigate", T35_POLICY.REF):
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
    nodes: list[str],
    years: list[int],
    *others: list,
    info: "message_ix_models.model.transport.factor.Factor",
    extra_dims: Optional[Sequence[str]] = None,
) -> "AnyQuantity":
    """Return a scaling factor for an SSP realization."""
    kw = dict(n=nodes, y=years, scenario=config["transport"].ssp)
    for dim, labels in zip(extra_dims or (), others):
        kw[dim] = labels
    return info.quantify(**kw)


def freight_usage_output(context: "Context") -> "AnyQuantity":
    """Output efficiency for ``transport F {MODE} usage`` pseudo-technologies.

    Returns
    -------
    Quantity
        with dimension |t|
    """
    modes = "F RAIL", "F ROAD"  # TODO Retrieve this from configuration
    return genno.Quantity(
        [context.transport.load_factor[m] for m in modes],
        coords=dict(t=[f"transport {m} usage" for m in modes]),
        units="Gt km",
    )


Groups = dict[str, dict[str, list[str]]]


def groups_iea_eweb(technologies: list[Code]) -> tuple[Groups, Groups, dict]:
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
    g2: dict = dict(t=[], t_new=[])

    def replace(value: str) -> str:
        """Map original codes to codes derived with :func:`.web.transform_C`."""
        return {"DOMESAIR": "_1", "TOTTRANS": "_2"}.get(value, value)

    # Add groups from base model commodity code list:
    # - IEA product list → MESSAGE commodity (e.g. "lightoil")
    # - IEA flow list → MESSAGE technology group (e.g. "transport")
    for c in get_codelist("commodity"):
        if products := c.eval_annotation(id="iea-eweb-product"):
            g0["product"][c.id] = products
        if flows := c.eval_annotation(id="iea-eweb-flow"):
            g0["flow"][c.id] = list(map(replace, flows))

    # Add groups from MESSAGEix-Transport technology code list
    for t in technologies:
        if flows := list(map(replace, t.eval_annotation(id="iea-eweb-flow") or [])):
            target = flows[0] if len(flows) == 1 and flows != ["_1"] else t.id

            g0["flow"][target] = flows

            # Append the mode name, for instance "AIR"
            g1["t"].setdefault(target, []).append(t.id)
            # # Append the name of individual technologies for this mode
            # g1["t"][target].extend(map(lambda c: c.id, t.child))

            g2["t"].append(target)
            g2["t_new"].append(t.id)

    g2["t"] = xr.DataArray(g2.pop("t"), coords=[("t_new", g2.pop("t_new"))])

    return g0, g1, g2


def groups_y_annual(duration_period: "AnyQuantity") -> dict[str, dict[int, list[int]]]:
    """Return a list of groupers for aggregating annual data to MESSAGE periods.

    .. todo:: Move to a more general module/location.
    """
    result = {}
    for (period,), duration in duration_period.to_series().items():
        result[period] = list(range(period + 1 - int(duration), period + 1))
    return dict(y=result)


def logit(
    x: "AnyQuantity", k: "AnyQuantity", lamda: "AnyQuantity", y: list[int], dim: str
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


def maybe_select(qty: "TQuantity", *, indexers: dict) -> "TQuantity":
    """Select from `qty` if possible, using :py:`"*"` wildcard.

    Same as :func:`genno.operator.select`, except:

    1. If not all the dimensions of `indexers` are in `qty`, no selection is performed.
    2. For each dimension of `indexers`, if the corresponding (scalar) value is not
       present in `qty`, it is replaced with "*". `qty` **should** contain this value
       along every dimension to be selected; otherwise, the result will be empty.
    """
    from genno.operator import select

    try:
        idx = {}
        for dim, value in indexers.items():
            if value in qty.coords[dim]:
                idx[dim] = value
            else:
                log.debug(f"Use {dim}='*' for missing {value!r}")
                idx[dim] = "*"
    except ValueError as e:
        msg = f"{e.args[0]} not among dims {qty.dims} of {qty.name}; no selection"
        log.info(msg)
        return qty
    else:
        return select(qty, indexers=idx)


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


def iea_eei_fv(name: str, config: dict) -> "AnyQuantity":
    """Returns base-year demand for freight from IEA EEI, with dimensions n-c-y."""
    from message_ix_models.tools.iea import eei

    result = eei.as_quantity(name, config["regions"])  # type: ignore [attr-defined]
    ym1 = result.coords["y"].data[-1]

    log.info(f"Use y={ym1} data for base-year freight transport activity")

    assert set("nyt") == set(result.dims)
    return result.sel(y=ym1, t="Total freight transport", drop=True)


def indexer_scenario(config: dict, *, with_LED: bool) -> dict[Literal["scenario"], str]:
    """Indexer for the ``scenario`` dimension.

    If `with_LED` **and** :py:`config.project["LDV"] = True`, then the single label is
    "LED". Otherwise it is the final part of the :attr:`.transport.config.Config.ssp`
    URN, e.g. "SSP(2024).1". In other words, this treats "LDV" as mutually exclusive
    with an SSP scenario identifier (instead of orthogonal).

    Parameters
    ----------
    config :
        The genno.Computer "config" dictionary, with a key "transport" mapped to an
        instance of :class:`.transport.Config`.
    """
    # Retrieve the .transport.Config object from the genno.Computer "config" dict
    c: "Config" = config["transport"]

    return dict(
        scenario="LED"
        if (with_LED and c.project.get("LED", False))
        else c.ssp.urn.rpartition(":")[2]
    )


def indexers_n_cd(config: dict) -> dict[str, xr.DataArray]:
    """Indexers for selecting (`n`, `census_division`) → `n`.

    Based on :attr:`.Config.node_to_census_division`.
    """
    n_cd_map = config["transport"].node_to_census_division
    n, cd = zip(*n_cd_map.items())
    return dict(
        n=xr.DataArray(list(n), dims="n"),
        census_division=xr.DataArray(list(cd), dims="n"),
    )


def indexers_usage(technologies: list[Code]) -> dict:
    """Indexers for replacing LDV `t` and `cg` with `t_new` for usage technologies."""
    labels: dict[str, list[str]] = dict(cg=[], t=[], t_new=[])
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


def price_units(qty: "TQuantity") -> "TQuantity":
    """Forcibly adjust price units, if necessary."""
    target = "USD_2010 / km"
    if not qty.units.is_compatible_with(target):
        log.warning(f"Adjust price units from {qty.units} to {target}")
    return apply_units(qty, target)


def quantity_from_config(
    config: dict, name: str, dimensionality: Optional[dict] = None
) -> "AnyQuantity":
    if dimensionality:
        raise NotImplementedError
    result = getattr(config["transport"], name)
    if not isinstance(result, genno.Quantity):
        result = as_quantity(result)
    return result


def relabel2(qty: "TQuantity", new_dims: dict) -> "TQuantity":
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


@minimum_version("python 3.10")
def uniform_in_dim(value: "AnyQuantity", dim: str = "y") -> "AnyQuantity":
    """Construct a uniform distribution from `value` along its :math:`y`-dimension.

    `value` must have a dimension `dim` with length 1 and a single value, :math:`k`. The
    sole `dim`-coordinate is taken as :math:`d_{max}`: the upper end of a uniform
    distribution of which the mean is :math:`d_{max} - k`.

    Returns
    -------
    genno.Quantity
        with dimension `dim`, and `dim` coordinates including all integer values up to
        and including :math:`d_{max}`. Values are the piecewise integral of the uniform
        distribution in the interval ending at the respective coordinate.
    """
    from itertools import pairwise

    d_max = value.coords[dim].item()  # Upper end of the distribution
    width = 2 * value.item()  # Width of a uniform distribution
    height = 1.0 / width  # Height of the distribution
    d_min = d_max - width  # Lower end of the distribution

    def _uniform(x: float) -> float:
        """Uniform distribution between `d_min` and `d_max`."""
        return height if d_min < x < d_max else 0.0

    # Points for piecewise integration of `_uniform`: integers from the first <= d_min
    # to d_max inclusive
    points = np.arange(np.floor(d_min), d_max + 1).astype(int)

    # - Group `points` pairwise: (d0, d1), (d1, d2)
    # - On each interval, compute the integral of _uniform() by quadrature.
    # - Convert to Quantity with y-dimension, labelled with interval upper bounds.
    return genno.Quantity(
        pd.Series(
            {b: integrate.quad(_uniform, a, b)[0] for a, b in pairwise(points)}
        ).rename_axis(dim),
        units="",
    )


def sales_fraction_annual(age: "TQuantity") -> "TQuantity":
    """Return fractions of current vehicle stock that should be added in prior years.

    Parameters
    ---
    age : genno.Quantity
        Mean age of vehicle stock. Must have dimension "y" and at least 1 other
        dimension. For every unique combination of those other dimensions, there must be
        only one value/|y|-coordinate. This is taken as the *rightmost* end of a uniform
        distribution with mean age given by the respective value.

    Returns
    -------
    genno.Quantity
        Same dimensionality as `age`, with sufficient |y| coordinates to cover all years
        in which. Every integer year is included, i.e. the result is **not** aggregated
        to multi-year periods (called ``year`` in MESSAGE).
    """
    # - Group by all dims other than `y`.
    # - Apply the function to each scalar value.
    dims = list(filter(lambda d: d != "y", age.dims))
    return age.groupby(dims).apply(uniform_in_dim)


def scenario_codes() -> list[str]:
    """Return valid codes for a `scenario` dimension of some quantities.

    The list includes:

    - Values like "SSP(2024).1" for every member of the :data:`SSP_2024` enumeration.
    - The value "LED".

    For use with, for instance :func:`.broadcast_wildcard`.
    """
    from message_ix_models.project.ssp import SSP_2024

    return [c.urn.rpartition(":")[2] for c in SSP_2024] + ["LED"]


def share_weight(
    share: "AnyQuantity",
    gdp: "AnyQuantity",
    cost: "AnyQuantity",
    lamda: "AnyQuantity",
    t_modes: list[str],
    y: list[int],
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
    y0: dict[Any, Any] = dict(y=y[0])
    y0_ = dict(y=[y[0]])  # Do not drop
    yC: dict[Any, Any] = dict(y=cfg.year_convergence)

    # Weights in y0 for all modes and nodes
    idx = dict(t=t_modes, n=nodes) | y0
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
        _1 = dict(n=node) | yC  # Same node, convergence year
        _2 = dict(n=ref_nodes) | y0  # Reference node(s), base year

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

    This exists to connect :func:`._add_transport_data` to :meth:`genno.Computer.add`.
    """
    pass  # pragma: no cover


def transport_check(scenario: "Scenario", ACT: "AnyQuantity") -> pd.Series:
    """Reporting operator for :func:`.check`."""
    info = ScenarioInfo(scenario)

    # Mapping from check name → bool
    checks = {}

    # Correct number of outputs
    ACT_lf = ACT.sel(t=["transport f load factor", "transport pax load factor"])
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


def write_report_debug(qty: "AnyQuantity", path: "Path", kwargs=None) -> None:
    """Similar to :func:`.genno.operator.write_report`, but include units.

    This version is used only in :func:`.add_debug`.

    .. todo:: Move upstream, to :mod:`genno`.
    """
    from genno import operator

    from message_ix_models.util import datetime_now_with_tz

    kwargs = kwargs or dict()
    kwargs.setdefault(
        "header_comment",
        f"""`{qty.name}` data from MESSAGEix-Transport calibration.

Generated: {datetime_now_with_tz().isoformat()}

Units: {qty.units:~}
""",
    )

    operator.write_report(qty, path, kwargs)


def write_sdmx_data(
    qty: "AnyQuantity",
    structure_message: "sdmx.message.StructureMessage",
    scenario: "ScenarioInfo",
    path: "Path",
    *,
    df_urn: str,
) -> None:
    """Write two files for `qty`.

    1. :file:`{path}/{dataflow_id}.csv` —an SDMX-CSV :class:`.DataMessage` with the
       values from `qty`.
    2. :file:`{path}/{dataflow_id}.xml` —an SDMX-ML :class:`.DataMessage` with the
       values from `qty`.

    …where `dataflow_id` is the ID of a dataflow referred to by `df_urn`.

    The `structure_message` is updated with the relevant structures.
    """
    import sdmx
    from genno.compat.sdmx.operator import quantity_to_message
    from sdmx.model import common, v21

    from message_ix_models.util.sdmx import DATAFLOW, collect_structures

    # Add a dataflow and related structures to `structure_message`
    dfd = DATAFLOW[df_urn].df
    assert isinstance(dfd, v21.DataflowDefinition)
    collect_structures(structure_message, dfd)

    # Convert `qty` to DataMessage
    # FIXME Remove exclusion once upstream type hint is improved
    dm = quantity_to_message(qty, structure=dfd.structure)  # type: ignore [arg-type]

    # Identify the first/only data set in the message
    ds = dm.data[0]

    # Add attribute values
    for attr_id, value in (
        ("MODEL", scenario.model),
        ("SCENARIO", scenario.scenario),
        ("VERSION", scenario.version),
        ("UNIT_MEASURE", f"{qty.units}"),
    ):
        ds.attrib[attr_id] = common.AttributeValue(
            value=str(value), value_for=dfd.structure.attributes.get(attr_id)
        )

    # Write SDMX-ML
    path.mkdir(parents=True, exist_ok=True)
    path.joinpath(f"{dfd.id}.xml").write_bytes(sdmx.to_xml(dm, pretty_print=True))

    # Convert to SDMX_CSV
    # FIXME Remove this once sdmx1 supports it directly
    # Fixed values in certain columns
    assert dfd.urn is not None
    fixed_cols = dict(
        STRUCTURE="dataflow", STRUCTURE_ID=dfd.urn.split("=")[-1], ACTION="I"
    )
    # SDMX-CSV column order
    columns = list(fixed_cols)
    columns.extend(dim.id for dim in dfd.structure.dimensions)
    columns.extend(measure.id for measure in dfd.structure.measures)

    # Write SDMX-CSV
    df = (
        qty.to_series()
        .rename("value")
        .reset_index()
        .assign(**fixed_cols)
        .reindex(columns=columns)
    )
    df.to_csv(path.joinpath(f"{dfd.id}.csv"), index=False)


def write_sdmx_structures(structure_message, path: "Path", *args) -> "Path":
    """Write `structure_message`.

    The message is written to :file:`{path}/structure.xml` in SDMX-ML format.
    """
    import sdmx

    path.mkdir(parents=True, exist_ok=True)
    path.joinpath("structure.xml").write_bytes(
        sdmx.to_xml(structure_message, pretty_print=True)
    )

    return path
