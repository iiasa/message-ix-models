"""Demand calculation for MESSAGEix-Transport."""
import logging
from functools import partial
from pathlib import Path

import message_ix
import numpy as np
import pandas as pd
import xarray as xr
from dask.core import quote
from genno import Computer, Key, KeyExistsError, Quantity, computations
from iam_units import registry
from ixmp.reporting import RENAME_DIMS
from message_ix import make_df
from message_ix.reporting import Reporter
from message_ix_models import Context, ScenarioInfo
from message_ix_models.util import adapt_R11_R14, broadcast, check_support

from message_data.model.transport.build import generate_set_elements
from message_data.model.transport.computations import dummy_prices, rename
from message_data.model.transport.data.groups import get_consumer_groups, population
from message_data.model.transport.plot import DEMAND_PLOTS
from message_data.model.transport.utils import path_fallback

log = logging.getLogger(__name__)


def dummy(info):
    """Dummy demands.

    Parameters
    ----------
    info : .ScenarioInfo
    """
    common = dict(
        year=info.Y,
        value=10 + np.arange(len(info.Y)),
        level="useful",
        time="year",
    )

    dfs = []

    for commodity in generate_set_elements("commodity"):
        try:
            commodity.get_annotation(id="demand")
        except KeyError:
            # Not a demand commodity
            continue

        dfs.append(
            make_df(
                "demand",
                commodity=commodity.id,
                unit="t km" if "freight" in commodity.id else "km",
                **common,
            )
        )

    # # Dummy demand for light oil
    # common['level'] = 'final'
    # dfs.append(
    #     make_df('demand', commodity='lightoil', **common)
    # )

    return pd.concat(dfs).pipe(broadcast, node=info.N[1:])


def from_scenario(scenario: message_ix.Scenario) -> Reporter:
    """Return a Reporter for calculating demand based on `scenario`.

    Parameters
    ----------
    Scenario
        Solved Scenario

    Returns
    -------
    Reporter
    """
    rep = Reporter.from_scenario(scenario)

    prepare_reporter(rep, Context.get_instance())

    return rep


def from_external_data(info: ScenarioInfo, context: Context) -> Computer:
    """Return a Reporter for calculating demand from external data."""
    c = Computer()
    prepare_reporter(c, context, exogenous_data=True, info=info)

    return c


def add_exogenous_data(c: Computer, context: Context) -> None:
    """Add exogenous data to `c` that mocks data coming from an actual Scenario.

    The specific quantities added are:

    - ``GDP:n-y``, from :file:`gdp.csv`.
    - ``MERtoPPP:n-y``, from :file:`mer-to-ppp.csv`.
    - ``PRICE_COMMODITY:n-c-y``, currently mocked based on the shape of ``GDP:n-y``
      using :func:`.dummy_prices`.

      .. todo:: Add an external data source.

    If ``context.regions`` is “R14”, data are adapted from R11 using
    :func:`.adapt_R11_R14`.

    See also
    --------
    :doc:`/reference/model/transport/data`
    """
    check_support(
        context,
        settings=dict(regions=frozenset(["R11", "R14"])),
        desc="Exogenous data for demand projection",
    )

    gdp_k = Key("GDP", "ny")

    # Add 3 computations per quantity
    for key, basename, units in (
        (gdp_k, "gdp", "GUSD/year"),
        (Key("MERtoPPP", "ny"), "mer-to-ppp", ""),
    ):
        # 1. Load the file
        k1 = Key(key.name, tag="raw")
        c.add(
            k1,
            partial(computations.load_file, units=units),
            path_fallback("R11", f"{basename}.csv"),
        )

        # 2. Rename dimensions
        k2 = key.add_tag("R11")
        c.add(k2, rename, k1, quote(RENAME_DIMS))

        # 3. Maybe transform from R11 to another node list
        if context.regions == "R11":
            c.add(key, k2, sums=True, index=True)  # No-op/pass-through
        elif context.regions == "R14":
            c.add(key, adapt_R11_R14, k2, sums=True, index=True)

    c.add(Key("PRICE_COMMODITY", "ncy"), (dummy_prices, gdp_k), sums=True, index=True)


def add_structure(c: Computer, info: ScenarioInfo):
    """Add keys to `c` for model structure required by demand computations.

    This uses `info` to mock the contents that would be reported from an already-
    populated Scenario for sets "node", "year", and "cat_year".
    """
    for key, value in (
        ("n", quote(list(map(str, info.set["node"])))),
        ("nodes", quote(info.set["node"])),
        ("y", quote(info.set["year"])),
        (
            "cat_year",
            pd.DataFrame([["firstmodelyear", info.y0]], columns=["type_year", "year"]),
        ),
    ):
        try:
            # strict=True to raise an exception if `key` exists
            c.add(key, value, strict=True)
        except KeyExistsError:
            # Already present; don't overwrite
            continue


def prepare_reporter(
    rep: Computer,
    context: Context,
    configure: bool = True,
    exogenous_data: bool = False,
    info: ScenarioInfo = None,
) -> None:
    """Prepare `rep` for calculating transport demand.

    Parameters
    ----------
    rep : Reporter
        Must contain the keys ``<GDP:n-y>``, ``<MERtoPPP:n-y>``.
    """
    if configure:
        # Configure the reporter; keys are stored
        rep.configure(transport=context["transport config"])

    add_structure(rep, info)

    if exogenous_data:
        add_exogenous_data(rep, context)

    rep.graph["config"].update(
        {
            "output_path": context.get("output_path", Path.cwd()),
            "regions": context.regions,
        }
    )

    # Existing keys, prepared by from_scenario() or from_external_data()
    gdp = rep.full_key("GDP")
    mer_to_ppp = rep.full_key("MERtoPPP")
    price_full = rep.full_key("PRICE_COMMODITY").drop("h", "l")

    # Values based on configuration
    rep.add("speed:t", speed, "config")
    rep.add("whour:", whour, "config")
    rep.add("lambda:", _lambda, "config")

    # List of nodes excluding "World"
    # TODO move upstream to message_ix
    rep.add("n:ex world", nodes_ex_world, "n")
    rep.add("n:ex world+code", nodes_ex_world, "nodes")

    # List of model years
    rep.add("y:model", model_periods, "y", "cat_year")

    # Base share data
    rep.add("base shares:n-t-y", base_shares, "n:ex world", "y", "config")

    # Population data from GEA
    pop_key = rep.add(
        "population:n-y", partial(population, extra_dims=False), "y", "config"
    )

    # Consumer group sizes
    # TODO ixmp is picky here when there is no separate argument to the callable; fix.
    cg_key = rep.add("cg share:n-y-cg", get_consumer_groups, quote(context))

    # PPP GDP, total and per capita
    gdp_ppp = rep.add("product", "GDP:n-y:PPP", gdp, mer_to_ppp)
    gdp_ppp_cap = rep.add("ratio", "GDP:n-y:PPP+capita", gdp_ppp, pop_key)

    # Total demand
    pdt_cap = rep.add("transport pdt:n-y:capita", pdt_per_capita, gdp_ppp_cap, "config")
    pdt_ny = rep.add("product", "transport pdt:n-y:total", pdt_cap, pop_key)

    # Value-of-time multiplier
    rep.add("votm:n-y", votm, gdp_ppp_cap)

    # Select only the price of transport services
    price_sel = rep.add(
        price_full.add_tag("transport"),
        rep.get_comp("select"),
        price_full,
        # TODO should be the full set of prices
        dict(c="transport"),
    )
    # Smooth prices to avoid zig-zag in share projections
    price = rep.add(price_sel.add_tag("smooth"), smooth, price_sel)

    # Transport costs by mode
    cost_key = rep.add(
        "cost:n-y-c-t",
        cost,
        price,
        gdp_ppp_cap,
        "whour:",
        "speed:t",
        "votm:n-y",
        "y:model",
    )

    # Share weights
    rep.add(
        "share weight:n-t-y",
        share_weight,
        "base shares:n-t-y",
        gdp_ppp_cap,
        cost_key,
        "n:ex world",
        "y:model",
        "t:transport",
        "cat_year",
        "config",
    )

    # Shares
    rep.add(
        "shares:n-t-y",
        partial(logit, dim="t"),
        cost_key,
        "share weight:n-t-y",
        "lambda:",
        "y:model",
    )

    # Total PDT shared out by mode
    pdt_nyt = rep.add("product", "transport pdt:n-y-t", pdt_ny, "shares:n-t-y")

    # Per capita
    rep.add("ratio", "transport pdt:n-y-t:capita", pdt_nyt, pop_key, sums=False)

    # LDV PDT shared out by mode
    rep.add("select", "transport ldv pdt:n-y:total", pdt_nyt, dict(t=["LDV"]))

    rep.add(
        "product",
        "transport ldv pdt",
        "transport ldv pdt:n-y:total",
        cg_key,
    )

    # Plots
    for plot in DEMAND_PLOTS:
        key = f"plot {plot.basename}"
        rep.add(key, plot.make_task())


def base_shares(nodes, y, config):
    """Return base mode shares."""
    modes = config["transport"]["demand modes"]
    # TODO replace with input data
    return Quantity(
        xr.DataArray(1.0 / len(modes), coords=[nodes, y, modes], dims=["n", "y", "t"])
    )


def model_periods(y, cat_year):
    """Return the elements of `y` beyond the firstmodelyear of `cat_year`."""
    return list(
        filter(
            lambda year: cat_year.query("type_year == 'firstmodelyear'")["year"].item()
            <= year,
            y,
        )
    )


def nodes_ex_world(nodes):
    """Nodes excluding 'World'."""
    return list(filter(lambda n_: "GLB" not in n_ and n_ != "World", nodes))


def share_weight(share, gdp_ppp_cap, cost, nodes, y, t, cat_year, config):
    """Calculate mode share weights."""
    # Modes from configuration
    modes = config["transport"]["demand modes"]

    # Lambda, from configuration
    lamda = config["transport"]["lambda"]

    # Selectors
    t0 = dict(t=modes[0])
    y0 = dict(y=y[0])
    yC = dict(y=config["transport"]["year convergence"])
    years = list(filter(lambda year: year <= yC["y"], y))

    # Share weights
    weight = xr.DataArray(coords=[nodes, years, modes], dims=["n", "y", "t"])

    # Weights in y0 for all modes and nodes
    s_y0 = share.sel(**y0, t=modes, n=nodes)
    c_y0 = cost.sel(**y0, t=modes, n=nodes).sel(c="transport", drop=True)
    tmp = s_y0 / c_y0 ** lamda

    # Normalize against first mode's weight
    # TODO should be able to avoid a cast and align here
    tmp = tmp / tmp.sel(t0, drop=True)
    *_, tmp = xr.align(weight.loc[y0], xr.DataArray.from_series(tmp).sel(y0))
    weight.loc[y0] = tmp

    # Normalize to 1 across modes
    weight.loc[y0] = weight.loc[y0] / weight.loc[y0].sum("t")

    # Weights at the convergence year, yC
    for node in nodes:
        # Set of 1+ nodes to converge towards
        ref_nodes = config["transport"]["share weight convergence"][node]

        # Ratio between this node's GDP and that of the first reference node
        scale = float(
            gdp_ppp_cap.sel(n=node, **yC, drop=True)
            / gdp_ppp_cap.sel(n=ref_nodes[0], **yC, drop=True)
        )

        # Scale weights in yC
        weight.loc[dict(n=node, **yC)] = scale * weight.sel(n=ref_nodes, **y0).mean(
            "n"
        ) + (1 - scale) * weight.sel(n=node, **y0)

    # Currently not enabled
    # “Set 2010 sweight to 2005 value in order not to have rail in 2010, where
    # technologies become available only in 2020”
    # weight.loc[dict(y=2010)] = weight.loc[dict(y=2005)]

    # Interpolate linearly between y0 and yC
    # NB this will not work if yC is before the final period; it will leave NaN
    #    after yC
    weight = weight.interpolate_na(dim="y")

    return Quantity(weight)


def speed(config):
    """Return travel speed [distance / time].

    The returned Quantity has dimension ``t`` (technology).
    """
    # Convert the dict from the YAML file to a Quantity
    data = pd.Series(config["transport"]["speeds"])
    dim = RENAME_DIMS.get(data.pop("_dim"))
    units = data.pop("_unit")
    return Quantity(data.rename_axis(dim), units=units)


def whour(config):
    """Return work duration [hours / person-year]."""
    q = registry(config["transport"]["work hours"])
    return Quantity(q.magnitude, units=q.units)


def _lambda(config):
    """Return lambda parameter for transport mode share equations."""
    return Quantity(config["transport"]["lambda"], units="")


def pdt_per_capita(gdp_ppp_cap, config):
    """Compute passenger distance traveled (PDT) per capita.

    Simplification of Schäefer et al. (2010): linear interpolation between (0, 0) and
    the configuration keys "fixed demand" and "fixed GDP".
    """
    fix_gdp = registry(config["transport"]["fixed GDP"])
    fix_demand = registry(config["transport"]["fixed demand"])

    result = (gdp_ppp_cap / fix_gdp.magnitude) * fix_demand.magnitude

    # Consistent output units
    result.attrs["_unit"] = (
        gdp_ppp_cap.attrs["_unit"] / fix_gdp.units
    ) * fix_demand.units

    return result


def assert_units(qty, exp):
    """Assert that `qty` has units `exp`."""
    assert (
        qty.units / qty.units._REGISTRY(exp)
    ).dimensionless, f"Units '{qty.units:~}'; expected {repr(exp)}"


def votm(gdp_ppp_cap):
    """Calculate value of time multiplier.

    A value of 1 means the VoT is equal to the wage rate per hour.

    Parameters
    ----------
    gdp_ppp_cap
        PPP GDP per capita.
    """
    assert_units(gdp_ppp_cap, "kUSD / passenger / year")

    result = Quantity(
        1 / (1 + np.exp((30 - gdp_ppp_cap) / 20)), units=registry.dimensionless
    )

    return result


def smooth(qty):
    """Smooth `qty` (e.g. PRICE_COMMODITY) in the ``y`` dimension."""
    # Convert to xr.DataArray because genno.AttrSeries lacks a .shift() method.
    # Conversion can be removed once Quantity is SparseDataArray.
    q = xr.DataArray.from_series(qty.to_series())

    y = q.coords["y"]

    # General smoothing
    result = 0.25 * q.shift(y=-1) + 0.5 * q + 0.25 * q.shift(y=1)

    # First period
    weights = xr.DataArray([0.4, 0.4, 0.2], coords=[y[:3]], dims=["y"])
    result.loc[dict(y=y[0])] = (q * weights).sum("y", min_count=1)

    # Final period. “closer to the trend line”
    # NB the inherited R file used a formula equivalent to weights like
    #    [-1/8, 0, 3/8, 3/4]; didn't make much sense.
    weights = xr.DataArray([0.2, 0.2, 0.6], coords=[y[-3:]], dims=["y"])
    result.loc[dict(y=y[-1])] = (q * weights).sum("y", min_count=1)

    # NB conversion can be removed once Quantity is SparseDataArray
    return Quantity(result, units=qty.attrs["_unit"])


def cost(price, gdp_ppp_cap, whours, speeds, votm, y):
    """Calculate cost of transport [money / distance].

    Calculated from two components:

    1. The inherent price of the mode.
    2. Value of time, in turn from:

       1. a value of time multiplier (`votm`),
       2. the wage rate per hour (`gdp_ppp_cap` / `whours`), and
       3. the travel time per unit distance (1 / `speeds`).
    """
    add = computations.add
    product = computations.product
    ratio = computations.ratio

    # NB for some reason, the 'y' dimension of result becomes `float`, rather than
    # `int`, in this step
    result = add(
        price,
        ratio(
            product(gdp_ppp_cap, votm),
            product(speeds, whours),
        ),
    )

    return result.sel(y=y)


def logit(x, k, lamda, y, dim):
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
    u = computations.product(k, computations.pow(x, lamda)).sel(y=y)

    # commented: for debugging
    # u.to_csv("u.csv")

    # Logit probability
    return computations.ratio(u, u.sum(dim))
