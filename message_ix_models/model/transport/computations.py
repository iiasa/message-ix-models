"""Reporting computations for MESSAGEix-Transport."""
import logging
from typing import Dict, Hashable, List

import numpy as np
import pandas as pd
import xarray as xr
from genno import Quantity, computations
from genno.computations import add, apply_units, product, ratio, relabel
from genno.testing import assert_units
from iam_units import registry
from ixmp import Scenario
from ixmp.reporting import RENAME_DIMS
from message_ix import make_df
from message_ix_models import ScenarioInfo

from message_data.model.transport.utils import path_fallback
from message_data.reporting.util import as_quantity
from message_data.tools.iea_eei import get_eei_data

log = logging.getLogger(__name__)


def base_shares(
    nodes: List[str], techs: List[str], y: List[int], config: dict
) -> Quantity:
    """Return base mode shares.

    The mode shares are read from a file at
    :file:`data/transport/{regions}/mode-shares/{name}.csv`, where `name` is from the
    configuration key ``mode-share:``, and `region` uses :func:`.path_fallback`.

    Labels on the t (technology) dimension must match the ``demand modes:`` from the
    configuration.

    If the data lack the n (node, spatial) and/or y (time) dimensions, they are
    broadcast over these.
    """
    # TODO write tests
    path = path_fallback(
        config["regions"], "mode-share", f"{config['transport']['mode-share']}.csv"
    )
    log.info(f"Read base mode shares from {path}")

    base = computations.load_file(path, dims=RENAME_DIMS, name="mode share", units="")

    missing = [d for d in "nty" if d not in base.dims]
    log.info(f"Broadcast base mode shares with dims {base.dims} over {missing}")

    coords = [("n", nodes), ("t", techs), ("y", y)]
    return product(base, Quantity(xr.DataArray(1.0, coords=coords), units=""))


def convert_units(qty: Quantity, units) -> Quantity:
    """Converty `qty` to `units`.

    .. todo:: move upstream to :mod:`genno`.
    """
    u = registry.Quantity(1.0, qty.units).to(units)
    result = u.magnitude * qty
    result.units = u.units
    return result


def cost(
    price: Quantity,
    gdp_ppp_cap: Quantity,
    whours: Quantity,
    speeds: Quantity,
    votm: Quantity,
    y: List[int],
) -> Quantity:
    """Calculate cost of transport [money / distance].

    Calculated from two components:

    1. The inherent price of the mode.
    2. Value of time, in turn from:

       1. a value of time multiplier (`votm`),
       2. the wage rate per hour (`gdp_ppp_cap` / `whours`), and
       3. the travel time per unit distance (1 / `speeds`).
    """
    # NB for some reason, the 'y' dimension of result becomes `float`, rather than
    # `int`, in this step
    result = add(price, ratio(product(gdp_ppp_cap, votm), product(speeds, whours)))
    return result.sel(y=y)


def demand_ixmp0(pdt1, pdt2) -> Dict[str, pd.DataFrame]:
    """Convert passenger transport demands to ixmp format.

    Expects the following inputs:

    - pdt1: "transport pdt:n-y-t"
    - pdt2: "transport ldv pdt:n-y-cg"
    """
    units = "Gp km / a"

    # Generate the demand data; convert to pd.DataFrame
    data = convert_units(pdt1, units).to_series().reset_index(name="value")

    common = dict(
        level="useful",
        time="year",
        unit=units,
    )

    # Convert to message_ix layout
    # TODO combine the two below in a loop or push the logic to demand.py
    data = make_df(
        "demand",
        node=data["n"],
        commodity="transport pax " + data["t"].str.lower(),
        year=data["y"],
        value=data["value"],
        **common,
    )
    data = data[~data["commodity"].str.contains("ldv")]

    data2 = convert_units(pdt2, units).to_series().reset_index(name="value")

    data2 = make_df(
        "demand",
        node=data2["n"],
        commodity="transport pax " + data2["cg"],
        year=data2["y"],
        value=data2["value"],
        **common,
    )

    return dict(demand=pd.concat([data, data2]))


def distance_ldv(config: dict) -> Quantity:
    """Return annual driving distance per LDV.

    - Regions other than R11_NAM have M/F values in same proportion to their A value as
      in NAM
    """
    # Load from config.yaml
    result = product(
        as_quantity(config["transport"]["ldv activity"]),
        as_quantity(config["transport"]["factor"]["activity"]["ldv"]),
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


def distance_nonldv(config: dict) -> Quantity:
    """Return annual travel distance per vehicle for non-LDV transport modes."""
    # Load from IEA EEI
    dfs = get_eei_data(config["transport"]["regions"])
    df = (
        dfs["vehicle use"]
        .rename(columns={"region": "nl", "year": "y", "Mode/vehicle type": "t"})
        .set_index(["nl", "t", "y"])
    )

    # Check units
    assert "kilovkm / vehicle" == registry.parse_units(
        df["units"].unique()[0].replace("10^3 ", "k")
    )
    units = "Mm / vehicle / year"

    # Rename IEA EEI technology IDs to model-internal ones
    result = relabel(
        Quantity(df["value"], name="non-ldv distance", units=units),
        dict(t=EEI_TECH_MAP),
    )

    # Select the latest year.
    # TODO check whether coverage varies by year; if so, then fill-forward or
    #      extrapolate
    y_m1 = result.coords["y"].data[-1]
    log.info(f"Return data for y={y_m1}")
    return result.sel(y=y_m1, drop=True)


def dummy_prices(gdp: Quantity) -> Quantity:
    # Commodity prices: all equal to 0.1

    # Same coords/shape as `gdp`, but with c="transport"
    coords = [(dim, item.data) for dim, item in gdp.coords.items()]
    coords.append(("c", ["transport"]))
    shape = list(len(c[1]) for c in coords)

    return Quantity(xr.DataArray(np.full(shape, 0.1), coords=coords), units="USD / km")


def _lambda(config: dict) -> Quantity:
    """Return (scalar) lambda parameter for transport mode share equations."""
    return Quantity(config["transport"]["lambda"], units="")


def logit(
    x: Quantity, k: Quantity, lamda: Quantity, y: List[int], dim: str
) -> Quantity:
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
    u = product(k, computations.pow(x, lamda)).sel(y=y)

    # commented: for debugging
    # u.to_csv("u.csv")

    # Logit probability
    return ratio(u, u.sum(dim))


def iea_eei(name: str, config: Dict) -> Quantity:
    """Returns base-year demand for freight from IEA EEI, with dimensions n-c-y."""
    from message_data.tools.iea_eei import as_quantity

    result = as_quantity(name, config["regions"])
    ym1 = result.coords["y"].data[-1]

    log.info(f"Use y={ym1} data for base-year freight transport activity")

    assert set("nyt") == set(result.dims)
    return result.sel(y=ym1, t="Total freight transport", drop=True)


def model_periods(y: List[int], cat_year: pd.DataFrame) -> List[int]:
    """Return the elements of `y` beyond the firstmodelyear of `cat_year`.

    .. todo:: move upstream, to :mod:`message_ix`.
    """
    return list(
        filter(
            lambda year: cat_year.query("type_year == 'firstmodelyear'")["year"].item()
            <= year,
            y,
        )
    )


def nodes_ex_world(nodes: list) -> List[str]:
    """Nodes excluding 'World'.

    .. todo:: move upstream, to :mod:`message_ix`.
    """
    return list(filter(lambda n_: "GLB" not in n_ and n_ != "World", nodes))


def nodes_world_agg(config, dim: Hashable = "nl") -> Dict[Hashable, Dict]:
    """Mapping to aggregate e.g. nl="World" from values for child nodes of "World".

    .. todo:: move upstream, to :mod:`message_ix_models`.
    """
    from message_ix_models.model.structure import get_codes

    for n in get_codes(f"node/{config['regions']}"):
        # "World" node should have no parent and some children. Countries (from
        # pycountry) that are omitted from a mapping have neither parent nor children.
        if len(n.child) and n.parent is None:
            return {dim: {str(n): list(map(str, n.child))}}

    # Failed to identify the World node.
    raise RuntimeError


def pdt_per_capita(gdp_ppp_cap: Quantity, config: dict) -> Quantity:
    """Compute passenger distance traveled (PDT) per capita.

    Simplification of Schäefer et al. (2010): linear interpolation between (0, 0) and
    the configuration keys "fixed demand" and "fixed GDP".
    """
    fix_gdp = as_quantity(config["transport"]["fixed GDP"])
    fix_demand = as_quantity(config["transport"]["fixed demand"])

    return product(ratio(gdp_ppp_cap, fix_gdp), fix_demand)


def price_units(qty: Quantity) -> Quantity:
    """Forcibly adjust price units, if necessary."""
    target = "USD_2010 / km"
    if not qty.units.is_compatible_with(target):
        log.warning(f"Adjust price units from {qty.units} to {target}")
    return apply_units(qty, target)


def share_weight(
    share: Quantity,
    gdp_ppp_cap: Quantity,
    cost: Quantity,
    lamda: Quantity,
    nodes: List[str],
    y: List[int],
    t: List[str],
    cat_year: pd.DataFrame,
    config: dict,
) -> Quantity:
    """Calculate mode share weights."""
    # Modes from configuration
    cfg = config["transport"]
    modes = cfg["demand modes"]

    # Selectors
    t0 = dict(t=modes[0])
    y0 = dict(y=y[0])
    yC = dict(y=cfg["year convergence"])
    years = list(filter(lambda year: year <= yC["y"], y))

    # Share weights
    coords = [("n", nodes), ("y", years), ("t", modes)]
    weight = xr.DataArray(np.nan, coords=coords)

    # Weights in y0 for all modes and nodes
    # NB here and below, with Python 3.9 one could do: dict(t=modes, n=nodes) | y0
    idx = dict(t=modes, n=nodes, **y0)
    s_y0 = share.sel(idx)
    c_y0 = cost.sel(idx).sel(c="transport", drop=True)
    tmp = computations.pow(ratio(s_y0, c_y0), lamda)

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
        ref_nodes = cfg["share weight convergence"][node]

        # Ratio between this node's GDP and that of the first reference node
        scale = (
            gdp_ppp_cap.sel(n=node, **yC, drop=True)
            / gdp_ppp_cap.sel(n=ref_nodes[0], **yC, drop=True)
        ).item()

        # Scale weights in yC
        _1, _2, _3 = dict(n=node, **yC), dict(n=ref_nodes, **y0), dict(n=node, **y0)
        weight.loc[_1] = scale * weight.sel(_2).mean("n") + (1 - scale) * weight.sel(_3)

    # Currently not enabled
    # “Set 2010 sweight to 2005 value in order not to have rail in 2010, where
    # technologies become available only in 2020”
    # weight.loc[dict(y=2010)] = weight.loc[dict(y=2005)]

    # Interpolate linearly between y0 and yC
    # NB this will not work if yC is before the final period; it will leave NaN after yC
    weight = weight.interpolate_na(dim="y")

    return Quantity(weight)


def smooth(qty: Quantity) -> Quantity:
    """Smooth `qty` (e.g. PRICE_COMMODITY) in the ``y`` dimension."""
    # General smoothing
    result = computations.add(
        0.25 * qty.shift(y=-1),
        0.5 * qty,
        0.25 * qty.shift(y=1),
    )

    y = qty.coords["y"].values

    # Shorthand for weights
    def _w(values, years):
        return Quantity(xr.DataArray(values, coords=[("y", years)]), units="")

    # First period
    r0 = (
        product(qty, _w([0.4, 0.4, 0.2], y[:3]))
        .sum("y", min_count=1)
        .expand_dims(dict(y=[y[0]]))
    )

    # Final period. “closer to the trend line”
    # NB the inherited R file used a formula equivalent to weights like
    #    [-1/8, 0, 3/8, 3/4]; didn't make much sense.
    r_m1 = (
        product(qty, _w([0.2, 0.2, 0.6], y[-3:]))
        .sum("y", min_count=1)
        .expand_dims(dict(y=[y[-1]]))
    )

    # apply_units() is to work around khaeru/genno#64
    # TODO remove when fixed upstream
    return apply_units(computations.concat(r0, result.sel(y=y[1:-1]), r_m1), qty.units)


def speed(config: dict) -> Quantity:
    """Return travel speed [distance / time].

    The returned Quantity has dimension ``t`` (technology).
    """
    return as_quantity(config["transport"]["speeds"])


def transport_check(scenario: Scenario, ACT: Quantity) -> pd.Series:
    """Reporting computation for :func:`check`.

    Imported into :mod:`.reporting.computations`.
    """
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


def votm(gdp_ppp_cap: Quantity) -> Quantity:
    """Calculate value of time multiplier.

    A value of 1 means the VoT is equal to the wage rate per hour.

    Parameters
    ----------
    gdp_ppp_cap
        PPP GDP per capita.
    """
    assert_units(gdp_ppp_cap, "kUSD / passenger / year")
    result = 1 / (1 + np.exp((30 - gdp_ppp_cap) / 20))
    result.units = ""
    return result


def whour(config: dict) -> Quantity:
    """Return work duration [hours / person-year]."""
    return as_quantity(config["transport"]["work hours"])
