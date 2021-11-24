"""Reporting computations for MESSAGEix-Transport."""
from typing import Hashable, Mapping, Union

import numpy as np
import pandas as pd
import xarray as xr
from genno import Quantity, computations
from genno.computations import add, product, ratio
from iam_units import registry
from ixmp import Scenario
from message_ix_models import ScenarioInfo

from message_data.reporting.util import as_quantity
from message_data.tools import assert_units
from message_data.tools.iea_eei import get_eei_data


def base_shares(nodes: list[int], y: list[int], config: dict) -> Quantity:
    """Return base mode shares."""
    modes = config["transport"]["demand modes"]
    # TODO replace with input data
    return Quantity(
        xr.DataArray(1.0 / len(modes), coords=[nodes, y, modes], dims=["n", "y", "t"])
    )


def cost(
    price: Quantity,
    gdp_ppp_cap: Quantity,
    whours: Quantity,
    speeds: Quantity,
    votm: Quantity,
    y: list[int],
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


def distance_nonldv(config: dict) -> Quantity:
    """Return annual travel distance per vehicle for non-LDV transport modes."""
    # Load from get_eei_data
    dfs = get_eei_data(config["transport"]["regions"])

    # TODO adjust get_eei_data() to clean these and return separate quantities, or long-
    #      form tidy data
    cols = [
        "ISO_code",
        "Year",
        "Mode/vehicle type",
        "Vehicle stock (10^6)",
        "Vehicle-kilometres (10^9 vkm)",
    ]

    df = (
        dfs["Activity"][cols]
        .rename(columns={"ISO_code": "nl", "Year": "y", "Mode/vehicle type": "t"})
        .set_index(["nl", "t", "y"])
    )
    # print(df)

    result = Quantity(df[cols[4]], name="non-ldv distance")
    # print(result)

    return result


def dummy_prices(gdp: Quantity) -> Quantity:
    # Commodity prices: all equal to 0.1

    # Same coords/shape as `gdp`, but with c="transport"
    coords = [(dim, item.data) for dim, item in gdp.coords.items()]
    coords.append(("c", ["transport"]))
    shape = list(len(c[1]) for c in coords)

    return Quantity(xr.DataArray(np.full(shape, 0.1), coords=coords), units="USD / km")


def _lambda(config: str) -> Quantity:
    """Return (scalar) lambda parameter for transport mode share equations."""
    return Quantity(config["transport"]["lambda"], units="")


def logit(
    x: Quantity, k: Quantity, lamda: Quantity, y: list[int], dim: str
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


def model_periods(y: list[int], cat_year: pd.DataFrame) -> list[int]:
    """Return the elements of `y` beyond the firstmodelyear of `cat_year`."""
    return list(
        filter(
            lambda year: cat_year.query("type_year == 'firstmodelyear'")["year"].item()
            <= year,
            y,
        )
    )


def nodes_ex_world(nodes: list) -> list[str]:
    """Nodes excluding 'World'."""
    return list(filter(lambda n_: "GLB" not in n_ and n_ != "World", nodes))


def pdt_per_capita(gdp_ppp_cap: Quantity, config: dict) -> Quantity:
    """Compute passenger distance traveled (PDT) per capita.

    Simplification of Schäefer et al. (2010): linear interpolation between (0, 0) and
    the configuration keys "fixed demand" and "fixed GDP".
    """
    fix_gdp = as_quantity(config["transport"]["fixed GDP"])
    fix_demand = as_quantity(config["transport"]["fixed demand"])

    return product(ratio(gdp_ppp_cap, fix_gdp), fix_demand)


def rename(
    qty: Quantity,
    new_name_or_name_dict: Union[Hashable, Mapping[Hashable, Hashable]] = None,
    **names: Hashable,
) -> Quantity:
    """Like :meth:`xarray.DataArray.rename`.

    .. todo:: Upstream to :mod:`genno`.
    """
    return qty.rename(new_name_or_name_dict, **names)


def share_weight(
    share: Quantity,
    gdp_ppp_cap: Quantity,
    cost: Quantity,
    lamda: Quantity,
    nodes: list[str],
    y: list[int],
    t: list[str],
    cat_year: pd.DataFrame,
    config: dict,
) -> Quantity:
    """Calculate mode share weights."""
    # Modes from configuration
    modes = config["transport"]["demand modes"]

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
    tmp = computations.pow(s_y0 / c_y0, lamda)

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
    # NB this will not work if yC is before the final period; it will leave NaN after yC
    weight = weight.interpolate_na(dim="y")

    return Quantity(weight)


def smooth(qty: Quantity) -> Quantity:
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

    return Quantity(
        1 / (1 + np.exp((30 - gdp_ppp_cap) / 20)), units=registry.dimensionless
    )


def whour(config: dict) -> Quantity:
    """Return work duration [hours / person-year]."""
    return as_quantity(config["transport"]["work hours"])
