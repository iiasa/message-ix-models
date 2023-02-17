"""Reporting computations for MESSAGEix-Transport."""
import logging
from functools import partial
from typing import Dict, Hashable, List, Mapping, Optional

import numpy as np
import pandas as pd
import xarray as xr
from genno import Quantity, computations
from genno.computations import (
    add,
    apply_units,
    convert_units,
    product,
    ratio,
    relabel,
    rename_dims,
)
from genno.testing import assert_qty_allclose, assert_units
from iam_units import registry
from ixmp import Scenario
from ixmp.reporting import RENAME_DIMS
from message_ix import make_df
from message_ix_models import ScenarioInfo
from message_ix_models.tools import advance
from message_ix_models.util import broadcast, eval_anno, nodes_ex_world

from message_data.model.transport.config import ScenarioFlags
from message_data.model.transport.utils import path_fallback
from message_data.reporting.computations import compound_growth
from message_data.reporting.util import as_quantity
from message_data.tools import iea_eei

log = logging.getLogger(__name__)

__all__ = [
    "advance_fv",
    "base_shares",
    "cost",
    "demand_ixmp0",
    "distance_ldv",
    "distance_nonldv",
    "dummy_prices",
    "iea_eei_fv",
    "logit",
    "nodes_ex_world",
    "nodes_world_agg",
    "pdt_per_capita",
    "price_units",
    "quantity_from_config",
    "share_weight",
    "smooth",
    "transport_check",
    "votm",
]


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
        config["regions"], "mode-share", f"{config['transport'].mode_share}.csv"
    )
    log.info(f"Read base mode shares from {path}")

    base = computations.load_file(path, dims=RENAME_DIMS, name="mode share", units="")

    missing = [d for d in "nty" if d not in base.dims]
    log.info(f"Broadcast base mode shares with dims {base.dims} over {missing}")

    coords = [("n", nodes), ("t", techs), ("y", y)]
    return product(base, Quantity(xr.DataArray(1.0, coords=coords), units=""))


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
        as_quantity(config["transport"].ldv_activity),
        as_quantity(config["transport"].factor["activity"]["ldv"]),
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
    dfs = iea_eei.get_eei_data(config["transport"]["regions"])
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


def factor_fv(n: List[str], y: List[int], config: dict) -> Quantity:
    """Scaling factor for freight activity.

    If :attr:`.Config.flags` is :data:`ScenarioFlags.ACT`, the value declines from 1.0
    at the first `y` to 0.865 (reduction of 13.5%) at y=2050, then constant thereafter.

    Otherwise, the value is 1.0 for every (`n`, `y`).
    """
    # Empty data frame
    df = pd.DataFrame(columns=["value"], index=pd.Index(y, name="y"))

    # Default value
    df.iloc[0, :] = 1.0

    # NAVIGATE T3.5 "act" demand-side scenario
    if ScenarioFlags.ACT & config["transport"].flags:
        years = list(filter(lambda y: y <= 2050, y))
        df.loc[years, "value"] = np.interp(years, [y[0], 2050], [1.0, 0.865])

    # - Fill all values forward from the latest.
    # - Convert to long format.
    # - Broadcast over all nodes `n`.
    # - Set dimensions as index.
    return Quantity(
        df.ffill()
        .reset_index()
        .assign(n=None)
        .pipe(broadcast, n=n)
        .set_index(["n", "y"])["value"],
        units="",
    )


def factor_input(y: List[int], t: List[str], t_agg: Dict, config: dict) -> Quantity:
    """Scaling factor for ``input`` (energy intensity of activity).

    If :attr:`.Config.flags` is :data:`ScenarioFlags.TEC`, the value declines from 1.0
    at the first `y` to 0.865 (reduction of 13.5%) at y=2050, then constant thereafter.

    Otherwise, the value is 1.0 for every (`t`, `y`).
    """

    def _not_disutility(tech):
        return eval_anno(tech, "is-disutility") is None

    techs = list(filter(_not_disutility, t))

    # Empty data frame
    df = pd.DataFrame(columns=pd.Index(techs, name="t"), index=pd.Index(y, name="y"))

    # Default value
    df.iloc[0, :] = 1.0

    # NAVIGATE T3.5 "tec" demand-side scenario
    if ScenarioFlags.TEC & config["transport"].flags:
        years = list(filter(lambda y: y < 2050, y))

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

    qty = Quantity(df.fillna(1.0).reset_index().set_index("y").stack())

    return compound_growth(qty, "y")


def factor_pdt(n: List[str], y: List[int], t: List[str], config: dict) -> Quantity:
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
    if ScenarioFlags.ACT & config["transport"].flags:
        # NAVIGATE T3.5 "act" demand-side scenario
        years = list(filter(lambda y: y <= 2050, y))
        df.loc[years, "LDV"] = np.interp(years, [y[0], 2050], [1.0, 0.8])

    # - Fill all values forward from the latest.
    # - Convert to long format.
    # - Broadcast over all nodes `n`.
    # - Set dimensions as index.
    return Quantity(
        df.ffill()
        .reset_index()
        .melt(id_vars="y", var_name="t")
        .assign(n=None)
        .pipe(broadcast, n=n)
        .set_index(["n", "y", "t"])["value"],
        units="",
    )


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


def _advance_data_for(config: dict, variable: str, units) -> Quantity:
    import plotnine as p9
    from genno.compat.plotnine import Plot

    assert "R12" == config["regions"], "ADVANCE data mapping only for R12 regions"

    class Plot1(Plot):
        basename = f"advance-check-{hash(variable)}"

        def generate(self, data):
            N = len(data)
            data = data.query("activity > 0")
            log.info(f"Discard {N-len(data)} rows with zero values")
            return (
                p9.ggplot(
                    p9.aes(
                        x="region",
                        y="activity",
                        color="model"
                        # shape="scenario",
                    ),
                    data,
                )
                + p9.geom_point()
                + p9.ggtitle(f"{variable}, 2020 [{units}]")
            )

    data = advance.advance_data(variable).sel(year=2020)
    data.name = "activity"

    # Debugging
    # Plot1().save(dict(output_dir=Path.cwd()), data)

    data = rename_dims(
        data.sel(model="MESSAGE", scenario="ADV3TRAr2_Base", drop=True),
        dict(region="n"),
    )

    # Map regions
    results = []
    for source, share, dest in (
        ("ASIA", 0.1, "R12_RCPA"),
        ("ASIA", 0.5 - 0.1, "R12_PAS"),
        ("China", 1.0, "R12_CHN"),
        ("EU", 0.1, "R12_EEU"),
        ("EU", 0.9, "R12_WEU"),
        ("India", 1.0, "R12_SAS"),
        ("LAM", 1.0, "R12_LAM"),
        ("MAF", 0.5, "R12_AFR"),
        ("MAF", 0.5, "R12_MEA"),
        ("OECD90", 0.08, "R12_PAO"),
        ("REF", 1.0, "R12_FSU"),
        ("USA", 1.0, "R12_NAM"),
    ):
        results.append(relabel(share * data.sel(n=source), n={source: dest}))
    result = computations.concat(*results)
    result.units = data.units  # FIXME should not be necessary

    # Check
    assert_qty_allclose(
        computations.sum(result, dimensions="n"),
        data.sel(n="World", drop=True),
        rtol=0.05,
    )
    # FIXME guard with an assertion
    result.units = units

    return result


advance_ldv_pdt = partial(
    _advance_data_for,
    variable="Transport|Service demand|Road|Passenger|LDV",
    units="Gp km / a",
)

advance_fv = partial(
    _advance_data_for,
    variable="Transport|Service demand|Road|Freight",
    units="Gt km",
)


def iea_eei_fv(name: str, config: Dict) -> Quantity:
    """Returns base-year demand for freight from IEA EEI, with dimensions n-c-y."""
    result = iea_eei.as_quantity(name, config["regions"])
    ym1 = result.coords["y"].data[-1]

    log.info(f"Use y={ym1} data for base-year freight transport activity")

    assert set("nyt") == set(result.dims)
    return result.sel(y=ym1, t="Total freight transport", drop=True)


def nodes_world_agg(config, dim: Hashable = "nl") -> Dict[Hashable, Mapping]:
    """Mapping to aggregate e.g. nl="World" from values for child nodes of "World".

    This mapping should be used with :func:`.genno.computations.aggregate`, giving the
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


def pdt_per_capita(gdp_ppp_cap: Quantity, config: dict) -> Quantity:
    """Compute passenger distance traveled (PDT) per capita.

    Simplification of Schäefer et al. (2010): linear interpolation between (0, 0) and
    the configuration keys "fixed demand" and "fixed GDP".
    """
    return product(
        ratio(
            gdp_ppp_cap,
            config["transport"].fixed_GDP,
        ),
        config["transport"].fixed_demand,
    )


def price_units(qty: Quantity) -> Quantity:
    """Forcibly adjust price units, if necessary."""
    target = "USD_2010 / km"
    if not qty.units.is_compatible_with(target):
        log.warning(f"Adjust price units from {qty.units} to {target}")
    return apply_units(qty, target)


def quantity_from_config(
    config: dict, name: str, dimensionality: Optional[Dict] = None
) -> Quantity:
    if dimensionality:
        raise NotImplementedError
    result = getattr(config["transport"], name)
    if not isinstance(result, Quantity):
        result = as_quantity(result)
    return result


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
    modes = cfg.demand_modes

    # Selectors
    t0 = dict(t=modes[0])
    y0 = dict(y=y[0])
    yC = dict(y=cfg.year_convergence)
    years = list(filter(lambda year: year <= yC["y"], y))

    # Share weights
    coords = [("n", nodes), ("y", years), ("t", modes)]
    weight = xr.DataArray(np.nan, coords=coords)

    # Weights in y0 for all modes and nodes
    # NB here and below, with Python 3.9 one could do: dict(t=modes, n=nodes) | y0
    idx = dict(t=modes, n=nodes, **y0)
    s_y0 = share.sel(idx)
    c_y0 = cost.sel(idx).sel(c="transport", drop=True)
    tmp = ratio(s_y0, computations.pow(c_y0, lamda))

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
        ref_nodes = cfg.share_weight_convergence[node]

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
    from genno.computations import add, concat, mul

    # General smoothing
    result = add(0.25 * qty.shift(y=-1), 0.5 * qty, 0.25 * qty.shift(y=1))

    y = qty.coords["y"].values

    # Shorthand for weights
    def _w(values, years):
        return Quantity(xr.DataArray(values, coords=[("y", years)]), units="")

    # First period
    r0 = mul(qty, _w([0.4, 0.4, 0.2], y[:3])).sum("y").expand_dims(dict(y=y[:1]))

    # Final period. “closer to the trend line”
    # NB the inherited R file used a formula equivalent to weights like [-⅛, 0, ⅜, ¾];
    # didn't make much sense.
    r_m1 = mul(qty, _w([0.2, 0.2, 0.6], y[-3:])).sum("y").expand_dims(dict(y=y[-1:]))

    # apply_units() is to work around khaeru/genno#64
    # TODO remove when fixed upstream
    return apply_units(concat(r0, result.sel(y=y[1:-1]), r_m1), qty.units)


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
