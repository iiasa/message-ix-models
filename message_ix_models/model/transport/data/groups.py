"""Consumer groups data."""
import logging
from copy import deepcopy
from typing import Dict, List

import pandas as pd
import xarray as xr
from genno import computations
from ixmp.reporting import RENAME_DIMS, Quantity
from message_ix_models import Context
from message_ix_models.util import adapt_R11_R14, check_support

from message_data.model.transport.utils import path_fallback
from message_data.tools.gdp_pop import population

log = logging.getLogger(__name__)

# Dimensions
DIMS = deepcopy(RENAME_DIMS)
DIMS.update(dict(region="n", variable="area_type"))


def cg_shares(ursu_ru: Quantity, context: Context) -> Quantity:
    """Return shares of transport consumer groups.

    .. todo:: explode the individual atomic steps here.

    Parameters
    ----------
    ursu_ru : Quantity
        Population split between "UR+SU" and "RU" on the ``area_type`` dimension.
    context : .Context
        The ``.regions`` attribute is passed to :func:`get_urban_rural_shares`.

    Returns
    -------
    .Quantity
        Dimensions: n, y, cg. Units.dimensionless.
    """
    cg_indexers = deepcopy(context["transport set"]["consumer_group"]["indexers"])
    consumer_group = cg_indexers.pop("consumer_group")

    check_support(
        context,
        settings=dict(regions=frozenset(["R11", "R12"])),
        desc="Exogenous data for consumer group calculations",
    )

    # Assumption: split of population between area_type 'UR' and 'SU'
    # - Fill forward along years, for nodes where only a year 2010 value is assumed.
    # - Fill backward 2010 to 2005, in order to compute.
    su_share = (
        computations.load_file(
            path=path_fallback(context.regions, "population-suburb-share.csv"),
            dims=RENAME_DIMS,
        )
        .ffill("y")
        .bfill("y")
    )

    if context.regions == "R14":
        su_share = adapt_R11_R14(su_share)

    # Assumption: each global node is equivalent to a certain U.S. census_division

    # Convert setting from config file into a set of indexers
    n_cd_map = context["transport config"]["node to census_division"]
    n, cd = zip(*n_cd_map.items())
    n_cd_indexers = dict(
        n=xr.DataArray(list(n), dims="n"),
        census_division=xr.DataArray(list(cd), dims="n"),
    )

    # Split the GEA 'UR+SU' population share using su_share
    pop_share = (
        computations.concat(
            ursu_ru.sel(area_type="UR+SU", drop=True) * (1 - su_share),
            ursu_ru.sel(area_type="UR+SU", drop=True) * su_share,
            ursu_ru.sel(area_type="RU", drop=True),
            dim=pd.Index(["UR", "SU", "RU"], name="area_type"),
        )
        .ffill("y")
        .bfill("y")
    )

    # Index of pop_share versus the previous period
    pop_share_index = (pop_share / pop_share.shift(y=1)).fillna(1.0)

    # Population shares between urban, suburban, and rural
    # DLM: “Values from MA3T are based on 2001 NHTS survey and some more recent
    # calculations done in 2008 timeframe. Therefore, I assume that the numbers here are
    # applicable to the US in 2005.”
    # NB in the spreadsheet, the data are also filled forward to 2110
    ma3t_pop = computations.load_file(
        path=path_fallback(context, "ma3t", "population.csv"), units=""
    )

    ma3t_attitude = computations.load_file(
        path=path_fallback(context, "ma3t", "attitude.csv"), units=""
    )

    ma3t_driver = computations.load_file(
        path=path_fallback(context, "ma3t", "driver.csv"), units=""
    )

    # - Apply the trajectory of pop_share to the initial values of ma3t_pop.
    # - Compute the group shares.
    # - Select using matched sequences, i.e. select a sequence of (node,
    #   census_division) coordinates.
    # - Drop the census_division.
    # - Collapse area_type, attitude, driver_type dimensions into consumer_group.
    # - Convert to short dimension names.
    groups = (
        computations.product(
            ma3t_pop, pop_share_index.cumprod("y"), ma3t_attitude, ma3t_driver
        )
        .sel(n_cd_indexers)
        .sel(cg_indexers)
        .assign_coords(consumer_group=consumer_group.values)
        .rename(dict(node="n", year="y", consumer_group="cg"))
    )

    # Normalize so the sum across groups is always 1; convert to Quantity
    return Quantity(groups / groups.sum("cg"))


def urban_rural_shares(years: List[int], config: Dict) -> Quantity:
    """Return shares of urban and rural population.

    The data are filled forward to cover the years indicated by the `years` setting.

    Parameters
    ----------
    years : list of int
        Years for which to return population
    config : dict
        The ``regions`` and ``data source/population`` keys are used.

    Returns
    -------
    .Quantity
        Dimensions: n, t, area_type. Units: dimensionless.

    See also
    --------
    population
    """
    scenario = config["data source"]["population"]

    # Let the population() method handle regions, scenarios, data source.
    # NB need to adapt the key/hierarchy here from the one on `context` to the one
    #    stored in a Computer/Reporter; a little messy.
    pop = population(
        years,
        config={
            "data source": {"population": scenario},
            "regions": config["regions"],
        },
        extra_dims=True,
    )

    if "GEA" in scenario:
        return computations.div(
            pop.sel(area_type=["UR+SU", "RU"]), pop.sel(area_type="total", drop=True)
        )
    elif any(source in scenario for source in ("SSP", "SHAPE")):
        log.warning(f"Need urban/suburban share data for {scenario}")

        share = Quantity(
            xr.DataArray([0.6, 0.4], coords=[("area_type", ["UR+SU", "RU"])]), units=""
        )
        return computations.product(pop, share)
    else:
        raise ValueError(scenario)
