"""Consumer groups data."""
import logging
from copy import deepcopy

import pandas as pd
import xarray as xr
from genno import computations
from ixmp.reporting import RENAME_DIMS, Quantity
from message_ix_models.util import adapt_R11_R14, check_support

from message_data.model.transport.utils import consumer_groups, path_fallback
from message_data.tools.gdp_pop import population

log = logging.getLogger(__name__)

# Dimensions
DIMS = deepcopy(RENAME_DIMS)
DIMS.update(dict(region="n", variable="area_type"))


def get_consumer_groups(context) -> Quantity:
    """Return shares of transport consumer groups.

    Parameters
    ----------
    context : .Context
        The ``.regions`` attribute is passed to :func:`get_urban_rural_shares`.

    Returns
    -------
    ixmp.reporting.Quantity
        Dimensions: n, y, cg.
    """
    cg_indexers = deepcopy(consumer_groups(rtype="indexers"))
    consumer_group = cg_indexers.pop("consumer_group")

    # Data: GEA population projections give split between 'UR+SU' and 'RU'
    ursu_ru = get_urban_rural_shares(context)

    check_support(
        context,
        settings=dict(regions=frozenset(["R11", "R14"])),
        desc="Exogenous data for consumer group calculations",
    )

    # Assumption: split of population between area_type 'UR' and 'SU'
    # - Fill forward along years, for nodes where only a year 2010 value is assumed.
    # - Fill backward 2010 to 2005, in order to compute.
    su_share = (
        computations.load_file(
            path=path_fallback("R11", "population-suburb-share.csv"),
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


def get_urban_rural_shares(context) -> Quantity:
    """Return shares of urban and rural population.

    The data are filled forward to cover the years indicated by ``context["transport
    build info"].set["year"]``.

    Parameters
    ----------
    context : .Context
        The ``regions`` setting determines the regional aggregation used.

    See also
    --------
    population
    """
    scenario = context["transport config"]["data source"]["population"]

    # Let the population() method handle regions, scenarios, data source.
    # NB need to adapt the key/hierarchy here from the one on `context` to the one
    #    stored in a Computer/Reporter; a little messy.
    pop = population(
        context["transport build info"].Y,
        config={"data source": {"population": scenario}, "regions": context.regions},
        extra_dims=True,
    )

    if "GEA" in scenario:
        return computations.div(
            pop.sel(area_type=["UR+SU", "RU"]), pop.sel(area_type="total", drop=True)
        )
    elif "SSP" in scenario:
        log.warning("Need urban/suburban share data for SSP scenarios")

        share = Quantity(
            xr.DataArray([0.8, 0.2], coords=[["UR+SU", "RU"]], dims=["area_type"]),
            units="",
        )
        return computations.product(pop, share)
    else:
        raise ValueError(scenario)
