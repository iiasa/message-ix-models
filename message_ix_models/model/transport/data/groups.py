import logging
from copy import deepcopy

import pandas as pd
import xarray as xr
from genno import computations
from ixmp.reporting import RENAME_DIMS, Quantity
from message_ix_models.model.structure import get_codes
from message_ix_models.util import private_data_path

from message_data.model.transport.utils import consumer_groups
from message_data.tools import gea

log = logging.getLogger(__name__)


# Query for retrieving GEA population data

GEA_DIMS = dict(
    variable={
        "Population|Total": "total",
        "Population|Urban": "UR+SU",
        "Population|Rural": "RU",
    },
    scenario={
        "geama_450_btr_full": "GEA mix",
        "geaha_450_atr_full": "GEA supply",
        "geala_450_atr_nonuc": "GEA eff",
    },
    region={},
)

# Dimensions
DIMS = deepcopy(RENAME_DIMS)


def get_consumer_groups(context):
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

    # Assumption: split of population between area_type 'UR' and 'SU'
    # - Fill forward along years, for nodes where only a year 2010 value is assumed.
    # - Fill backward 2010 to 2005, in order to compute.
    su_share = (
        computations.load_file(
            path=private_data_path("transport", "population-suburb-share.csv"),
            dims=RENAME_DIMS,
        )
        .ffill("y")
        .bfill("y")
    )

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
    pop_share_index = pop_share / pop_share.shift(y=1)

    # Population shares between urban, suburban, and rural
    # DLM: “Values from MA3T are based on 2001 NHTS survey and some more recent
    # calculations done in 2008 timeframe. Therefore, I assume that the numbers
    # here are applicable to the US in 2005.”
    # NB in the spreadsheet, the data are also filled forward to 2010
    ma3t_pop = computations.load_file(
        path=private_data_path("transport", "ma3t", "population.csv")
    )

    ma3t_attitude = computations.load_file(
        path=private_data_path("transport", "ma3t", "attitude.csv")
    )

    ma3t_driver = computations.load_file(
        path=private_data_path("transport", "ma3t", "driver.csv")
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


def get_urban_rural_shares(context):
    """Return shares of urban and rural population from GEA.

    The data are filled forward to cover the years indicated by ``context["transport
    build info"].set["year"]``.

    Parameters
    ----------
    context : .Context
        The ``regions`` setting determines the regional aggregation used.

    See also
    --------
    .get_gea_population
    """
    # Ensure the current settings are supported
    gea.supports(context)

    # Retrieve region info for the selected regional aggregation
    nodes = get_codes(f"node/{context.regions}")
    # List of regions according to the context
    regions = nodes[nodes.index("World")].child

    # Retrieve the data, and select the scenario to use, e.g. "GEA mix"
    # TODO pass the scenario selector through get_gea_population() to get_gea_data()
    pop = get_gea_population(regions).sel(
        scenario=context["transport config"]["data source"]["population"], drop=True
    )

    # Duplicate 2100 data for 2110
    # TODO use some kind of ffill operation
    years = context["transport build info"].Y
    idx = years.index(2100) + 1
    pop = computations.concat(
        pop,
        pop.sel(y=2100).expand_dims(y=years[idx:]).transpose("n", "area_type", "y"),
    )

    # Compute and return shares
    return computations.div(
        pop.sel(area_type=["UR+SU", "RU"]), pop.sel(area_type="total", drop=True)
    )


def get_gea_population(regions=[]):
    """Load population data from the GEA database.

    Parameters
    ----------
    regions : list of str
        Regions for which to return population. Prefixes before and including "_" are
        stripped, e.g. "R11_AFR" results in a query for "AFR".

    See also
    --------
    .get_gea_data
    """
    # Identify the regions to query from the GEA data, which has R5 and other mappings
    GEA_DIMS["region"].update({r.split("_")[-1]: r for r in map(str, regions)})

    # Assemble query string and retrieve data from GEA snapshot
    pop = gea.get_gea_data(
        " and ".join(
            f"{dim} in {list(values.keys())}" for dim, values in GEA_DIMS.items()
        )
    )

    # Rename values along dimensions
    for dim, values in GEA_DIMS.items():
        pop = pop.rename(values, level=dim)

    # Units are as expected
    assert ["million"] == pop.index.levels[pop.index.names.index("unit")]

    # - Remove model, units dimensions.
    # - Rename other dimensions.
    # - Convert to Quantity.
    return Quantity(
        pop.droplevel(["model", "unit"]).rename_axis(
            index={"variable": "area_type", "region": "n", "year": "y"}
        ),
        units="Mpassenger",
    )
