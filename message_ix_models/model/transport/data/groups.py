"""Consumer groups data."""
import logging
from copy import deepcopy
from typing import Dict, List

import pandas as pd
import xarray as xr
from genno import computations
from iam_units import registry
from ixmp.reporting import RENAME_DIMS, Quantity
from message_ix_models.model.structure import Code, get_codes
from message_ix_models.util import adapt_R11_R14, check_support

from message_data.model.transport.utils import consumer_groups, path_fallback

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
        path=path_fallback(context, "ma3t", "population.csv"),
        units=registry.dimensionless,
    )

    ma3t_attitude = computations.load_file(
        path=path_fallback(context, "ma3t", "attitude.csv"),
        units=registry.dimensionless,
    )

    ma3t_driver = computations.load_file(
        path=path_fallback(context, "ma3t", "driver.csv"),
        units=registry.dimensionless,
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
        config=dict(transport=context["transport config"], regions=context.regions),
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
            units=registry.dimensionless,
        )
        return computations.product(pop, share)
    else:
        raise ValueError(scenario)


def population(periods: List[int], config: Dict, extra_dims: bool = False) -> Quantity:
    """Return population data.

    :func:`population` either retrieves data from GEA or SSP, depending on
    ``config["transport"]["data source"]["population"]``.

    Parameters
    ----------
    periods
        Periods for returned data.
    config
        Dictionary, e.g. from the ``config`` key of a :class:`genno.Computer`.
    extra_dims
        :data:`False` (default) to return data with the dimensions (n, y); :data:`True`
        to return additional dimensions.

    Returns
    -------
    Quantity
        with dimensions (n, y) and possibly extra dimensions if `extra_dims` is
        :data:`True`; units 10⁶ passenger.

    See also
    --------
    get_gea_population
    get_shape_population
    get_ssp_population
    """
    pop_scenario = config["transport"]["data source"]["population"]

    nodes = get_codes(f"node/{config['regions']}")
    nodes = nodes[nodes.index("World")].child

    if "GEA" in pop_scenario:
        data = get_gea_population(nodes, pop_scenario)
        if not extra_dims:
            data = data.sel(area_type="total", drop=True)
    elif "SSP" in pop_scenario:
        data = get_ssp_population(nodes, pop_scenario)
    elif "SHAPE" in pop_scenario:
        data = get_shape_population(nodes)
    else:
        raise ValueError(pop_scenario)

    # Interpolate to the desired set of periods
    return computations.interpolate(
        data,
        coords=dict(y=periods),
        kwargs=dict(fill_value="extrapolate"),
    )


def get_gea_population(nodes: List[Code], scenario: str) -> Quantity:
    """Load population data from the GEA database.

    Parameters
    ----------
    node
        Regions for which to return population. Prefixes before and including "_" are
        stripped, e.g. "R11_AFR" results in a query for "AFR".
    scenario
        Specific name of a GEA scenario.

    See also
    --------
    .get_gea_data
    """
    from message_data.tools.gea import get_gea_data

    # Identify the regions to query from the GEA data, which has R5 and other mappings
    GEA_DIMS["region"].update({r.split("_")[-1]: r for r in map(str, nodes)})

    # Assemble query string and retrieve data from GEA snapshot
    data = get_gea_data(
        " and ".join(
            f"{dim} in {list(values.keys())}" for dim, values in GEA_DIMS.items()
        )
    ).droplevel("model")

    # Discard index levels that aren't used as labels
    data.index = data.index.remove_unused_levels()

    # Replace values along dimensions
    for dim, values in GEA_DIMS.items():
        data = data.rename(values, level=dim)

    # Convert to genno.Quantity
    qty = Quantity(data, units="Mpassenger").rename(DIMS)

    # Units are as expected
    assert ["million"] == qty.coords["unit"], qty.coords["unit"]

    # - Select the target scenario.
    # - Remove unit dimension.
    # - Interpolate to desired `periods`.
    return qty.sel(scenario=scenario).drop(["unit", "scenario"])


def _agg(qty: Quantity, nodes: List[Code]) -> Quantity:
    """Common aggregation code."""
    return computations.aggregate(
        qty,
        groups=dict(n={node.id: list(map(str, node.child)) for node in nodes}),
        keep=False,
    )


def get_shape_population(nodes: List[Code]) -> Quantity:
    """Load population data from the SHAPE source.

    There is only one SHAPE population scenario, labelled “all_SHAPE_SDPs”. Country-
    resolution data from this projection is aggregated according to the hierarchy of
    `nodes`.

    Parameters
    ----------
    nodes :
        Regions for returned data.
    """
    from message_data.projects import shape

    qty = (
        shape.get_shape_data("population", rtype="quantity")
        .drop("scenario")
        .rename(DIMS)
        .pipe(_agg, nodes)
    )

    # Convert bare "million" to [persons]
    assert "million" == qty.units, qty.units
    qty.units = "Mpassenger"

    return qty


def get_ssp_population(nodes: List[Code], scenario: str) -> Quantity:
    """Load population data from the SSP database.

    Always uses ``model="IIASA GDP"`` and the country-resolution data from the SSP
    database, aggregated according to the children of each element of `nodes`.

    Parameters
    ----------
    nodes
        Regions for returned data.
    scenario
        Full or partial name of an SSP scenario, e.g. "SSP2" to match "SSP2_v4_…".

    See also
    --------
    .get_ssp_data
    """
    from message_data.tools.ssp import get_ssp_data

    # Retrieve country-level data from SSP snapshot
    data = get_ssp_data(
        kind="country",
        query="variable == 'Population' and model == 'IIASA GDP'",
    ).droplevel(["variable", "model"])

    # Discard index levels that aren't used as labels
    data.index = data.index.remove_unused_levels()

    # Convert to genno.Quantity
    qty = Quantity(data, units="Mpassenger").rename(DIMS)

    # Units are as expected
    assert ["million"] == qty.coords["unit"]

    # Find the scenario label that includes `scenario`, e.g. "SSP2_v4_…" for "SSP2"
    _scenario = list(filter(lambda s: scenario in s, qty.coords["scenario"].data))
    assert len(_scenario) == 1

    # - Select & drop on the 'unit' and 'scenario' dimensions.
    # - Convert the list of nodes into a country → region mapping.
    # - Use the genno aggregate operation from country to regional resolution.
    # - Interpolate and select only the required `periods`.
    return qty.sel(scenario=_scenario[0]).drop(["unit", "scenario"]).pipe(_agg, nodes)
