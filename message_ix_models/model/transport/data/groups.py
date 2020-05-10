import pandas as pd
import xarray as xr

from message_data.tools import get_gea_data, set_info

# Query for retrieving GEA population data

GEA_DIMS = dict(
    variable={
        'Population|Total': 'total',
        'Population|Urban': 'UR+SU',
        'Population|Rural': 'RU',
    },
    scenario={
        'geama_450_btr_full': 'GEA mix',
        'geaha_450_atr_full': 'GEA supply',
        'geala_450_atr_nonuc': 'GEA eff',
    },
    region={},
)


def get_consumer_groups(context):
    """Return shares of transport consumer groups.

    Returns
    -------
    pandas.Series
        Dimensions: region, scenario, year.
    """
    # Ensure MA3T data is loaded
    from message_data.model.transport.utils import consumer_groups, read_config
    read_config(context)
    cg_indexers = consumer_groups(context, rtype='indexers')
    consumer_group = cg_indexers.pop('consumer_group')

    # Data: GEA population projections give split between 'UR+SU' and 'RU'
    ursu_ru = get_urban_rural_shares(context)

    # Assumption: split of population between area_type 'UR' and 'SU'
    # - Fill forward along years, for nodes where only a year 2010 value is
    #   assumed.
    # - Fill backward 2010 to 2005, in order to compute
    su_share = context.data['population-suburb-share'] \
        .ffill('year') \
        .bfill('year')

    # Assumption: global nodes are assumed to match certain U.S.
    # census_divisions
    n_cd_map = context['transport config']['node to census_division']
    n, cd = zip(*n_cd_map.items())
    n_cd_indexers = dict(
        node=xr.DataArray(list(n), dims='node'),
        census_division=xr.DataArray(list(cd), dims='node'))

    # Split the GEA 'UR+SU' population share using su_share
    pop_share = xr.concat([
        ursu_ru.sel(area_type='UR+SU', drop=True) * (1 - su_share),
        ursu_ru.sel(area_type='UR+SU', drop=True) * su_share,
        ursu_ru.sel(area_type='RU', drop=True),
    ], dim=pd.Index(['UR', 'SU', 'RU'], name='area_type'))

    # Index of pop_share versus the previous period
    pop_share_index = pop_share / pop_share.shift(year=1)

    # DLM: “Values from MA3T are based on 2001 NHTS survey and some more recent
    # calculations done in 2008 timeframe. Therefore, I assume that the numbers
    # here are applicable to the US in 2005.”
    # NB in the spreadsheet, the data are also filled forward to 2010
    ma3t_pop = context.data['ma3t/population'].assign_coords(year=2010)

    # - Apply the trajectory of pop_share to the initial values of ma3t_pop.
    # - Compute the group shares.
    # - Select using matched sequences, i.e. select a sequence of (node,
    #   census_division) coordinates.
    # - Drop the census_division.
    # - Collapse area_type, attitude, driver_type dimensions into
    #   consumer_group.
    groups = (
        ma3t_pop * pop_share_index.cumprod('year')
                 * context.data['ma3t/attitude']
                 * context.data['ma3t/driver']
        ) \
        .sel(**n_cd_indexers) \
        .drop_vars('census_division') \
        .sel(**cg_indexers) \
        .drop_vars(cg_indexers.keys()) \
        .assign_coords(consumer_group=consumer_group)

    return groups


def get_urban_rural_shares(context) -> xr.DataArray:
    """Return sares of urban and rural population from GEA.

    See also
    --------
    .get_gea_data
    """
    # Retrieve region info
    nodes = set_info("node")
    # List of regions according to the context
    regions = nodes[nodes.index(context.regions)].child

    # Identify the regions to query from the GEA data, which has R5 and other
    # mappings
    GEA_DIMS['region'].update(
        {r.split('_')[-1]: r for r in map(str, regions)})

    # Assemble query string and retrieve data from GEA snapshot
    query = []
    for dim, values in GEA_DIMS.items():
        query.append(f'{dim} in {list(values.keys())}')
    pop = get_gea_data(context, ' and '.join(query))

    # Rename values along dimensions
    for dim, values in GEA_DIMS.items():
        pop = pop.rename(values, level=dim)

    # - Remove model, units dimensions
    # - Rename 'variable' to 'area_type'
    # - Convert to xarray
    pop = pop.droplevel(['model', 'unit']) \
             .rename_axis(index={'variable': 'area_type', 'region': 'node'}) \
             .pipe(xr.DataArray.from_series)

    # Compute shares, select the appropriate scenario
    return (pop.sel(area_type=['UR+SU', 'RU']) / pop.sel(area_type='total')) \
        .sel(scenario=context['transport population scenario'], drop=True)
