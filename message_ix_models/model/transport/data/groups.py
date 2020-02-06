from message_data.tools import get_gea_data, regions

# Query for retrieving GEA population data

GEA_DIMS = dict(
    variable={
        'Population|Total': 'total',
        'Population|Urban': 'urban',
        'Population|Rural': 'rural',
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
    # Retrieve region info
    region_info = regions.get_info(context)

    # Identify the regions to subset from the GEA data, which has R5 and other
    # mappings
    GEA_DIMS['region'].update(
        {r.split('_')[-1]: r for r in region_info.keys()})

    # Assemble query string
    query = []
    for dim, values in GEA_DIMS.items():
        query.append(f'{dim} in {list(values.keys())}')

    pop = get_gea_data(context, ' and '.join(query))
    for dim, values in GEA_DIMS.items():
        pop = pop.rename(values, level=dim)

    shares = (pop / pop.xs('total', level='variable')) \
        .query("variable in ['urban', 'rural']") \
        .rename_axis(index={'variable': 'area_type'}) \
        .droplevel('unit')

    return shares


def get_ma3t_data(context):
    # Read MA3T data files
    from message_data.model.transport.data import load_data

    data = {}
    for var in 'attitude', 'driver', 'population':
        data[var] = load_data(context, 'ma3t', var)

    return data
