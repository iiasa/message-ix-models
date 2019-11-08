"""Generate input data."""
import logging

import numpy as np
import pandas as pd

from message_data.model.transport.utils import (
    config,
    iter_parameters,
    make_df,
    )
from message_data.tools import ScenarioInfo


log = logging.getLogger(__name__)


def add_data(scenario, data_from, dry_run=False):
    info = ScenarioInfo(scenario)

    # Add data
    for data_func in (demand, conversion, freight):
        log.info(f'Adding data from {data_func}')

        # Generate or load the data
        data = data_func(info)

        # Add to the scenario
        add_par_data(scenario, data, dry_run=dry_run)


def add_par_data(scenario, data, dry_run=False):
    total = 0

    for par_name, values in data.items():
        N = len(values)

        log.info(f'{N} rows in {par_name!r}')
        log.info(str(values))

        if not dry_run:
            # Reset index
            # TODO ixmp should do this automatically
            scenario.add_par(par_name, values.reset_index())

        total += N

    return total


def strip_par_data(scenario, set_name, value, dry_run=False, dump=None):
    """Remove data from parameters of *scenario* where *value* in *set_name*.

    Returns
    -------
    Total number of rows removed across all parameters.
    """
    par_list = scenario.par_list()
    no_data = []
    total = 0

    for par_name in iter_parameters(set_name):
        if par_name not in par_list:
            continue

        # Check for contents of par_name that include *value*
        par_data = scenario.par(par_name, filters={set_name: value})
        N = len(par_data)

        if N == 0:
            # No data; no need to do anything further
            no_data.append(par_name)
            continue
        elif dump is not None:
            dump[par_name] = pd.concat([
                dump.get(par_name, pd.DataFrame()),
                par_data,
                ])

        log.info(f'Remove {N} rows in {par_name!r}.')

        # Show some debug info
        for col in 'commodity level technology'.split():
            if col == set_name or col not in par_data.columns:
                continue

            log.info('  with {}={}'
                     .format(col, sorted(par_data[col].unique())))

        if not dry_run:
            # Actually remove the data
            scenario.remove_par(par_name, key=par_data)

            # # NB would prefer to do the following, but raises an exception:
            # scenario.remove_par(par_name, key={set_name: [value]})

        total += N

    log.info(f'{total} rows removed.')
    log.info(f'No data removed from {len(no_data)} other parameters.')

    return total


def demand(info):
    """Dummy demand data."""
    result = pd.DataFrame(zip(info.Y, np.arange(len(info.Y)) + 0.1),
                          columns=['year', 'value'])

    # Set other dimension
    result['node'] = info.N[1]
    result['level'] = 'useful'
    result['commodity'] = 'transport freight'
    result['time'] = 'year'
    result['unit'] = 't km'

    return dict(demand=result)


def conversion(info):
    """Dummy input and output data for conversion technologies."""

    input = make_df('input', 'VDT to freight',
                    year_vtg=info.Y, year_act=info.Y)
    input['node_loc'] = info.N[1]

    # Same node
    input['node_origin'] = input['node_loc']

    output = make_df('output', 'VDT to freight',
                     year_vtg=info.Y, year_act=info.Y,
                     value=config['model']['freight load factor'])
    output['node_loc'] = info.N[1]

    # Same node
    output['node_dest'] = output['node_loc']

    return dict(input=input, output=output)


def freight(info):
    """Data for freight technologies."""
    output = []
    for tech in config['tech']['technology group']['freight truck']['tech']:
        output.append(make_df('output', tech, year_vtg=info.Y,
                              year_act=info.Y))

    output = pd.concat(output)

    # Modify the concatenated data
    output['node_loc'] = info.N[1]

    # Same node
    output['node_dest'] = output['node_loc']

    return dict(output=output)
