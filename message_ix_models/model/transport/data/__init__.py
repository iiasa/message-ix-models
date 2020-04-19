"""Generate input data."""
from collections import defaultdict
import logging

import pandas as pd

from message_data.tools import (
    ScenarioInfo,
    broadcast,
    get_context,
    iter_parameters,
    make_df,
)
from message_data.model.transport.demand import demand
from .groups import get_consumer_groups  # noqa: F401
from .ikarus import get_ikarus_data  # noqa: F401
from .ldv import get_ldv_data  # noqa: F401


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


def conversion(info):
    """Dummy input and output data for conversion technologies."""
    cfg = get_context()['transport config']

    data = {}

    common = dict(
        year_vtg=info.Y,
        year_act=info.Y,
        level='useful',
        mode='all',
        # No subannual detail
        time='year',
        time_origin='year',
        time_dest='year',
    )

    base = dict(
        input=(make_df('input', value=1.0, unit='km', **common)
               .pipe(broadcast, node_loc=info.N[1:])),
        output=(make_df('output', **common)
                .pipe(broadcast, node_loc=info.N[1:])),
    )

    mode_info = [
        ('freight', cfg['factor']['freight load'], 't km'),
        ('pax', 1.0, 'km'),
    ]

    data = defaultdict(list)

    for mode, factor, output_unit in mode_info:
        tech = f'transport {mode} load factor'

        data['input'].append(
            base['input'].assign(
                technology=tech,
                commodity=f'transport {mode} vehicle',
            )
        )

        data['output'].append(
            base['output'].assign(
                technology=tech,
                commodity=f'transport {mode}',
                value=factor,
                unit=output_unit,
            )
        )

    for par, dfs in data.items():
        df = pd.concat(dfs)

        # Copy 'node' into another column
        col = 'node_origin' if par == 'input' else 'node_dest'
        df[col] = df['node_loc']

        # Store
        data[par] = df

    return data


def freight(info):
    """Data for freight technologies."""
    cfg = get_context()['transport technology']

    common = dict(
        year_vtg=info.Y,
        year_act=info.Y,
        node_loc=info.N[1],
        node_dest=info.N[1],
        mode='all',
        time='year', time_dest='year',  # no subannual detail
    )

    output = []
    for tech in cfg['technology group']['freight truck']['tech']:
        output.append(make_df(
            'output',
            technology=tech,
            commodity='transport freight vehicle',
            level='useful',
            value=1.0,  # placeholder
            unit='km',
            **common))

    return dict(output=pd.concat(output))
