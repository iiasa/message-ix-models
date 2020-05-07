"""Generate input data."""
from collections import defaultdict
import logging

import pandas as pd

from message_data.tools import (
    ScenarioInfo,
    broadcast,
    copy_column,
    get_context,
    iter_parameters,
    make_df,
    make_io,
    make_matched_dfs,
)
from message_data.model.transport.demand import demand
from message_data.model.transport.utils import add_commodity_and_level
from .groups import get_consumer_groups  # noqa: F401
from .ldv import get_ldv_data
from .non_ldv import get_non_ldv_data


log = logging.getLogger(__name__)


DATA_FUNCTIONS = [
    demand,
    'conversion',
    'freight',
    'passenger',
    get_ldv_data,
    get_non_ldv_data,
    'dummy_supply',
]


def add_data(scenario, dry_run=False):
    """Populate *senario* with MESSAGE-Transport data."""
    # Add data
    info = ScenarioInfo(scenario)

    for func in DATA_FUNCTIONS:
        func = globals()[func] if isinstance(func, str) else func
        log.info(f'from {func.__name__}()')

        # Generate or load the data; add to the Scenario
        add_par_data(scenario, func(info), dry_run=dry_run)

    log.info('done')


def add_par_data(scenario, data, dry_run=False):
    total = 0

    for par_name, values in data.items():
        N = values.shape[0]
        log.info(f'{N} rows in {repr(par_name)}')
        log.debug(str(values))

        total += N

        if dry_run:
            continue

        scenario.add_par(par_name, values)

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
    """Input and output data for conversion technologies:

    The technologies are named 'transport {mode} load factor'.
    """
    cfg = get_context()['transport config']

    common = dict(
        year_vtg=info.y0,
        year_act=info.Y,
        mode='all',
        # No subannual detail
        time='year',
        time_origin='year',
        time_dest='year',
    )

    mode_info = [
        ('freight', cfg['factor']['freight load'], 't km'),
        ('pax', 1.0, 'km'),
    ]

    data = defaultdict(list)
    for mode, factor, output_unit in mode_info:
        i_o = make_io(
            (f'transport {mode} vehicle', 'useful', 'km'),
            (f'transport {mode}', 'useful', output_unit),
            factor,
            on='output',
            technology=f'transport {mode} load factor',
            **common
        )
        for par, df in i_o.items():
            node_col = 'node_origin' if par == 'input' else 'node_dest'
            data[par].append(
                df.pipe(broadcast, node_loc=info.N[1:])
                .assign(**{node_col: copy_column('node_loc')})
            )

    data = {par: pd.concat(dfs) for par, dfs in data.items()}

    data.update(
        make_matched_dfs(
            base=data['input'],
            capacity_factor=1,
            technical_lifetime=100,
        )
    )

    return data


def freight(info):
    """Data for freight technologies."""
    cfg = get_context()['transport technology']

    common = dict(
        year_vtg=info.y0,
        year_act=info.Y,
        mode='all',
        time='year',  # no subannual detail
        time_dest='year',
        time_origin='year',
    )

    data = defaultdict(list)
    for tech in cfg['technology group']['freight truck']['tech']:
        i_o = make_io(
            src=(None, None, 'GWa'),
            dest=('transport freight vehicle', 'useful', 'km'),
            efficiency=1.,
            on='input',
            technology=tech,
            **common,
        )

        i_o['input'] = add_commodity_and_level(i_o['input'])

        for par, df in i_o.items():
            node_col = 'node_origin' if par == 'input' else 'node_dest'
            data[par].append(
                df.pipe(broadcast, node_loc=info.N[1:])
                .assign(**{node_col: copy_column('node_loc')})
            )

    data = {par: pd.concat(dfs) for par, dfs in data.items()}

    data.update(
        make_matched_dfs(
            base=data['input'],
            capacity_factor=1,
            technical_lifetime=100,
        )
    )

    return data


def passenger(info):
    """Data for passenger technologies."""
    cfg = get_context()['transport technology']

    common = dict(
        year_vtg=info.Y,
        year_act=info.Y,
        commodity='transport pax vehicle',
        level='useful',
        value=1.0,  # placeholder
        unit='km',
        mode='all',
        time='year', time_dest='year',  # no subannual detail
    )

    output = []
    for tech in cfg['technology group']['BUS']['tech']:
        output.append(
            make_df('output', technology=tech, **common)
            .pipe(broadcast, node_loc=info.N[1:])
            .assign(node_dest=copy_column('node_loc'))
        )

    return dict(output=pd.concat(output))


def dummy_supply(info):
    """Dummy fuel supply for the bare RES."""
    common = dict(
        commodity='lightoil',
        level='final',
        mode='all',
        technology='DUMMY transport fuel',
        time='year',
        time_dest='year',
        unit='GWa',
        value=1.0,
        year_act=info.Y,
        year_vtg=info.y0,
    )

    result = dict(
        output=(
            make_df('output', **common)
            .pipe(broadcast, node_loc=info.N[1:])
            .assign(node_dest=copy_column('node_loc'))
        )
    )

    result.update(
        make_matched_dfs(
            base=result['output'],
            capacity_factor=1,
            var_cost=1,
            technical_lifetime=100,
        )
    )

    return result
