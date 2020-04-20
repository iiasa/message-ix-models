import numpy as np
import pandas as pd

from message_data.tools import broadcast, get_context, make_df


def demand(info):
    """Return transport demands.

    Parameters
    ----------
    info : .ScenarioInfo
    """
    config = get_context()['transport config']['data source']
    func = globals()[config['demand']]

    return dict(demand=func(info))


def dummy(info):
    """Dummy demands.

    Parameters
    ----------
    info : .ScenarioInfo
    """
    data = dict(
        year=info.Y,
        value=np.arange(len(info.Y)) + 0.1,
        level='useful',
        commodity='transport freight',
        unit='t km',
        time='year',
    )

    # - Assemble into a message_ix-ready DataFrame
    # - Broadcast over all nodes
    data = make_df('demand', **data)\
        .pipe(broadcast, node=info.N[1:])

    # Original data plus a copy
    return pd.concat([
        data,
        data.assign(commodity='transport pax', unit='km'),
    ])
