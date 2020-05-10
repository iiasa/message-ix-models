import numpy as np
import pandas as pd

from message_data.tools import broadcast, get_context, make_df
from .build import generate_set_elements


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
    common = dict(
        year=info.Y,
        value=np.arange(len(info.Y)) + 0.1,
        level='useful',
        time='year',
    )

    dfs = []

    for commodity in filter(
        lambda c: c.anno.get("demand", False),
        generate_set_elements("commodity"),
    ):
        unit = "t km" if "freight" in commodity.id else "km"
        dfs.append(make_df(
            'demand',
            commodity=commodity.id,
            unit=unit,
            **common,
        ))

    # # Dummy demand for light oil
    # common['level'] = 'final'
    # dfs.append(
    #     make_df('demand', commodity='lightoil', **common)
    # )

    return pd.concat(dfs).pipe(broadcast, node=info.N[1:])
