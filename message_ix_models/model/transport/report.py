import logging

from message_ix.reporting import Reporter
import pandas as pd

from message_data.reporting.util import infer_keys
from message_data.tools import ScenarioInfo, get_context


log = logging.getLogger(__name__)


#: Reporting configuration for :func:`check`. Adds a single key,
#: 'transport check', that depends on others and returns a
#: :class:`pandas.Series` of :class:`bool`.
CONFIG = dict(
    general=[dict(
        key='transport check',
        comp='transport_check',
        inputs=['scenario', 'ACT']
    )],
)


def check(scenario):
    """Check that the transport model solution is complete.

    Parameters
    ----------
    scenario : message_ix.Scenario
        Scenario with solution.

    Returns
    -------
    pd.Series
        Index entries are str descriptions of checks. Values are the
        :obj:`True` if the respective check passes.
    """
    # NB this is here to avoid circular imports
    from message_data.reporting.core import prepare_reporter

    rep, key = prepare_reporter(scenario, CONFIG, 'transport check')
    return rep.get(key)


def check_computation(scenario, ACT):
    """Reporting computation for :func:`check`.

    Imported into :mod:`.reporting.computations`.
    """
    info = ScenarioInfo(scenario)

    # Mapping from check name → bool
    checks = {}

    # Correct number of outputs
    ACT_lf = ACT.sel(t=['transport freight load factor',
                        'transport pax load factor'])
    checks["'transport * load factor' technologies are active"] = \
        len(ACT_lf) == 2 * len(info.Y) * (len(info.N) - 1)

    # # Force the check to fail
    # checks['(fail for debugging)'] = False

    return pd.Series(checks)


def callback(rep: Reporter):
    """:meth:`.prepare_reporter` callback for MESSAGE-Transport.

    Adds:

    - ``out::transport`` that aggregates outputs by technology group.
    """
    tech_cfg = get_context()['transport technology']

    out = infer_keys(rep, 'out')

    # Aggregate transport technologies
    t_groups = {g: i['tech'] for g, i in tech_cfg['technology group'].items()}
    keys = rep.aggregate(out, 'transport', dict(t=t_groups), sums=True)

    log.info(f'Add {repr(keys[0])} + {len(keys)-1} partial sums')
