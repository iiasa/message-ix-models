import pandas as pd

from message_data.tools import ScenarioInfo

#: Reporting configuration for :func:`check`. Adds a single key,
#: 'transport check', that depends on others and returns a
#: :class:`pandas.Series` of :class:`bool`.
CONFIG = dict(
    filters=dict(
        t=[
            'transport freight load factor',
            'transport pax load factor',
        ]),
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


def transport_check(scenario, ACT):
    """Reporting computation for :func:`check`.

    Imported into :mod:`.reporting.computations`.
    """
    info = ScenarioInfo(scenario)

    # Mapping from check name → bool
    checks = {}

    # Correct number of outputs
    checks["'transport * load factor' technologies are active"] = \
        len(ACT) == 2 * len(info.Y) * (len(info.N) - 1)

    # # Force the check to fail
    # checks['(fail for debugging)'] = False

    return pd.Series(checks)
