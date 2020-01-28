import logging

import message_ix

from . import MODEL
from .data import add_data, strip_par_data
from .utils import (
    config,
    transport_technologies,
    )
from message_data.tools import ScenarioInfo

log = logging.getLogger(__name__)


def clone(p_source, p_dest):
    """Clone the base scenario on *p_source* to *p_dest*."""
    s_base = message_ix.Scenario(p_source, **MODEL['base'])

    # # “Poor person's clone”
    # s_base.to_excel('clone.xlsx')

    scen = s_base.clone(**MODEL['message-transport'], keep_solution=True,
                        platform=p_dest)

    return scen


def main(scenario, data_from=None, dry_run=False, quiet=True, fast=False):
    """Set up MESSAGE-Transport on *scenario*.

    With dry_run=True, don't modify *scenario*; only describe what would be
    done. This also serves as a check that *scenario* has the required features
    for setting up MESSAGE-Transport.

    In order to set up the model, the following steps take place:

    1. Sets are updated. For each set in the model that's modified:

       a. Required elements are checked; if they are missing, setup() raises
          ValueError.
       b. Set elements are removed. In this process, any parameter values which
          reference the set are also removed.
       c. New set elements are added.

    2. Transport technologies are added.

    """
    if quiet:
        log.setLevel(logging.ERROR)

    s = scenario

    if not dry_run:
        s.remove_solution()
        s.check_out()

    s_ = ScenarioInfo(s)

    if s_.y0 < 0:
        raise ValueError(f'firstmodelyear not set on Scenario; years {s_.y!r} '
                         'cannot be used to set up MESSAGE-Transport.')

    dump = {}  # Removed data

    for set_name, set_cfg in config['set'].items():
        if set_name not in s_.sets:
            log.info(f'Skip {set_name}.')
            continue

        # Base contents of the set
        base = s.set(set_name).tolist()
        log.info(f'Configure set {set_name!r}; {len(base)} elements')
        # log.info('\n'.join(base))  # All elements; verbose

        # Check for required elements
        for name in set_cfg.get('require', []):
            if name in base:
                log.info(f'{name!r} found.')
            else:
                log.info(f'{name!r} missing.')
                raise KeyError(f'{name} not among {base}')

        # Remove elements and associated parameter values
        for name in set_cfg.get('remove', []):
            log.info(f'{name!r} removed.')
            strip_par_data(s, set_name, name, dry_run=dry_run or fast,
                           dump=dump)

        # Add elements
        to_add = list(set_cfg.get('add', {}).items())

        if set_name == 'technology':
            log.info("Generate 'technology' elements")
            to_add.extend(transport_technologies(with_desc=True))

        if len(to_add):
            log.info(f'Add {len(to_add)} element(s)' +
                     '\n  '.join([''] + [f'{e[0]}: {e[1]}' for e in to_add]))

        if not dry_run:
            s.add_set(set_name, [e[0] for e in to_add])

        log.info(f'--- {set_name!r} done.')

    N_removed = sum(len(d) for d in dump.values())
    log.info(f'{N_removed} parameter rows removed.')

    # Add units
    for u, desc in config['set']['unit']['add'].items():
        log.info(f'Add unit {u!r}')
        s.platform.add_unit(u, comment=desc)

    # Add data
    add_data(scenario, data_from, dry_run=dry_run)

    # Finalize
    log.info('Commit results.')
    if not dry_run:
        s.commit('MESSAGEix-Transport setup.')
