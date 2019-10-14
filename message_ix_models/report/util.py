from functools import partial
import logging

from message_ix.reporting import Reporter, configure
from message_ix.reporting.computations import write_report

log = logging.getLogger(__name__)


def prepare_reporter(scenario, config, key, output_path):
    # Apply global reporting configuration, e.g. unit definitions
    configure(config)

    log.info('Preparing reporter')

    # Create a Reporter for *scenario* and apply Reporter-specific config
    rep = Reporter.from_scenario(scenario) \
                  .configure(config)

    # If needed, get the full key for *quantity*
    key = rep.check_keys(key)[0]

    if output_path:
        # Add a new computation that writes *key* to the specified file
        rep.add('cli-output', (partial(write_report, path=output_path), key))
        key = 'cli-output'

    log.info('…done')

    return rep, key
