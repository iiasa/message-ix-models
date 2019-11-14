"""Reporting for the MESSAGEix-GLOBIOM global model."""
import logging
from pathlib import Path

import click

from ixmp.utils import logger
from .core import prepare_reporter


log = logging.getLogger(__name__)

CONFIG = Path(__file__).parent / 'data' / 'global.yaml'


@click.command(name='report')
@click.argument('key', default='message:default')
@click.option('-o', '--output', 'output_path', type=Path,
              help='Write output to file instead of console.')
@click.option('--verbose', is_flag=True, help='Set log level to DEBUG.')
@click.option('--dry-run', '-n', is_flag=True,
              help='Only show what would be done.')
@click.pass_context
def cli(ctx, key, output_path, verbose, dry_run):
    """Postprocess results.

    KEY defaults to the comprehensive report 'message:default', but may also
    be the name of a specific model quantity, e.g. 'output'.
    """
    from time import process_time

    times = [process_time()]

    def mark():
        times.append(process_time())
        log.info(' {:.2f} seconds'.format(times[-1] - times[-2]))

    if verbose:
        logger().setLevel('DEBUG')

    s = ctx.obj.get_scenario()
    mark()

    # Read reporting configuration from a file
    rep, key = prepare_reporter(s, CONFIG, key, output_path)
    mark()

    print('Preparing to report:', rep.describe(key), sep='\n')
    mark()

    if dry_run:
        return

    result = rep.get(key)
    print(f'Result written to {output_path}' if output_path else
          f'Result: {result}', sep='\n')
    mark()


def report(scenario, path, legacy=None):
    """Run complete reporting on *scenario* with output to *path*.

    If *legacy* is not None, it is used as keyword arguments to the old-
    style reporting.
    """
    if legacy is None:
        rep = prepare_reporter(scenario, CONFIG, 'default', path)
        rep.get('default')
    else:
        from message_data.tools.post_processing import iamc_report_hackathon

        legacy_args = dict(merge_hist=True)
        legacy_args.update(**legacy)

        iamc_report_hackathon.report(
            mp=scenario.platform,
            scen=scenario,
            model=scenario.name,
            scenario=scenario.name,
            out_dir=path,
            **legacy_args,
        )
