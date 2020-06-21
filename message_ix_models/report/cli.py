import logging
import sys
from copy import copy
from pathlib import Path
from time import process_time

import click
import yaml

from . import prepare_reporter, register


log = logging.getLogger(__name__)


_TIMES = []


def mark():
    _TIMES.append(process_time())
    log.info(' {:.2f} seconds'.format(_TIMES[-1] - _TIMES[-2]))


@click.command(name='report')
@click.pass_obj
@click.option('--config', 'config_file', default='global', show_default=True,
              help='Path or stem for reporting config file.')
@click.option("--module", "-m", metavar="MODULES",
              help="Add extra reporting for MODULES.")
@click.option('-o', '--output', 'output_path', type=Path,
              help='Write output to file instead of console.')
@click.option('--from-file', type=click.Path(exists=True, dir_okay=False),
              help='Report multiple Scenarios listed in FILE.')
@click.option('--verbose', is_flag=True, help='Set log level to DEBUG.')
@click.option('--dry-run', '-n', is_flag=True,
              help='Only show what would be done.')
@click.argument('key', default='message:default')
def cli(
    context,
    config_file,
    module,
    output_path,
    from_file,
    verbose,
    dry_run,
    key
):
    """Postprocess results.

    KEY defaults to the comprehensive report 'message:default', but may also
    be the name of a specific model quantity, e.g. 'output'.

    --config can give either the absolute path to a reporting configuration
    file, or the stem (i.e. name without .yaml extension) of a file in
    data/report.

    With --from-file, read multiple Scenario identifiers from FILE, and report
    each one. In this usage, --output-path may only be a directory.
    """
    _TIMES.append(process_time())

    # --config: use the option value as if it were an absolute path
    config = Path(config_file)
    if not config.exists():
        # Path doesn't exist; treat it as a stem in the metadata dir
        config = context.get_config_file('report', config_file)

    if not config.exists():
        # Can't find the file
        raise click.BadOptionUsage(f'--config={config_file} not found')

    if verbose:
        log.setLevel('DEBUG')
        logging.getLogger('ixmp').setLevel('DEBUG')

    # Load modules
    module = module or ""
    for name in module.split(","):
        name = f"message_data.{name}.report"
        __import__(name)
        register(sys.modules[name].callback)

    # Prepare a list of Context objects, each referring to one Scenario
    contexts = []

    if from_file:
        # Multiple URLs
        if not output_path:
            output_path = Path.cwd()
        if not output_path.is_dir():
            msg = '--output-path must be directory with --from-file'
            raise click.BadOptionUsage(msg)

        for item in yaml.safe_load(open(from_file)):
            # Copy the existing Context to a new object
            ctx = copy(context)

            # Update with Scenario info from file
            ctx.handle_cli_args(**item)

            # Construct an output path from the parsed info/URL
            ctx.output_path = Path(
                output_path,
                '_'.join([ctx.platform_info['name'],
                          ctx.scenario_info['model'],
                         ctx.scenario_info['scenario']]),
                ).with_suffix('.xlsx')

            contexts.append(ctx)
    else:
        # Single Scenario; identifiers were supplied to the top-level CLI
        context.output_path = output_path
        contexts.append(context)

    for ctx in contexts:
        # Load the Platform and Scenario
        scenario = context.get_scenario()
        mark()

        report(scenario, key, config, ctx.output_path, dry_run)


def report(scenario, key, config, output_path, dry_run):
    rep, key = prepare_reporter(scenario, config, key, output_path)
    mark()

    print('', 'Preparing to report:', rep.describe(key), '', sep='\n\n')
    mark()

    if dry_run:
        return

    result = rep.get(key)
    print(f'Result written to {output_path}' if output_path else
          f'Result: {result}', sep='\n')
    mark()


def _report(scenario, path, legacy=None):
    """Run complete reporting on *scenario* with output to *path*.

    If *legacy* is not None, it is used as keyword arguments to the old-
    style reporting.
    """
    if legacy is None:
        config = Path(__file__).parents[2] / 'data' / 'report' / 'global.yaml'
        rep = prepare_reporter(scenario, config, 'default', path)
        rep.get('default')
    else:
        from message_data.tools.post_processing import iamc_report_hackathon

        legacy_args = dict(merge_hist=True)
        legacy_args.update(**legacy)

        iamc_report_hackathon.report(
            mp=scenario.platform,
            scen=scenario,
            model=scenario.model,
            scenario=scenario.scenario,
            out_dir=path,
            **legacy_args,
        )
