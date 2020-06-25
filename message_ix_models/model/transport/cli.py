from pathlib import Path

import click
import message_ix

from . import SCENARIO_INFO
from message_data.tools.cli import clone_to_dest, common_params


@click.group('transport')
def cli():
    """MESSAGE-Transport model."""
    pass


@cli.command()
@click.option('--version', default='geam_ADV3TRAr2_BaseX2_0',
              metavar='VERSION', help='Model version to read.')
@click.option('--check-base/--no-check-base', is_flag=True,
              help='Check properties of the base scenario (default: no).')
@click.option('--parse/--no-parse', is_flag=True,
              help='(Re)parse MESSAGE V data files (default: no).')
@click.option('--region', default='', metavar='REGIONS',
              help='Comma-separated region(s).')
@click.argument('SOURCE_PATH', required=False,
                default=Path('reference', 'data'))
@click.pass_obj
def migrate(context, version, check_base, parse, region, source_path):
    """Migrate data from MESSAGE(V)-Transport.

    If --parse is given, data from .chn, .dic, and .inp files is read from
    SOURCE_PATH for VERSION. Values are extracted and cached.

    Data is transformed to be suitable for the target scenario, and stored in
    migrate/VERSION/*.csv.
    """
    from .build import main as build
    from .migrate import import_all, load_all, transform
    from .utils import silence_log
    from message_data.tools import ScenarioInfo

    # Load the target scenario from database
    mp = context.get_platform()
    s_target = message_ix.Scenario(mp, **SCENARIO_INFO)
    info = ScenarioInfo(s_target)

    # Check that it has the required features
    if check_base:
        with silence_log():
            build(s_target, dry_run=True)
            print(f'Scenario {s_target} is a valid target for building '
                  'MESSAGE-Transport.')

    if parse:
        # Parse raw data
        data = import_all(source_path, nodes=region.split(','),
                          version=version)
    else:
        # Load cached data
        data = load_all(version=version)

    # Transform the data
    transform(data, version, info)


@cli.command("build")
@common_params("dest dry_run quiet")
@click.option("--fast", is_flag=True,
              help="Skip removing data for removed set elements.")
@click.pass_obj
def build_cmd(context, dest, **options):
    """Prepare the model."""
    from .build import main

    scenario, platform = clone_to_dest(context, defaults=SCENARIO_INFO)

    main(scenario, **options)

    del platform


@cli.command()
@click.option("--macro", is_flag=True)
@click.pass_obj
def solve(context, macro):
    """Run the model."""
    args = dict()

    scenario = context.get_scenario()

    if macro:
        from .callback import main as callback
        args['callback'] = callback

    scenario.solve(**args)
    scenario.commit()
