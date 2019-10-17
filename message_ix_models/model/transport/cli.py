import os
from pathlib import Path
import shutil
import stat

import click
import ixmp
import message_ix
import pandas as pd

from . import MODEL


# Path to ixmp local HSQL databases
db_path = Path('~', '.local', 'share', 'ixmp', 'localdb').expanduser()


def get_platform(name):
    """Return a Platform instance."""
    if name == 'local':
        return ixmp.Platform(db_path / 'message-transport-dev',
                             dbtype='HSQLDB')
    elif name == 'GP3':
        return ixmp.Platform('gp3.properties')
    else:
        raise ValueError(name)


@click.group()
def main():
    """Command-line tool for MESSAGEix-Transport."""
    pass


@main.command()
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
def migrate(version, check_base, parse, region, source_path):
    """Migrate data from MESSAGE(V)-Transport.

    If --parse is given, data from .chn, .dic, and .inp files is read from
    SOURCE_PATH for VERSION. Values are extracted and cached.

    Data is transformed to be suitable for the target scenario, and stored in
    migrate/VERSION/*.csv.
    """
    from .build import main as build
    from .migrate import import_all, load_all, transform
    from .utils import ScenarioInfo, silence_log

    # Load the target scenario from database
    mp = get_platform('local')
    s_target = message_ix.Scenario(mp, **MODEL['message-transport'])
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


@main.command('build')
@click.option('--version', default='geam_ADV3TRAr2_BaseX2_0',
              metavar='VERSION', help='Model version to read.')
@click.option('--dry-run', is_flag=True)
@click.option('--quiet', is_flag=True)
@click.option('--fast', is_flag=True,
              help='Skip removing data for removed set elements.')
def build_cmd(version, dry_run, fast, quiet):
    """Prepare the model."""
    from .build import main

    # Load from database
    mp = get_platform('local')
    s = message_ix.Scenario(mp, **MODEL['message-transport'])

    main(s, data_from=version, dry_run=dry_run, quiet=quiet, fast=fast)


@main.command()
@click.option('--macro', is_flag=True)
def solve(macro):
    """Run the model."""
    args = dict()

    # Load from database
    mp = get_platform('local')
    s = message_ix.Scenario(mp, **MODEL['message-transport'])

    if macro:
        from .callback import main as callback
        args['callback'] = callback

    s.solve(**args)
    s.commit()


@main.command('clone')
@click.argument('source', type=click.Choice('backup GP3'.split()))
def clone_cmd(source):
    """Clone base scenario to the local database.

    SOURCE is the name of source platform: either 'GP3' or 'backup'. If
    'backup', the local database is manually overwritten with archive files.
    """
    if source == 'backup':
        # NB do this by copying files instead of using ixmp Scenario.clone()
        for src in db_path.glob('gp3-clone-2019-07-08*'):
            if src.suffix == '.tmp':
                continue
            dst = src.parent / src.name.replace('gp3-clone-2019-07-08',
                                                'message-transport-dev')
            shutil.copy(src, dst)
            os.chmod(dst, stat.S_IRUSR | stat.S_IWUSR)

        p_dest = get_platform('local')

    elif source == 'GP3':
        from .build import clone

        # Clone over the network from the server.
        p_dest = get_platform('local')
        clone(get_platform(source), p_dest)

    # Show the results
    show(p_dest)


def show(mp):
    """Show the contents of a database."""
    with pd.option_context('display.max_rows', None,
                           'display.max_columns', None):
        cols = ['model', 'scenario', 'version']
        print(mp.scenario_list(default=False)[cols])


@main.command('show')
def show_cmd():
    """Show the contents of the local database."""
    show(get_platform('local'))


@main.command()
def debug():
    """Temporary code for debugging."""
    # Don't commit anything here
    pass
