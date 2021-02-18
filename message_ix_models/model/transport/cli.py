import logging
from pathlib import Path

import click

from message_data.logging import mark_time
from message_data.tools.cli import clone_to_dest, common_params

log = logging.getLogger(__name__)


@click.group("transport")
@click.pass_obj
def cli(context):
    """MESSAGEix-Transport variant."""
    from .utils import read_config

    # Ensure transport model configuration is loaded
    read_config(context)


@cli.command()
@common_params("dest")
@click.option(
    "--version",
    default="geam_ADV3TRAr2_BaseX2_0",
    metavar="VERSION",
    help="Model version to read.",
)
@click.option(
    "--check-base/--no-check-base",
    is_flag=True,
    help="Check properties of the base scenario (default: no).",
)
@click.option(
    "--parse/--no-parse",
    is_flag=True,
    help="(Re)parse MESSAGE V data files (default: no).",
)
@click.option(
    "--region", default="", metavar="REGIONS", help="Comma-separated region(s)."
)
@click.argument("SOURCE_PATH", required=False, default=Path("reference", "data"))
@click.pass_obj
def migrate(context, version, check_base, parse, region, source_path, dest):
    """Migrate data from MESSAGE(V)-Transport.

    If --parse is given, data from .chn, .dic, and .inp files is read from SOURCE_PATH
    for VERSION. Values are extracted and cached.

    Data is transformed to be suitable for the target scenario, and stored in
    migrate/VERSION/*.csv.
    """
    from message_data.tools import ScenarioInfo

    from .build import main as build
    from .migrate import import_all, load_all, transform
    from .utils import silence_log

    # Load the target scenario from database
    # mp = context.get_platform()
    s_target = dest
    info = ScenarioInfo(s_target)

    # Check that it has the required features
    if check_base:
        with silence_log():
            build(s_target, dry_run=True)
            print(
                f"Scenario {s_target} is a valid target for building "
                "MESSAGEix-Transport."
            )

    if parse:
        # Parse raw data
        data = import_all(source_path, nodes=region.split(","), version=version)
    else:
        # Load cached data
        data = load_all(version=version)

    # Transform the data
    transform(data, version, info)


@cli.command("build")
@common_params("dest dry_run regions quiet")
@click.option(
    "--fast", is_flag=True, help="Skip removing data for removed set elements."
)
@click.pass_obj
def build_cmd(context, dest, **options):
    """Prepare the model."""
    from .build import main

    # Handle --regions
    regions = options.get("regions")
    if not regions:
        print("Using default --regions=R11")
        regions = "R11"
    context.regions = regions

    # No defaults; force the user to provide these
    scenario, platform = clone_to_dest(context, defaults=dict())

    main(context, scenario, **options)

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

        args["callback"] = callback

    scenario.solve(**args)
    scenario.commit()
