"""Command-line interface for MESSAGEix-Transport.

Use the :doc:`CLI <message_ix_models:cli>` command :program:`mix-models transport` to
invoke the commands defined in this module. Each individual command also has its own
help text; try e.g. :program:`mix-models transport run --help`. For the main CLI entry
point, see :ref:`transport-usage`.
"""

import logging
from pathlib import Path

import click

from message_ix_models import ScenarioInfo
from message_ix_models.util._logging import silence_log
from message_ix_models.util.click import PARAMS, common_params, exec_cb
from message_ix_models.workflow import make_click_command

log = logging.getLogger(__name__)


@click.group("transport")
@click.pass_obj
def cli(context):
    """MESSAGEix-Transport variant."""


# "run" subcommand to interact with the transport workflow
cli.add_command(
    make_click_command(
        f"{__package__}.workflow.generate",
        name="MESSAGEix-Transport",
        slug="transport",
        params=[
            click.Option(
                ["--base", "base_scenario"],
                type=click.Choice(["auto", "bare"]),
                default="bare",
                help="Base scenario (if no --url)",
            ),
            click.Option(
                ["--future", "futures_scenario"], help="Transport futures scenario."
            ),
            click.Option(
                ["--fast"],
                is_flag=True,
                help="Skip removing data for removed set elements.",
            ),
            click.Option(
                ["--model-extra", "target_model_name"],
                callback=exec_cb("context.core.dest_scenario['model'] = value"),
                default="",
                help="Model name suffix.",
            ),
            click.Option(
                ["--scenario-extra", "target_scenario_name"],
                callback=exec_cb("context.core.dest_scenario['scenario'] = value"),
                default="",
                help="Scenario name suffix.",
            ),
            click.Option(
                ["--key", "report_key"], default="transport all", help="Key to report."
            ),
        ]
        + [PARAMS[n] for n in "dest nodes quiet".split()],
    )
)


@cli.command()
@click.pass_obj
def debug(context):
    """Temporary code for development."""


@cli.command("export-emi")
@click.pass_obj
@click.argument("path_stem")
def export_emissions_factors(context, path_stem):
    """Export emissions factors from base scenarios.

    Only the subset corresponding to removed transport technologies are exported.
    """
    from datetime import datetime

    from message_ix_models.util import package_data_path

    # List of techs
    techs = context.transport.spec.remove.set["technology"]

    # Load the targeted scenario
    scenario = context.get_scenario()

    # Output path
    out_dir = package_data_path("transport", "emi")
    out_dir.mkdir(exist_ok=True, parents=True)

    for name in ("emission_factor", "relation_activity"):
        df = scenario.par(name, filters=dict(technology=techs))

        path = out_dir / f"{path_stem}-{name}.csv"
        log.info(f"Write {len(df)} rows to {path}")

        # Write header
        path.write_text(
            f"# Exported {datetime.today().isoformat()} from\n# {context.url}\n"
        )
        # Write data
        df.to_csv(path, mode="a", index=False)


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
    from .build import main as build
    from .migrate import import_all, load_all, transform

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
