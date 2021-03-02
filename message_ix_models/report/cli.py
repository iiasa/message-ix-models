import logging
import sys
from copy import copy
from pathlib import Path

import click
import yaml
from message_ix_models.util.logging import mark_time

from message_data.reporting import register, report

log = logging.getLogger(__name__)


@click.command(name="report")
@click.pass_obj
@click.option(
    "--config",
    "config_file",
    default="global",
    show_default=True,
    help="Path or stem for reporting config file.",
)
@click.option(
    "--module", "-m", metavar="MODULES", help="Add extra reporting for MODULES."
)
@click.option(
    "-o",
    "--output",
    "output_path",
    type=Path,
    help="Write output to file instead of console.",
    default=Path.cwd(),
)
@click.option(
    "--from-file",
    type=click.Path(exists=True, dir_okay=False),
    help="Report multiple Scenarios listed in FILE.",
)
@click.option("--dry-run", "-n", is_flag=True, help="Only show what would be done.")
@click.argument("key", default="message:default")
def cli(context, config_file, module, output_path, from_file, dry_run, key):
    """Postprocess results.

    KEY defaults to the comprehensive report 'message:default', but may alsobe the name
    of a specific model quantity, e.g. 'output'.

    --config can give either the absolute path to a reporting configuration file, or
    the stem (i.e. name without .yaml extension) of a file in data/report.

    With --from-file, read multiple Scenario identifiers from FILE, and report each one.
    In this usage, --output-path may only be a directory.
    """
    # --config: use the option value as if it were an absolute path
    config = Path(config_file)
    if not config.exists():
        # Path doesn't exist; treat it as a stem in the metadata dir
        config = context.get_config_file("report", config_file)

    if not config.exists():
        # Can't find the file
        raise click.BadOptionUsage(f"--config={config_file} not found")

    # --output/-o: handle "~"
    output_path = output_path.expanduser()

    # Load modules
    module = module or ""
    for name in filter(lambda n: len(n), module.split(",")):
        name = f"message_data.{name}.report"
        __import__(name)
        register(sys.modules[name].callback)
        log.info(f"Registered reporting config from {name}")

    # Prepare a list of Context objects, each referring to one Scenario
    contexts = []

    if from_file:
        # Multiple URLs
        if not output_path.is_dir():
            raise click.BadOptionUsage(
                "--output-path must be directory with --from-file"
            )

        for item in yaml.safe_load(open(from_file)):
            # Copy the existing Context to a new object
            ctx = copy(context)

            # Update with Scenario info from file
            ctx.handle_cli_args(**item)

            # Construct an output path from the parsed info/URL
            ctx.output_path = Path(
                output_path,
                "_".join(
                    [
                        ctx.platform_info["name"],
                        ctx.scenario_info["model"],
                        ctx.scenario_info["scenario"],
                    ]
                ),
            ).with_suffix(".xlsx")

            contexts.append(ctx)
    else:
        # Single Scenario; identifiers were supplied to the top-level CLI
        context.output_path = output_path
        contexts.append(context)

    for ctx in contexts:
        # Load the Platform and Scenario
        scenario = context.get_scenario()
        mark_time()

        report(
            scenario,
            config=config,
            key=key,
            output_path=ctx.output_path,
            dry_run=dry_run,
        )
