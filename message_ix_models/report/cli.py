import logging
from copy import copy
from pathlib import Path

import click
import yaml

from message_ix_models.report import register, report
from message_ix_models.util import local_data_path, private_data_path
from message_ix_models.util._logging import mark_time
from message_ix_models.util.click import common_params

log = logging.getLogger(__name__)


@click.command(name="report")
@common_params("dry_run")
@click.option(
    "--config",
    "config_file",
    default="global",
    show_default=True,
    help="Path or stem for reporting config file.",
)
@click.option("--legacy", "-L", is_flag=True, help="Invoke legacy reporting.")
@click.option(
    "--module", "-m", metavar="MODULES", help="Add extra reporting for MODULES."
)
@click.option(
    "-o",
    "--output",
    "output_path",
    type=Path,
    help="Write output to file instead of console.",
)
@click.option(
    "--from-file",
    type=click.Path(exists=True, dir_okay=False),
    help="Report multiple Scenarios listed in FILE.",
)
@click.argument("key", default="message::default")
@click.pass_obj
def cli(context, config_file, legacy, module, output_path, from_file, key, dry_run):
    """Postprocess results.

    KEY defaults to the comprehensive report 'message::default', but may also be the
    name of a specific model quantity, e.g. 'output'.

    --config can give either the absolute path to a reporting configuration file, or
    the stem (i.e. name without .yaml extension) of a file in data/report.

    With --from-file, read multiple Scenario identifiers from FILE, and report each one.
    In this usage, --output-path may only be a directory.
    """
    # --config: use the option value as if it were an absolute path
    config = Path(config_file)
    if not config.exists():
        # Path doesn't exist; treat it as a stem in the metadata dir
        config = private_data_path("report", config_file).with_suffix(".yaml")

    if not config.exists():
        # Can't find the file
        raise FileNotFoundError(f"Reporting configuration --config={config}")

    # --output/-o: handle "~"
    output_path = output_path.expanduser() if output_path else None

    # --module/-m: load extra reporting config from modules
    module = module or ""
    for m in filter(len, module.split(",")):
        name = register(m)
        log.info(f"Registered reporting from {name}")

    # Common settings to apply in all contexts
    common = dict(config=config, key=key)

    # --legacy/-L: cue report() to invoke the legacy, instead of genno-based reporting
    if legacy:
        common["legacy"] = dict()

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
        context["report"] = dict(output_dir=local_data_path("report"))
        contexts.append(context)

    for ctx in contexts:
        # Update with common settings
        context["report"].update(common)
        report(ctx)
        mark_time()
