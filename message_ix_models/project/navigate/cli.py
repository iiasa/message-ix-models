"""Command-line interface for NAVIGATE project."""
import logging
from pathlib import Path

import click

log = logging.getLogger(__name__)


@click.group("navigate")
@click.pass_obj
def cli(context):
    """NAVIGATE project."""


@cli.command("gen-config")
@click.argument("f1", type=click.Path(exists=True, path_type=Path))
@click.argument("f2", type=click.Path(exists=True, path_type=Path))
@click.pass_obj
def gen_config_cmd(context, f1, f2):
    """Generate configuration for :mod:`.prep_submission`.

    F1 is the path to a reporting output file in .xlsx format.
    F2 is the path to the variables.yaml in the NAVIGATE workflow repository.
    """
    from .report import gen_config

    # Fixed values
    context.regions = "R12"

    gen_config(context, f1, f2)
