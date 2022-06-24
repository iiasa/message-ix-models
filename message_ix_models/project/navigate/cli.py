"""Command-line interface for NAVIGATE project."""
import logging

import click

log = logging.getLogger(__name__)


@click.group("navigate")
@click.pass_obj
def cli(context):
    """NAVIGATE project."""
