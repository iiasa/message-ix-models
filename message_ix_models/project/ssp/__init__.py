import click

from message_ix_models.util.click import common_params

from .structure import generate


@click.group("ssp")
def cli():
    pass


@cli.command("gen-structures")
@common_params("dry_run")
@click.pass_obj
def gen_structures(context, **kwargs):
    """(Re)Generate the SSP data structures in SDMX."""
    generate(context)
