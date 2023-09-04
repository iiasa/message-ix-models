import click

from .structure import generate


@click.group("ssp")
def cli():
    pass


@cli.command("gen-structures")
@click.pass_obj
def gen_structures(context):
    """(Re)Generate the SSP data structures in SDMX."""
    generate(context)
