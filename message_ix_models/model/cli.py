import click

from message_ix_models.util.click import common_params


@click.group("res")
@click.pass_obj
def cli(context):
    """MESSAGEix-GLOBIOM reference energy system (RES)."""


@cli.command("create-bare")
@common_params("nodes")
@click.pass_obj
def create_bare(context, nodes):
    """Create the RES from scratch."""
    from .bare import create_res

    if nodes:
        context.regions = nodes

    create_res(context)
