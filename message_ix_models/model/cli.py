import click


@click.group("res")
@click.pass_obj
def cli(context):
    """MESSAGEix-GLOBIOM reference energy system (RES)."""


@cli.command("create-bare")
@click.option("--regions", type=click.Choice(["R11", "R14"]))
@click.pass_obj
def create_bare(context, regions):
    """Create the RES from scratch."""
    from .bare import create_res

    if regions:
        context.regions = regions

    create_res(context)
