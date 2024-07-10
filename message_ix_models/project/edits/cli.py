import click


@click.group("edits")
def cli():
    """EDITS project.

    https://docs.messageix.org/projects/models/en/latest/project/edits.html
    """


@cli.command("_debug")
@click.pass_obj
def debug(context, **kwargs):  # pragma: no cover
    """Development/debugging code."""
    from . import gen_demand, pasta_native_to_sdmx  # noqa: F401

    # commented: Only needs to occur once, then cached
    # pasta_native_to_sdmx()
    gen_demand()
