import click


@click.group("edits")
def cli():
    """EDITS project.

    https://docs.messageix.org/projects/models/en/latest/project/edits.html
    """


@cli.command("_debug")
@click.pass_obj
def debug(context, **kwargs):
    """Development/debugging code."""
    from . import read_pasta_data

    read_pasta_data()
