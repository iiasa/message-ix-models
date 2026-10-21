from typing import TYPE_CHECKING

import click

if TYPE_CHECKING:
    from message_ix_models import Context


@click.group("edits")
def cli():
    """EDITS project.

    https://docs.messageix.org/projects/models/en/latest/project/edits.html
    """


@cli.command("_debug")
@click.pass_obj
def debug(context: "Context", /, **kwargs) -> None:  # pragma: no cover
    """Development/debugging code."""
    from . import gen_demand, pasta_native_to_sdmx  # noqa: F401

    # commented: Only needs to occur once, then cached
    # pasta_native_to_sdmx()
    gen_demand(context)
