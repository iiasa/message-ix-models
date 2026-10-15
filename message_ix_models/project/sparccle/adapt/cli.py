"""Command-line tools for the SPARCCLE adaptation scenarios."""

from pathlib import Path

import click

from message_ix_models.workflow import make_click_command


@click.group("sparccle_adapt")
@click.pass_obj
def cli(context):
    """SPARCCLE adaptation scenarios (BAU / maladaptation / +adaptation)."""


cli.add_command(
    make_click_command(
        f"{__package__}.workflow.generate",
        name="SPARCCLE-adapt",
        slug="sparccle_adapt",
        params=[
            click.Option(
                ["--config", "config_path"],
                type=click.Path(exists=True, dir_okay=False, path_type=Path),
                default=None,
                help="Path to config.yaml (default: packaged copy).",
            ),
        ],
    )
)
