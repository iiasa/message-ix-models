"""Command-line tools specific to the SPARCCLE project."""

from pathlib import Path

import click

from message_ix_models.workflow import make_click_command


@click.group("sparccle")
@click.pass_obj
def cli(context):
    """SPARCCLE project."""


cli.add_command(
    make_click_command(
        f"{__package__}.workflow.generate",
        name="SPARCCLE",
        slug="sparccle",
        params=[
            click.Option(
                ["--config", "config_path"],
                type=click.Path(exists=True, dir_okay=False, path_type=Path),
                default=None,
                help="Path to scenario_config.yaml (default: packaged copy).",
            ),
        ],
    )
)
