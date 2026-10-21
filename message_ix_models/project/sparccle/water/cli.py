"""Command-line tools for the SPARCCLE water adaptation scenarios."""

from pathlib import Path

import click

from message_ix_models.workflow import make_click_command

CONFIG_OPTION = click.Option(
    ["--config", "config_path"],
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Path to config.yaml (default: packaged copy).",
)


@click.group("sparccle_water")
@click.pass_obj
def cli(context):
    """SPARCCLE water adaptation scenarios (base / BAU / maladapt / +adapt)."""


cli.add_command(
    make_click_command(
        f"{__package__}.workflow.generate",
        name="SPARCCLE-water",
        slug="sparccle_water",
        params=[CONFIG_OPTION],
    )
)


@cli.command("names", params=[CONFIG_OPTION])
@click.argument("ssp")
@click.argument("key")
def names(config_path, ssp, key):
    """Print "PLATFORM MODEL SCENARIO" of scenario KEY of starter SSP."""
    from .workflow import load_config, platform_of, scenario_name

    config = load_config(config_path)
    model = config["starters"][ssp]["model"]
    key = key if key != "starter" else None
    scenario = (
        config["starters"][ssp]["scenario"]
        if key is None
        else scenario_name(config, ssp, key)
    )
    click.echo(f"{platform_of(config, ssp)} {model} {scenario}")
