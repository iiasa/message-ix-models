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
            click.Option(
                ["--magicc-root", "magicc_root"],
                type=click.Path(exists=True, file_okay=False, path_type=Path),
                default=None,
                help=(
                    "MAGICC output root. When set, per-starter directories"
                    " resolve to <root>/<model><suffix>/<scenario without _PHY>/"
                    " and any magicc_output_dir in the YAML is ignored."
                ),
            ),
            click.Option(
                ["--magicc-file", "magicc_file"],
                type=click.Path(exists=True, dir_okay=False, path_type=Path),
                default=None,
                help=(
                    "Single MAGICC xlsx path; only valid with one starter."
                    " Overrides --magicc-root and YAML."
                ),
            ),
            click.Option(
                ["--magicc-model-suffix", "magicc_model_suffix"],
                type=str,
                default="",
                help=(
                    "Suffix appended to <model> when composing paths under"
                    " --magicc-root (e.g. '_p95' for the p95 envelope tree)."
                ),
            ),
        ],
    )
)
