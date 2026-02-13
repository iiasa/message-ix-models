import click

from message_ix_models.workflow import make_click_command


@click.group("circeular")
def cli():
    """CircEUlar project.

    https://docs.messageix.org/projects/models/en/latest/project/circeular.html
    """


cli.add_command(
    make_click_command(
        f"{__package__}.workflow.generate",
        name="CircEUlar",
        slug="circeular",
    )
)
