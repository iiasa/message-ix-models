import click
from message_ix.cli import main


@click.group("model")
@click.pass_obj
def model_group(context):
    """MESSAGEix-GLOBIOM tools."""


main.add_command(model_group)
