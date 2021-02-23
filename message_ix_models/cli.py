import sys

import click


@click.group()
def main():
    """MESSAGEix-GLOBIOM tools."""


@main.command()
def model():
    """Commands for models."""


try:
    from message_data.cli import modules_with_cli as message_data_modules_with_cli
except ImportError:
    message_data_modules_with_cli = []

for name in message_data_modules_with_cli:  # pragma: no cover
    name = "message_data." + name
    __import__(name)
    main.add_command(getattr(sys.modules[name], "cli"))

    # TODO use this in the future
    # name = f"message_data.{name}.cli"
    # __import__(name)
    # main.add_command(sys.modules[name].main)
