import click


@click.group()
def main():
    """MESSAGEix-GLOBIOM tools."""


@main.command()
def model():
    """Commands for models."""
