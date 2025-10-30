import click


@click.group("alps")
def cli():
    """ALPS project workflows."""


# Import and add the emissions-report command from run_emissions_workflow
from .run_emissions_workflow import main as emissions_report_main
cli.add_command(emissions_report_main, name="emissions-report")
