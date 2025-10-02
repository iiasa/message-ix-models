import click
import pyam
from message_ix.report import Reporter
from .h2_reporting import run_h2_reporting

@click.group("hydrogen")
def cli():
    """Hydrogen-related commands."""
    pass

@cli.command("report")
@click.pass_obj
def run_h2_reporting_cli(context):
    """Run hydrogen-specific reporting."""
    scenario = context.get_scenario()
    rep = Reporter.from_scenario(scenario)

    dfs = run_h2_reporting(rep, scenario.model, scenario.scenario)
    py_df = pyam.concat(dfs)

    print("Successfully generated H2 reporting dataframe:")
    print(py_df.timeseries())
