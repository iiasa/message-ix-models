"""Command-line tools specific to the NAVIGATE project."""

import logging

import click

from message_ix_models.project.navigate import Config
from message_ix_models.util.click import PARAMS
from message_ix_models.workflow import make_click_command

log = logging.getLogger(__name__)


_DSD = click.Option(
    ["--dsd"],
    type=click.Choice(["navigate", "iiasa-ece"]),
    default=Config.dsd,  # Use the same default value as the .navigate.Config class
    help="Target data structure for submission prep.",
)
_SCENARIO = click.Option(
    ["-s", "--scenario", "navigate_scenario"],
    default="baseline",
    help="NAVIGATE T3.5 scenario ID.",
)


@click.group("navigate", params=[_DSD, _SCENARIO])
@click.option("--no-transport", is_flag=True, help="Omit MESSAGEix-Transport.")
@click.option(
    "--ctax",
    type=float,
    default=Config.carbon_tax,
    help="Starting value of carbon tax for Ctax-* scenarios.",
)
@click.pass_obj
def cli(context, dsd, navigate_scenario, no_transport, ctax):
    """NAVIGATE project."""

    context["navigate"] = Config(scenario=navigate_scenario)

    # Copy settings to the Config object
    context.navigate.dsd = dsd
    context.navigate.transport = not no_transport
    context.navigate.carbon_tax = ctax


cli.add_command(
    make_click_command(
        f"{__package__}.workflow.generate",
        name="NAVIGATE",
        slug="navigate",
        params=[PARAMS["dry_run"]],
    )
)


@cli.command("check-budget")
@click.pass_obj
def check_budget(context):
    import numpy as np
    import pandas as pd
    from message_data.tools import interpolate_budget
    from message_ix import Scenario

    # Model name
    # TODO make this configurable
    m = "MESSAGEix-GLOBIOM 1.1-BM-R12 (NAVIGATE)"

    mp = context.get_platform()
    dfs = []
    target = dict()
    constraint = dict()

    # Iterate over scenario names, target emission budgets, and constraint values
    # TODO don't hard-code these values from .navigate.CLIMATE_POLICY
    # TODO make the list configurable
    for s_name, version, t, c in (
        ("NPi-Default", None, np.nan, np.nan),
        #
        # From 2023-05-31
        # ("NPi-Default_ENGAGE_15C_step-3+B", None, 850, 1840),
        # ("NPi-Default_ENGAGE_20C_step-3+B", 2, 900, 1931),
        # ("NPi-Default_ENGAGE_20C_step-3+B", 1, 1150, 2700),
        #
        # From 2023-08-03
        # ("NPi-act+MACRO_ENGAGE_20C_step-3+B", 3, 900, 1931),
        # ("NPi-act+MACRO_ENGAGE_20C_step-3+B", 3, 900, 1931),
        # ("NPi-act+MACRO", 2, np.nan, np.nan), # TODO check version
        # ("NPi-ele+MACRO", 2, np.nan, np.nan), # TODO check version
        # ("NPi-tec+MACRO", 2, np.nan, np.nan),
        # ("NPi-all+MACRO", 4, np.nan, np.nan),
        ("NPi-ref+MACRO", 26, np.nan, np.nan),
        # ("20C-ref ENGAGE_20C_step-3+B", 2, 1150, 2700),
        # ("20C-ref ENGAGE_20C_step-3+B", 3, 1150, 2600),
        # ("20C-ref ENGAGE_20C_step-3+B", 4, 1150, 2585),
        ("2C-Default ENGAGE_20C_step-3+B", 2, 900, 2511),
        ("2C-Default ENGAGE_20C_step-3+B", 4, 900, 1889),
        ("2C-Default ENGAGE_20C_step-3+B", 5, 900, 1840),
        # ("NPi-act+MACRO_ENGAGE_20C_step-3+B", 3, 900, 1931),
        # ("NPi-ref+MACRO", 26, np.nan, np.nan),
        # ("Ctax-ref+B", 9, np.nan, np.nan),  # --ctax=380
        # ("Ctax-ref+B", 8, np.nan, np.nan),  # --ctax=400
        # ("Ctax-ref+B", 7, np.nan, np.nan),
        # ("Ctax-ref+B", 6, np.nan, np.nan),
        # ("Ctax-ref+B", 5, np.nan, np.nan),
        # NB c=552 is the "Actual cumulative emissions". c=920 is the output (suggested
        #    value for c) below when c=552 is set.
        ("Ctax-ref+B", 1, 650, 920),
        # ("Ctax-ref+B", 1, 650, 920),
        # ("Ctax-ref+B", 1, np.nan, np.nan),
        # WP6
        # ("NPi-Default+MACRO", 2, np.nan, np.nan),
        # ("Ctax-Default+B", 1, np.nan, np.nan),
    ):
        try:
            s = Scenario(mp, model=m, scenario=s_name, version=version)
        except Exception as e:
            print(repr(e))
            continue

        key = f"{s_name}#{s.version}"
        # Retrieve the time series data stored by legacy reporting for one variable and
        # region.
        # NB this region ID is due to the automatic renaming that happens on ixmp-dev.
        dfs.append(
            s.timeseries(region="GLB region (R12)", variable="Emissions|CO2")
            .set_index("year")["value"]
            .rename(key)
        )
        target[key] = t
        constraint[key] = c

    mp.close_db()

    data = pd.concat(dfs, axis=1)

    print(f'Data for v="Emissions|CO2" stored with scenarios:\n{data.to_string()}')

    if data.isna().any(axis=None):
        print(
            "\nFill data to the right to cover missing values.\n"
            "(Adjust scenario order to change fill values used.)"
        )
        mask = data.isna().any(axis=1)
        data[mask] = data[mask].fillna(method="ffill", axis=1)
        print(data[mask].to_string())

    result = interpolate_budget(data, target, constraint)

    print("")
    for key, value in result.items():
        if np.isnan(value):
            print(f"{key}: no result")
            continue
        print(
            f"{key}: set budget={value:.3f} (currently {constraint[key]}) average"
            f" Mt C-eq / y to achieve {target[key]} Gt CO₂ total"
        )
