"""Command-line tools specific to the NAVIGATE project."""
import logging
import re
from pathlib import Path

import click
from message_ix_models.util.click import common_params

from . import Config

log = logging.getLogger(__name__)


_DSD = click.Option(
    ["--dsd"],
    type=click.Choice(["navigate", "iiasa-ece"]),
    default="navigate",
    help="Target data structure for submission prep.",
)
_SCENARIO = click.Option(
    ["-s", "--scenario", "navigate_scenario"],
    default="baseline",
    help="NAVIGATE T3.5 scenario ID.",
)


@click.group("navigate", params=[_SCENARIO])
@click.pass_obj
def cli(context, navigate_scenario):
    """NAVIGATE project."""

    context["navigate"] = Config(scenario=navigate_scenario)


@cli.command("prep-submission", params=[_DSD])
@click.argument("wf_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.pass_obj
def prep_submission(context, wf_dir, dsd):
    """Prepare data for NAVIGATE submission.

    WF_DIR is the base path of the NAVIGATE workflow repository.
    """
    from message_data.projects.navigate.report import gen_config
    from message_data.tools.prep_submission import main

    context.navigate.dsd = dsd
    # Fixed values
    context.regions = "R12"

    # Generate a prep_submission.Config object
    config = gen_config(context, wf_dir, [context.get_scenario()])
    print(config)

    main(config)


COMMANDS = (
    """
    $ mix-models
    --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-R12/baseline_DEFAULT#{v[0]}"
    --local-data "./data" material build --tag "NAVIGATE_test"
    """,
    """
    $ mix-models
    --url="ixmp://ixmp-dev/MESSAGEix-Materials/baseline_DEFAULT_NAVIGATE_test#{v[1]}"
    transport build --fast
    --dest="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-MT-R12 (NAVIGATE)/{s[0]}"
    """,
    "# CHECK RESULTING VERSION",
    """
    $ message-ix
    --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-MT-R12 (NAVIGATE)/{s[0]}#{v[2]}"
    solve
    """,
    """
    $ mix-models
    --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-MT-R12 (NAVIGATE)/{s[0]}#{v[2]}"
    buildings build-solve
    --dest="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BMT-R12 (NAVIGATE)/{s[0]}"
    --run-access --iterations=1 --scenario={s[0]}
    """,
    "# CHECK RESULTING VERSION",
    """
    $ mix-models
    --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BMT-R12 (NAVIGATE)/{s[0]}#{v[3]}"
    report -m projects.navigate "remove all ts data"
    """,
    """
    $ mix-models
    --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BMT-R12 (NAVIGATE)/{s[0]}#{v[3]}"
    report -m projects.navigate "navigate all"
    """,
    """
    $ mix-models
    --url="ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-BMT-R12 (NAVIGATE)/baseline#{v[3]}"
    report --legacy --config=navigate
    """,
    r"""
    $ mix-models navigate prep-submission
    $HOME/data/messageix/report/legacy/MESSAGEix-GLOBIOM\ 1.1-BMT-R12\ \(NAVIGATE\)_baseline.xlsx
    $HOME/vc/iiasa/navigate-workflow
    """,  # noqa: E501
)


@cli.command("gen-workflow")
@click.argument("versions", nargs=4)
@click.pass_obj
def gen_workflow(context, versions):
    """Print commands for the NAVIGATE workflow.

    VERSIONS are 4 version numbers for the scenarios used at different stages. Use
    values like A B C Das placeholders.
    """
    import re

    s = [context.navigate.scenario]

    whitespace = re.compile(r"\s\s+")

    for cmd in COMMANDS:
        print(whitespace.sub(" ", cmd).strip().format(v=versions, s=s), end="\n\n")


@cli.command("run", params=[_DSD])
@common_params("dry_run")
@click.option("--from", "truncate_step", help="Run workflow from this step.")
@click.option("--no-transport", is_flag=True, help="Omit MESSAGEix-Transport.")
@click.argument("target_step", metavar="TARGET")
@click.pass_obj
def run(context, dry_run, truncate_step, no_transport, dsd, target_step):
    """Run the NAVIGATE workflow up to step TARGET.

    --from is interpreted as a regular expression, and the workflow is truncated at
    every point matching this expression.
    """
    from . import workflow

    # Copy settings to the Config object
    context.navigate.dsd = dsd
    context.navigate.transport = not no_transport

    wf = workflow.generate(context)

    # TODO move the following upstream to message-ix-models as a utility function, since
    #      most packages/modules using the Workflow pattern will also likely need to
    #      define a CLI.

    # Truncate the workflow
    try:
        expr = re.compile(truncate_step.replace("\\", ""))
    except AttributeError:
        pass  # truncate_step is None
    else:
        for step in filter(expr.fullmatch, wf.keys()):
            log.info(f"Truncate workflow at {step!r}")
            wf.truncate(step)

    # Select 1 or more targets based on a regular expression in `target_step`
    target_expr = re.compile(target_step)
    target_steps = sorted(filter(lambda k: target_expr.fullmatch(k), wf.keys()))
    if len(target_steps):
        # Create a new target that collects the selected ones
        target_step = "cli-targets"
        wf.add(target_step, target_steps)
    else:
        raise click.ClickException(
            f"No step(s) matched {target_expr!r} among:\n{sorted(wf.keys())}"
        )

    log.info(f"Execute workflow:\n{wf.describe(target_step)}")

    if dry_run:
        path = context.get_local_path("navigate-workflow.svg")
        wf.visualize(str(path))
        log.info(f"Workflow diagram written to {path}")
        return

    wf.run(target_step)


@cli.command("check-budget")
@click.pass_obj
def check_budget(context):
    import numpy as np
    import pandas as pd
    from message_ix import Scenario

    from message_data.tools import interpolate_budget

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
    for s_name, t, c in (
        ("NPi-Default_ENGAGE_15C_step-3+B", 850, 1840),
        ("NPi-Default_ENGAGE_20C_step-3+B", 900, 2700),
        ("NPi-Default", np.nan, np.nan),
    ):
        try:
            s = Scenario(mp, model=m, scenario=s_name)
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
    print(f"{data =}")

    result = interpolate_budget(data, target, constraint)
    for key, value in result.items():
        if np.isnan(value):
            print(f"{key}: no result")
            continue
        print(
            f"{key}: set budget={value:.3f} (currently {constraint[key]}) average"
            f" Mt C-eq / y to achieve {target[key]} Gt CO₂ total"
        )
