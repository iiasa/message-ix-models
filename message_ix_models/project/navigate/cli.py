"""Command-line tools specific to the NAVIGATE project."""
import logging
from pathlib import Path

import click
from message_ix_models.util.click import common_params, store_context

log = logging.getLogger(__name__)


#: Codes for NAVIGATE T3.5 scenarios. All but "baseline" are abbreviated by removing
#: "NAV_Dem-".
SCENARIOS = [
    "baseline",
    "NPi-ref",
    "NPi-act",
    "NPi-tec",
    "NPi-ele",
    "NPi-all",
]

scenario_option = click.Option(
    ["-s", "--scenario", "navigate_scenario"],
    default="baseline",
    type=click.Choice(SCENARIOS),
    callback=store_context,
    help="NAVIGATE T3.5 scenario ID.",
)


@click.group("navigate", params=[scenario_option])
@click.pass_obj
def cli(context, navigate_scenario):
    """NAVIGATE project."""


@cli.command("prep-submission")
@click.argument("f1", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument("f2", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.pass_obj
def prep_submission(context, f1, f2):
    """Prepare data for NAVIGATE submission.

    F1 is the path to a reporting output file in .xlsx format.
    F2 is the base path of the NAVIGATE workflow repository.
    """
    from message_data.projects.navigate.report import gen_config
    from message_data.tools.prep_submission import main

    # Fixed values
    context.regions = "R12"

    # Generate a prep_submission.Config object
    config = gen_config(context, f1, f2)
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

    s = [context.navigate_scenario]

    whitespace = re.compile(r"\s\s+")

    for cmd in COMMANDS:
        print(whitespace.sub(" ", cmd).strip().format(v=versions, s=s), end="\n\n")


@cli.command("run")
@common_params("dry_run")
@click.option("--from", "truncate_step", help="Run workflow from this step.")
@click.argument("target_step", metavar="TARGET")
@click.pass_obj
def run(context, dry_run, truncate_step, target_step):
    """Run the NAVIGATE workflow up to step TARGET."""
    from . import workflow

    wf = workflow.generate(context)

    try:
        wf.truncate(truncate_step)
    except KeyError:
        if truncate_step:
            raise

    log.info(f"Execute workflow:\n{wf._computer.describe(target_step)}")

    if dry_run:
        return

    wf.run(target_step)
