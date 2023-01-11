"""Command-line tools specific to the NAVIGATE project."""
import logging
import re
from pathlib import Path

import click
from message_ix_models.util.click import common_params, store_context

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
    callback=store_context,
    help="NAVIGATE T3.5 scenario ID.",
)


@click.group("navigate", params=[_SCENARIO])
@click.pass_obj
def cli(context, navigate_scenario):
    """NAVIGATE project."""


@cli.command("prep-submission", params=[_DSD])
@click.argument("wf_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.pass_obj
def prep_submission(context, wf_dir, dsd):
    """Prepare data for NAVIGATE submission.

    WF_DIR is the base path of the NAVIGATE workflow repository.
    """
    from message_data.projects.navigate.report import gen_config
    from message_data.tools.prep_submission import main

    context.navigate_dsd = dsd
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

    s = [context.navigate_scenario]

    whitespace = re.compile(r"\s\s+")

    for cmd in COMMANDS:
        print(whitespace.sub(" ", cmd).strip().format(v=versions, s=s), end="\n\n")


@cli.command("run", params=[_DSD])
@common_params("dry_run")
@click.option("--from", "truncate_step", help="Run workflow from this step.")
@click.argument("target_step", metavar="TARGET")
@click.pass_obj
def run(context, dry_run, truncate_step, dsd, target_step):
    """Run the NAVIGATE workflow up to step TARGET.

    --from is interpreted as a regular expression, and the workflow is truncated at
    every point matching this expression.
    """
    from . import workflow

    context.navigate_dsd = dsd

    wf = workflow.generate(context)

    # Truncate the workflow
    try:
        expr = re.compile(truncate_step.replace("\\", ""))
    except AttributeError:
        pass  # truncate_step is None
    else:
        for step in filter(expr.search, wf.keys()):
            log.info(f"Truncate workflow at {step!r}")
            wf.truncate(step)

    log.info(f"Execute workflow:\n{wf.describe(target_step)}")

    if dry_run:
        path = context.get_local_path("navigate-workflow.svg")
        wf.visualize(str(path))
        log.info(f"Workflow diagram written to {path}")
        return

    wf.run(target_step)
