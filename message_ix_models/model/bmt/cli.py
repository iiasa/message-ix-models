"""Command-line tools specific to MESSAGEix-BMT runs."""

import logging
import re
import click

from message_ix_models.util.click import common_params

log = logging.getLogger(__name__)

# Define a command-line option for BMT-run scenarios
_SCENARIO = click.Option(
    ["--scenario", "bmt_scenario"],
    default="baseline",
    help="The scenario ID of the specific step in the bmt workflow.",
)


# Define a Click command group for BMT-related commands
@click.group("bmt", params=[_SCENARIO])
@click.pass_obj
def cli(context, bmt_scenario):
    """MESSAGEix-BMT runs."""
    # Store the scenario in the context for use in commands
    context.bmt = bmt_scenario
    pass


# Define run command
@cli.command("run")
@common_params("dry_run")
@click.option("--from", "truncate_step", help="Run workflow from this step.")
@click.argument("target_step", metavar="TARGET")
@click.pass_obj
def run(context, truncate_step, target_step):
    """Run the BMT workflow up to step TARGET.

    --from is interpreted as a regular expression, and the workflow is truncated at
    every point matching this expression.
    """
    from . import workflow

    wf = workflow.generate(context)

    # Compile the regular expression for truncating the workflow
    try:
        expr = re.compile(truncate_step.replace("\\", ""))
    except AttributeError:
        pass  # truncate_step is None
    else:
        # Truncate the workflow at steps matching the expression
        for step in filter(expr.fullmatch, wf.keys()):
            log.info(f"Truncate workflow at {step!r}")
            wf.truncate(step)

    # Compile the regular expression for the target step
    target_expr = re.compile(target_step)
    target_steps = sorted(filter(lambda k: target_expr.fullmatch(k), wf.keys()))
    if len(target_steps) > 1:
        # If multiple target steps match, create a composite target
        target_step = "cli-targets"
        wf.add(target_step, target_steps)

    log.info(f"Execute workflow:\n{wf.describe(target_step)}")

    # If dry_run is enabled, visualize the workflow instead of executing it
    if context.dry_run:
        path = context.get_local_path("bmt-workflow.svg")
        wf.visualize(str(path))
        log.info(f"Workflow diagram written to {path}")
        return

    # Run the workflow up to the specified target step
    wf.run(target_step)
