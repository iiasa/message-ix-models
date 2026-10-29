"""Command-line tools for EFC overshoot scenarios."""

import logging
import re

import click

from message_ix_models.util.click import common_params

log = logging.getLogger(__name__)


@click.group("efc-overshoot")
def cli():
    """EFC overshoot (peak-and-decline) scenarios."""


@cli.command("run")
@common_params("dry_run")
@click.option("--from", "truncate_step", help="Run workflow from this step.")
@click.argument("target_step", metavar="TARGET")
@click.pass_obj
def run(context, truncate_step, target_step):
    """Run the overshoot workflow up to step TARGET.

    --from and TARGET are regular expressions; several matching targets run together.
    """
    from . import workflow

    wf = workflow.generate(context)

    if truncate_step:
        expr = re.compile(truncate_step)
        for step in filter(expr.fullmatch, wf.keys()):
            log.info(f"Truncate workflow at {step!r}")
            wf.truncate(step)

    target_expr = re.compile(target_step)
    target_steps = sorted(filter(target_expr.fullmatch, wf.keys()))
    if not target_steps:
        raise click.BadParameter(f"No step matches {target_step!r}")
    if len(target_steps) > 1:
        target_step = "cli-targets"
        wf.add(target_step, target_steps)
    else:
        target_step = target_steps[0]

    log.info(f"Execute workflow:\n{wf.describe(target_step)}")

    if context.dry_run:
        return

    wf.run(target_step)
