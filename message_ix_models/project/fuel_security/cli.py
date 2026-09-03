"""Command-line tools specific to fuel security scenarios."""                                                                                   

import logging
import re

import click                                                                                                                                    

from message_ix_models.util.click import common_params

log = logging.getLogger(__name__)


@click.group("fuel-security")
@click.pass_obj
def cli(context):
  """MESSAGEix fuel security scenarios."""
  pass


@cli.command("run")                                                                                                                             
@common_params("dry_run")
@click.option("--from", "truncate_step", help="Run workflow from this step.")
@click.argument("target_step", metavar="TARGET")
@click.pass_obj
def run(context, truncate_step, target_step):
  """Run the fuel security workflow up to step TARGET.

  --from is interpreted as a regular expression, and the workflow is truncated at                                                             
  every point matching this expression.
  """
  from . import workflow

  wf = workflow.generate(context)  # <-- matches renamed function                                                                             

  try:
      expr = re.compile(truncate_step.replace("\\", ""))
  except AttributeError:
      pass
  else:
      for step in filter(expr.fullmatch, wf.keys()):
          log.info(f"Truncate workflow at {step!r}")                                                                                          
          wf.truncate(step)

  target_expr = re.compile(target_step)
  target_steps = sorted(filter(lambda k: target_expr.fullmatch(k), wf.keys()))
  if len(target_steps) > 1:
      target_step = "cli-targets"                                                                                                             
      wf.add(target_step, target_steps)

  log.info(f"Execute workflow:\n{wf.describe(target_step)}")

  if context.dry_run:                                                                                                                         
      path = context.get_local_path("fuel_security_workflow.svg")
      wf.visualize(str(path), rankdir="LR")
      log.info(f"Workflow diagram written to {path}")
      return

  wf.run(target_step)
