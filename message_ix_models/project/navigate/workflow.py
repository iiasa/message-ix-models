import logging
from pathlib import Path

from message_ix import Scenario
from message_ix_models import Context
from message_ix_models.util import identify_nodes
from message_ix_models.workflow import Workflow

from .report import gen_config

log = logging.getLogger(__name__)


def base_scenario() -> Scenario:
    s, mp = Scenario.from_url(
        "ixmp://ixmp-dev/MESSAGEix-GLOBIOM 1.1-R12/baseline_DEFAULT#7"
    )
    return s


def build_materials(s: Scenario) -> Scenario:
    raise NotImplementedError("Requires code on the materials-R12-rebase branch.")


def build_transport(s: Scenario) -> Scenario:
    """Workflow steps 3–4."""
    from message_data.model.transport import build

    # Configure transport build
    # TODO adjust per upstream changes
    context = Context(regions=identify_nodes(s), dest_scenario={"…"})
    context.set_scenario(s)

    build.main(context, scenario=s, fast=True)


def build_buildings(s: Scenario) -> Scenario:
    raise NotImplementedError


def report(s: Scenario) -> Scenario:
    """Workflow steps 8–10."""
    from message_data.reporting import (
        _invoke_legacy_reporting,
        log_before,
        prepare_reporter,
        register,
    )
    from message_data.tools import prep_submission

    context = Context(regions=identify_nodes(s))
    context.set_scenario(s)

    register("projects.navigate")
    rep, _ = prepare_reporter(context)

    # Step 8
    key = "remove all ts data"
    log_before(context, rep, key)
    rep.get(key)

    key = "navigate bmt"
    log_before(context, rep, key)
    rep.get(key)

    # Display information about the result
    log.info(
        f"File output(s), if any, written under:\n{rep.graph['config']['output_path']}"
    )

    # Step 9
    _invoke_legacy_reporting(context)

    # Step 10
    f1 = Path("~/data/messageix/report/legacy/{s.url}.xlsx").expanduser()
    f2 = Path("~/vc/iiasa/navigate-workflow")
    config = gen_config(context, f1, f2)
    prep_submission.main(config)


def generate(context):
    wf = Workflow(context, solve=False)

    # Step 1
    wf.add("Base/base", None, base_scenario)

    # Step 2
    s2 = "MESSAGEix-Materials/baseline_DEFAULT_NAVIGATE_test"
    wf.add(s2, "Base/base", build_materials)

    # Steps 3 & 4
    s3 = "MESSAGEix-GLOBIOM 1.1-MT-R12 (NAVIGATE)/baseline"
    wf.solve = True  # FIXME don't use a class variable like this; pass through add()
    wf.add(s3, s2, build_transport)
    wf.solve = False

    # Steps 5–7
    s4 = "MESSAGEix-GLOBIOM 1.1-BMT-R12 (NAVIGATE)/baseline"
    wf.add(s4, s3, build_buildings)

    # Steps 8–10
    wf.add("FINAL", s4, report)

    return wf
