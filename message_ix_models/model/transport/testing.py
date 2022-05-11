"""Utilities for testing :mod:`~message_data.model.transport`."""
import logging
from contextlib import nullcontext

import pytest
from message_ix import Reporter, Scenario
from message_ix_models import ScenarioInfo, testing

from message_data import reporting
from message_data.model import transport
from message_data.reporting.sim import add_simulated_solution
from message_data.tools import silence_log

log = logging.getLogger(__name__)

# Common marks for transport code
MARK = (
    pytest.mark.xfail(
        reason="Missing R14 input data/assumptions", raises=FileNotFoundError
    ),
)


def built_transport(request, context, options=dict(), solved=False) -> Scenario:
    """Analogous to :func:`.testing.bare_res`, with transport detail added."""
    options.setdefault("quiet", True)

    # Retrieve (maybe generate) the bare RES with the same settings
    res = testing.bare_res(request, context, solved)

    # Derive the name for the transport scenario
    model_name = res.model.replace("-GLOBIOM", "-Transport")

    try:
        scenario = Scenario(context.get_platform(), model_name, "baseline")
    except ValueError:
        log.info(f"Create '{model_name}/baseline' for testing")

        # Optionally silence logs for code used via build.main()
        log_cm = (
            silence_log(["genno", "message_data.model.transport", "message_ix_models"])
            if options["quiet"]
            else nullcontext()
        )

        with log_cm:
            scenario = res.clone(model=model_name)
            transport.build.main(context, scenario, options, fast=True)
    else:
        # Loaded existing Scenario; ensure config files are loaded on `context`
        transport.configure(context)

    if solved and not scenario.has_solution():
        log.info(f"Solve '{scenario.model}/{scenario.scenario}'")
        scenario.solve(solve_options=dict(lpmethod=4))

    log.info(f"Clone to '{model_name}/{request.node.name}'")
    return scenario.clone(scenario=request.node.name, keep_solution=solved)


def simulated_solution(request, context) -> Reporter:
    """Return a :class:`.Reporter` with a simulated model solution.

    The contents allow for fast testing of reporting code, without solving an actual
    :class:`.Scenario`.
    """
    from message_data.model.transport.report import callback, transport_technologies

    # Build the base model
    scenario = built_transport(request, context, solved=False)

    # Info about the built model
    info = ScenarioInfo(scenario)

    spec, technologies, t_info = transport_technologies(context)

    # Create a reporter
    rep = Reporter.from_scenario(scenario)

    # Add simulated solution data
    # TODO expand
    data = dict(
        ACT=dict(
            nl=info.N[-1],
            t=technologies,
            yv=2020,
            ya=2020,
            m="all",
            h="year",
            value=1.0,
        ),
        CAP=dict(
            nl=[info.N[-1]] * 2,
            t=["ELC_100", "ELC_100"],
            yv=[2020, 2020],
            ya=[2020, 2025],
            value=[1.0, 1.1],
        ),
    )
    add_simulated_solution(rep, info, data)

    # Register the callback to set up transport reporting
    reporting.register(callback)

    # Prepare the reporter
    with silence_log("genno", logging.CRITICAL):
        reporting.prepare_reporter(rep, dict())

    return rep
