import logging

import pytest
from message_ix import Reporter, Scenario
from message_ix_models import ScenarioInfo, testing

from message_data import reporting
from message_data.model import transport
from message_data.reporting.sim import add_simulated_solution


log = logging.getLogger(__name__)

# Common marks for transport code
MARK = (
    pytest.mark.xfail(
        reason="Missing R14 input data/assumptions", raises=FileNotFoundError
    ),
)


def built_transport(request, context, options=dict(), solved=False) -> Scenario:
    """Analogous to :func:`.testing.bare_res`, with transport detail added."""
    # Retrieve (maybe generate) the bare RES with the same settings
    res = testing.bare_res(request, context, solved)

    # Derive the name for the transport scenario
    model_name = res.model.replace("-GLOBIOM", "-Transport")

    try:
        scenario = Scenario(context.get_platform(), model_name, "baseline")
    except ValueError:
        log.info(f"Create '{model_name}/baseline' for testing")
        scenario = res.clone(model=model_name)
        transport.build.main(context, scenario, options, fast=True, quiet=False)

    if solved and not scenario.has_solution():
        log.info(f"Solve '{scenario.model}/{scenario.scenario}'")
        scenario.solve(solve_options=dict(lpmethod=4))

    log.info(f"Clone to '{model_name}/{request.node.name}'")
    return scenario.clone(scenario=request.node.name, keep_solution=solved)


def simulated_solution(request, context) -> Reporter:
    # Build the base model
    scenario = built_transport(request, context, solved=False)

    # Info about the built model
    info = ScenarioInfo(scenario)

    # Create a reporter
    rep = Reporter.from_scenario(scenario)

    # Add simulated solution data
    # TODO expand
    data = dict(
        CAP=dict(
            nl=[info.N[0]] * 2,
            t=["ELC_100", "ELC_100"],
            yv=[2020, 2020],
            ya=[2020, 2025],
            value=[1.0, 1.1],
        )
    )
    add_simulated_solution(rep, info, data)

    # Register the callback to set up transport reporting
    reporting.register(transport.report.callback)

    # Prepare the reporter
    reporting.prepare_reporter(rep, dict())

    return rep
