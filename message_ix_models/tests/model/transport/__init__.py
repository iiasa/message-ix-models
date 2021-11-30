import logging

from message_ix import Scenario
from message_ix_models import testing

from message_data.model.transport import build

log = logging.getLogger(__name__)


def built_transport(request, context, options=dict(), solved=False):
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
        build.main(context, scenario, options, fast=True, quiet=False)

    if solved and not scenario.has_solution():
        log.info(f"Solve '{scenario.model}/{scenario.scenario}'")
        scenario.solve(solve_options=dict(lpmethod=4))

    log.info(f"Clone to '{model_name}/{request.node.name}'")
    return scenario.clone(scenario=request.node.name, keep_solution=solved)
