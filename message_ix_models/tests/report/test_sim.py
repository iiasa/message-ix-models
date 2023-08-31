from message_ix import Reporter

from message_ix_models import ScenarioInfo
from message_ix_models.report.sim import add_simulated_solution


def test_add_simulated_solution():
    r = Reporter()

    add_simulated_solution(r, ScenarioInfo())
