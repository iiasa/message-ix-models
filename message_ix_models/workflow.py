"""Tools for modeling workflows."""
from typing import Callable, Optional, cast

from genno import Computer
from message_ix import Scenario

from message_ix_models.util.context import Context


class WorkflowStep:
    model_name: str
    scenario_name: str

    def __init__(self, name: str, callback: Callable):
        self.model_name, self.scenario_name = name.split("/")
        self.callback = callback

    def __call__(self, scenario: Optional[Scenario]) -> Scenario:
        if scenario is None:
            s = None
        else:
            s = scenario.clone(
                model=self.model_name, scenario=self.scenario_name, keep_solution=False
            )

        result = cast(Scenario, self.callback(s) or s)

        result.solve()

        return result

    def __repr__(self):
        return f"<Step: {repr(self.callback)}>"


class Workflow:
    comp: Computer

    def __init__(self, context: Context):
        self.reporting_only = context.get("run_reporting_only", False)
        self.comp = Computer()

    def add(self, name: str, base: Optional[str], callback: Callable):
        step = WorkflowStep(name, callback)
        self.comp.add_single(name, step, base, strict=True)
        print(self.comp.graph)  # DEBUG

    def run(self, scenarios: str):
        return self.comp.get(scenarios)
