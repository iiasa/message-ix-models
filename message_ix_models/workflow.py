"""Tools for modeling workflows."""
from typing import Callable, List, Optional, Union, cast

from genno import Computer
from message_ix import Scenario

from message_ix_models.util.context import Context


class WorkflowStep:
    """Single step in a multi-scenario workflow.

    Parameters
    ----------
    name : str
        ``"model name/scenario name"`` for the :class:`.Scenario` produced by the step.
    callback : Callable
        Function to be executed to modify the base into the target Scenario.
    solve : bool, optional
        If :obj:`True`, the created scenario is solved when it is created.
    report: bool, optional
        If :obj:`True`, the created scenario is reported after it is created and maybe
        (according to `solve`) solved.
    """

    model_name: str
    scenario_name: str
    solve: bool = True
    report: bool = False

    def __init__(self, name: str, callback: Callable, solve=True):
        # Unpack the target model/scenario name
        self.model_name, self.scenario_name = name.split("/")

        # Store the callback and options
        self.callback = callback
        self.solve = solve

    def __call__(self, scenario: Optional[Scenario]) -> Scenario:
        """Execute the workflow step."""
        if scenario is None:
            s = None  # No precursor scenario
        else:
            # Clone to target model/scenario name
            s = scenario.clone(
                model=self.model_name, scenario=self.scenario_name, keep_solution=False
            )

        # Invoke the callback. If it does not return, assume `s` contains the
        # modifications
        result = cast(Scenario, self.callback(s) or s)

        if self.solve:
            result.solve()

        if self.report:  # pragma: no cover
            raise NotImplementedError  # TODO

        return result

    def __repr__(self):
        return f"<Step {repr(self.callback)}>"


class Workflow:
    """Workflow containing multiple :class:`Scenarios <.Scenario>`.

    Parameters
    ----------
    context : .Context
        Context object with settings common to the entire workflow.
    solve : bool, optional
        Passed to every :class:`.WorkflowStep` created using :meth:`.add`.
    """

    _computer: Computer

    def __init__(self, context: Context, solve=True):
        # NB has no effect; only an example of how Context settings can control the
        #    workflow
        self.reporting_only = context.get("run_reporting_only", False)
        self.solve = solve

        self._computer = Computer()

    def add(self, name: str, base: Optional[str], callback: Callable):
        """Add a step to the workflow.

        Parameters
        ----------
        name : str
            ``"model name/scenario name"`` for the :class:`.Scenario` produced by the
            step.
        base : str or None
            Base scenario, if any.
        callback : Callable
            Function to be executed to modify the base into the target Scenario.
        """
        # Create the workflow step
        step = WorkflowStep(name, callback, solve=self.solve)

        # Add to the Computer
        self._computer.add_single(name, step, base, strict=True)

        print(self._computer.graph)  # DEBUG

    def run(self, scenarios: Union[str, List[str]]):
        """Run the workflow to generate one or more scenarios.

        Parameters
        ----------
        scenarios: str or list of str
            Identifier(s) of scenario(s) to generate.
        """
        return self._computer.get(scenarios)
