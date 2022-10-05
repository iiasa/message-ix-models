"""Tools for modeling workflows."""
import logging
from typing import Callable, List, Optional, Union

from genno import Computer
from ixmp.utils import parse_url
from message_ix import Scenario

from message_ix_models.util.context import Context

log = logging.getLogger(__name__)

CallbackType = Callable[[Context, Scenario], Scenario]


class WorkflowStep:
    """Single step in a multi-scenario workflow.

    Nothing occurs when the WorkflowStep is instantiated. When called:

    1. A base scenario is obtained:

      - if the `scenario` argument is provided, this is used directly.
      - if the `scenario` argument and :attr:`action` are both :data:`None`: the
        scenario identified by :attr:`platform_info`, :attr:`scenario_info` is loaded.

    2. If attr:`clone` is :obj:`True`, this scenario is then cloned with
       `keep_solution=False`.

    3. The :attr:`action`, if any, is called on the scenario. This function may return
       the base scenario (equivalently :data:`None`) or a different scenario.

    The step returns the resulting scenario.

    Parameters
    ----------
    name : str
        ``"model name/scenario name"`` for the :class:`.Scenario` produced by the step.
    action : CallbackType, optional
        Function to be executed to modify the base into the target Scenario.
    target : str, optional
        URL for the scenario produced by the workflow step.
    clone : bool, optional
        :obj:`True` to clone the base scenario the target.
    kwargs
        Keyword arguments for `action`.
    """

    #: Function to be executed on the subject scenario.
    action: Optional[CallbackType] = None

    #: :obj:`True` to clone before :attr:`action` is executed.
    clone: bool = False

    #: Keyword arguments passed to :attr:`action`.
    kwargs: dict

    #: Target platform name and additional options.
    platform_info: dict

    #: Target model name, scenario name, and optional version.
    scenario_info: dict

    def __init__(
        self, action: Optional[CallbackType], target=None, clone=False, **kwargs
    ):
        try:
            # Store platform and scenario info by parsing the `target` URL
            self.platform_info, self.scenario_info = parse_url(target)
        except (AttributeError, ValueError):
            if clone:
                raise TypeError("target= must be supplied for clone=True")
            self.platform_info = self.scenario_info = dict()

        # Store the callback and options
        self.action = action
        self.clone = clone
        self.kwargs = kwargs

    def __call__(
        self, context: Context, scenario: Optional[Scenario] = None
    ) -> Scenario:
        """Execute the workflow step."""
        if scenario is None:
            # No base scenario
            if self.action:
                raise RuntimeError(
                    "Workflow step with action {self.action!r} requires a base scenario"
                )
            # Use Context to retrieve the identified scenario
            context.platform_info.update(self.platform_info)
            context.scenario_info.update(self.scenario_info)
            s = context.get_scenario()
            log.info(f"Loaded ixmp://{s.platform.name}/{s.url}")
        else:
            # Modify the context to identify source and destination scenarios
            context.set_scenario(scenario)
            context.dest_scenario.update(self.scenario_info)
            s = scenario

        if self.clone:
            # Clone to target model/scenario name
            log.info(f"Clone to {repr(self.scenario_info)}")
            s = s.clone(**self.scenario_info, keep_solution=False)

        # Invoke the callback. If it does not return, assume `s` contains the
        # modifications
        if self.action:
            log.info("Execute {self.action!r}")
            result = self.action(context, s, **self.kwargs)
            if result is None:
                log.info("…nothing returned, continue with {s.url}")
                result = s
        else:
            result = s

        return result

    def __repr__(self):
        action = f"{self.action.__name__}()" if self.action else "load"
        dest = ""
        if self.scenario_info:
            dest = " -> {model}/{scenario}".format(**self.scenario_info)
        return f"<Step {action}{dest}>"


class Workflow:
    """Workflow for operations on multiple :class:`Scenarios <.Scenario>`.

    Parameters
    ----------
    context : .Context
        Context object with settings common to the entire workflow.
    """

    _computer: Computer

    def __init__(self, context: Context):
        self._computer = Computer()
        self._computer.add("context", context)

    def add_step(
        self,
        name: str,
        base: Optional[str] = None,
        action: Optional[CallbackType] = None,
        **kwargs,
    ) -> WorkflowStep:
        """Add a :class:`WorkflowStep` to the workflow.

        Parameters
        ----------
        name : str
            Name for the new step.
        base : str or None
            Previous step that produces the a pre-requisite scenario for this step.
        action : CallbackType
            Function to be executed to modify the base into the target Scenario.
        kwargs
            Keyword arguments for `action`; passed to and stored on the
            :class:`WorkflowStep` until used.

        Returns
        -------
        WorkflowStep
            a reference to the added step, for optional further modification.
        """
        # Create the workflow step
        step = WorkflowStep(action, **kwargs)

        # Add to the Computer
        self._computer.add_single(name, step, "context", base, strict=True)

        return step

    def run(self, name_or_names: Union[str, List[str]]):
        """Run all workflow steps necessary to produce `name_or_names`.

        Parameters
        ----------
        name_or_names: str or list of str
            Identifier(s) of steps to run.
        """
        return self._computer.get(name_or_names)

    def truncate(self, name: str):
        """Truncate the workflow at the step `name`.

        The step `name` is replaced with a new :class:`WorkflowStep` that simply loads
        the target :class:`Scenario` that would be produced by the original step.

        Raises
        ------
        KeyError
            if step `name` does not exist.
        """
        # Retrieve the existing step
        existing = self._computer.graph[name][0]

        # Generate a new step that merely loads the scenario identified by `existing`
        step = WorkflowStep(None)
        step.platform_info = existing.platform_info
        step.scenario_info = existing.scenario_info

        # Replace the existing step
        self._computer.add_single(name, step, "context", None)
