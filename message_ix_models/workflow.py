"""Tools for modeling workflows."""
import logging
from typing import Callable, List, Optional, Union

from genno import Computer
from ixmp.utils import parse_url
from message_ix import Scenario

from message_ix_models.util.context import Context

log = logging.getLogger(__name__)

# commented: this conflicts with option keyword arguments to workflow step functions
# CallbackType = Callable[[Context, Scenario], Scenario]
CallbackType = Callable


class WorkflowStep:
    """Single step in a multi-scenario workflow.

    Nothing occurs when the WorkflowStep is instantiated.

    Parameters
    ----------
    name : str
        ``"model name/scenario name"`` for the :class:`.Scenario` produced by the step.
    action : CallbackType, optional
        Function to be executed to modify the base into the target Scenario.
    clone : bool, optional
        :obj:`True` to clone the base scenario the target.
    target : str, optional
        URL for the scenario produced by the workflow step. Parsed to
        :attr:`scenario_info` and :attr:`platform_info`.
    kwargs
        Keyword arguments for `action`.
    """

    #: Function to be executed on the subject scenario. If :obj:`None`, the target
    #: scenario is loaded via :meth:`Context.get_scenario`.
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
                    f"Step with action {self.action!r} requires a base scenario"
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
            log.info(f"Step runs on ixmp://{s.platform.name}/{s.url}")
            log.info(f"  with context.dest_scenario={context.dest_scenario}")

        if self.clone:
            # Clone to target model/scenario name
            log.info("Clone to {model}/{scenario}".format(**self.scenario_info))
            s = s.clone(**self.scenario_info, keep_solution=False)

        if not self.action:
            return s

        log.info(f"Execute {self.action!r}")
        try:
            # Invoke the callback
            result = self.action(context, s, **self.kwargs)
        except Exception:  # pragma: no cover
            s.platform.close_db()  # Avoid locking the scenario
            raise

        if result is None:
            log.info(f"…nothing returned, continue with {s.url}")
            result = s

        return result

    def __repr__(self):
        action = f"{self.action.__name__}()" if self.action else "load"
        dest = ""
        if self.scenario_info:
            dest = " -> {model}/{scenario}".format(**self.scenario_info)
        return f"<Step {action}{dest}>"


class Workflow(Computer):
    """Workflow for operations on multiple :class:`Scenarios <.Scenario>`.

    Parameters
    ----------
    context : .Context
        Context object with settings common to the entire workflow.
    """

    def __init__(self, context: Context):
        super().__init__()
        self.add_single("context", context)

    def add_step(
        self,
        name: str,
        base: Optional[str] = None,
        action: Optional[CallbackType] = None,
        replace=False,
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
        replace : bool
            :data:`True` to replace an existing step.
        kwargs
            Keyword arguments for `action`; passed to and stored on the
            :class:`WorkflowStep` until used.

        Returns
        -------
        WorkflowStep
            a reference to the added step, for optional further modification.

        Raises
        ------
        genno.KeyExistsError
            if the step `name` already exists. Use `replace` to force overwriting an
            existing step.
        """
        # Create the workflow step
        step = WorkflowStep(action, **kwargs)

        if replace:
            # Remove any existing step
            self.graph.pop(name, None)

        # Add to the Computer
        self.add_single(name, step, "context", base, strict=True)

        return step

    def run(self, name_or_names: Union[str, List[str]]):
        """Run all workflow steps necessary to produce `name_or_names`.

        Parameters
        ----------
        name_or_names: str or list of str
            Identifier(s) of steps to run.
        """
        return self.get(name_or_names)

    def truncate(self, name: str):
        """Truncate the workflow at the step `name`.

        The step `name` is replaced with a new :class:`WorkflowStep` that simply loads
        the target :class:`Scenario` that would be produced by the original step.

        Raises
        ------
        KeyError
            if step `name` does not exist.
        """

        def _recurse_info(kind: str, step_name: str):
            """Traverse the graph looking for non-empty platform_info/scenario_info."""
            task = self.graph[step_name]
            return getattr(task[0], f"{kind}_info") or _recurse_info(kind, task[2])

        # Generate a new step that merely loads the scenario identified by `name` or
        # its base
        step = WorkflowStep(None)
        step.scenario_info = _recurse_info("scenario", name)
        try:
            step.platform_info = _recurse_info("platform", name)
        except KeyError as e:
            if e.args[0] is None:
                raise RuntimeError(
                    f"Unable to locate platform info for {step.scenario_info}"
                )
            else:  # pragma: no cover
                raise  # Something else

        # Replace the existing step
        self.add_single(name, step, "context", None)


def solve(context, scenario, **kwargs):
    scenario.solve(**kwargs)
    return scenario
