"""Tools for modeling workflows."""
import logging
from typing import Callable, List, Literal, Mapping, Optional, Tuple, Union

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
        ``"model name/scenario name"`` for the |Scenario| produced by the step.
    action : CallbackType, *optional*
        Function to be executed to modify the base into the target Scenario.
    clone : bool, *optional*
        :obj:`True` to clone the base scenario the target.
    target : str, *optional*
        URL for the scenario produced by the workflow step. Parsed to
        :attr:`scenario_info` and :attr:`platform_info`.
    kwargs
        Keyword arguments for `action`.
    """

    #: Function to be executed on the subject scenario. If :obj:`None`, the target
    #: scenario is loaded via :meth:`Context.get_scenario`.
    action: Optional[CallbackType] = None

    #: :obj:`True` or a :class:`dict` with keyword arguments to clone before
    #: :attr:`action` is executed. Default: :obj:`False`, do not clone.
    clone: Union[bool, dict] = False

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
            if clone is not False:
                raise TypeError("target= must be supplied for clone=True")
            self.platform_info = dict()
            self.scenario_info = dict()

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
            # Modify the context to identify destination scenario; possibly nothing
            context.dest_scenario.update(self.scenario_info)
            s = scenario
            log.info(f"Step runs on ixmp://{s.platform.name}/{s.url}")

        if context.dest_scenario:
            log.info(f"  with context.dest_scenario={context.dest_scenario}")

        if self.clone is not False:
            # Clone to target model/scenario name
            log.info("Clone to {model}/{scenario}".format(**self.scenario_info))
            kw = self.scenario_info.copy()
            # If clone contains keyword arguments, e.g. shift_first_model_year, use them
            # NB user code should give clone = dict(keep_solution=True) if desired
            kw.update(
                self.clone
                if isinstance(self.clone, dict)
                else dict(keep_solution=False)
            )
            s = s.clone(**kw)

        if not self.action:
            return s

        log.info(f"Execute {self.action!r}")

        # Modify context to identify the target scenario
        context.set_scenario(s)

        try:
            # Invoke the callback
            result = self.action(context, s, **self.kwargs)
        except Exception:  # pragma: no cover
            s.platform.close_db()  # Avoid locking the scenario
            raise

        if result is None:
            log.info(f"…nothing returned, workflow will continue with {s.url}")
            result = s

        return result

    def __repr__(self):
        action = f"{self.action.__name__}()" if self.action else "load"
        dest = ""
        if self.scenario_info:
            dest = " -> {model}/{scenario}".format(**self.scenario_info)
        return f"<Step {action}{dest}>"


class Workflow(Computer):
    """Workflow for operations on multiple :class:`Scenarios <message_ix.Scenario>`.

    Parameters
    ----------
    context : Context
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
    ) -> str:
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
        str
            The same as `name`.

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

        # Add to the Computer; return the name of the added step
        return str(self.add_single(name, step, "context", base, strict=True))

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
        the target |Scenario| that would be produced by the original step.

        Raises
        ------
        KeyError
            if step `name` does not exist.
        """
        # Generate a new step that merely loads the scenario identified by `name` or its
        # base
        step = WorkflowStep(None)
        step.scenario_info.update(self.guess_target(name, "scenario")[0])
        try:
            step.platform_info.update(self.guess_target(name, "platform")[0])
        except KeyError as e:
            if e.args[0] is None:
                raise RuntimeError(
                    f"Unable to locate platform info for {step.scenario_info}"
                )
            else:  # pragma: no cover
                raise  # Something else

        # Replace the existing step
        self.add_single(name, step, "context", None)

    def guess_target(
        self, step_name: str, kind: Literal["platform", "scenario"] = "scenario"
    ) -> Tuple[Mapping, str]:
        """Traverse the graph looking for non-empty platform_info/scenario_info.

        Returns the info, and the step name containing it. Usually, this will identify
        the name of the platform, model, and/or scenario that is received and acted upon
        by `step_name`. This may not be the case if preceding workflow steps perform
        clone steps that are not recorded in the `target` parameter to
        :class:`WorkflowStep`.

        Parameters
        ----------
        step_name : str
           Initial step from which to work backwards.
        kind : str, "platform" or "scenario"
           Whether to look up :attr:`~WorkflowStep.platform_info` or
           :attr:`~WorkflowStep.scenario_info`.
        """
        task = self.graph[step_name]
        i = getattr(task[0], f"{kind}_info")
        return (i.copy(), step_name) if len(i) else self.guess_target(task[2], kind)


def solve(context, scenario, **kwargs):
    scenario.solve(**kwargs)
    return scenario
