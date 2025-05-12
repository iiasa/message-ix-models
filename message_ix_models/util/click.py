"""Command-line utilities.

These are used for building CLIs using :mod:`click`.
"""

import logging
import sys
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from importlib.metadata import version
from pathlib import Path
from typing import Literal, Optional, Union, cast

import click
import click.testing
from click import Argument, Choice, Option

from message_ix_models import Context, model
from message_ix_models.model.structure import codelists

from ._logging import preserve_log_level
from .scenarioinfo import ScenarioInfo

log = logging.getLogger(__name__)


def common_params(param_names: str):
    """Decorate a click.command with common parameters `param_names`.

    `param_names` must be a space-separated string of names appearing in :data:`PARAMS`,
    for instance :py:`"ssp force output_model"`. The decorated function receives keyword
    arguments with these names; some are also stored on the

    Example
    -------

    >>> @click.command
    ... @common_params("ssp force output_model")
    ... @click.pass_obj
    ... def mycmd(context, ssp, force, output_model):
    ...     assert context.force == force
    """

    # Create the decorator
    # Simplified from click.decorators._param_memo
    def decorator(f):
        # - Ensure f.__click_params__ exists
        # - Append each param given in `param_names`
        f.__dict__.setdefault("__click_params__", []).extend(
            PARAMS[name] for name in reversed(param_names.split())
        )

        return f

    return decorator


def default_path_cb(*default_parts):
    """Return a callback function for click.Option handling.

    If no option value is given, the callback uses :meth:`.Context.get_local_path` and
    `default_parts` to provide a path that is relative to local data directory, e.g.
    the current working directory (see :doc:`/data`).
    """

    def _callback(context, param, value):
        value = value or context.obj.get_local_path(*default_parts)
        setattr(context.obj, param.name, value)
        return value

    return _callback


def exec_cb(expression: str) -> Callable:
    """Return a callback that :func:`exec`-utes an `expression`.

    The `expression` is executed in a limited context that has only two names available:

    - :py:`context`: the :class:`.Context` instance.
    - :py:`value`: the value passed to the :mod:`click.Parameter`.

    Example
    -------
    >>> @click.command
    ... @click.option(
    ...     "--myopt", callback=exec_cb("context.my_mod.my_opt = value + 3")
    ... )
    ... def cmd(...):
    ...     ...
    """

    def _cb(context: Union[click.Context, Context], param, value):
        ctx = context.obj if isinstance(context, click.Context) else context
        exec(expression, {}, {"context": ctx, "value": value})
        return value

    return _cb


def format_sys_argv() -> str:
    """Format :data:`sys.argv` in a readable manner."""
    lines = ["Invoked:"]
    indent = ""
    for item in sys.argv:
        lines.append(f"{indent}{item} \\")
        indent = "  "

    return "\n".join(lines)[:-2]


def scenario_param(
    param_decls: Union[str, list[str]],
    *,
    values: Optional[list[str]] = None,
    default: Optional[str] = None,
):
    """Add an SSP or scenario option or argument to a :class:`click.Command`.

    The parameter uses :func:`.store_context` to store the given value (if any) on
    the :class:`.Context`.

    Parameters
    ----------
    param_decls :
        :py:`"--ssp"` (or any other name prefixed by ``--``) to generate a
        :class:`click.Option`; :py:`"ssp"` to generate a :class:`click.Argument`.
        Click-style declarations are also supported; see below.
    values :
        Allowable values. If not given, the allowable values are
        ["LED", "SSP1", "SSP2", "SSP3", "SSP4", "SSP5"].
    default :
        Default value.

    Raises
    ------
    ValueError
        if `default` is given with `param_decls` that indicate a
        :class:`click.Argument`.

    Examples
    --------
    Add a (mandatory, positional) :class:`click.Argument`. This is nearly the same as
    using :py:`common_params("ssp")`, except the decorated function does not receive an
    :py:`ssp` argument. The value is still stored on :py:`context` automatically.

    >>> @click.command
    ... @scenario_param("ssp")
    ... @click.pass_obj
    ... def mycmd(context):
    ...     print(context.ssp)

    Add a :class:`click.Option` with certain, limited values and a default:

    >>> @click.command
    ... @scenario_param("--ssp", values=["SSP1", "SSP2", "SSP3"], default="SSP3")
    ... @click.pass_obj
    ... def mycmd(context):
    ...     print(context.ssp)

    An option given by the user as :command:`--scenario` but stored as
    :py:`Context.ssp`:

    >>> @click.command
    ... @scenario_param(["--scenario", "ssp"])
    ... @click.pass_obj
    ... def mycmd(context):
    ...     print(context.ssp)
    """
    if values is None:
        values = ["LED", "SSP1", "SSP2", "SSP3", "SSP4", "SSP5"]

    # Handle param_decls; identify the first string element
    if isinstance(param_decls, list):
        decl0 = param_decls[0]
    else:
        decl0 = param_decls
        param_decls = [param_decls]  # Ensure list for use by click

    # Choose either click.Option or click.Argument
    if decl0.startswith("-"):
        cls: type = Option
    else:
        cls = Argument
        if default is not None:
            raise ValueError(f"{default=} given for {cls}")

    # Create the decorator
    def decorator(f):
        # - Ensure f.__click_params__ exists
        # - Generate and append the parameter
        f.__dict__.setdefault("__click_params__", []).append(
            cls(
                param_decls,
                callback=store_context,
                type=Choice(values),
                default=default,
                expose_value=False,
            )
        )

        return f

    return decorator


def store_context(context: Union[click.Context, Context], param, value):
    """Callback that simply stores a value on the :class:`.Context` object.

    Use this for parameters that are not used directly in a @click.command() function,
    but need to be carried by the Context for later use.
    """
    setattr(
        context.obj if isinstance(context, click.Context) else context,
        param.name,
        value,
    )
    return value


@contextmanager
def temporary_command(group: "click.Group", command: "click.Command"):
    """Temporarily attach command `command` to `group`."""
    assert command.name is not None
    try:
        group.add_command(command)
        yield
    finally:
        group.commands.pop(command.name)


def urls_from_file(
    context: Union[click.Context, Context], param, value
) -> list[ScenarioInfo]:
    """Callback to parse scenario URLs from `value`."""
    si: list[ScenarioInfo] = []

    if value is None:
        return si

    with click.open_file(value) as f:
        for line in f:
            si.append(ScenarioInfo.from_url(url=line))

    # Store on context
    mm_context = context.obj if isinstance(context, click.Context) else context
    mm_context.core.scenarios = si

    return si


def unique_id() -> str:
    """Return a unique ID for a CLI invocation.

    The return value resembles "mix-models-debug-3332d415ef65bf2a-2023-02-02T162931" and
    contains:

    - The CLI name and (sub)commands.
    - A hash of all the CLI parameters (options and arguments).
    - The current date and time, in ISO format with Windows-incompatible ":" removed.

    """
    click_context = click.get_current_context()

    # Collapse CLI (sub)commands and their arguments to a hashable data structure
    # This also includes CLI options *not* given
    c: Optional[click.Context] = click_context
    data = []
    while c is not None:
        data.append((c.command.name, tuple(sorted(c.params.items()))))
        c = c.parent

    # Assemble parts
    return "-".join(
        [
            click_context.command_path.replace(" ", "-"),
            f"{hash(tuple(reversed(data))):x}",
            datetime.now().isoformat(timespec="seconds").replace(":", ""),
        ]
    )


#: Common command-line parameters (arguments and options). See :func:`common_params`.
PARAMS = {
    "dest": Option(
        ["--dest"],
        callback=store_context,
        expose_value=False,
        help="Destination URL for created scenario(s).",
    ),
    "dry_run": Option(
        ["--dry-run"],
        is_flag=True,
        callback=store_context,
        expose_value=False,
        help="Only show what would be done.",
    ),
    "force": Option(
        ["--force"],
        is_flag=True,
        callback=store_context,
        help="Overwrite or modify existing model/scenario.",
    ),
    "nodes": Option(
        ["--nodes"],
        help="Code list to use for 'node' dimension.",
        callback=exec_cb("context.model.regions = value"),
        type=Choice(codelists("node")),
        default=model.Config.regions,
        expose_value=False,
    ),
    "output_model": Option(
        ["--output-model"], help="Model name under which scenarios should be generated."
    ),
    "policy_path": Option(
        ["--policy-path"],
        callback=default_path_cb("scenario_generation", "policies"),
        help="Path to policy scripts.",
    ),
    "platform_dest": Option(["--platform-dest"], help="Name of destination Platform."),
    "quiet": Option(
        ["--quiet"],
        is_flag=True,
        expose_value=False,
        help="Show less or no output.",
    ),
    "regions": Option(
        ["--regions"],
        help="Code list to use for 'node' dimension.",
        callback=exec_cb("context.model.regions = value or context.model.regions"),
        type=Choice(codelists("node")),
    ),
    "rep_out_path": Option(
        ["--rep-out-path"],
        callback=default_path_cb("reporting_output"),
        help="Path for reporting output.",
    ),
    "rep_template": Option(
        ["--rep-template"],
        callback=default_path_cb(
            "message_data", "tools", "post_processing", "MESSAGEix_WorkDB_Template.xlsx"
        ),
        help="Path incl. filename and extension to reporting template.",
    ),
    "run_reporting_only": Option(
        ["--run-reporting-only"],
        is_flag=True,
        callback=store_context,
        help="Run only reporting.",
    ),
    "ssp": Argument(
        ["ssp"],
        callback=store_context,
        type=Choice(["LED", "SSP1", "SSP2", "SSP3", "SSP4", "SSP5"]),
    ),
    "urls_from_file": Option(
        ["--urls-from-file", "-f"],
        type=click.Path(
            exists=True,
            dir_okay=False,
            resolve_path=True,
            allow_dash=True,
            path_type=Path,
        ),
        callback=urls_from_file,
    ),
    "verbose": Option(
        # NB cannot use store_callback here; this is processed in the top-level CLI
        #    before the message_ix_models.Context() object is set up
        ["--verbose", "-v"],
        is_flag=True,
        help="Print DEBUG-level log messages.",
    ),
    "years": Option(
        ["--years"],
        help="Code list to use for the 'year' dimension.",
        callback=exec_cb("context.model.years = value"),
        type=Choice(codelists("year")),
        default=model.Config.years,
        # expose_value=False,
    ),
}


@dataclass
class CliRunner:
    """Similar to :class:`click.testing.CliRunner`, with extra features."""

    #: CLI entry point
    cli_cmd: click.Command
    #: CLI module
    cli_module: str

    env: Mapping[str, str] = field(default_factory=dict)
    charset: str = "utf-8"

    #: Method for invoking the command
    method: Literal["click", "subprocess"] = "click"

    def invoke(self, *args, **kwargs) -> click.testing.Result:
        method = kwargs.pop("method", self.method)

        if method == "click":
            runner = click.testing.CliRunner(env=self.env)
            with preserve_log_level():
                result = runner.invoke(self.cli_cmd, *args, **kwargs)
        elif method == "subprocess":
            result = self.invoke_subprocess(*args, **kwargs)

        # Store the result to be used by assert_exit_0()
        self.last_result = result

        return result

    def invoke_subprocess(self, *args, **kwargs) -> click.testing.Result:
        """Invoke the CLI in a subprocess."""
        import subprocess

        assert 1 == len(args)
        all_args: list[str] = [sys.executable, "-m", self.cli_module, *args[0]]

        # Run; capture in a subprocess.CompletedProcess
        cp = subprocess.run(all_args, capture_output=True, env=self.env, **kwargs)

        # Convert to a click.testing.Result

        # Mandatory argument introduced in click 8.2
        kw = dict(output_bytes=bytes()) if version("click") >= "8.2" else {}
        return click.testing.Result(
            runner=cast(click.testing.CliRunner, self),
            stdout_bytes=cp.stdout or bytes(),
            stderr_bytes=cp.stderr or bytes(),
            **kw,
            return_value=None,
            exit_code=cp.returncode,
            exception=None,
            exc_info=None,
        )

    def assert_exit_0(self, *args, **kwargs) -> click.testing.Result:
        """Assert a result has exit_code 0, or print its traceback.

        If any `args` or `kwargs` are given, :meth:`.invoke` is first called. Otherwise,
        the result from the last call of :meth:`.invoke` is used.

        Raises
        ------
        AssertionError
            if the result exit code is not 0.
        """
        __tracebackhide__ = True

        if len(args) + len(kwargs):
            self.invoke(*args, **kwargs)

        # Retrieve the last result
        result = self.last_result

        if result.exit_code != 0:
            print(f"{result.exit_code = }", f"{result.output = }", sep="\n")
            raise RuntimeError(result.exit_code) from result.exception

        return result
