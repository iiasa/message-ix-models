"""Command-line utilities.

These are used for building CLIs using :mod:`click`.
"""
import logging

from click import Argument, Choice, Option

log = logging.getLogger(__name__)


def common_params(param_names: str):
    """Decorate a click.command with common parameters `param_names`.

    `param_names` must be a space-separated string of names appearing in :data:`PARAMS`,
    e.g. ``"ssp force output_model"``. The decorated function receives keyword
    arguments with these names::

        @click.command()
        @common_params("ssp force output_model")
        def mycmd(ssp, force, output_model)
            # ...
    """
    # Simplified from click.decorators._param_memo
    def decorator(f):
        if not hasattr(f, "__click_params__"):
            f.__click_params__ = []
        f.__click_params__.extend(
            PARAMS[name] for name in reversed(param_names.split())
        )
        return f

    return decorator


def default_path_cb(*default_parts):
    """Return a callback function for click.Option handling.

    If no option value is given, the callback uses :meth:`Context.get_local_path` and
    `default_parts` to provide a path that is relative to local data directory, e.g.
    the current working directory (see :doc:`/data`).
    """

    def _callback(context, param, value):
        value = value or context.obj.get_local_path(*default_parts)
        setattr(context.obj, param.name, value)
        return value

    return _callback


def store_context(context, param, value):
    """Callback that simply stores a value on the :class:`Context` object.

    Use this for parameters that are not used directly in a @click.command() function,
    but need to be carried by the Context for later use.
    """
    setattr(context.obj, param.name, value)
    return value


#: Common command-line parameters (arguments and options). See :func:`common_params`.
PARAMS = {
    "dest": Option(
        ["--dest"],
        callback=store_context,
        help="Destination URL for created scenario(s).",
    ),
    "dry_run": Option(
        ["--dry-run"], is_flag=True, help="Only show what would be done."
    ),
    "force": Option(
        ["--force"],
        is_flag=True,
        callback=store_context,
        help="Overwrite or modify existing model/scenario.",
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
        help="Show less or no output.",
    ),
    "regions": Option(
        ["--regions"],
        help="Code list to use for 'node' dimension.",
        # TODO make this list dynamic, e.g. using a callback to check against data/node/
        type=Choice(["ISR", "R11", "R14", "R32", "RCP"]),
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
        ["ssp"], callback=store_context, type=Choice(["SSP1", "SSP2", "SSP3"])
    ),
    "verbose": Option(
        ["--verbose", "-v"], is_flag=True, help="Print DEBUG-level log messages."
    ),
}
