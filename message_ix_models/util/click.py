"""Command-line utilities.

These are used for building CLIs using :mod:`click`.
"""

import logging
from typing import Tuple

import ixmp
import message_ix
from click import Argument, Choice, Option
from ixmp.utils import parse_url

from message_ix_models.util.context import Context

log = logging.getLogger(__name__)


def clone_to_dest(
    context: Context, defaults=dict()
) -> Tuple[message_ix.Scenario, ixmp.Platform]:
    """Return a scenario based on the ``--dest`` the option.

    To use this method, decorate a command:

    .. code-block:: python

       from message_data.tools.cli import common_params

       @click.command()
       @common_params("dest")
       @click.pass_obj
       def foo(context, dest):
           defaults = dict(model="foo model", scenario="foo scenario")
           scenario = clone_to_dest(context, defaults)

    The resulting `scenario` has model and scenario names from ``--dest``, if given,
    else from `defaults`.

    If ``--url`` (or ``--platform``, ``--model``, ``--scenario`` and optionally
    ``--version``) are given, the identified scenario is used as a 'base' scenario, and
    is cloned. If ``--url``/``--platform`` and ``--dest`` refer to different
    :class:`ixmp.Platform` s, then this is a two-platform clone.

    If no base scenario can be loaded, :meth:`.bare.create_res` is called to generate a
    base scenario. This code is controlled by `context`.

    Parameters
    ----------
    context : Context
    defaults : dict
        Keys are 'model' and 'scenario', as accepted by :class:`.Scenario`.

    Returns
    -------
    Scenario
    Platform
        The platform for the returned scenario. Keep a reference to this Platform in
        order to prevent the Scenario being garbage collected.

    See also
    --------
    create_res
    """
    import ixmp

    # Default target model name and scenario name
    scenario_info = defaults.copy()

    try:
        # Handle the --dest= command-line option
        dest_platform, dest_info = parse_url(context.dest)
    except ValueError:
        # --dest not given
        dest_platform = {}
    else:
        # --dest URL was provided
        scenario_info.update(dest_info)

    try:
        # Get the base scenario from the --url argument
        scenario_base = context.get_scenario()

        # Destination platform
        if dest_platform == context.platform_info:
            # Same as origin; don't re-instantiate
            mp_dest = scenario_base.platform
        else:
            # Different platform
            mp_dest = ixmp.Platform(**dest_platform)
    except Exception:
        log.info("No base scenario given")
        from message_data.model.bare import create_res

        # Create
        context.platform_info.update(dest_platform)

        scenario_base = create_res(context)

        # Same platform
        mp_dest = scenario_base.platform

    # Clone
    log.info(f"Clone to {repr(scenario_info)}")
    return scenario_base.clone(platform=mp_dest, **scenario_info), mp_dest


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
