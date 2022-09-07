import logging
import sys
from copy import deepcopy
from functools import partial
from pathlib import Path
from typing import Callable, List, Optional, Tuple, Union

import genno.config
from dask.core import literal
from genno import Key
from genno.compat.pyam import iamc as handle_iamc
from message_ix import Reporter, Scenario
from message_ix_models import Context
from message_ix_models.util import local_data_path, private_data_path
from message_ix_models.util._logging import mark_time


__all__ = [
    "prepare_reporter",
    "register",
    "report",
]


log = logging.getLogger(__name__)

# Add to the configuration keys stored by Reporter.configure().
genno.config.STORE.add("output_path")
genno.config.STORE.add("output_dir")

#: List of callbacks for preparing the Reporter.
CALLBACKS: List[Callable] = []


# Ignore a section in global.yaml used to define YAML anchors
@genno.config.handles("_iamc formats")
def _(c: Reporter, info):
    pass


@genno.config.handles("iamc")
def iamc(c: Reporter, info):
    """Handle one entry from the ``iamc:`` config section.

    This version overrides the version fron :mod:`genno.config` to:

    - Set some defaults for the `rename` argument for :meth:`.convert_pyam`:

      - The `n` and `nl` dimensions are mapped to the "region" IAMC column.
      - The `y`, `ya`, and `yv` dimensions are mapped to the "year" column.

    - Use the MESSAGEix-GLOBIOM custom :func:`.util.collapse` callback to perform
      renaming etc. while collapsing dimensions to the IAMC ones. The "var" key from
      the entry, if any, is passed to the `var` argument of that function.
    """
    from message_data.reporting.util import collapse

    # Use message_data custom collapse() method
    info.setdefault("collapse", {})
    info["collapse"]["callback"] = partial(collapse, var=info.pop("var", []))

    # Add standard renames
    info.setdefault("rename", {})
    for dim, target in (
        ("n", "region"),
        ("nl", "region"),
        ("y", "year"),
        ("ya", "year"),
        ("yv", "year"),
    ):
        info["rename"].setdefault(dim, target)

    # Invoke the genno built-in handler
    handle_iamc(c, info)


def register(name_or_callback: Union[Callable, str]) -> str:
    """Register a callback function for :meth:`prepare_reporter`.

    Each registered function is called by :meth:`prepare_reporter`, in order to add or
    modify reporting keys. Specific model variants and projects can register a callback
    to extend the reporting graph.

    Callback functions must take two arguments: the Reporter, and a :class:`.Context`:

    .. code-block:: python

        from message_ix.reporting import Reporter
        from message_ix_models import Context
        from message_data.reporting import register

        def cb(rep: Reporter, ctx: Context):
            # Modify `rep` by calling its methods ...
            pass

        register(cb)

    Parameters
    ----------
    name_or_callback
        If a string, this may be a submodule of :mod:`.message_data`, in which case the
        function :func:`message_data.{name}.report.callback` is used. Or, it may be a
        fully-resolved package/module name, in which case :func:`{name}.callback` is
        used. If a callable (function), it is used directly.
    """
    if isinstance(name_or_callback, str):
        # Resolve a string
        try:
            # …as a submodule of message_data
            name = f"message_data.{name_or_callback}.report"
            __import__(name)
        except ImportError:
            # …as a fully-resolved package/module name
            name = name_or_callback
            __import__(name)
        callback = sys.modules[name].callback
    else:
        callback = name_or_callback

    if callback in CALLBACKS:
        log.info(f"Already registered: {callback}")
        return

    CALLBACKS.append(callback)
    return name


def report(context: Context):
    """Run complete reporting on a :class:`.message_ix.Scenario`.

    This function provides a single, common interface to call both the 'new'
    (:mod:`.reporting`) and 'legacy' (:mod:`.tools.post_processing`) reporting codes.

    The code responds to the following settings on `context`:

    .. list-table::
       :width: 100%
       :widths: 25 25 50
       :header-rows: 1

       * - Setting
         - Type
         - Description
       * - scenario_info
         -
         - Identifies the (solved) scenario to be reported.
       * - report/dry_run
         - bool
         - Only show what would be done. Default: :data:`False`.
       * - report/legacy
         - dict or None
         - If given, the old-style reporting in :mod:`.iamc_report_hackathon` is used,
           with `legacy` as keyword arguments.

    As well:

    - ``report/key`` is set to ``default``, if not set.
    - ``report/config`` is set to :file:`report/globa.yaml`, if not set.

    """
    if "legacy" in context.report:
        log.info("Using legacy tools.post_processing.iamc_report_hackathon")
        from message_data.tools.post_processing import iamc_report_hackathon

        # Default settings
        context.report["legacy"].setdefault("merge_hist", True)

        # Retrieve the Scenario and Platform
        scenario = context.get_scenario()
        mark_time()

        return iamc_report_hackathon.report(
            mp=scenario.platform, scen=scenario, **context.report["legacy"]
        )

    # Default arguments
    context.report.setdefault("key", "default")
    context.report.setdefault("config", private_data_path("report", "global.yaml"))

    rep, key = prepare_reporter(context)

    log.info(f"Prepare to report {'(DRY RUN)' if context.dry_run else ''}")
    log.info(key)
    log.log(
        logging.INFO if (context.dry_run or context.verbose) else logging.DEBUG,
        "\n" + rep.describe(key),
    )
    mark_time()

    if context.dry_run:
        return

    result = rep.get(key)

    # Display information about the result
    op = rep.graph["config"]["output_path"]
    log.info("Result" + (f" written to {op}" if op else f":\n{result}"))


def prepare_reporter(
    context: Context,
    scenario: Optional[Scenario] = None,
    reporter: Optional[Reporter] = None,
) -> Tuple[Reporter, Key]:
    """Return a :class:`.Reporter` and `key` prepared to report a :class:`.Scenario`.

    Parameters
    ----------
    context : Context
        Containing settings in the ``report/*`` tree.
    scenario : message_ix.Scenario, optional
        Scenario to report. If not given, :meth:`.Context.get_scenario` is used to
        retrieve a Scenario.
    reporter : .Reporter, optional
        Existing reporter to extend with computations. If not given, it is created
        using :meth:`.Reporter.from_scenario`.

    The code responds to the following settings on `context`:

    .. list-table::
       :width: 100%
       :widths: 25 25 50
       :header-rows: 1

       * - Setting
         - Type
         - Description
       * - scenario_info
         -
         - Identifies the (solved) scenario to be reported.
       * - report/key
         - str or :class:`ixmp.reporting.Key`
         - Quantity or node to compute. The computation is not triggered (i.e.
           :meth:`get <ixmp.reporting.Reporter.get>` is not called); but the
           corresponding, full-resolution Key, if any, is returned.
       * - report/config
         - dict or Path-like or None
         - If :class:`dict`, then this is passed to :meth:`.Reporter.configure`. If
           Path-like, then this is the path to the reporting configuration file. If not
           given, defaults to :file:`report/global.yaml`.
       * - report/output_path
         - Path-like, optional
         - Path to write reporting outputs. If given, a computation ``cli-output`` is
           added to the Reporter which writes ``report/key`` to this path.

    Returns
    -------
    .Reporter
        Reporter prepared with MESSAGEix-GLOBIOM calculations; if `reporter` is given,
        this is a reference to the same object.
    .Key
        Same as ``context.report["key"]`` if any, but in full resolution; else one of
        ``default`` or ``cli-output`` according to the other settings.
    """
    log.info("Prepare reporter")

    if reporter:
        # Existing `Reporter` provided
        rep = reporter
        has_solution = True
        if scenario:
            log.warning(f"{scenario = } argument ignored")
    else:
        # Retrieve the scenario
        scenario = scenario or context.get_scenario()
        # Create a new Reporter
        rep = Reporter.from_scenario(scenario)
        has_solution = scenario.has_solution()

    # Append the message_data computations
    rep.require_compat("message_data.reporting.computations")
    rep.require_compat("message_data.tools.gdp_pop")

    # Handle `report/config` setting passed from calling code
    context.report.setdefault("config", dict())
    if isinstance(context.report["config"], dict):
        # Dictionary of existing settings; deepcopy to protect from destructive
        # operations
        config = deepcopy(context.report["config"])
    else:
        # Otherwise, must be Path-like
        config = dict(path=Path(context.report["config"]))

    # Check location of the reporting config file
    p = config.get("path")
    if p and not p.exists() and not p.is_absolute():
        # Try to resolve relative to the data/ directory
        p = private_data_path("report", p)
        assert p.exists(), p
        config.update(path=p)

    # Set defaults
    # Directory for reporting output
    default_output_dir = local_data_path("report")
    config.setdefault(
        "output_path", context.report.get("output_path", default_output_dir)
    )
    # For genno.compat.plot
    # FIXME use a consistent set of names
    config.setdefault("output_dir", default_output_dir)
    config["output_dir"].mkdir(exist_ok=True, parents=True)

    # Pass configuration to the reporter
    rep.configure(**config, fail="raise" if has_solution else logging.NOTSET)

    # Apply callbacks for other modules which define additional reporting computations
    for callback in CALLBACKS:
        callback(rep, context)

    key = context.report.setdefault("key", None)
    if key:
        # If just a bare name like "ACT" is given, infer the full key
        if Key.bare_name(key):
            msg = f"for {key!r}"
            key = rep.infer_keys(key)
            log.info(f"Infer {key!r} {msg}")

        if config["output_path"] and not config["output_path"].is_dir():
            # Add a new computation that writes *key* to the specified file
            key = rep.add(
                "cli-output", "write_report", key, literal(config["output_path"])
            )
    else:
        key = rep.default_key
        log.info(f"No key given; will use default: {key!r}")

    log.info("…done")

    return rep, key
