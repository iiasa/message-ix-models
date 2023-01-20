import logging
import sys
from copy import deepcopy
from functools import partial
from pathlib import Path
from typing import Callable, List, Optional, Tuple, Union
from warnings import warn

import genno.config
import yaml
from dask.core import literal
from genno import Key
from genno.compat.pyam import iamc as handle_iamc
from message_ix import Reporter, Scenario
from message_ix_models import Context
from message_ix_models.model.structure import get_codes
from message_ix_models.util import local_data_path, private_data_path
from message_ix_models.util._logging import mark_time

from .util import add_replacements


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


def register(name_or_callback: Union[Callable, str]) -> Optional[str]:
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
        name = callback.__name__

    if callback in CALLBACKS:
        log.info(f"Already registered: {callback}")
        return None

    CALLBACKS.append(callback)
    return name


def log_before(context, rep, key):
    log.info(f"Prepare to report {'(DRY RUN)' if context.dry_run else ''}")
    log.info(key)
    log.log(
        logging.INFO if (context.dry_run or context.verbose) else logging.DEBUG,
        "\n" + rep.describe(key),
    )
    mark_time()


def report(context: Context, *args, **kwargs):
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
    # Handle deprecated usage that appears in:
    # - .model.cli.new_baseline()
    # - .model.create.solve()
    # - .projects.covid.scenario_runner.ScenarioRunner.solve()
    if isinstance(context, Scenario):
        warn(
            "Calling report(scenario, path, legacy=…); pass a Context instead",
            category=DeprecationWarning,
        )
        # Ensure `context` is actually a Context object for the following code
        scenario = context
        context = Context.get_instance(-1)

        # Transfer args, kwargs to context
        context.set_scenario(scenario)
        context.report["legacy"] = kwargs.pop("legacy")

        if len(args) + len(set(kwargs.keys()) & {"path"}) != 1:
            raise TypeError(
                f"Unknown mix of deprecated positional {args!r} "
                f"and keyword arguments {kwargs!r}"
            )
        elif len(args) == 1:
            out_dir = args[0]
        else:
            out_dir = kwargs.pop("path")
        context.report["legacy"].setdefault("out_dir", out_dir)

    if "legacy" in context.report:
        return _invoke_legacy_reporting(context)

    # Default arguments for genno-based reporting
    context.report.setdefault("key", "default")
    context.report.setdefault("config", private_data_path("report", "global.yaml"))

    rep, key = prepare_reporter(context)

    log_before(context, rep, key)

    if context.dry_run:
        return

    result = rep.get(key)

    # Display information about the result
    log.info(f"Result:\n\n{result}\n")
    log.info(
        f"File output(s), if any, written under:\n{rep.graph['config']['output_path']}"
    )


def _invoke_legacy_reporting(context):
    log.info("Using tools.post_processing.iamc_report_hackathon")
    from message_data.tools.post_processing import iamc_report_hackathon

    # Convert "legacy" config to keyword arguments for .iamc_report_hackathon.report()
    args = context.report.setdefault("legacy", dict())
    if not isinstance(args, dict):
        raise TypeError(
            f'Cannot handle Context["report"]["legacy"]={args!r} of type {type(args)}'
        )

    # Read a configuration file and update the arguments
    config = context.report.get("config")
    if isinstance(config, Path) and config.exists():
        with open(config, "r") as f:
            args.update(yaml.safe_load(f))

    # Default settings
    args.setdefault("merge_hist", True)

    # Retrieve the Scenario and Platform
    scen = context.get_scenario()
    mp = scen.platform

    mark_time()

    # `context` is passed only for the "dry_run" setting
    return iamc_report_hackathon.report(mp=mp, scen=scen, context=context, **args)


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
    context.setdefault("report", dict())
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

    for k in ("output_dir", "output_path"):
        config[k] = config[k].expanduser()
        config[k].mkdir(exist_ok=True, parents=True)

    # Pass configuration to the reporter
    rep.configure(**config, fail="raise" if has_solution else logging.NOTSET)

    # Add mappings for conversions to IAMC data structures
    add_replacements("c", get_codes("commodity"))
    add_replacements("t", get_codes("technology"))

    # Apply callbacks for other modules which define additional reporting computations
    for callback in CALLBACKS:
        callback(rep, context)

    key = context.report.setdefault("key", None)
    if key:
        # If just a bare name like "ACT" is given, infer the full key
        if Key.bare_name(key):
            msg = f"for {key!r}"
            inferred = rep.infer_keys(key)
            if inferred != key:
                log.info(f"Infer {key!r} {msg}")
                key = inferred

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
