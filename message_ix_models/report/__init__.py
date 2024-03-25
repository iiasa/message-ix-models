import logging
from contextlib import nullcontext
from copy import deepcopy
from functools import partial
from importlib import import_module
from operator import itemgetter
from pathlib import Path
from typing import Callable, List, Optional, Tuple, Union
from warnings import warn

import genno.config
import yaml
from genno import Key, KeyExistsError
from genno.compat.pyam import iamc as handle_iamc
from message_ix import Reporter, Scenario

from message_ix_models import Context, ScenarioInfo
from message_ix_models.util import minimum_version
from message_ix_models.util._logging import mark_time, silence_log

from .config import Config
from .legacy import legacy_report

__all__ = [
    "Config",
    "legacy_report",
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

    This version overrides the version from :mod:`genno.config` to:

    - Set some defaults for the `rename` argument for :meth:`.convert_pyam`:

      - The `n` and `nl` dimensions are mapped to the "region" IAMC column.
      - The `y`, `ya`, and `yv` dimensions are mapped to the "year" column.

    - Use the MESSAGEix-GLOBIOM custom :func:`.util.collapse` callback to perform
      renaming etc. while collapsing dimensions to the IAMC ones. The "var" key from
      the entry, if any, is passed to the `var` argument of that function.

    - Provide optional partial sums. The "sums" key of the entry can give a list of
      strings such as ``["x", "y", "x-y"]``; in this case, the conversion to IAMC format
      is also applied to the same "base" key with a partial sum over the dimension "x";
      over "y", and over both "x" and "y". The corresponding dimensions are omitted from
      "var". All data are concatenated.
    """
    # FIXME the upstream key "variable" for the configuration is confusing; choose a
    #       better name
    from message_ix_models.report.util import collapse

    # Common
    base_key = Key(info["base"])

    # Use message_ix_models custom collapse() method
    info.setdefault("collapse", {})

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

    # Iterate over partial sums
    # TODO move some or all of this logic upstream
    keys = []  # Resulting keys
    for dims in [""] + info.pop("sums", []):
        # Dimensions to partial
        # TODO allow iterable of str
        dims = dims.split("-")

        label = f"{info['variable']} {'-'.join(dims) or 'full'}"

        # Modified copy of `info` for this invocation
        _info = info.copy()
        # Base key: use the partial sum over any `dims`. Use a distinct variable name.
        _info.update(base=base_key.drop(*dims), variable=label)
        # Exclude any summed dimensions from the IAMC Variable to be constructed
        _info["collapse"].update(
            callback=partial(
                collapse, var=list(filter(lambda v: v not in dims, info.get("var", [])))
            )
        )

        # Invoke the genno built-in handler
        handle_iamc(c, _info)

        keys.append(f"{label}::iamc")

    # Concatenate together the multiple tables
    c.add("concat", f"{info['variable']}::iamc", *keys)


def register(name_or_callback: Union[Callable, str]) -> Optional[str]:
    """Register a callback function for :meth:`prepare_reporter`.

    Each registered function is called by :meth:`prepare_reporter`, in order to add or
    modify reporting keys. Specific model variants and projects can register a callback
    to extend the reporting graph.

    Callback functions must take two arguments: the Reporter, and a :class:`.Context`:

    .. code-block:: python

        from message_ix.report import Reporter
        from message_ix_models import Context
        from message_ix_models.report import register

        def cb(rep: Reporter, ctx: Context):
            # Modify `rep` by calling its methods ...
            pass

        register(cb)

    Parameters
    ----------
    name_or_callback
        If a string, this may be a submodule of :mod:`.message_ix_models`, or
        :mod:`message_data`, in which case the function
        ``{message_data,message_ix_models}.{name}.report.callback`` is used. Or, it may
        be a fully-resolved package/module name, in which case ``{name}.callback`` is
        used. If a callable (function), it is used directly.
    """
    if isinstance(name_or_callback, str):
        # Resolve a string
        candidates = [
            # As a fully-resolved package/module name
            name_or_callback,
            # As a submodule of message_ix_models
            f"message_ix_models.{name_or_callback}.report",
            # As a submodule of message_data
            f"message_data.{name_or_callback}.report",
        ]
        mod = None
        for name in candidates:
            try:
                mod = import_module(name)
            except ModuleNotFoundError:
                continue
            else:
                break
        if mod is None:
            raise ModuleNotFoundError(" or ".join(candidates))
        callback = mod.callback
    else:
        callback = name_or_callback
        name = callback.__name__

    if callback in CALLBACKS:
        log.info(f"Already registered: {callback}")
        return None

    CALLBACKS.append(callback)
    return name


def log_before(context, rep, key) -> None:
    log.info(f"Prepare to report {'(DRY RUN)' if context.dry_run else ''}")
    log.info(key)
    log.log(
        logging.INFO
        if (context.core.dry_run or context.core.verbose)
        else logging.DEBUG,
        "\n" + rep.describe(key),
    )
    mark_time()


def report(context: Context, *args, **kwargs):
    """Report (post-process) solution data in a |Scenario| and store time series data.

    This function provides a single, common interface to call both the :mod:`genno`
    -based (:mod:`message_ix_models.report`) and ‘legacy’ (
    :mod:`message_data.tools.post_processing`) reporting codes.

    Parameters
    ----------
    context : Context
        The code responds to:

        - :attr:`.dry_run`: if :obj:`True`, reporting is prepared but nothing is done.
        - :attr:`~.Config.scenario_info` and :attr:`~.Config.platform_info`: used to
          retrieve the Scenario to be reported.

        - :py:`context.report`, which is an instance of :class:`.report.Config`; see
          there for available configuration settings.
    """
    from message_ix_models.util.ixmp import discard_on_error

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
        context.report.legacy.update(kwargs.pop("legacy", {}))

        if len(args) + len(set(kwargs.keys()) & {"path"}) != 1:
            raise TypeError(
                f"Unknown mix of deprecated positional {args!r} "
                f"and keyword arguments {kwargs!r}"
            )
        elif len(args) == 1:
            out_dir = args[0]
        else:
            out_dir = kwargs.pop("path")
        context.report.legacy.setdefault("out_dir", out_dir)

    if context.report.legacy["use"]:
        return _invoke_legacy_reporting(context)

    with (
        nullcontext()
        if context.core.verbose
        else silence_log("genno message_ix_models")
    ):
        rep, key = prepare_reporter(context)

    log_before(context, rep, key)

    if context.dry_run:
        return

    with discard_on_error(rep.graph["scenario"]):
        result = rep.get(key)

    # Display information about the result
    log.info(f"Result:\n\n{result}\n")
    log.info(
        f"File output(s), if any, written under:\n{rep.graph['config']['output_dir']}"
    )


def _invoke_legacy_reporting(context):
    log.info("Using tools.post_processing.iamc_report_hackathon")
    from message_data.tools.post_processing import iamc_report_hackathon

    # Convert "legacy" config to keyword arguments for .iamc_report_hackathon.report()
    kwargs = deepcopy(context.report.legacy)
    kwargs.pop("use")

    # Read a legacy reporting configuration file and update the arguments
    config_file_path = kwargs.pop("config_file_path", None)
    if isinstance(config_file_path, Path) and config_file_path.exists():
        with open(config_file_path, "r") as f:
            kwargs.update(yaml.safe_load(f))

    # Retrieve the Scenario and Platform
    scen = context.get_scenario()
    mp = scen.platform

    mark_time()

    # `context` is passed only for the "dry_run" setting; the function receives all its
    # other settings via the `kwargs`
    return iamc_report_hackathon.report(mp=mp, scen=scen, context=context, **kwargs)


@minimum_version("message_ix 3.6")
def prepare_reporter(
    context: Context,
    scenario: Optional[Scenario] = None,
    reporter: Optional[Reporter] = None,
) -> Tuple[Reporter, Key]:
    """Return a :class:`message_ix.Reporter` and `key` prepared to report a |Scenario|.

    Parameters
    ----------
    context : Context
        The code responds to :py:`context.report`, which is an instance of
        :class:`.report.Config`.
    scenario : Scenario, *optional*
        Scenario to report. If not given, :meth:`.Context.get_scenario` is used to
        retrieve a Scenario.
    reporter : message_ix.Reporter, *optional*
        Existing reporter to extend with computations. If not given, it is created
        using :meth:`message_ix.Reporter.from_scenario`.

    Returns
    -------
    .Reporter
        Reporter prepared with MESSAGEix-GLOBIOM calculations; if `reporter` is given,
        this is a reference to the same object.

        If :attr:`.cli_output` is given, a task with the key "cli-output" is added that
        writes the :attr:`.Config.key` to that path.
    .Key
        Same as :attr:`.Config.key` if any, but in full resolution; else either
        "default" or "cli-output" according to the other settings.
    """
    log.info("Prepare reporter")

    if reporter:
        # Existing `Reporter` provided
        rep = reporter
        has_solution = True
        if scenario:
            log.warning(f"{scenario = } argument ignored")
        scenario = rep.graph["scenario"]
    else:
        # Retrieve the scenario
        scenario = scenario or context.get_scenario()
        # Create a new Reporter
        rep = Reporter.from_scenario(scenario)
        has_solution = scenario.has_solution()

    # Append the message_data operators
    rep.require_compat("message_ix_models.report.operator")

    # Force re-installation of the function iamc() in this file as the handler for
    # "iamc:" sections in global.yaml. Until message_data.reporting is removed, then
    # importing it will cause the iamc() function in *that* file to override the one
    # registered above.
    # TODO Remove, once message_data.reporting is removed.
    genno.config.handles("iamc")(iamc)

    if context.report.use_scenario_path:
        # Construct ScenarioInfo
        si = ScenarioInfo(scenario, empty=True)
        # Use the scenario URL to extend the path
        context.report.set_output_dir(context.report.output_dir.joinpath(si.path))

    # Pass values to genno's configuration; deepcopy to protect from destructive
    # operations
    rep.configure(
        **deepcopy(context.report.genno_config),
        fail="raise" if has_solution else logging.NOTSET,
    )
    rep.configure(model=deepcopy(context.model))

    # Apply callbacks for other modules which define additional reporting computations
    for callback in CALLBACKS:
        callback(rep, context)

    key = context.report.key
    if key:
        # If just a bare name like "ACT" is given, infer the full key
        if Key.bare_name(key):
            inferred = rep.infer_keys(key)
            if inferred != key:
                log.info(f"Infer {inferred!r} for {key!r}")
                key = inferred

        if context.report.cli_output:
            # Add a new task that writes `key` to the specified file
            key = rep.add(
                "cli-output", "write_report", key, path=context.report.cli_output
            )
    else:
        key = rep.default_key
        log.info(f"No key given; will use default: {key!r}")

    # Create the output directory
    context.report.mkdir()

    log.info("…done")

    return rep, key


def defaults(rep: Reporter, context: Context) -> None:
    from message_ix_models.model.structure import get_codes

    from .util import add_replacements

    # Add mappings for conversions to IAMC data structures
    add_replacements("c", get_codes("commodity"))
    add_replacements("t", get_codes("technology"))

    # Ensure "y::model" and "y0" are present
    # TODO remove this once message-ix-models depends on message_ix > 3.7.0 at minimum
    for comp in (
        ("y::model", "model_periods", "y", "cat_year"),
        ("y0", itemgetter(0), "y::model"),
    ):
        try:
            rep.add(*comp, strict=True)
        except KeyExistsError:
            pass  # message_ix > 3.7.0; these are already defined


register(defaults)
register("message_ix_models.report.plot")
