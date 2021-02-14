import logging
from copy import deepcopy
from functools import partial
from pathlib import Path
from typing import Callable, List

import genno.config
from genno.compat.pyam import iamc as handle_iamc
from message_ix.reporting import Reporter

from . import computations, util
from message_data.tools import Context


__all__ = [
    "prepare_reporter",
    "register",
    "report",
]


log = logging.getLogger(__name__)

# Add to the configuration keys stored by Reporter.configure().
genno.config.STORE.add("output_path")

#: List of callbacks for preparing the Reporter.
CALLBACKS: List[Callable] = []


@genno.config.handles("iamc")
def iamc(c: Reporter, info):
    """Handle one entry from the ``iamc:`` config section.

    This version overrides the version fron :mod:`genno.config` to:

    - Set some defaults for the `rename` argument for :meth:`.convert_pyam`:

      - The `n` and `nl` dimensions are mapped to the "region" IAMC column.
      - The `y`, `ya`, and `yv` dimensions are mapped to the "year" column.

    - Use the MESSAGEix-GLOBIOM custom :func:`.util.collapse` callback to perform
      renaming etc. while collapsing dimensions to the IAMC ones.
    """
    # Use message_data custom collapse() method
    info.setdefault("collapse", {})
    info["collapse"]["callback"] = util.collapse

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


def register(callback) -> None:
    """Register a callback function for :meth:`prepare_reporter`.

    Each registered function is called by :meth:`prepare_reporter`, in order to add or
    modify reporting keys. Specific model variants and projects can register a callback
    to extend the reporting graph.

    Callback functions must take one argument, the Reporter:

    .. code-block:: python

        from message_ix.reporting import Reporter
        from message_data.reporting import register

        def cb(rep: Reporter):
            # Modify `rep` by calling its methods ...
            pass

        register(cb)
    """
    if callback in CALLBACKS:
        log.info(f"Already registered: {callback}")
        return

    CALLBACKS.append(callback)


def report(scenario, key=None, config=None, output_path=None, dry_run=False, **kwargs):
    """Run complete reporting on *scenario* with output to *output_path*.

    This function provides a common interface to call both the 'new'
    (:mod:`.reporting`) and 'legacy' (:mod:`.tools.post_processing`) reporting
    codes.

    Parameters
    ----------
    scenario : Scenario
        Solved Scenario to be reported.
    key : str or Key
        Key of the report or quantity to be computed. Default: ``'default'``.
    config : Path-like, optional
        Path to reporting configuration file. Default: :file:`global.yaml`.
    output_path : Path-like
        Path to reporting
    dry_run : bool, optional
        Only show what would be done.

    Other parameters
    ----------------
    path : Path-like
        Deprecated alias for `output_path`.
    legacy : dict
        If given, the old-style reporting in
        :mod:`.tools.post_processing.iamc_report_hackathon` is used, and
        `legacy` is used as keyword arguments.
    """

    if "path" in kwargs:
        log.warning("Deprecated: path= kwarg to report(); use output_path=")
        if output_path:
            raise RuntimeError(
                f"Ambiguous: output_path={output_path}, path={kwargs['path']}"
            )
        output_path = kwargs.pop("path")

    if "legacy" in kwargs:
        log.info("Using legacy tools.post_processing.iamc_report_hackathon")
        from message_data.tools.post_processing import iamc_report_hackathon

        legacy_args = dict(merge_hist=True)
        legacy_args.update(**kwargs["legacy"])

        return iamc_report_hackathon.report(
            mp=scenario.platform,
            scen=scenario,
            model=scenario.model,
            scenario=scenario.scenario,
            out_dir=output_path,
            **legacy_args,
        )

    # Default arguments
    key = key or "default"
    config = config or (
        Path(__file__).parents[2].joinpath("data", "report", "global.yaml")
    )

    rep, key = prepare_reporter(scenario, config, key, output_path)

    log.info(f"Prepare to report:\n\n{rep.describe(key)}")

    if dry_run:
        return

    result = rep.get(key)

    msg = f" written to {output_path}" if output_path else f":\n{result}"
    log.info(f"Result{msg}")


def prepare_reporter(scenario, config, key, output_path=None):
    """Prepare to report *key* from *scenario*.

    Parameters
    ----------
    scenario : ixmp.Scenario
        MESSAGE-GLOBIOM scenario containing a solution, to be reported.
    config : os.Pathlike or dict-like
        Reporting configuration path or dictionary.
    key : str or ixmp.reporting.Key
        Quantity or node to compute. The computation is not triggered (i.e.
        :meth:`get <ixmp.reporting.Reporter.get>` is not called); but the
        corresponding, full-resolution Key is returned.
    output_path : os.Pathlike
        If given, a computation ``cli-output`` is added to the Reporter which
        writes *key* to this path.

    Returns
    -------
    ixmp.reporting.Reporter
        Reporter prepared with MESSAGE-GLOBIOM calculations.
    ixmp.reporting.Key
        Same as *key*, in full resolution, if any.

    """
    log.info("Preparing reporter")

    # Create a Reporter for *scenario*
    rep = Reporter.from_scenario(scenario)

    # Append the message_data computations
    rep.modules.append(computations)

    # Apply configuration
    if isinstance(config, dict):
        if len(config):
            # Deepcopy to avoid destructive operations below
            config = deepcopy(config)
        else:
            config = dict(
                path=Context.get_instance(-1).get_config_file("report", "global")
            )
    else:
        # A non-dict *config* argument must be a Path
        path = Path(config)
        if not path.exists() and not path.is_absolute():
            # Try to resolve relative to the data directory
            path = Context.get_instance(-1).message_data_path.joinpath("report", path)
        config = dict(path=path)

    # Directory for reporting output
    config.setdefault("output_path", output_path)
    # For genno.compat.plot
    # FIXME use a consistent set of names
    config.setdefault("output_dir", output_path)

    # Handle configuration
    rep.configure(**config)

    for callback in CALLBACKS:
        callback(rep)

    # If needed, get the full key for *quantity*
    key = rep.infer_keys(key)

    if output_path and not output_path.is_dir():
        # Add a new computation that writes *key* to the specified file
        key = rep.add(
            "cli-output", (partial(rep.get_comp("write_report"), path=output_path), key)
        )

    log.info("…done")

    return rep, key
