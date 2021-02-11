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

log = logging.getLogger(__name__)


# Add to the configuration keys stored by Reporter.configure().
genno.config.STORE.add("output_path")

#: List of callbacks for preparing the Reporter
CALLBACKS: List[Callable] = []


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


@genno.config.handles("iamc")
def iamc(c: Reporter, info):
    """Handle one entry from the ``iamc:`` config section."""
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
