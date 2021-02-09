import logging
from copy import deepcopy
from functools import partial
from pathlib import Path

from message_ix.reporting import Reporter

log = logging.getLogger(__name__)


# Equivalent of some content in global.yaml
CONFIG = dict(units=dict(replace={"-": ""}))


def register(callback) -> None:
    """Register a callback function for :meth:`prepare_reporter`.

    Each registered function is called by :meth:`prepare_reporter`, in order to
    add or modify reporting keys. Specific model variants and projects can
    register a callback to extend the reporting graph.

    Callback functions must take one argument, with a type annotation::

        from message_ix.reporting import Reporter
        from message_data.reporting import register

        def cb(rep: Reporter):
            # Modify `rep` by calling its methods ...
            pass

        register(cb)
    """
    from genno.config import CALLBACKS

    if callback in CALLBACKS:
        log.info(f"Already registered: {callback}")
        return

    CALLBACKS.append(callback)


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

    if isinstance(config, dict):
        # Deepcopy to avoid destructive operations below
        config = deepcopy(config)
    else:
        # Load and apply configuration
        # A non-dict *config* argument must be a Path
        config = dict(path=Path(config))

    # Directory for reporting output
    config.setdefault("output_path", output_path)

    rep.configure(**config)

    # Reference to the configuration as stored in the reporter
    config = rep.graph["config"]

    # Variable name replacement: dict, not list of entries
    rep.add("iamc variable names", config.pop("iamc variable names", {}))

    # Tidy the config dict by removing any YAML sections starting with '_'
    [config.pop(k) for k in list(config.keys()) if k.startswith("_")]

    # If needed, get the full key for *quantity*
    key = rep.infer_keys(key)

    if output_path and not output_path.is_dir():
        # Add a new computation that writes *key* to the specified file
        key = rep.add(
            "cli-output", (partial(rep.get_comp("write_report"), path=output_path), key)
        )

    log.info("…done")

    return rep, key
