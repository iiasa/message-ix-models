"""Utility code for MESSAGEix-Transport."""
import logging
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Mapping, MutableMapping, Tuple, Union

import pandas as pd
from iam_units import registry  # noqa: F401
from message_ix import Scenario
from message_ix_models import Context, Spec
from message_ix_models.model import bare
from message_ix_models.model.structure import get_codes
from message_ix_models.util import (
    eval_anno,
    identify_nodes,
    load_private_data,
    private_data_path,
)

from message_data.tools import generate_set_elements

log = logging.getLogger(__name__)

#: Valid values of Context.regions for MESSAGEix-Transport; default first.
SETTINGS = dict(
    regions=["R11", "R12", "R14", "ISR"],
)

#: Configuration files for :func:`read_config`, located in :file:`data/transport/` or a
#: subdirectory.
METADATA = [
    # Information about MESSAGEix-Transport
    ("config.yaml",),
    ("set.yaml",),
    ("technology.yaml",),
    ("report.yaml",),
    # Information about MESSAGE(V)-Transport
    ("migrate", "set.yaml"),
]


def configure(
    context: Context, scenario: Scenario = None, options: Dict = None
) -> None:
    """Configure `context` for building `MESSAGEix-Transport`.

    Handle or set defaults for the following keys:

    - ``years``, ``res_with_dummies``: per :data:`.bare.SETTINGS`.
    - ``regions``: either by examining the ``node`` set of `scenario`, or per
      :data:`.bare.SETTINGS`.
    - ``future``: ID of a Transport Futures scenario.
    """
    # Handle arguments
    options = options or dict()

    try:
        # Identify the node codelist used in `scenario`
        regions = identify_nodes(scenario) if scenario else context.get("regions")
    except (AttributeError, ValueError):
        pass
    else:
        if context.get("regions") != regions:
            log.info(f"Set Context.regions = {repr(regions)} from scenario contents")
            context.regions = regions

    # Use defaults for other settings
    context.use_defaults(bare.SETTINGS)

    # Read configuration files
    read_config(context)

    cfg = context["transport config"]  # Shorthand

    # Set some defaults
    _setdefault_recursive(
        cfg,
        {
            "mode-share": "default",
            "regions": context.regions,
            "report": dict(filter=False),
        },
    )
    _update_recursive(cfg, {"data source": options.pop("data source", dict())})

    # future: set the mode-share key
    future = options.pop("futures-scenario", None)
    if future is not None:
        if future not in ("default", "base", "A---", "debug"):
            raise ValueError(f"unrecognized Transport Futures scenario {repr(future)}")
        cfg["mode-share"] = future

        if future == "A---":
            log.info(f"Set fixed demand for TF scenario {repr(future)}")
            cfg["fixed demand"] = "275000 km / year"

    # mode-share: overwrite
    cfg["mode-share"] = options.pop("mode-share", cfg.get("mode-share", "default"))


def _setdefault_recursive(dest: MutableMapping, src: Mapping) -> None:
    """Recursive version of :meth:`dict.setdefault`.

    .. todo:: move upstream, e.g. to :mod:`message_ix_models.util.context`.
    """
    for key, value in src.items():
        if isinstance(value, Mapping):
            # Ensure a dictionary exist to copy into; then recurse
            _setdefault_recursive(dest.setdefault(key, dict()), value)
        else:
            dest.setdefault(key, value)


def _update_recursive(dest: MutableMapping, src: Mapping) -> None:
    """Recursive version of :meth:`dict.update`.

    .. todo:: move upstream, e.g. to :mod:`message_ix_models.util.context`.
    """
    for key, value in src.items():
        if isinstance(value, Mapping):
            # Ensure a dictionary exist to copy into; then recurse
            _update_recursive(dest.setdefault(key, dict()), value)
        else:
            dest[key] = value


def read_config(context):
    """Read the transport model configuration / metadata and store on `context`.

    The files listed in :data:`.METADATA` are stored with keys like "transport set",
    corresponding to :file:`data/transport/set.yaml`.

    If a subdirectory of :file:`data/transport/` exists corresponding to
    ``context.regions`` then the files are loaded from that subdirectory, e.g.
    e.g. :file:`data/transport/ISR/set.yaml` is preferred to
    :file:`data/transport/set.yaml`.

    Keys in the main configuration file, if not set in the region-specific subdirectory,
    (:file:`data/transport/{regions}/config.yaml`), default to the values in the base
    directory (:file:`data/transport/config.yaml`).
    """
    # Load transport configuration YAML files and store on the Context

    # Default configuration from the base directory
    context["transport defaults"] = load_private_data("transport", *METADATA[0])

    # Preserve any existing configuration
    context.pop("transport config", None)
    # prior_config = deepcopy(context.get("transport config", dict()))

    # Overrides specific to regional versions
    for parts in METADATA:
        # Key for storing in the context, e.g. "transport config"
        key = f"transport {' '.join(parts)}".split(".yaml")[0]

        # Load and store the data from the YAML file: either in a subdirectory for
        # context.regions, or the top-level data directory
        path = path_fallback(context, *parts).relative_to(private_data_path())
        context[key] = load_private_data(*path.parts) or context.get(key, dict())

    # Apply defaults where not set in the region-specific config
    _setdefault_recursive(context["transport config"], context["transport defaults"])

    # Merge contents of technology.yaml into set.yaml
    context["transport set"]["technology"]["add"] = context.pop("transport technology")

    # Convert some values to codes
    for set_name, info in context["transport set"].items():
        generate_set_elements(context, set_name)


def input_commodity_level(df: pd.DataFrame, default_level=None) -> pd.DataFrame:
    """Add input 'commodity' and 'level' to `df` based on 'technology'."""
    # Retrieve transport technology information from configuration
    # FIXME don't depend on the most recent Context instance being the correct one
    ctx = Context.get_instance(-1)
    t_info = ctx["transport set"]["technology"]["add"]

    # Retrieve general commodity information
    c_info = get_codes("commodity")

    @lru_cache()
    def t_cl(t: str) -> pd.Series:
        """Return the commodity and level given technology `t`."""
        # Retrieve the "input" annotation for this technology
        input = eval_anno(t_info[t_info.index(t)], "input")

        # Commodity ID
        commodity = input["commodity"]

        # Retrieve the code for this commodity
        c_code = c_info[c_info.index(commodity)]

        # Level, in order of precedence:
        # 1. Technology-specific input level from `t_code`.
        # 2. Default level for the commodity from `c_code`.
        # 3. `default_level` argument to this function.
        level = (
            input.get("level", None) or eval_anno(c_code, id="level") or default_level
        )

        return pd.Series(dict(commodity=commodity, level=level))

    # Process every row in `df`; return a new DataFrame
    return df.combine_first(df["technology"].apply(t_cl))


def path_fallback(context_or_regions: Union[Context, str], *parts) -> Path:
    """Return a :class:`.Path` constructed from `parts`.

    If ``context.regions`` (or a string value as the first argument) is defined and the
    file exists in a subdirectory of :file:`data/transport/{regions}/`, return its
    path; otherwise, return the path in :file:`data/transport/`.
    """
    if isinstance(context_or_regions, str):
        regions = context_or_regions
    else:
        # Use a value from a Context object, or a default
        regions = context_or_regions.get("regions", "")

    for candidate in (
        private_data_path("transport", regions, *parts),
        private_data_path("transport", *parts),
    ):
        if candidate.exists():
            return candidate
    raise FileNotFoundError(candidate)


def get_techs(context) -> Tuple[Spec, List, Dict]:
    """Return info about transport technologies, given `context`."""
    from . import build

    # Get a specification that describes this setting
    spec = build.get_spec(context)

    # Set of all transport technologies
    technologies = spec["add"].set["technology"].copy()

    # Subsets of transport technologies for aggregation and filtering
    t_groups: Dict[str, List[str]] = {"non-ldv": []}
    for tech in filter(  # Only include those technologies with children
        lambda t: len(t.child), context["transport set"]["technology"]["add"]
    ):
        t_groups[tech.id] = list(c.id for c in tech.child)
        # Store non-LDV technologies
        if tech.id != "LDV":
            t_groups["non-ldv"].extend(t_groups[tech.id])

    return spec, technologies, t_groups
