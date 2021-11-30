"""Utility code for MESSAGEix-Transport."""
import logging
from functools import lru_cache
from itertools import product
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import xarray as xr
from message_ix import Scenario
from message_ix_models import Context
from message_ix_models.model import bare
from message_ix_models.model.structure import get_codes
from message_ix_models.util import (
    as_codes,
    eval_anno,
    identify_nodes,
    load_private_data,
    private_data_path,
)
from sdmx.model import Code

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
        regions = identify_nodes(scenario)
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
    for key, value in {
        "data source": dict(),
        "mode-share": "default",
        "regions": context.regions,
    }.items():
        cfg.setdefault(key, value)

    # data source: update
    cfg["data source"].update(options.pop("data source", dict()))

    # future: set the mode-share key
    future = options.pop("futures-scenario", None)
    if future is not None:
        if future not in ("base", "A---"):
            raise ValueError(f"unrecognized Transport Futures scenario {repr(future)}")
        cfg["mode-share"] == future


def read_config(context):
    """Read the transport model configuration / metadata and store on `context`.

    The files listed in :data:`.METADATA` are stored with keys like "transport set",
    corresponding to :file:`data/transport/set.yaml`.

    If a subdirectory of :file:`data/transport/` exists corresponding to
    ``context.regions`` then the files are loaded from that subdirectory, e.g.
    e.g. :file:`data/transport/ISR/set.yaml` instead of :file:`data/transport/set.yaml`.
    """
    try:
        # Confirm that the loaded config.yaml matches the current context.regions
        if context["transport config"]["regions"] == context.regions:
            return  # Already loaded
    except KeyError:
        pass  # "transport config" not present

    # Temporary
    if context.get("regions") == "ISR":
        raise NotImplementedError(
            "ISR transport config; see https://github.com/iiasa/message_data/pull/190"
        )

    # Load transport configuration YAML files and store on the Context
    for parts in METADATA:
        # Key for storing in the context, e.g. "transport config"
        key = f"transport {' '.join(parts)}".split(".yaml")[0]

        # Load and store the data from the YAML file: either in a subdirectory for
        # context.regions, or the top-level data directory
        path = path_fallback(context, *parts).relative_to(private_data_path())
        context[key] = load_private_data(*path.parts)

    # Merge technology.yaml with set.yaml
    context["transport set"]["technology"]["add"] = context.pop("transport technology")

    # Convert some values to codes
    for set_name, info in context["transport set"].items():
        generate_set_elements(context, set_name)


def generate_product(
    context: Context, name: str, template: Code
) -> Optional[Tuple[List[Code], Dict[str, xr.DataArray]]]:
    """Generates codes from a product along 1 or more `dims`.

    :func:`generate_set_elements` is called for each of the `dims`, and these values
    are used to format `base`.

    Parameters
    ----------
    set_name : str

    template : .Code
        Must have Python format strings for its its :attr:`id` and :attr:`name`
        attributes.
    dims : dict of (str -> value)
        (key, value) pairs are passed as arguments to :func:`generate_set_elements`.
    """
    # eval() and remove the original annotation
    dims = eval_anno(template, "_generate")
    template.pop_annotation(id="_generate")

    def _base(dim, match):
        """Return codes along dimension `dim`; if `match` is given, only children."""
        dim_codes = context["transport set"][dim]["add"]
        return dim_codes[dim_codes.index(match)].child if match else dim_codes

    codes = []  # Accumulate codes and indices
    indices = []

    # Iterate over the product of filtered codes for each dimension in
    for item in product(*[_base(*dm) for dm in dims.items()]):
        result = template.copy()  # Duplicate the template

        fmt = dict(zip(dims.keys(), item))  # Format the ID and name
        result.id = result.id.format(**fmt)
        result.name = str(result.name).format(**fmt)

        codes.append(result)  # Store code and indices
        indices.append(item)

    # - Convert length-N sequence of D-tuples to D iterables each of length N.
    # - Convert to D × 1-dimensional xr.DataArrays, each of length N.
    tmp = zip(*indices)
    indexers = {d: xr.DataArray(list(i), dims=name) for d, i in zip(dims.keys(), tmp)}
    # Corresponding indexer with the full code IDs
    indexers[name] = xr.DataArray([c.id for c in codes], dims=name)

    return codes, indexers


def generate_set_elements(context, name, match=None) -> List[Code]:
    """Generate elements for set `name`.

    This function converts the contents of :file:`transport/set.yaml` and
    :file:`transport/technology.yaml` into lists of codes, of which the IDs are the
    elements of sets (dimensions) in a scenario.

    Parameters
    ----------
    set_name : str
        Name of the set for which to generate elements.
    match: str, optional
        If given, only return Codes whose ID matches this value exactly, *or* children
        of such codes.
    """
    hierarchical = name in {"technology"}

    codes = []  # Accumulate codes
    deferred = []
    for code in as_codes(context["transport set"][name].get("add", [])):
        if eval_anno(code, "_generate"):
            # Requires a call to generate_product(); do these last
            deferred.append(code)
            continue

        codes.append(code)

        if hierarchical:
            # Store the children of `code`
            codes.extend(filter(lambda c: c not in codes, code.child))

    # Store codes processed so far, in case used recursively by generate_product()
    context["transport set"][name]["add"] = codes

    # Use generate_product() to generate codes and indexers based on others sets
    for code in deferred:
        generated, indexers = generate_product(context, name, code)

        # Store
        context["transport set"][name]["add"].extend(generated)

        # NB if there are >=2 generated groups, only indexers for the last are kept
        context["transport set"][name]["indexers"] = indexers


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
    try:
        # Use a value from a Context object, or a default
        regions = context_or_regions.get("regions", "")
    except AttributeError:
        # Value was a str instead
        regions = context_or_regions

    for candidate in (
        private_data_path("transport", regions, *parts),
        private_data_path("transport", *parts),
    ):
        if candidate.exists():
            return candidate
    raise FileNotFoundError(candidate)
