"""Build MESSAGEix-Transport on a base model.

.. autodata:: TEMPLATE
"""
import logging
from importlib import import_module
from typing import Dict, Optional

import pandas as pd
import xarray as xr
from genno import Computer, KeyExistsError, Quantity, quote
from message_ix import Scenario
from message_ix_models import Context, ScenarioInfo, Spec
from message_ix_models.model import bare, build, disutility
from message_ix_models.model.structure import get_region_codes
from message_ix_models.util._logging import mark_time
from sdmx.model.v21 import Annotation, Code

from . import Config
from .util import get_techs

log = logging.getLogger(__name__)

#: Template for disutility technologies.
TEMPLATE = Code(
    id="{technology} usage by {group}",
    annotations=[
        Annotation(
            id="input",
            text=repr(
                dict(
                    commodity="transport vehicle {technology}",
                    level="useful",
                    unit="km",
                )
            ),
        ),
        Annotation(
            id="output",
            text=repr(
                dict(commodity="transport pax {group}", level="useful", unit="km")
            ),
        ),
        Annotation(id="is-disutility", text=repr(True)),
    ],
)


def add_structure(c: Computer):
    """Add keys to `c` for model structure required by demand computations.

    This uses `info` to mock the contents that would be reported from an already-
    populated Scenario for sets "node", "year", and "cat_year".
    """
    context = c.graph["context"]
    info = context["transport build info"]

    # Create a quantity for broadcasting y to (yv, ya)
    tmp = (
        info.yv_ya.rename(columns={"year_vtg": "yv", "year_act": "ya"})
        .eval("y = yv")
        .assign(value=1.0)
    )
    qty = Quantity(tmp.set_index(["y", "ya", "yv"])["value"])
    c.add("broadcast:y-yv-ya", qty)

    for key, value in (
        ("c::transport", quote(info.set["commodity"])),
        ("cg", quote(info.set["consumer_group"])),
        ("n", quote(list(map(str, info.set["node"])))),
        ("nodes", quote(info.set["node"])),
        ("t::transport modes", quote(context.transport.demand_modes)),
        ("y", quote(info.set["year"])),
        (
            "cat_year",
            pd.DataFrame([["firstmodelyear", info.y0]], columns=["type_year", "year"]),
        ),
    ):
        try:
            c.add(key, value, strict=True)  # Raise an exception if `key` exists
        except KeyExistsError:
            continue  # Already present; don't overwrite

    # Retrieve information about the model structure
    spec, technologies, t_groups = get_techs(context)

    # Lists and subsets
    c.add("c::transport", quote(spec["add"].set["commodity"]))
    c.add("t::transport", quote(technologies))

    # Create a quantity for broadcasting t to t, c, l
    c.add("input_commodity_level", "broadcast:t-c-l", "t::transport", quote("final"))

    # List of nodes excluding "World"
    # TODO move upstream, to message_ix
    c.add("nodes_ex_world", "n::ex world", "n"),
    c.add(
        "n:n:ex world",
        lambda data: Quantity(xr.DataArray(1, dims="n", coords={"n": data})),
        "n::ex world",
    )
    c.add("nodes_ex_world", "n::ex world+code", "nodes"),
    c.add("nodes_world_agg", "nl::world agg", "config"),

    # Model periods only
    c.add("model_periods", "y::model", "y", "cat_year")

    # Mappings for use with aggregate, select, etc.
    c.add("t::transport agg", quote(dict(t=t_groups)))
    # Sum across modes, including "non-ldv"
    c.add("t::transport modes 0", quote(dict(t=list(t_groups.keys()))))
    # Sum across modes, excluding "non-ldv"
    c.add(
        "t::transport modes 1",
        quote(dict(t=list(filter(lambda k: k != "non-ldv", t_groups.keys())))),
    )
    for id, techs in t_groups.items():
        c.add(f"t::transport {id}", quote(dict(t=techs)))
    c.add("t::transport all", quote(dict(t=technologies)))


def get_spec(context: Context) -> Spec:
    """Return the specification for MESSAGEix-Transport.

    Parameters
    ----------
    context : .Context
        The key ``regions`` determines the regional aggregation used.
    """
    s = Spec()

    for set_name, config in context.transport.set.items():
        # Elements to add, remove, and require
        for action in {"add", "remove", "require"}:
            s[action].set[set_name].extend(config.get(action, []))

    # The set of required nodes varies according to context.model.regions
    codelist = context.model.regions
    try:
        s["require"].set["node"].extend(map(str, get_region_codes(codelist)))
    except FileNotFoundError:
        raise ValueError(
            f"Cannot get spec for MESSAGEix-Transport with regions={codelist!r}"
        ) from None

    # Generate a spec for the generalized disutility formulation for LDVs
    s2 = get_disutility_spec(context)

    # Merge the items to be added by the two specs
    s["add"].update(s2["add"])

    return s


def get_computer(
    context: Context, obj: Optional[Computer] = None, **kwargs
) -> Computer:
    """Return a :class:`genno.Computer` set up for model-building calculations."""
    from .demand import add_exogenous_data

    # Configure
    Config.from_context(context, **kwargs)

    # Structure information for the base model
    scenario = kwargs.get("scenario")
    if scenario:
        base_info = ScenarioInfo(scenario)
    else:
        base_spec = bare.get_spec(context)
        base_info = base_spec["add"]

    context["transport build info"] = base_info

    # Structure information for MESSAGEix-Transport
    spec = get_spec(context)
    context["transport spec"] = spec
    context["transport spec disutility"] = get_disutility_spec(context)

    # Create a Computer, attach the context
    c = obj or Computer()
    c.add("context", context)
    if scenario:
        c.add("scenario", scenario)

    # .report._handle_config() does more of the low-level setup, including
    # - Require modules with computations.
    # - Transfer data from `context` to `config`.
    c.configure(config={"MESSAGEix-Transport": {}})

    # Add structure-related keys
    add_structure(c)

    # Add exogenous data
    if context.transport.exogenous_data:
        add_exogenous_data(c, base_info)

    # Prepare other calculations
    for name in "demand freight ikarus ldv non_ldv plot data".split():
        module = import_module(f"..{name}", __name__)
        module.prepare_computer(c)

    path = context.get_local_path("transport", "build.svg")
    c.visualize(filename=path, key="add transport data")
    log.info(f"Visualization written to {path}")

    return c


def get_disutility_spec(context: Context) -> Spec:
    """Return the spec for the disutility formulation on LDVs.

    See also
    --------
    TEMPLATE
    """
    # Identify LDV technologies
    techs = context.transport.set["technology"]["add"]
    LDV_techs = techs[techs.index("LDV")].child

    return disutility.get_spec(
        groups=context.transport.set["consumer_group"]["add"],
        technologies=LDV_techs,
        template=TEMPLATE,
    )


def main(
    context: Context,
    scenario: Scenario,
    options: Optional[Dict] = None,
    **option_kwargs,
):
    """Build MESSAGEix-Transport on `scenario`.

    See also
    --------
    add_data
    apply_spec
    get_spec
    """
    # Check arguments
    options = dict() if options is None else options.copy()
    dupe = set(options.keys()) & set(option_kwargs.keys())
    if len(dupe):
        raise ValueError(f"Option(s) {repr(dupe)} appear in both `options` and kwargs")
    options.update(option_kwargs)

    log.info("Configure MESSAGEix-Transport")
    mark_time()

    # Set up a Computer for input data calculations. This also:
    # - Creates a Config instance
    # - Generates and stores context["transport spec"], i.e the specification of the
    #   MESSAGEix-Transport structure: required, added, and removed set items
    # - Prepares the "add transport data" key used below
    c = get_computer(context, scenario=scenario, options=options)

    def _add_data(s, **kw):
        assert s is c.graph["scenario"]
        result = c.get("add transport data")
        log.info(f"Added {sum(result)} total obs")

    # Apply the structural changes AND add the data
    log.info("Build MESSAGEix-Transport")
    build.apply_spec(scenario, context["transport spec"], data=_add_data, **options)

    mark_time()

    scenario.set_as_default()
    log.info(f"Built {scenario.url} and set as default version")

    return scenario
