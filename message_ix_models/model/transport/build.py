"""Build MESSAGEix-Transport on a base model.

.. autodata:: TEMPLATE
"""
import logging
from typing import Dict, Optional

import pandas as pd
from genno import Computer, KeyExistsError, quote
from message_ix import Scenario
from message_ix_models import Context, ScenarioInfo, Spec
from message_ix_models.model import bare, build, disutility
from message_ix_models.model.structure import get_codes, get_region_codes
from message_ix_models.util._logging import mark_time
from sdmx.model import Annotation, Code

from .config import Config
from .utils import get_techs

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

    # `info` contains only structure to be added, not existing/required structure. Add
    # information about the year dimension, to be used below.
    # TODO accomplish this by 'merging' the ScenarioInfo/spec.
    if not len(info.set["years"]):
        info.year_from_codes(get_codes(f"year/{context.years}"))
    if not len(info.set["node"]):
        info.set["node"] = get_region_codes(context.model.regions)

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
    # TODO move upstream, e.g. to message_ix
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
    from . import data, demand, plot
    from .data import ldv

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

    # From .data.add_data()
    # # Reference values: the Context, Scenario, ScenarioInfo, and dry_run parameter
    # for key, value in dict(
    #     # scenario=scenario,
    #     # info=info,
    #     # dry_run=dry_run,
    # ).items():
    #     c.add(key, quote(value))

    # Add structure-related keys
    add_structure(c)

    # Add exogenous data
    if context.transport.exogenous_data:
        demand.add_exogenous_data(c, base_info)

    # Prepare other calculations
    for module in (demand, ldv, plot, data):
        module.prepare_computer(c)

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
    from .data import add_data

    # Check arguments
    options = dict() if options is None else options.copy()
    dupe = set(options.keys()) & set(option_kwargs.keys())
    if len(dupe):
        raise ValueError(f"Option(s) {repr(dupe)} appear in both `options` and kwargs")
    options.update(option_kwargs)

    log.info("Configure MESSAGEix-Transport")
    mark_time()

    # Configure; consumes some of the `options`
    Config.from_context(context, scenario, options)

    # Generate the specification of the MESSAGEix-Transport structure: required, added,
    # and removed set items
    spec = get_spec(context)
    context["transport spec"] = spec
    context["transport spec disutility"] = get_disutility_spec(context)

    # Apply the structural changes AND add the data
    log.info("Build MESSAGEix-Transport")
    build.apply_spec(scenario, spec, partial(add_data, context=context), **options)

    mark_time()

    scenario.set_as_default()
    log.info(f"Built {scenario.url} and set as default version")

    return scenario
