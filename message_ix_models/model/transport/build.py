"""Build MESSAGEix-Transport on a base model.

.. autodata:: TEMPLATE
"""
import logging
from functools import partial
from typing import Dict

from message_ix import Scenario
from message_ix_models import Context, Spec
from message_ix_models.model import build, disutility
from message_ix_models.util._logging import mark_time
from sdmx.model import Annotation, Code

from message_data.model.transport.utils import configure
from message_data.tools import get_region_codes

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
    ],
)


def get_spec(context: Context) -> Spec:
    """Return the specification for MESSAGEix-Transport.

    Parameters
    ----------
    context : .Context
        The key ``regions`` determines the regional aggregation used.
    """
    s = Spec()

    for set_name, config in context["transport set"].items():
        # Elements to add, remove, and require
        for action in {"add", "remove", "require"}:
            s[action].set[set_name].extend(config.get(action, []))

    # The set of required nodes varies according to context.regions
    s["require"].set["node"].extend(map(str, get_region_codes(context.regions)))

    # Generate a spec for the generalized disutility formulation for LDVs
    s2 = get_disutility_spec(context)

    # Merge the items to be added by the two specs
    s["add"].update(s2["add"])

    return s


def get_disutility_spec(context: Context) -> Spec:
    """Return the spec for the disutility formulation on LDVs.

    See also
    --------
    TEMPLATE
    """
    # Identify LDV technologies
    techs = context["transport set"]["technology"]["add"]
    LDV_techs = techs[techs.index("LDV")].child

    return disutility.get_spec(
        groups=context["transport set"]["consumer_group"]["add"],
        technologies=LDV_techs,
        template=TEMPLATE,
    )


def main(context: Context, scenario: Scenario, options: Dict = None, **option_kwargs):
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
    configure(context, scenario, options)

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
