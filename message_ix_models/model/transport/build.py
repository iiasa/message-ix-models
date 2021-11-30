"""Build MESSAGEix-Transport on a base model.

.. autodata:: TEMPLATE

"""
import logging
from functools import partial
from itertools import product
from typing import Dict, List

from message_ix import Scenario
from message_ix_models import Context, Spec
from message_ix_models.model import build, disutility
from message_ix_models.model.structure import get_codes
from message_ix_models.util import add_par_data, eval_anno
from message_ix_models.util._logging import mark_time
from sdmx.model import Annotation, Code

from message_data.model.transport.utils import configure, consumer_groups

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


def generate_set_elements(context, set_name, match=None) -> List[Code]:
    """Generate elements for set `set_name`.

    This function converts the contents of :file:`transport/set.yaml` and
    :file:`transport/technology.yaml` into lists of codes, of which the IDs are the
    elements of sets (dimensions) in a scenario.

    Parameters
    ----------
    set_name : str
        Name of the set for which to generate elements.
        If "consumer_group", calls :func:`consumer_groups`
    match: str, optional
        If given, only the Code whose IDs matches this value are returned.
    """

    if set_name == "consumer_group":
        return consumer_groups()

    hierarchical = set_name in {"technology"}

    codes = context["transport set"][set_name].get("add", [])

    results = []
    for code in codes:
        if match and code.id != match:
            continue
        elif isinstance(code, str):
            results.append(code)

        # Get an annotation named "_generate"
        generate_info = eval_anno(code, "_generate")
        if generate_info:
            # Generate codes using a template
            code.pop_annotation(id="_generate")
            results.extend(generate_codes(context, code, generate_info))
            continue

        if hierarchical and len(code.child):
            results.extend(code.child)
        else:
            results.append(code)

    return results


def generate_codes(context, base: Code, dims):
    """Generates codes from a product along `dims`.

    :func:`generate_set_elements` is called for each of the `dims`, and these values
    are used to format `base`.

    Parameters
    ----------
    base : .Code
        Must have Python format strings for its its :attr:`id` and :attr:`name`
        attributes.
    dims : dict of (str -> value)
        (key, value) pairs are passed as arguments to :func:`generate_set_elements`.
    """
    codes = [
        generate_set_elements(context, set_name, match)
        for set_name, match in dims.items()
    ]

    for item in product(*codes):
        result = base.copy()

        # Format the ID and name
        fmt = dict(zip(dims.keys(), item))
        result.id = result.id.format(**fmt)
        result.name = str(result.name).format(**fmt)

        yield result


def get_spec(context: Context) -> Spec:
    """Return the specification for MESSAGEix-Transport.

    Parameters
    ----------
    context : .Context
        The key ``regions`` determines the regional aggregation used.
    """
    s = Spec()

    for set_name, config in context["transport set"].items():
        # Required elements
        s["require"].set[set_name].extend(config.get("require", []))

        # Elements to remove
        s["remove"].set[set_name].extend(config.get("remove", []))

        # Elements to add
        s["add"].set[set_name].extend(generate_set_elements(context, set_name))

    # The set of required nodes varies according to context.regions
    nodes = get_codes(f"node/{context.regions}")
    s["require"].set["node"].extend(map(str, nodes[nodes.index("World")].child))

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
    return disutility.get_spec(
        groups=consumer_groups(),
        technologies=generate_set_elements(context, "technology", "LDV"),
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

    # Apply the structural changes AND add the data
    log.info("Build MESSAGEix-Transport")
    build.apply_spec(scenario, spec, partial(add_data, context=context), **options)

    mark_time()

    # Add disutility data separately
    d_spec = get_disutility_spec(context)
    add_par_data(scenario, partial(disutility.get_data, spec=d_spec), **options)

    mark_time()
