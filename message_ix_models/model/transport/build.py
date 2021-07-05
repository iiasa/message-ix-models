import logging
from functools import lru_cache, partial
from itertools import product
from typing import List, Mapping

from message_ix import Scenario
from message_ix_models import Context, ScenarioInfo
from message_ix_models.model import bare, build, disutility
from message_ix_models.model.structure import get_codes
from message_ix_models.util import eval_anno, identify_nodes, load_private_data
from message_ix_models.util._logging import mark_time
from sdmx.model import Annotation, Code

from message_data.model.transport.utils import consumer_groups

log = logging.getLogger(__name__)


def get_spec(context: Context) -> Mapping[str, ScenarioInfo]:
    """Return the specification for MESSAGEix-Transport.

    Parameters
    ----------
    context : .Context
        The key ``regions`` determines the regional aggregation used.
    """
    context.use_defaults(bare.SETTINGS)

    require = ScenarioInfo()
    remove = ScenarioInfo()
    add = ScenarioInfo()

    for set_name, config in context["transport set"].items():
        # Required elements
        require.set[set_name].extend(config.get("require", []))

        # Elements to remove
        remove.set[set_name].extend(config.get("remove", []))

        # Elements to add
        add.set[set_name].extend(generate_set_elements(set_name))

    # The set of required nodes varies according to context.regions
    nodes = get_codes(f"node/{context.regions}")
    require.set["node"].extend(map(str, nodes[nodes.index("World")].child))

    return dict(require=require, remove=remove, add=add)


@lru_cache()
def generate_set_elements(set_name, match=None) -> List[Code]:
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

    codes = load_private_data("transport", "set.yaml")[set_name].get("add", [])

    hierarchical = set_name in {"technology"}

    results = []
    for code in codes:
        if match and code.id != match:
            continue

        # Get an annotation named "_generate"
        generate_info = eval_anno(code, "_generate")
        if generate_info:
            # Generate codes using a template
            code.pop_annotation(id="_generate")
            results.extend(generate_codes(code, generate_info))
            continue

        if hierarchical and len(code.child):
            results.extend(code.child)
        else:
            results.append(code)

    return results


def generate_codes(base: Code, dims):
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
    codes = [generate_set_elements(set_name, match) for set_name, match in dims.items()]

    for item in product(*codes):
        result = base.copy()

        # Format the ID and name
        fmt = dict(zip(dims.keys(), item))
        result.id = result.id.format(**fmt)
        result.name = str(result.name).format(**fmt)

        yield result


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


def main(context: Context, scenario: Scenario, **options):
    """Build MESSAGEix-Transport on `scenario`.

    See also
    --------
    add_data
    apply_spec
    get_spec
    """
    from .data import add_data

    log.info("Build MESSAGEix-Transport")
    mark_time()

    regions = identify_nodes(scenario)
    if context.get("regions") != regions:
        log.info(f"Set Context.regions = {repr(regions)} from scenario contents")

    # Generate the description of the structure / structure changes
    spec = get_spec(context)

    # Apply the structural changes AND add the data
    build.apply_spec(scenario, spec, partial(add_data, context=context), **options)

    mark_time()

    # Add generalized disutility structure to LDV technologies
    disutility.add(
        scenario,
        groups=consumer_groups(),
        technologies=generate_set_elements("technology", "LDV"),
        template=TEMPLATE,
        **options,
    )

    mark_time()


def get_disutility_spec():
    """Return the spec for the disutility formulation on LDVs."""
    return disutility.get_spec(
        groups=consumer_groups(),
        technologies=generate_set_elements("technology", "LDV"),
        template=TEMPLATE,
    )
