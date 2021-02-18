import logging
from functools import lru_cache, partial
from itertools import product
from typing import Mapping

from message_data.model import bare, build, disutility
from message_data.tools import Code, ScenarioInfo, get_context, set_info

from .utils import consumer_groups

log = logging.getLogger(__name__)


def get_spec(context) -> Mapping[str, ScenarioInfo]:
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
    nodes = set_info(f"node/{context.regions}")
    require.set["node"].extend(map(str, nodes[nodes.index("World")].child))

    return dict(require=require, remove=remove, add=add)


@lru_cache()
def generate_set_elements(set_name, match=None):
    if set_name == "consumer_group":
        return consumer_groups()

    codes = get_context()["transport set"][set_name].get("add", [])

    hierarchical = set_name in {"technology"}

    results = []
    for code in codes:
        if match and code.id != match:
            continue

        if "generate" in code.anno:
            results.extend(generate_codes(**code.anno["generate"]))
            continue

        if hierarchical and len(code.child):
            results.extend(code.child)
        else:
            results.append(code)

    return results


def generate_codes(dims, template):
    codes = [generate_set_elements(set_name, match) for set_name, match in dims.items()]

    for item in product(*codes):
        fmt = dict(zip(dims.keys(), item))
        args = {}
        for attr, t in template.items():
            args[attr] = t.format(**fmt) if isinstance(t, str) else t

        yield Code(**args)


def main(context, scenario, **options):
    """Build MESSAGEix-Transport on `scenario`.

    See also
    --------
    add_data
    apply_spec
    get_spec
    """
    from .data import add_data

    log.info("Build MESSAGEix-Transport")

    # Generate the description of the structure / structure changes
    spec = get_spec(context)

    # Apply the structural changes AND add the data
    build.apply_spec(scenario, spec, partial(add_data, context=context), **options)

    # Add generalized disutility structure to LDV technologies
    disutility.add(
        scenario,
        consumer_groups=consumer_groups(),
        technologies=generate_set_elements("technology", "LDV"),
        template=Code(
            id="transport {technology} usage",
            input=dict(
                commodity="transport vehicle {technology}",
                level="useful",
                unit="km",
            ),
            output=dict(
                commodity="transport pax {mode}",
                level="useful",
                unit="km",
            ),
        ),
        **options,
    )

    # Uncomment to dump for debugging
    # scenario.to_excel('debug.xlsx')
