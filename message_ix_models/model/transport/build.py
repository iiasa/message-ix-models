from functools import lru_cache
from itertools import product
import logging
from typing import Mapping

from message_data.model import build, disutility
from message_data.tools import Code, ScenarioInfo
from .utils import consumer_groups, read_config

log = logging.getLogger(__name__)


def get_spec() -> Mapping[str, ScenarioInfo]:
    """Return the specification for MESSAGEix-Transport."""
    require = ScenarioInfo()
    remove = ScenarioInfo()
    add = ScenarioInfo()

    context = read_config()

    for set_name, config in context["transport set"].items():
        # Required elements
        require.set[set_name].extend(config.get("require", []))

        # Elements to remove
        remove.set[set_name].extend(config.get("remove", []))

        # Elements to add
        add.set[set_name].extend(generate_set_elements(set_name))

    return dict(require=require, remove=remove, add=add)


@lru_cache()
def generate_set_elements(set_name, match=None):
    if set_name == "consumer_group":
        return consumer_groups()

    codes = read_config()["transport set"][set_name].get("add", [])

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
    codes = [
        generate_set_elements(set_name, match)
        for set_name, match in dims.items()
    ]

    for item in product(*codes):
        fmt = dict(zip(dims.keys(), item))
        args = {}
        for attr, t in template.items():
            args[attr] = t.format(**fmt) if isinstance(t, str) else t

        yield Code(**args)


def main(scenario, **options):
    """Set up MESSAGE-Transport on `scenario`.

    See also
    --------
    add_data
    apply_spec
    get_spec
    """
    from .data import add_data

    log.info('Set up MESSAGE-Transport')

    # Core transport structure
    spec = get_spec()
    build.apply_spec(scenario, spec, add_data, **options)

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
