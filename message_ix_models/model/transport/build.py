import logging
from typing import Mapping

from message_data.model.build import apply_spec
from message_data.tools import Code, ScenarioInfo
from .data import add_data
from .utils import read_config, transport_technologies

log = logging.getLogger(__name__)


def get_spec() -> Mapping[str, ScenarioInfo]:
    """Return the specification for MESSAGEix-Transport."""
    require = ScenarioInfo()
    remove = ScenarioInfo()
    add = ScenarioInfo()

    context = read_config()

    for set_name, config in context['transport set'].items():
        # Required elements
        require.set[set_name].extend(config.get('require', []))

        # Elements to remove
        remove.set[set_name].extend(config.get('remove', []))

        # Elements to add
        if set_name == 'technology':
            log.info("Generate 'technology' elements")
            elements = transport_technologies(with_desc=True)
        else:
            elements = map(
                lambda e: Code(id=e[0], name=e[1]),
                config.get('add', {}).items(),
            )

        add.set[set_name].extend(elements)

    return dict(require=require, remove=remove, add=add)


def main(scenario, **options):
    """Set up MESSAGE-Transport on `scenario`.

    See also
    --------
    get_spec
    apply_spec
    """

    spec = get_spec()

    apply_spec(scenario, spec, add_data)

    scenario.to_excel('debug.xlsx')
