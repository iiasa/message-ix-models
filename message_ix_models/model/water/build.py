from functools import lru_cache, partial
import logging
import message_ix
from typing import Mapping
from message_ix_models.model import bare, build
from message_ix_models import ScenarioInfo
from .utils import read_config

log = logging.getLogger(__name__)

def get_spec(context) -> Mapping[str, ScenarioInfo]:

    """Return the specification for water implementation"""

    context.use_defaults(bare.SETTINGS)

    require = ScenarioInfo()
    remove = ScenarioInfo()
    add = ScenarioInfo()

    # Load configuration
    context = read_config()

    # Update the ScenarioInfo objects with required and new set elements
    for set_name, config in context["water set"].items():
        # Required elements
        require.set[set_name].extend(config.get("require", []))

        # Elements to remove
        remove.set[set_name].extend(config.get("remove", []))

        # Elements to add
        add.set[set_name].extend(config.get("add", []))

    return dict(require=require, remove=remove, add=add)


@lru_cache()
def generate_set_elements(set_name, match = None):

    codes = read_config()["water set"][set_name].get("add", [])

    hierarchical = set_name in {"technology"}

    results = []
    for code in codes:
        if match and code.id != match:
            continue
        elif hierarchical:
            results.extend(code)

    return results

def get_water_reference_scenario(context):

    """
    This functions clones a global scenario and returns for using it's data and
    update the scenario with updated water parameters
    """

    #mp = ix.Platform()
    mp = context.get_platform()
    #ssp = context.ssp
    data_path = context.metadata_path / 'model'

    # Model and scenario name for storing the RES
    model_name = context.scenario_info['model']
    scenario_name = context.scenario_info['scenario']

    # Clone from a global scenario
    clone_from = dict(model='ENGAGE_SSP2_v4.1.7',
                      scenario='baseline')
    base = message_ix.Scenario(mp, **clone_from, cache=True)
    scenario = base.clone(model_name, scenario_name, keep_solution=True)

    # Solve the scenario and set default version
    #scenario.solve()
    scenario.set_as_default()

    return scenario


def main(scenario, **options):
    """Set up MESSAGE-Water on `scenario`.

    See also
    --------
    add_data
    apply_spec
    get_spec
    """
    from .data import add_data

    log.info('Set up MESSAGE-Water')

    # Core water structure
    spec = get_spec(context)

    build.apply_spec(scenario, spec, add_data, **options)

    # Uncomment to dump for debugging
    # scenario.to_excel('debug.xlsx')
