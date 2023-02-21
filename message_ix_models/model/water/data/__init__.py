"""Generate input data."""

import logging

from message_ix_models import ScenarioInfo
from message_ix_models.util import add_par_data

from .demands import add_irrigation_demand, add_sectoral_demands, add_water_availability
from .infrastructure import add_desalination, add_infrastructure_techs
from .irrigation import add_irr_structure
from .water_for_ppl import cool_tech, non_cooling_tec
from .water_supply import add_e_flow, add_water_supply

log = logging.getLogger(__name__)

DATA_FUNCTIONS = [
    add_water_supply,
    cool_tech,  # Water & parasitic_electricity requirements for cooling technologies
    non_cooling_tec,
    add_sectoral_demands,
    add_water_availability,
    add_irrigation_demand,
    add_infrastructure_techs,
    add_desalination,
    add_e_flow,
    add_irr_structure,
]

DATA_FUNCTIONS_COUNTRY = [
    add_water_supply,
    cool_tech,  # Water & parasitic_electricity requirements for cooling technologies
    non_cooling_tec,
    add_sectoral_demands,
    add_water_availability,
    # add_irrigation_demand, # not used and coming from GLOBIOM for the global region
    add_infrastructure_techs,
    add_desalination,
    add_e_flow,
    # add if statement: if irrigation: land component from external model
]


def add_data(scenario, context, dry_run=False):
    """Populate `scenario` with MESSAGEix-Nexus data."""

    info = ScenarioInfo(scenario)
    context["water build info"] = info

    data_funcs = (
        [add_water_supply, cool_tech, non_cooling_tec]
        if context.nexus_set == "cooling"
        else DATA_FUNCTIONS
        if context.type_reg == "global"
        else DATA_FUNCTIONS_COUNTRY
    )

    for func in data_funcs:
        # Generate or load the data; add to the Scenario
        log.info(f"from {func.__name__}()")
        add_par_data(scenario, func(context), dry_run=dry_run)

    log.info("done")
