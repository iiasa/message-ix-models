import ixmp
import message_ix

from message_ix_models.tools.costs.config import MODULE, Config
from message_ix_models.tools.costs.projections import create_cost_projections
from message_ix_models.tools.costs.scenario import update_scenario_costs

# Example 1: Get cost projections for all scenarios in R12,
# for the base suite of technologies,
# with NAM as reference region,
# using GDP as the cost driver,
# and the updated data version
# and outputs in MESSAGE format.
# The function will also run for all SSP scenarios (using scenario="all")
# for all years from 2021 to 2100.

# Defaults for all configuration settings:
# - base_year=BASE_YEAR,
# - convergence_year=2050,
# - fom_rate=0,
# - format="message",
# - method="gdp",
# - module=MODULE.energy,
# - use_vintages=False,
# - node="R12",
# - ref_region —automatically determined from node
# - scenario="all",
# - scenario_version="updated",
cfg = Config()

res_r12_energy = create_cost_projections(cfg)

# The results are stored in the inv_cost and fix_cost attributes of the output object.
inv = res_r12_energy["inv_cost"]
fix = res_r12_energy["fix_cost"]

# Example 2: Get cost projections for all scenarios in R11,
# using WEU as the reference region,
# with convergence as the method,
# for the energy module,
# using the updated data version
# and outputs in IAMC format.

cfg = Config(
    format="iamc",
    method="convergence",
    node="R11",
    ref_region="R11_WEU",
)

r11_energy_convergence = create_cost_projections(cfg)

r11_energy_convergence["inv_cost"]
r11_energy_convergence["fix_cost"]

# Example 3: Get cost projections for SSP2 scenario in R12,
# using NAM as the reference region,
# with convergence as the method,
# for materials technologies,
# using GDP (updated data)
# and outputs in MESSAGE format.


cfg = Config(
    module=MODULE.materials,
    scenario="SSP2",
)

r12_materials_ssp2 = create_cost_projections(cfg)

r12_materials_ssp2["inv_cost"]
r12_materials_ssp2["fix_cost"]


# Example 4: Modify a MESSAGEix scenario to include updated costs.
# This also changes the pre-base year costs in the scenario.
# For ScenarioMIP, also change the reduction_year to 2150.
cfg = Config(
    module=MODULE.materials, method="gdp", scenario="SSP2", reduction_year=2150
)
mp = ixmp.Platform("ixmp_dev")
scen = message_ix.Scenario(
    mp, model="model", scenario="scenario"
)  # replace with actual model and scenario names
update_scenario_costs(scen, cfg)
