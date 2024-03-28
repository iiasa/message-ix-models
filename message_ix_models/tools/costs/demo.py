from message_ix_models.tools.costs.config import Config
from message_ix_models.tools.costs.projections import create_cost_projections

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
# - fom_rate=0.025,
# - format="message",
# - method="gdp",
# - module="energy",
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
    module="materials",
    scenario="SSP2",
)

r12_materials_ssp2 = create_cost_projections(cfg)

r12_materials_ssp2["inv_cost"]
r12_materials_ssp2["fix_cost"]
