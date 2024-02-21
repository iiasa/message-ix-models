from message_ix_models.tools.costs.config import BASE_YEAR
from message_ix_models.tools.costs.projections import create_cost_projections

# Example 1: Get cost projections for all scenarios in R12,
# for the base suite of technologies,
# with NAM as reference region,
# using GDP as the cost driver,
# and the updated data version
# and outputs in MESSAGE format.
# The function will also run for all SSP scenarios (using scenario="all")
# for all years from 2021 to 2100.

res_r12_energy = create_cost_projections(
    node="R12",
    ref_region="R12_NAM",
    base_year=BASE_YEAR,
    module="energy",
    method="gdp",
    convergence_year=2050,
    scenario_version="updated",
    scenario="all",
    fom_rate=0.025,
    format="message",
)

# The results are stored in the inv_cost and fix_cost attributes of the output object.
inv = res_r12_energy.inv_cost
fix = res_r12_energy.fix_cost

# Example 2: Get cost projections for all scenarios in R11,
# using WEU as the reference region,
# with convergence as the method,
# for the energy module,
# using the updated data version
# and outputs in IAMC format.

r11_energy_convergence = create_cost_projections(
    node="R11",
    ref_region="R11_WEU",
    base_year=BASE_YEAR,
    module="energy",
    method="convergence",
    scenario_version="updated",
    scenario="all",
    convergence_year=2050,
    fom_rate=0.025,
    format="iamc",
)

r11_energy_convergence.inv_cost
r11_energy_convergence.fix_cost

# Example 3: Get cost projections for SSP2 scenario in R12,
# using NAM as the reference region,
# with convergence as the method,
# for materials technologies,
# using GDP (updated data)
# and outputs in MESSAGE format.

r12_materials_ssp2 = create_cost_projections(
    node="R12",
    ref_region="R12_NAM",
    base_year=BASE_YEAR,
    module="materials",
    method="gdp",
    scenario_version="updated",
    scenario="ssp2",
    convergence_year=2050,
    fom_rate=0.025,
    format="message",
)

r12_materials_ssp2.inv_cost
r12_materials_ssp2.fix_cost
