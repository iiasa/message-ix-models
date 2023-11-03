from message_ix_models.tools.costs.config import Config
from message_ix_models.tools.costs.projections import create_cost_projections

# Example 1: By default, the Config fill will run for:
# R12
# for the base suite of technologies,
# with NAM as reference region,
# using GDP as the cost driver,
# and the updated data version
# and outputs in MESSAGE format.
# The function will also run for all SSP scenarios,
# for all years from 2021 to 2100.
default = Config()
out_default = create_cost_projections(
    node=default.node,
    ref_region=default.ref_region,
    base_year=default.base_year,
    module=default.module,
    method=default.method,
    scenario_version=default.scenario_version,
    scenario=default.scenario,
    convergence_year=default.convergence_year,
    fom_rate=default.fom_rate,
    format=default.format,
)

# Example 2: Get cost projections for SSP2 scenario in R12,
# using WEU as the reference region,
# with convergence as the method,
# for materials technologies,
# using GDP (updated data)
# You can either put the inputs directly into the create_cost_projections function,
# or you can create a Config object and pass that in.
default = Config()

# Option 1: Directly input the parameters
out_materials_ssp2 = create_cost_projections(
    node=default.node,
    ref_region="R12_WEU",
    base_year=default.base_year,
    module="materials",
    method="convergence",
    scenario_version=default.scenario_version,
    scenario="SSP2",
    convergence_year=default.convergence_year,
    fom_rate=default.fom_rate,
    format=default.format,
)

# Option 2: Create a Config object and pass that in
config = Config(
    module="materials", scenario="SSP2", ref_region="R12_WEU", method="convergence"
)

out_materials_ssp2 = create_cost_projections(
    node=config.node,
    ref_region=config.ref_region,
    base_year=config.base_year,
    module=config.module,
    method=config.method,
    scenario_version=config.scenario_version,
    scenario=config.scenario,
    convergence_year=config.convergence_year,
    fom_rate=config.fom_rate,
    format=config.format,
)

# Example 3: Get cost projections for SSP5 scenario in R12,
# using LAM as the reference region,
# with learning as the method,
# for materials technologies,

config = Config(
    module="materials", scenario="SSP5", ref_region="R12_LAM", method="learning"
)

out_materials_ssp5 = create_cost_projections(
    node=config.node,
    ref_region=config.ref_region,
    base_year=config.base_year,
    module=config.module,
    method=config.method,
    scenario_version=config.scenario_version,
    scenario=config.scenario,
    convergence_year=config.convergence_year,
    fom_rate=config.fom_rate,
    format=config.format,
)
