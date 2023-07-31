from message_ix_models.tools.costs.projections import (
    create_all_costs,
    get_cost_projections,
)

import pandas as pd
import numpy as np

inv_ssp1_conv = get_cost_projections(
    cost_type = "inv_cost",
    scenario = "ssp1",
    format = "message",
    converge_costs = True,
    convergence_year = 2050,
).assign(type = 'converge', convergence_year = int(2050))

inv_ssp2_conv = get_cost_projections(
    cost_type = "inv_cost",
    scenario = "ssp2",
    format = "message",
    converge_costs = True,
    convergence_year = 2050,
).assign(type = 'converge', convergence_year = int(2050))

inv_ssp3_conv = get_cost_projections(
    cost_type = "inv_cost",
    scenario = "ssp3",
    format = "message",
    converge_costs = True,
    convergence_year = 2050,
).assign(type = 'converge', convergence_year = int(2050))

inv_ssp1_learning = get_cost_projections(
    cost_type = "inv_cost",
    scenario = "ssp1",
    format = "message",
    use_gdp=False,
).assign(type = 'learning', convergence_year = np.NaN)

inv_ssp1_gdp = get_cost_projections(
    cost_type = "inv_cost",
    scenario = "ssp1",
    format = "message",
    use_gdp=True,
).assign(type = 'gdp', convergence_year = np.NaN)


inv_ssp_conv = pd.concat([inv_ssp1_learning, 
                          inv_ssp1_gdp, 
                          inv_ssp1_conv, 
                          inv_ssp2_conv, 
                          inv_ssp3_conv])



# Example: Get data for investment cost in SSP3 scenario in MESSAGE format,
# using GDP
df_inv_ssp3_message = get_cost_projections("inv_cost", scenario: str = "ssp2")

# Example: Get data for fixed cost in SSP1 scenario in IAMC format
df_fix_ssp1_iamc = create_cost_inputs("fix_cost", scenario="ssp1", format="iamc")

# Can also get all cost data (all scenarios, investment and fixed costs)
df_all_costs = create_all_costs()
