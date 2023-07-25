from message_ix_models.tools.costs.projections import (
    create_all_costs,
    create_cost_inputs,
)

# Example: Get data for investment cost in SSP3 scenario in MESSAGE format
df_inv_ssp3_message = create_cost_inputs("inv_cost", scenario="ssp3", format="message")

# Example: Get data for fixed cost in SSP1 scenario in IAMC format
df_fix_ssp1_iamc = create_cost_inputs("fix_cost", scenario="ssp1", format="iamc")

# Can also get all cost data (all scenarios, investment and fixed costs)
df_all_costs = create_all_costs()
