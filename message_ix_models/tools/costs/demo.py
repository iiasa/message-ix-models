from message_ix_models.tools.costs.projections import get_cost_projections

# Example 1: Get cost projections for SSP2 scenario, using learning rates
ssp2_learn = get_cost_projections(
    cost_type="inv_cost",
    scenario="ssp2",
    format="message",
    converge_costs=False,
    use_gdp=False,
)

# Example 2: Get investment cost projections for SSP1 scenario, using GDP
ssp1_gdp = get_cost_projections(
    cost_type="inv_cost",
    scenario="ssp1",
    format="message",
    converge_costs=False,
    use_gdp=True,
)

# Example 3: Get investment cost projections for SSP3 scenario, using cost convergence
# And assuming convergence year is 2060
ssp3_converge = get_cost_projections(
    cost_type="inv_cost",
    scenario="ssp3",
    format="message",
    converge_costs=True,
    convergence_year=2060,
    use_gdp=False,
)
