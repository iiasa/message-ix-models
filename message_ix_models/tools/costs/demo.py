from message_ix_models.tools.costs.projections import get_cost_projections

# Example 1: Get cost projections for SSP2 scenario in R12, using GDP (updated data)
r12_gdp_ssp2 = get_cost_projections(
    sel_node="r12",
    sel_ref_region="R12_NAM",
    sel_base_year=2021,
    sel_scenario_version="updated",
    sel_scenario="ssp2",
    sel_method="gdp",
)

# Example 2: Get cost projections in R11 (with WEU as reference region), using learning
# (this will run for all SSP scenarios)
r11_learning = get_cost_projections(
    sel_node="r11",
    sel_ref_region="R11_WEU",
    sel_base_year=2021,
    sel_method="learning",
    sel_scenario_version="updated",
)

# Example 3: Get cost projections in R12, using convergence
r12_convergence = get_cost_projections(
    sel_node="r12",
    sel_base_year=2021,
    sel_method="convergence",
)
