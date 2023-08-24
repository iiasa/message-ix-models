from message_ix_models.tools.costs.projections import create_cost_projections

# By default, the create_cost_projections() function will run for R12, with NAM as
# reference region, using GDP as the cost driver, and the updated data version.
# The function will also run for all SSP scenarios, and for all years from 2021 to 2100.
default = create_cost_projections()

# Example 1: Get cost projections for SSP2 scenario in R12, using GDP (updated data)
r12_gdp_ssp2 = create_cost_projections(
    sel_node="r12",
    sel_ref_region="R12_NAM",
    sel_base_year=2021,
    sel_scenario_version="updated",
    sel_scenario="ssp2",
    sel_method="gdp",
)

# Example 2: Get cost projections in R11 (with WEU as reference region), using learning
# (this will run for all SSP scenarios)
r11_learning = create_cost_projections(
    sel_node="r11",
    sel_ref_region="R11_WEU",
    sel_base_year=2021,
    sel_method="learning",
    sel_scenario_version="updated",
)

# Example 3: Get cost projections in R12, using convergence
r12_convergence = create_cost_projections(
    sel_node="r12",
    sel_base_year=2021,
    sel_method="convergence",
)

# Example 4: Get cost projections in R11 using previous/original SSP scenarios
r11_previous = create_cost_projections(sel_node="r11", sel_scenario_version="original")
