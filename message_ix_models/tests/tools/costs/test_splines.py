import numpy as np

from message_ix_models.tools.costs.config import FIRST_MODEL_YEAR
from message_ix_models.tools.costs.learning import (
    project_ref_region_inv_costs_using_learning_rates,
)
from message_ix_models.tools.costs.regional_differentiation import (
    get_weo_region_differentiated_costs,
)
from message_ix_models.tools.costs.splines import apply_splines_to_convergence


def test_apply_splines_to_convergence():
    in_node = "r12"
    in_ref_region = "R12_NAM"
    in_base_year = 2021
    in_module = "materials"
    in_convergence_year = 2060
    in_scenario = "SSP2"

    df_region_diff = get_weo_region_differentiated_costs(
        input_node=in_node,
        input_ref_region=in_ref_region,
        input_base_year=in_base_year,
        input_module=in_module,
    )

    df_ref_reg_learning = project_ref_region_inv_costs_using_learning_rates(
        df_region_diff,
        input_node=in_node,
        input_ref_region=in_ref_region,
        input_base_year=in_base_year,
        input_module=in_module,
    )

    if in_scenario is not None:
        df_ref_reg_learning = df_ref_reg_learning.query("scenario == @in_scenario")

    df_pre_costs = df_region_diff.merge(
        df_ref_reg_learning, on="message_technology"
    ).assign(
        inv_cost_converge=lambda x: np.where(
            x.year <= FIRST_MODEL_YEAR,
            x.reg_cost_base_year,
            np.where(
                x.year < in_convergence_year,
                x.inv_cost_ref_region_learning * x.reg_cost_ratio,
                x.inv_cost_ref_region_learning,
            ),
        ),
    )

    df_splines = apply_splines_to_convergence(
        df_pre_costs,
        column_name="inv_cost_converge",
        input_convergence_year=in_convergence_year,
    )

    # Assert that all regions are present
    regions = [
        "R12_AFR",
        "R12_CHN",
        "R12_EEU",
        "R12_FSU",
        "R12_LAM",
        "R12_MEA",
        "R12_NAM",
        "R12_PAO",
        "R12_PAS",
        "R12_SAS",
        "R12_WEU",
    ]
    assert bool(all(i in df_splines.region.unique() for i in regions)) is True

    # Assert that materials and base technologies are present
    tech = [
        "coal_ppl",
        "gas_ppl",
        "gas_cc",
        "biomass_NH3",
        "biomass_NH3",
        "furnace_foil_steel",
    ]
    assert bool(all(i in df_splines.message_technology.unique() for i in tech)) is True

    # For each region, using coal_ppl as an example, assert that the costs converge
    # to approximately the reference region costs
    # in the convergence year
    for i in regions:
        assert (
            np.allclose(
                df_splines.query(
                    "region == @in_ref_region \
                                and message_technology == 'coal_ppl' \
                                and year >= @in_convergence_year"
                ).inv_cost_splines,
                df_splines.query(
                    "region == @i \
                                and message_technology == 'coal_ppl' \
                                and year >= @in_convergence_year"
                ).inv_cost_splines,
                rtol=3,
            )
            is True
        )
