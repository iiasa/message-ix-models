import numpy as np

from message_ix_models.tools.costs.config import BASE_YEAR, FIRST_MODEL_YEAR
from message_ix_models.tools.costs.learning import (
    project_ref_region_inv_costs_using_learning_rates,
)
from message_ix_models.tools.costs.regional_differentiation import (
    apply_regional_differentiation,
)
from message_ix_models.tools.costs.splines import apply_splines_to_convergence


def test_apply_splines_to_convergence():
    # Set parameters
    sel_convergence_year = 2050
    sel_ref_region = "R12_NAM"

    # Get results for energy module
    energy_r12_reg = apply_regional_differentiation(
        module="energy", node="r12", ref_region=sel_ref_region
    )

    # Project costs using learning rates
    energy_r12_learn = project_ref_region_inv_costs_using_learning_rates(
        regional_diff_df=energy_r12_reg,
        module="energy",
        ref_region=sel_ref_region,
        base_year=BASE_YEAR,
    )

    energy_pre_costs = energy_r12_reg.merge(
        energy_r12_learn, on="message_technology"
    ).assign(
        inv_cost_converge=lambda x: np.where(
            x.year <= FIRST_MODEL_YEAR,
            x.reg_cost_base_year,
            np.where(
                x.year < sel_convergence_year,
                x.inv_cost_ref_region_learning * x.reg_cost_ratio,
                x.inv_cost_ref_region_learning,
            ),
        ),
    )

    # Select subset of technologies for tests (otherwise takes too long)
    energy_tech = ["coal_ppl", "gas_ppl", "gas_cc", "solar_pv_ppl", "wind_ppl"]
    energy_pre_costs = energy_pre_costs.query("message_technology in @energy_tech")

    # Apply splines to convergence costs
    energy_r12_splines = apply_splines_to_convergence(
        df_reg=energy_pre_costs,
        column_name="inv_cost_converge",
        convergence_year=2050,
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
    assert bool(all(i in energy_r12_splines.region.unique() for i in regions)) is True

    # Assert that all scenarios are present
    scenarios = ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"]
    assert (
        bool(all(i in energy_r12_splines.scenario.unique() for i in scenarios)) is True
    )

    # Assert that subset energy technologies are present
    assert (
        bool(
            all(
                i in energy_r12_splines.message_technology.unique() for i in energy_tech
            )
        )
        is True
    )

    # For each region, using coal_ppl as an example, assert that the costs converge
    # to approximately the reference region costs
    # in the convergence year
    for i in regions:
        assert (
            np.allclose(
                energy_r12_splines.query(
                    "region == @sel_ref_region \
                                and message_technology == 'coal_ppl' \
                                and year >= @sel_convergence_year"
                ).inv_cost_splines,
                energy_r12_splines.query(
                    "region == @i \
                                and message_technology == 'coal_ppl' \
                                and year >= @sel_convergence_year"
                ).inv_cost_splines,
                rtol=3,
            )
            is True
        )

    # Do same for materials
    materials_r12_reg = apply_regional_differentiation(
        module="materials", node="r12", ref_region=sel_ref_region
    )

    materials_r12_learn = project_ref_region_inv_costs_using_learning_rates(
        regional_diff_df=materials_r12_reg,
        module="materials",
        ref_region=sel_ref_region,
        base_year=BASE_YEAR,
    )

    materials_pre_costs = materials_r12_reg.merge(
        materials_r12_learn, on="message_technology"
    ).assign(
        inv_cost_converge=lambda x: np.where(
            x.year <= FIRST_MODEL_YEAR,
            x.reg_cost_base_year,
            np.where(
                x.year < sel_convergence_year,
                x.inv_cost_ref_region_learning * x.reg_cost_ratio,
                x.inv_cost_ref_region_learning,
            ),
        ),
    )

    # Select subset of technologies for tests (otherwise takes too long)
    materials_tech = ["biomass_NH3", "furnace_foil_steel", "meth_h2"]
    materials_pre_costs = materials_pre_costs.query(
        "message_technology in @materials_tech"
    )

    # Apply splines to convergence costs
    materials_r12_splines = apply_splines_to_convergence(
        df_reg=materials_pre_costs,
        column_name="inv_cost_converge",
        convergence_year=2050,
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
    assert (
        bool(all(i in materials_r12_splines.region.unique() for i in regions)) is True
    )

    # Assert that all scenarios are present
    scenarios = ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"]
    assert (
        bool(all(i in materials_r12_splines.scenario.unique() for i in scenarios))
        is True
    )

    # Assert that subset materials technologies are present
    assert (
        bool(
            all(
                i in materials_r12_splines.message_technology.unique()
                for i in materials_tech
            )
        )
        is True
    )

    # For each region, using meth_h2 as an example, assert that the costs converge
    # to approximately the reference region costs
    # in the convergence year
    for i in regions:
        assert (
            np.allclose(
                materials_r12_splines.query(
                    "region == @sel_ref_region \
                                and message_technology == 'meth_h2' \
                                and year >= @sel_convergence_year"
                ).inv_cost_splines,
                materials_r12_splines.query(
                    "region == @i \
                                and message_technology == 'meth_h2' \
                                and year >= @sel_convergence_year"
                ).inv_cost_splines,
                rtol=3,
            )
            is True
        )
