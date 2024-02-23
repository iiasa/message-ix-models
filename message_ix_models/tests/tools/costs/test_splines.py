import numpy as np
import numpy.testing as npt
import pytest

from message_ix_models.model.structure import get_codelist
from message_ix_models.tools.costs import Config
from message_ix_models.tools.costs.config import FIRST_MODEL_YEAR
from message_ix_models.tools.costs.learning import (
    project_ref_region_inv_costs_using_learning_rates,
)
from message_ix_models.tools.costs.regional_differentiation import (
    apply_regional_differentiation,
)
from message_ix_models.tools.costs.splines import apply_splines_to_convergence


@pytest.mark.parametrize(
    "module, techs",
    (
        ("energy", {"coal_ppl", "gas_ppl", "gas_cc", "solar_pv_ppl", "wind_ppl"}),
        ("materials", {"biomass_NH3", "furnace_foil_steel", "meth_h2"}),
    ),
)
def test_apply_splines_to_convergence(module, techs) -> None:
    # Set up
    config = Config(module=module)
    reg_diff = apply_regional_differentiation(config)

    # Project costs using learning rates
    inv_cost = project_ref_region_inv_costs_using_learning_rates(reg_diff, config)

    # - Merge
    # - Query a subset of technologies for testing
    pre_costs = (
        reg_diff.merge(inv_cost, on="message_technology")
        .assign(
            inv_cost_converge=lambda x: np.where(
                x.year <= FIRST_MODEL_YEAR,
                x.reg_cost_base_year,
                np.where(
                    x.year < config.convergence_year,
                    x.inv_cost_ref_region_learning * x.reg_cost_ratio,
                    x.inv_cost_ref_region_learning,
                ),
            ),
        )
        .query("message_technology in @techs")
    )

    # Apply splines to convergence costs
    splines = apply_splines_to_convergence(
        pre_costs, column_name="inv_cost_converge", convergence_year=2050
    )

    # Retrieve list of node IDs for children of the "World" node; convert to string
    regions = set(map(str, get_codelist(f"node/{config.node}")["World"].child))

    # All regions are present
    assert regions <= set(splines.region.unique())

    # All scenarios are present
    assert {"SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"} <= set(
        splines.scenario.unique()
    )

    # The subset of technologies are present
    assert techs <= set(splines.message_technology.unique())

    # Costs converge to approximately the reference region costs in the convergence year

    # Subset of the "inv_cost_splines" column as a pd.Series
    splines_cy = (
        splines.query("year >= @config.convergence_year")
        .set_index(["message_technology", "region", "scenario", "year"])
        .inv_cost_splines
    )
    # Further subset, only the reference region
    ref = splines_cy.xs(config.ref_region, level="region")

    # Group on technologies
    for t, group_data in splines_cy.groupby(level="message_technology"):
        # Compute the ratio versus reference region data for the same technology
        check = group_data / ref.xs(t, level="message_technology")
        try:
            npt.assert_allclose(1.0, check, rtol=5e-2)
        except AssertionError:
            # Diagnostic output
            print(f"{t=}\n", check[(check - 1.0).abs() > 5e-2].to_string())
            raise
