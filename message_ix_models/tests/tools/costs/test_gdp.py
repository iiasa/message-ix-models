import pytest

from message_ix_models.model.structure import get_codes
from message_ix_models.tools.costs.config import BASE_YEAR
from message_ix_models.tools.costs.gdp import (
    adjust_cost_ratios_with_gdp,
    process_raw_ssp_data,
    process_raw_ssp_data1,
)
from message_ix_models.tools.costs.regional_differentiation import (
    apply_regional_differentiation,
)


@pytest.mark.parametrize("func", (process_raw_ssp_data, process_raw_ssp_data1))
@pytest.mark.parametrize("node", ("R11", "R12"))
def test_process_raw_ssp_data(func, node):
    # Assert that all regions are present in each node configuration

    # Retrieve list of node IDs
    nodes = get_codes(f"node/{node}")
    # Convert to string
    regions = set(map(str, nodes[nodes.index("World")].child))

    # Function runs
    result = func(node=node, ref_region=f"{node}_NAM")

    # Data is present for all nodes
    assert regions == set(result.region.unique())

    # Data extends to 2100
    assert result.year.max() == 2100

    # Data for SSP1-5 and LED are present
    scens = {"SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"}
    assert scens == set(result.scenario.unique())


def test_adjust_cost_ratios_with_gdp():
    # Set parameters
    sel_node = "R12"
    sel_ref_region = "R12_NAM"

    # Get regional differentation for each module in R12
    energy_r12_reg = apply_regional_differentiation(
        module="energy", node=sel_node, ref_region=sel_ref_region
    )
    materials_r12_reg = apply_regional_differentiation(
        module="materials", node=sel_node, ref_region=sel_ref_region
    )

    # Get adjusted cost ratios based on GDP per capita
    adj_ratios_energy = adjust_cost_ratios_with_gdp(
        region_diff_df=energy_r12_reg,
        node=sel_node,
        ref_region=sel_ref_region,
        scenario="SSP2",
        scenario_version="updated",
        base_year=BASE_YEAR,
    )
    adj_ratios_materials = adjust_cost_ratios_with_gdp(
        region_diff_df=materials_r12_reg,
        node=sel_node,
        ref_region=sel_ref_region,
        scenario="SSP2",
        scenario_version="updated",
        base_year=BASE_YEAR,
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
        "R12_RCPA",
        "R12_SAS",
        "R12_WEU",
    ]
    assert bool(all(i in adj_ratios_energy.region.unique() for i in regions)) is True
    assert bool(all(i in adj_ratios_materials.region.unique() for i in regions)) is True

    # Assert that the maximum year is 2100
    assert adj_ratios_energy.year.max() == 2100
    assert adj_ratios_materials.year.max() == 2100

    # Assert that all cost ratios for reference region
    # R12_NAM are equal to 1
    assert all(
        adj_ratios_energy.query("region == @sel_ref_region").reg_cost_ratio_adj.values
        == 1.0
    )
    assert all(
        adj_ratios_materials.query(
            "region == @sel_ref_region"
        ).reg_cost_ratio_adj.values
        == 1.0
    )
