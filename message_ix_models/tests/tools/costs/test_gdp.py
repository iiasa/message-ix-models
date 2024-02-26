import pytest

from message_ix_models.model.structure import get_codes
from message_ix_models.tools.costs import Config
from message_ix_models.tools.costs.gdp import (
    adjust_cost_ratios_with_gdp,
    process_raw_ssp_data,
)
from message_ix_models.tools.costs.regional_differentiation import (
    apply_regional_differentiation,
)


@pytest.mark.parametrize("node", ("R11", "R12"))
def test_process_raw_ssp_data(test_context, node) -> None:
    # Set the "regions" value on the context
    test_context.model.regions = node
    config = Config(node=node)

    # Retrieve list of node IDs
    nodes = get_codes(f"node/{node}")
    # Convert to string
    regions = set(map(str, nodes[nodes.index("World")].child))

    # Function runs
    # - context is ignored by process_raw_ssp_data
    # - node is ignored by process_raw_ssp_data1
    result = process_raw_ssp_data(context=test_context, config=config)

    # Data have the expected structure
    assert {
        "region",
        "year",
        "scenario",
        "scenario_version",
        "total_population",
        "total_gdp",
        "gdp_ppp_per_capita",
        "gdp_ratio_reg_to_reference",
    } == set(result.columns)

    # Data is present for all nodes
    assert regions == set(result.region.unique())

    # Data extends to at least 2100
    # NB(PNK) process_raw_ssp_data1() automatically fills the whole horizon;
    #         process_raw_ssp_data() does not
    assert result.year.max() >= 2100

    # Data for SSP1-5 and LED are present
    scens = {"SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"}
    assert scens == set(result.scenario.unique())


@pytest.mark.parametrize("module", ("energy", "materials"))
def test_adjust_cost_ratios_with_gdp(test_context, module) -> None:
    # Set parameters
    test_context.model.regions = "R12"

    # Mostly defaults
    config = Config(module=module, node="R12", scenario="SSP2")

    # Get regional differentiation
    region_diff = apply_regional_differentiation(config)

    # Get adjusted cost ratios based on GDP per capita
    result = adjust_cost_ratios_with_gdp(region_diff, config)

    assert all(
        [
            "scenario_version",
            "scenario",
            "message_technology",
            "region",
            "year",
            "gdp_ratio_reg_to_reference",
            "reg_cost_ratio_adj",
        ]
        == result.columns
    )

    # Retrieve list of node IDs
    nodes = get_codes(f"node/{test_context.model.regions}")
    # Convert to string
    regions = set(map(str, nodes[nodes.index("World")].child))

    # Assert that all regions are present
    assert regions == set(result.region.unique())

    # Assert that the maximum year is 2100
    assert result.year.max() >= 2100

    # Assert that all cost ratios for reference region R12_NAM are equal to 1
    assert all(
        result.query("region == @config.ref_region").reg_cost_ratio_adj.values == 1.0
    )
