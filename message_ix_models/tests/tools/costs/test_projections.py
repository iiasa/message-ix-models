import pytest
from message_ix import make_df

from message_ix_models.model.structure import get_codelist
from message_ix_models.tools.costs import Config, create_cost_projections


@pytest.mark.parametrize(
    "config, exp_fix, exp_inv",
    (
        (
            Config(node="R11", scenario="SSP2"),
            {"technology": {"coal_ppl", "gas_ppl", "wind_ppl", "solar_pv_ppl"}},
            {"technology": {"coal_ppl", "gas_ppl", "wind_ppl", "solar_pv_ppl"}},
        ),
        (
            Config(
                module="materials", method="convergence", scenario="SSP2", format="iamc"
            ),
            {
                "Variable": {
                    "OM Cost|Electricity|MTO_petro|Vintage=2020",
                    "OM Cost|Electricity|biomass_NH3|Vintage=2050",
                    "OM Cost|Electricity|furnace_foil_steel|Vintage=2090",
                }
            },
            {
                "Variable": {
                    "Capital Cost|Electricity|MTO_petro",
                    "Capital Cost|Electricity|biomass_NH3",
                    "Capital Cost|Electricity|furnace_foil_steel",
                }
            },
        ),
    ),
)
def test_create_cost_projections(config, exp_fix, exp_inv) -> None:
    # Function runs without error
    result = create_cost_projections(config)

    inv_cost = result["inv_cost"]
    fix_cost = result["fix_cost"]

    if config.format == "message":
        # Columns needed for MESSAGE input are present
        extra_cols = {"scenario", "scenario_version"}
        assert set(make_df("fix_cost").columns) | extra_cols == set(fix_cost.columns)
        assert set(make_df("inv_cost").columns) | extra_cols == set(inv_cost.columns)

    # Retrieve list of node IDs for children of the "World" node; convert to string
    nodes = set(map(str, get_codelist(f"node/{config.node}")["World"].child))

    # All regions are present in both data frames
    column = {"message": "node_loc", "iamc": "Region"}[config.format]
    assert nodes <= set(inv_cost[column].unique())
    assert nodes <= set(fix_cost[column].unique())

    # Expected values are in fix_cost columns
    for column, values in exp_fix.items():
        assert values <= set(fix_cost[column].unique())

    # Expected values are in inv_cost columns
    for column, values in exp_inv.items():
        assert values <= set(inv_cost[column].unique())
