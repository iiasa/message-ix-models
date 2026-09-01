import pandas as pd
import pytest

from message_ix_models import Context
from message_ix_models.model.buildings.sturm import (
    _MIXB_DEMAND_CSV,
    call_buildings_demand,
)
from message_ix_models.testing import bare_res


def test_call_buildings_demand(
    request: pytest.FixtureRequest, mock_buildings_context: Context
) -> None:
    """:func:`call_buildings_demand` reads CSVs and updates ``demand`` data."""
    ctx = mock_buildings_context

    scenario = bare_res(request, ctx)

    result = call_buildings_demand(ctx, scenario)

    assert result is scenario
    demand = scenario.par("demand")
    assert len(demand) > 0
    assert {"electr"} <= set(demand["commodity"])
    assert (demand["level"] == "useful").all()
    assert 2110 in demand["year"].values


def test_call_buildings_demand_excludes_materials(
    request: pytest.FixtureRequest, mock_buildings_context: Context
) -> None:
    """:func:`call_buildings_demand` drops excluded commodities."""
    ctx = mock_buildings_context

    linking_dir = ctx.buildings.sturm_code_dir.joinpath("message_linking")

    extra = pd.DataFrame(
        {
            "node": ["R12_AFR"],
            "commodity": ["electr_mat_floor"],
            "year": [2100],
            "time": ["year"],
            "value": [9.9],
            "unit": ["GWa"],
        }
    )
    path = linking_dir / _MIXB_DEMAND_CSV[0].format("R")
    pd.concat([pd.read_csv(path), extra]).to_csv(path, index=False)

    scenario = bare_res(request, ctx)

    with scenario.transact("Add commodity for exclude test"):
        scenario.add_set("commodity", "electr_mat_floor")
        scenario.add_set("level", "useful")

    call_buildings_demand(ctx, scenario)

    commodities = set(scenario.par("demand")["commodity"])
    assert "electr" in commodities
    assert "electr_mat_floor" not in commodities
