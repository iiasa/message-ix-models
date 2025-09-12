import pytest
from message_ix import make_df

from message_ix_models.model.material.data_generic import (
    get_furnace_inputs,
    get_thermal_industry_emi_coefficients,
)
from message_ix_models.testing import bare_res


@pytest.mark.usefixtures("ssp_user_data")
@pytest.mark.parametrize(
    "regions, years, relations, solve",
    [
        pytest.param("R12", "B", "B", False),
    ],
)
def test_get_thermal_industry_emi_coefficients(
    request, tmp_path, test_context, regions, years, relations, solve
) -> None:
    """.materials.build() works on the bare RES, and the model solves."""
    # Generate the relevant bare RES
    ctx = test_context
    ctx.update(regions=regions, years=years, ssp="SSP2", relations=relations)
    scenario = bare_res(request, ctx)
    print(scenario.set("node"))
    dims = {
        "node_loc": "R12_CHN",
        "node_origin": "R12_CHN",
        "node_rel": "R12_CHN",
        "year_act": 2020,
        "year_vtg": 2020,
        "year_rel": 2020,
        "technology": "coal_i",
        "commodity": "coal",
        "level": "final",
        "unit": "GWa",
        "time": "year",
        "time_origin": "year",
        "mode": "all",
        "relation": "BCA_Emission",
    }
    inp = make_df("input", **dims, value=1.0)
    emi = make_df("relation_activity", **dims, value=0.1)

    with scenario.transact():
        scenario.add_par("input", inp)
        scenario.add_par("relation_activity", emi)

    df = get_thermal_industry_emi_coefficients(scenario)
    assert df["emi_factor"].iloc[0] == 0.1
    assert df["value_in"].iloc[0] == 1.0


@pytest.mark.parametrize(
    "regions, years, relations, solve",
    [
        pytest.param("R12", "B", "B", False),
    ],
)
def test_get_furnace_inputs(
    request, tmp_path, test_context, regions, years, relations, solve
) -> None:
    ctx = test_context
    ctx.update(regions=regions, years=years, ssp="SSP2", relations=relations)
    scenario = bare_res(request, ctx)
    df = get_furnace_inputs(scenario, 2020).reset_index()

    assert sorted(make_df("input").columns) == sorted(df.columns)
