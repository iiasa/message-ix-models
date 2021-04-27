import pandas as pd
import pandas.testing as pdt
from message_data.tools.utilities import (
    add_budget,
    adjust_curtailment_cap_to_act,
    update_CO2_td_cost,
)
from message_data.tools.utilities.update_fix_and_inv_cost import add_missing_years
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.testing import bare_res


def test_add_budget(request, test_context):
    # Create a empty Scenario (no data) with the RES structure for R14 (using test
    # utilities)
    test_context.regions = "R14"
    scen = bare_res(request, test_context, solved=False)

    # Add minimal structure for add_budget() to work
    scen.check_out()
    scen.add_set("type_emission", "TCE")
    scen.add_set("year", 2000)
    scen.add_set("cat_year", ("cumulative", 2000))
    scen.commit("Test prep")

    # Call the function on the prepared scenario
    add_budget(scen, 1000.0)

    # commented: for debugging, show the state of the scenario after the call
    # print(scen.par("bound_emission"))

    # The call above results in the expected contents in bound_emission
    expected = make_df(
        "bound_emission",
        node="World",
        type_emission="TCE",
        type_tec="all",
        type_year="cumulative",
        value=1000.0,
        unit="tC",
    )
    pdt.assert_frame_equal(expected, scen.par("bound_emission"))

    # Call again with adjust_cumulative=True

    assert 1 == len(
        scen.set("cat_year").query("type_year == 'cumulative' and year == 2000")
    )
    add_budget(scen, 1000.0, adjust_cumulative=True)

    # The year "2000" has been removed from the "cumulative" category of years because
    # it is before the firstmodelyear (2010 via bare_res())
    assert 0 == len(
        scen.set("cat_year").query("type_year == 'cumulative' and year == 2000")
    )


def test_add_missing_years(request, test_context):
    # Create a empty Scenario (no data) with the RES structure for R14 (using test
    # utilities)
    test_context.regions = "R11"
    scen = bare_res(request, test_context, solved=False)

    info = ScenarioInfo(scen)
    # Sample of model years, where 2095 does not have any data assigned
    model_years = [2090, 2095, 2100]
    # Year that we want to fill with values through interpolation
    missing_years = [2095]
    # Index
    index_years = "year_vtg"

    # Create sample df
    data = {
        "technology": ["coal_ppl"],
        "node_loc": [info.N[0]],
        "unit": ["GWa"],
        # Assign test values to 2090 and 2100
        model_years[0]: 10,
        model_years[-1]: 20,
    }
    df = pd.DataFrame.from_dict(data)

    # Call the function
    df, df_tec = add_missing_years(df, model_years, missing_years, index_years)

    # Check structure of *df_tec*
    assert "coal_ppl" == df_tec[0]
    # Check that values for 2110 are equal to values in 2100
    pdt.assert_frame_equal(
        df.xs(2110, level=3), df.xs(2100, level=3), check_names=False
    )

    # Check that missing year was added and interpolation was correctly conducted
    assert df.xs(2095, level=3)["value1"][0] == 15


def test_co2_td_cost(request, test_context):
    # Create a empty Scenario (no data) with the RES structure for R14 (using test
    # utilities)
    test_context.regions = "R11"
    scen = bare_res(request, test_context, solved=False)
    update_CO2_td_cost(scen)

    # Check that CO2 T/D values from both biomass and fossil fuel origins match
    # assumptions from literature
    df = scen.par("var_cost", filters={"technology": ["co2_tr_dis"]})
    assert all(df["value"] == 75)
    df = scen.par("var_cost", filters={"technology": ["bco2_tr_dis"]})
    assert all(df["value"] == 150)


def test_adjust_curtailment_cap_to_act(request, test_context):
    # Create a empty Scenario (no data) with the RES structure for R14 (using test
    # utilities)
    test_context.regions = "R11"
    scen = bare_res(request, test_context, solved=False)
    adjust_curtailment_cap_to_act(scen)
    df = scen.par(
        "relation_total_capacity",
        filters={
            "technology": ["h2_elec", "stor_ppl"],
            "relation": [
                "solar_curtailment_1",
                "solar_curtailment_2",
                "solar_curtailment_3",
                "wind_curtailment_1",
                "wind_curtailment_2",
                "wind_curtailment_3",
            ],
        },
    )
    assert df.empty
    # Edit *df* to match what's in the model
    df = scen.par(
        "relation_activity",
        filters={
            "technology": "stor_ppl",
            "relation": [
                "solar_curtailment_1",
                "solar_curtailment_2",
                "solar_curtailment_3",
                "wind_curtailment_1",
                "wind_curtailment_2",
                "wind_curtailment_3",
            ],
        },
    )
    assert all(df[df["relation"] == "wind_curtailment_1"] == -1.4)
    assert all(df[df["relation"] == "wind_curtailment_1"] == -0.8)
    assert all(df[df["relation"] == "wind_curtailment_1"] == -0.4)
    assert all(df[df["relation"] == "solar_curtailment_1"] == -1.0)
    assert all(df[df["relation"] == "solar_curtailment_2"] == -0.6)
    assert all(df[df["relation"] == "solar_curtailment_3"] == -0.32)
