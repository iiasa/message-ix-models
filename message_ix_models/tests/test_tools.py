from typing import TYPE_CHECKING

import pandas as pd
import pandas.testing as pdt
import pytest
from message_ix import make_df
from message_ix.models import MACRO

from message_ix_models import ScenarioInfo
from message_ix_models.testing import MARK, bare_res
from message_ix_models.tools import (
    add_AFOLU_CO2_accounting,
    add_alternative_TCE_accounting,
    add_CO2_emission_constraint,
    add_emission_trajectory,
    add_FFI_CO2_accounting,
    add_tax_emission,
    remove_emission_bounds,
    update_h2_blending,
)
from message_ix_models.tools.add_budget import main as add_budget
from message_ix_models.util import broadcast

if TYPE_CHECKING:
    from message_ix import Scenario
    from pytest import FixtureRequest

    from message_ix_models import Context


@pytest.fixture
def scenario(request: "FixtureRequest", test_context: "Context") -> "Scenario":
    test_context.model.regions = "R12"
    return bare_res(request, test_context, solved=False)


def afolu_co2_accounting_test_data(
    scenario: "Scenario", commodity: str, land_scenario: list[str]
) -> "pd.DataFrame":
    info = ScenarioInfo(scenario)

    land_output = make_df(
        "land_output",
        commodity=commodity,
        level="primary",
        value=123.4,
        unit="-",
        time="year",
    ).pipe(broadcast, year=info.Y, node=info.N, land_scenario=land_scenario)

    with scenario.transact("Prepare for test of add_AFOLU_CO2_accounting()"):
        scenario.add_set("commodity", commodity)
        scenario.add_set("land_scenario", land_scenario)
        scenario.add_par("land_output", land_output)

    return land_output


def test_add_AFOLU_CO2_accounting_A(scenario: "Scenario") -> None:
    """:attr:`add_AFOLU_CO2_accounting.METHOD.A`."""
    info = ScenarioInfo(scenario)

    commodity = ["LU_CO2"]
    land_scenario = ["BIO00GHG000", "BIO06GHG3000"]
    mode = ["M1"]
    node = ["R12_GLB"]

    land_output = make_df(
        "land_output", level="primary", value=1.0, unit="-", time="year"
    ).pipe(
        broadcast,
        commodity=commodity,
        year=info.Y,
        node=info.N + node,
        land_scenario=land_scenario,
    )

    with scenario.transact("Prepare for test of add_AFOLU_CO2_accounting()"):
        scenario.add_set("commodity", commodity)
        scenario.add_set("land_scenario", land_scenario)
        scenario.add_set("mode", mode)
        scenario.add_set("node", node)
        scenario.add_par("land_output", land_output)

    # Function runs without error
    add_AFOLU_CO2_accounting.add_AFOLU_CO2_accounting(
        scenario,
        relation_name="CO2_Emission_Global_Total",
        reg="R12_GLB",  # NB Previously 'reg'
        constraint_value=1.0,
        method=add_AFOLU_CO2_accounting.METHOD.A,
    )


def test_add_AFOLU_CO2_accounting_B(scenario: "Scenario") -> None:
    """:attr:`add_AFOLU_CO2_accounting.METHOD.B`."""
    # commodity in expected land_output data == `emission` parameter to the function
    commodity, land_scenario, land_output = add_AFOLU_CO2_accounting.test_data(scenario)

    # Other parameter values
    relation_name = "CO2_Emission_Global_Total"
    level = "LU"
    suffix = "_foo"

    # Function runs without error
    add_AFOLU_CO2_accounting.add_AFOLU_CO2_accounting(
        scenario,
        relation_name=relation_name,
        emission=commodity,
        level=level,
        suffix=suffix,
    )

    exp = [f"{x}{suffix}" for x in land_scenario]

    # relation_name is present
    assert relation_name in set(scenario.set("relation"))

    # Commodity and technology sets have expected added elements
    assert set(exp) <= set(scenario.set("commodity"))
    assert set(exp) <= set(scenario.set("technology"))

    # balance_quality entries are present
    pdt.assert_frame_equal(
        pd.DataFrame([[c, level] for c in exp], columns=["commodity", "level"]),
        scenario.set("balance_equality").sort_values("commodity"),
    )

    data_post = scenario.par("land_output", filters={"commodity": list(exp)})

    # 1 row of data was added for every row of original land_input
    assert len(land_output) == len(data_post)

    # 'level' and 'value' as expected
    assert {level} == set(data_post.level.unique())
    assert (1.0 == data_post.value).all()

    # 'commodity' corresponds to 'land_scenario'
    assert (data_post.land_scenario + suffix == data_post.commodity).all()


def test_add_CO2_emission_constraint(scenario: "Scenario") -> None:
    node = ["R12_GLB"]
    with scenario.transact("Prepare for test of add_CO2_emission_constraint()"):
        scenario.add_set("node", node)

    # Function runs without error()
    add_CO2_emission_constraint.main(
        scenario,
        relation_name="CO2_Emission_Global_Total",
        constraint_value=0.0,
        type_rel="lower",
        reg=node[0],
    )

    # TODO Add assertions about modified structure & data


def test_add_FFI_CO2_accounting(scenario: "Scenario") -> None:
    # Function runs without error()
    add_FFI_CO2_accounting.main(scenario, relation_name="CO2_Emission_Global_Total")

    # TODO Add assertions about modified structure & data


def test_add_alternative_TCE_accounting_A(scenario: "Scenario") -> None:
    """:attr:`add_alternative_TCE_accounting.METHOD.A`."""
    add_alternative_TCE_accounting.test_data(scenario, emission=["LU_CO2", "TCE"])

    # Function runs without error
    add_alternative_TCE_accounting.main(
        scenario, method=add_alternative_TCE_accounting.METHOD.A
    )

    # TODO Add assertions about modified structure & data


def test_add_alternative_TCE_accounting_B(scenario: "Scenario") -> None:
    """:attr:`add_alternative_TCE_accounting.METHOD.B`.

    Currently the only thing that differs versus the _A test is "LU_CO2_orig" instead
    of "LU_CO2".
    """
    add_alternative_TCE_accounting.test_data(scenario, emission=["LU_CO2_orig", "TCE"])

    # Function runs without error
    te_all = ["TCE_CO2_FFI", "TCE_CO2", "TCE_non-CO2", "TCE_other"]
    add_alternative_TCE_accounting.main(scenario, type_emission=te_all, use_gains=True)

    # TODO Add assertions about modified structure & data


def test_add_budget(request: "FixtureRequest", test_context: "Context") -> None:
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


def test_add_emission_trajectory(scenario: "Scenario") -> None:
    add_emission_trajectory.main(scenario, pd.DataFrame())


@MARK[2]  # Migrated with this test module prior to the code itself
def test_add_missing_years(request, test_context):
    from message_data.tools.utilities.update_fix_and_inv_cost import add_missing_years

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


def test_add_tax_emission(scenario: "Scenario") -> None:
    MACRO.initialize(scenario)
    info = ScenarioInfo(scenario)

    with scenario.transact("Prepare scenario for test of add_tax_emission()"):
        scenario.add_set("type_emission", ["TCE"])
        scenario.add_par("drate", make_df("drate", node=info.N, value=0.05, unit="-"))

    # Function runs without error
    add_tax_emission.main(scenario, price=42.1)

    # TODO Add assertions about modified structure & data


@MARK[2]  # Migrated with this test module prior to the code itself
def test_co2_td_cost(request, test_context):
    from message_data.tools.utilities import update_CO2_td_cost

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


@MARK[2]  # Migrated with this test module prior to the code itself
def test_adjust_curtailment_cap_to_act(request, test_context):
    from message_data.tools.utilities import adjust_curtailment_cap_to_act

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


def test_remove_emission_bounds(scenario: "Scenario") -> None:
    remove_emission_bounds.main(scenario)


def test_update_h2_blending(scenario: "Scenario") -> None:
    with scenario.transact("Prepare for test of update_h2_blending()"):
        scenario.add_set("relation", "h2mix_direct")

    # Function runs without error
    update_h2_blending.main(scenario)

    # Item was removed
    assert "h2mix_direct" not in scenario.set("relation")
