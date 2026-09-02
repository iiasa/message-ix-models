import os.path

import numpy as np
import pandas as pd
import pytest
from message_ix import Scenario, make_df

from message_ix_models import Context, ScenarioInfo
from message_ix_models.model.water.config import Config
from message_ix_models.model.water.report import (
    ScenarioMetadata,
    aggregate_totals,
    get_population_values,
    get_rates_data,
    process_rates,
    report,
    report_full,
)
from message_ix_models.testing import SOLVE_OPTIONS, bare_res
from message_ix_models.util import package_data_path


@pytest.fixture
def solved_water_scenario(
    request: pytest.FixtureRequest, test_context: Context
) -> Scenario:
    """A fixture with a solved scenario.

    .. todo:: Expand this to a mock or complete solved water scenario, such that
       :func:`test_report` runs through completely.
    """

    test_context.regions = "R12"

    # Prepare water configuration
    cfg = Config.from_context(test_context)
    cfg.time = ["year"]
    cfg.type_reg = "global"

    # Generate a bare_res
    s = bare_res(request, test_context)

    common = dict(
        commodity="electr",
        emission="CO2",
        level="final",
        mode="all",
        node_dest="R12_AFR",
        node_loc="R12_AFR",
        technology="coal_ppl",
        time="year",
        time_dest="year",
        year_act=2020,
        year_vtg=2020,
        value=1.0,
        unit="kg",
    )

    with s.transact("Add minimal data for testing .water.report"):
        ef = "emission_factor"
        s.add_par(ef, make_df(ef, **common))

        # Force at least one technology to be active
        bal = "bound_activity_lo"
        s.add_par(bal, make_df(bal, **common))
        tl = "technical_lifetime"
        s.add_par(tl, make_df(tl, **common))

        # Force values for one tech so that "CAP_NEW|new capacity|extract_gw_fossil" is
        # in the output
        t = "extract_gw_fossil"
        s.add_set("technology", t)
        bncu = "bound_new_capacity_lo"
        s.add_par(bncu, make_df(bncu, **(common | dict(technology=t))))

        o = "output"
        s.add_par(o, make_df(o, **common))

    s.solve(**SOLVE_OPTIONS)

    test_context["water build info"] = ScenarioInfo(s)

    return s


@pytest.mark.xfail(
    # Currently fails in report() around `Add water prices`; different exceptions by
    # upstream version
    raises=(KeyError, ValueError),
    reason="Incomplete test or fixture",
)
def test_report(test_context: Context, solved_water_scenario: Scenario) -> None:
    report(solved_water_scenario, reg=test_context.model.regions, ssp="SSP2")


@pytest.mark.xfail(
    # Currently fails in/around run_old_reporting(); different exceptions by upstream
    # version
    raises=(SystemExit, TypeError),
    reason="Incomplete test or fixture",
)
def test_report_full(test_context: Context, solved_water_scenario: Scenario) -> None:
    """Test all functions in :mod:`.model.water.report`."""
    s = solved_water_scenario
    # Run the function to be tested
    report_full(sc=s, reg=test_context.regions, ssp="SSP2")

    # Since the function doesn't return anything, check that output file is produced in
    # correct location
    result_file = (
        package_data_path().parents[0] / f"reporting_output/{s.model}_{s.scenario}.csv"
    )
    assert os.path.isfile(result_file)


@pytest.mark.parametrize(
    "population_type,expected_connection_var,expected_access_var",
    [
        (
            "urban",
            "Connection Rate|Drinking Water|Urban",
            "Population|Drinking Water Access|Urban",
        ),
        (
            "rural",
            "Connection Rate|Drinking Water|Rural",
            "Population|Drinking Water Access|Rural",
        ),
    ],
)
def test_process_rates(
    population_type: str, expected_connection_var: str, expected_access_var: str
) -> None:
    """Test process_rates function handles urban/rural rate processing correctly."""
    # Create mock rates data
    rates_data = pd.DataFrame(
        [
            {
                "variable": f"{population_type}_water_connection_rate",
                "value": 0.8,
            },
            {
                "variable": f"{population_type}_water_treatment_rate",
                "value": 0.6,
            },
        ]
    )

    population_value = 1000.0
    region = "R12_AFR"
    year = 2030
    metadata: ScenarioMetadata = {
        "model": "test_model",
        "scenario": "test_scenario",
        "unit": "million",
    }

    result = process_rates(
        population_type, population_value, rates_data, region, year, metadata
    )

    # Should return 5 entries: population + 2 rates + 2 access calculations
    assert len(result) == 5

    # Check population entry
    population_var = f"Population|{population_type.capitalize()}"
    pop_entry = next(r for r in result if r["variable"] == population_var)
    assert pop_entry["value"] == population_value
    assert pop_entry["region"] == region
    assert pop_entry["year"] == year

    # Check connection rate entry
    conn_rate_entry = next(
        r for r in result if r["variable"] == expected_connection_var
    )
    assert conn_rate_entry["value"] == 0.8

    # Check drinking water access calculation
    access_entry = next(r for r in result if r["variable"] == expected_access_var)
    assert access_entry["value"] == 800.0  # 1000 * 0.8


def test_get_population_values() -> None:
    """Test get_population_values extracts urban/rural population correctly."""
    # Create mock population data
    pop_data = pd.DataFrame(
        [
            {
                "region": "R12_AFR",
                "year": 2030,
                "variable": "Population|Urban",
                "value": 500.0,
            },
            {
                "region": "R12_AFR",
                "year": 2030,
                "variable": "Population|Rural",
                "value": 300.0,
            },
            {
                "region": "R12_CHN",
                "year": 2030,
                "variable": "Population|Urban",
                "value": 800.0,
            },
        ]
    )

    # Test successful extraction
    urban_val, rural_val = get_population_values(pop_data, "R12_AFR", 2030)
    assert urban_val == 500.0
    assert rural_val == 300.0

    # Test missing rural data
    urban_val, rural_val = get_population_values(pop_data, "R12_CHN", 2030)
    assert urban_val == 800.0
    assert np.isnan(rural_val)

    # Test missing region/year combination
    urban_val, rural_val = get_population_values(pop_data, "R12_IND", 2040)
    assert np.isnan(urban_val)
    assert np.isnan(rural_val)


def test_get_rates_data() -> None:
    """:func:`.get_rates_data` runs with mock region mapping."""
    reg_map = pd.DataFrame(
        [["R12_CHN", "", "", ""]],
        columns=["region", "mapped_to", "parent", "hierarchy"],
    )

    # Function runs
    result = get_rates_data(reg="R12", ssp="SSP2", _reg_map=reg_map)

    # TODO Expand with assertions about the result

    del result


def test_aggregate_totals() -> None:
    """Test aggregate_totals creates correct regional aggregations."""
    # Create mock result data
    result_df = pd.DataFrame(
        [
            {
                "region": "R12_AFR",
                "year": 2030,
                "variable": "Population|Drinking Water Access|Urban",
                "value": 400.0,
                "model": "test_model",
                "scenario": "test_scenario",
                "unit": "million",
            },
            {
                "region": "R12_AFR",
                "year": 2030,
                "variable": "Population|Drinking Water Access|Rural",
                "value": 240.0,
                "model": "test_model",
                "scenario": "test_scenario",
                "unit": "million",
            },
            {
                "region": "R12_AFR",
                "year": 2030,
                "variable": "Population|Urban",
                "value": 500.0,
                "model": "test_model",
                "scenario": "test_scenario",
                "unit": "million",
            },
            {
                "region": "R12_AFR",
                "year": 2030,
                "variable": "Population|Rural",
                "value": 300.0,
                "model": "test_model",
                "scenario": "test_scenario",
                "unit": "million",
            },
        ]
    )

    totals = aggregate_totals(result_df)

    # Should return 2 aggregated DataFrames (drinking water access + population totals)
    assert len(totals) == 2

    # Check drinking water access total
    drink_total = next(
        df for df in totals if "Drinking Water Access" in df["variable"].iloc[0]
    )
    assert len(drink_total) == 1
    assert drink_total["variable"].iloc[0] == "Population|Drinking Water Access"
    assert drink_total["value"].iloc[0] == 640.0  # 400 + 240

    # Check population total
    pop_total = next(df for df in totals if df["variable"].iloc[0] == "Population")
    assert len(pop_total) == 1
    assert pop_total["value"].iloc[0] == 800.0  # 500 + 300
