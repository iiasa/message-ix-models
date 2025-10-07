import os.path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from message_ix import Scenario

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.report import (
    ScenarioMetadata,
    aggregate_totals,
    get_population_values,
    process_rates,
    report_full,
)
from message_ix_models.util import package_data_path


# NB: this tests all functions in model/water/reporting
@pytest.mark.xfail(reason="Temporary, for #106")
def test_report_full(test_context: Any, request: pytest.FixtureRequest) -> None:
    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    test_context.time = "year"
    test_context.type_reg = "global"
    test_context.regions = "R12"
    codes = get_codes(f"node/{test_context.regions}")
    world_code = [n for n in codes if str(n) == "World"][0]
    nodes = [str(n) for n in world_code.child]
    # test_context.map_ISO_c = {test_context.regions: nodes[0]}

    mp = test_context.get_platform()
    scenario_info = {
        "mp": mp,
        "model": f"{request.node.name}/test water model",
        "scenario": f"{request.node.name}/test water scenario",
        "version": "new",
    }
    s = Scenario(**scenario_info)
    s.add_horizon(year=[2020, 2030, 2040])
    s.add_set("technology", ["tech1", "tech2"])
    s.add_set("year", [2020, 2030, 2040])
    s.add_set("node", nodes)

    s.commit(comment="basic water report_full test model")
    s.set_as_default()
    # Remove quiet=True to debug using the output
    s.solve(quiet=True)

    test_context.set_scenario(s)

    # FIXME same as above
    test_context["water build info"] = ScenarioInfo(s)

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
