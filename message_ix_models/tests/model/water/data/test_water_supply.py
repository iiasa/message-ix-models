import pandas as pd
import pytest
from message_ix import Scenario

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data.water_supply import (
    add_e_flow,
    add_water_supply,
    map_basin_region_wat,
)
from message_ix_models.tests.model.water.conftest import (
    setup_timeslices,
    setup_valid_basins,
)


@pytest.mark.parametrize("n_time", [1, 2, 12])
def test_map_basin_region_wat(request, test_context, n_time):
    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    # Personalize the context
    sets = {"year": [2020, 2030, 2040]}
    test_context["water build info"] = ScenarioInfo(y0=2020, set=sets)
    test_context.type_reg = "global"
    test_context.regions = "R12"
    nodes = get_codes(f"node/{test_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    map_ISO_c = {test_context.regions: nodes[0]}
    test_context.map_ISO_c = map_ISO_c
    test_context.RCP = "2p6"
    test_context.REL = "med"

    # Create minimal scenario for timeslice setup
    mp = test_context.get_platform()
    scenario_info = {
        "mp": mp,
        "model": f"{request.node.name}/test water model",
        "scenario": f"{request.node.name}/test water scenario",
        "version": "new",
    }
    s = Scenario(**scenario_info)
    s.add_horizon(year=[2020, 2030, 2040])
    s.add_set("year", [2020, 2030, 2040])
    s.commit(comment="Test scenario")

    # Set up timeslices
    setup_timeslices(test_context, s, n_time)

    # Set up valid_basins for basin filtering
    setup_valid_basins(test_context, regions=test_context.regions)

    result = map_basin_region_wat(test_context)

    # Assert the results
    assert isinstance(result, pd.DataFrame)
    assert all(
        col in result.columns
        for col in ["region", "mode", "date", "MSGREG", "share", "year", "time"]
    )

    # Verify timeslice-specific assertions
    result_times = set(result["time"].unique())
    if n_time == 1:
        # Annual only
        assert result_times == {"year"}, f"Expected only 'year', got {result_times}"
    else:
        # Sub-annual timeslices
        expected_times = {f"h{i+1}" for i in range(n_time)}
        assert result_times == expected_times, (
            f"Expected {expected_times}, got {result_times}"
        )

        # Verify we have data for each timeslice
        for time in expected_times:
            time_data = result[result["time"] == time]
            assert len(time_data) > 0, f"No data for timeslice {time}"


@pytest.mark.parametrize("n_time", [1, 2, 12])
def test_add_water_supply(request, test_context, n_time):
    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    # Personalize the context
    sets = {"year": [2020, 2030, 2040]}
    test_context["water build info"] = ScenarioInfo(y0=2020, set=sets)
    test_context.type_reg = "global"
    test_context.regions = "R12"
    nodes = get_codes(f"node/{test_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    map_ISO_c = {test_context.regions: nodes[0]}
    test_context.map_ISO_c = map_ISO_c
    test_context.RCP = "2p6"
    test_context.REL = "med"
    test_context.nexus_set = "nexus"
    # Set up valid_basins for basin filtering
    setup_valid_basins(test_context, regions=test_context.regions)

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
    s.add_set("node", ["loc1", "loc2"])
    s.add_set("year", [2020, 2030, 2040])
    s.commit(comment="Commit test scenario")

    # Set up timeslices
    setup_timeslices(test_context, s, n_time)

    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    test_context.set_scenario(s)
    test_context["water build info"] = ScenarioInfo(s)

    result = add_water_supply(test_context)
    result["input"].to_csv("supply_inp.csv", index=False)
    result["output"].to_csv("supply_out.csv", index=False)
    # Assert the results
    assert isinstance(result, dict)
    assert "input" in result
    assert "output" in result
    assert "var_cost" in result
    assert "technical_lifetime" in result
    assert "inv_cost" in result

    for df in result.values():
        assert isinstance(df, pd.DataFrame)

    # Check for NaN values in input and output DataFrames
    assert not result["input"]["value"].isna().any(), (
        "Input DataFrame contains NaN values"
    )
    assert not result["output"]["value"].isna().any(), (
        "Output DataFrame contains NaN values"
    )
    # Check that time values are not individual characters (common bug)
    input_time_values = result["input"]["time"].unique()
    assert not any(len(str(val)) == 1 for val in input_time_values), (
        f"Input DataFrame contains time values: {input_time_values}. "
    )

    output_time_values = result["output"]["time"].unique()
    assert not any(len(str(val)) == 1 for val in output_time_values), (
        f"Output DataFrame contains time values: {output_time_values}. "
    )
    input_duplicates = result["input"].duplicated().sum()
    assert input_duplicates == 0, (
        f"Input DataFrame contains {input_duplicates} duplicate rows"
    )

    output_duplicates = result["output"].duplicated().sum()
    assert output_duplicates == 0, (
        f"Output DataFrame contains {output_duplicates} duplicate rows"
    )

    # Verify timeslice-specific assertions
    input_times = set(result["input"]["time"].unique())
    output_times = set(result["output"]["time"].unique())

    if n_time == 1:
        # Annual only
        assert input_times == {"year"}, f"Expected only 'year' in input, got {input_times}"
        assert output_times == {"year"}, f"Expected only 'year' in output, got {output_times}"
    else:
        # Sub-annual timeslices
        expected_times = {f"h{i+1}" for i in range(n_time)}
        assert input_times == expected_times, (
            f"Expected {expected_times} in input, got {input_times}"
        )
        assert output_times == expected_times, (
            f"Expected {expected_times} in output, got {output_times}"
        )

        # Verify we have data for each timeslice
        for time in expected_times:
            input_time_data = result["input"][result["input"]["time"] == time]
            assert len(input_time_data) > 0, f"No input data for timeslice {time}"
            output_time_data = result["output"][result["output"]["time"] == time]
            assert len(output_time_data) > 0, f"No output data for timeslice {time}"


@pytest.mark.parametrize("n_time", [1, 2, 12])
def test_add_e_flow(request, test_context, n_time):
    # FIXME You probably want this to be part of a common setup rather than writing
    # something like this for every test
    # Personalize the context
    sets = {"year": [2020, 2030, 2040]}
    test_context["water build info"] = ScenarioInfo(y0=2020, set=sets)
    test_context.regions = "R12"
    nodes = get_codes(f"node/{test_context.regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    map_ISO_c = {test_context.regions: nodes[0]}
    test_context.map_ISO_c = map_ISO_c
    test_context.RCP = "2p6"
    test_context.REL = "med"
    test_context.SDG = True

    # Create minimal scenario for timeslice setup
    mp = test_context.get_platform()
    scenario_info = {
        "mp": mp,
        "model": f"{request.node.name}/test water model",
        "scenario": f"{request.node.name}/test water scenario",
        "version": "new",
    }
    s = Scenario(**scenario_info)
    s.add_horizon(year=[2020, 2030, 2040])
    s.add_set("year", [2020, 2030, 2040])
    s.commit(comment="Test scenario")

    # Set up timeslices
    setup_timeslices(test_context, s, n_time)

    # Set up valid_basins for basin filtering
    setup_valid_basins(test_context, regions=test_context.regions)

    # Call the function to be tested
    result = add_e_flow(test_context)

    # Assert the results
    assert isinstance(result, dict)
    assert "bound_activity_lo" in result
    assert isinstance(result["bound_activity_lo"], pd.DataFrame)

    # Verify timeslice-specific assertions
    if len(result["bound_activity_lo"]) > 0:
        bound_times = set(result["bound_activity_lo"]["time"].unique())

        if n_time == 1:
            # Annual only
            assert bound_times == {"year"}, f"Expected only 'year', got {bound_times}"
        else:
            # Sub-annual timeslices
            expected_times = {f"h{i+1}" for i in range(n_time)}
            assert bound_times == expected_times, (
                f"Expected {expected_times}, got {bound_times}"
            )

            # Verify we have data for each timeslice
            for time in expected_times:
                time_data = result["bound_activity_lo"][result["bound_activity_lo"]["time"] == time]
                assert len(time_data) > 0, f"No data for timeslice {time}"
