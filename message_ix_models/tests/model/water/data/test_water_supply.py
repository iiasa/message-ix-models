import pandas as pd
from message_ix import Scenario

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.model.water.data.water_supply import (
    add_e_flow,
    add_water_supply,
    map_basin_region_wat,
)


def test_map_basin_region_wat(test_context):
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
    test_context.time = "year"
    # Set up valid_basins for water_for_ppl functions
    # Read all basins from the basin delineation file to avoid filtering
    from message_ix_models.util import package_data_path

    basin_file = f"basins_by_region_simpl_{test_context.regions}.csv"
    basin_path = package_data_path("water", "delineation", basin_file)
    df_basins = pd.read_csv(basin_path)
    test_context.valid_basins = set(df_basins["BCU_name"].astype(str))

    result = map_basin_region_wat(test_context)

    # Assert the results
    assert isinstance(result, pd.DataFrame)
    assert all(
        col in result.columns
        for col in ["region", "mode", "date", "MSGREG", "share", "year", "time"]
    )


def test_add_water_supply(request, test_context):
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
    test_context.time = "year"
    test_context.nexus_set = "nexus"
    # Set up valid_basins for water_for_ppl functions
    # Read all basins from the basin delineation file to avoid filtering
    from message_ix_models.util import package_data_path

    basin_file = f"basins_by_region_simpl_{test_context.regions}.csv"
    basin_path = package_data_path("water", "delineation", basin_file)
    df_basins = pd.read_csv(basin_path)
    test_context.valid_basins = set(df_basins["BCU_name"].astype(str))

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
    assert "growth_activity_up" in result

    # Verify growth_activity_up DataFrame is properly populated
    growth_df = result["growth_activity_up"]
    assert isinstance(growth_df, pd.DataFrame), (
        "growth_activity_up should be a DataFrame"
    )
    assert not growth_df.empty, (
        "growth_activity_up DataFrame should not be empty"
    )

    # Check for extract_surfacewater entries
    extract_sw = growth_df[growth_df["technology"] == "extract_surfacewater"]
    assert not extract_sw.empty, (
        "growth_activity_up should contain extract_surfacewater entries"
    )

    # Verify correct value
    assert (extract_sw["value"] == 0.02).all(), (
        f"growth_activity_up for extract_surfacewater should be 0.02, "
        f"got values: {extract_sw['value'].unique()}"
    )

    # Verify time column is not null
    assert not extract_sw["time"].isna().any(), (
        "growth_activity_up time column should not contain NaN values"
    )

    # Verify required columns exist
    required_cols = ["technology", "node_loc", "year_act", "time", "value", "unit"]
    assert all(col in growth_df.columns for col in required_cols), (
        f"growth_activity_up missing required columns. "
        f"Has: {growth_df.columns.tolist()}, needs: {required_cols}"
    )

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


def test_add_e_flow(test_context):
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
    test_context.time = "year"
    test_context.SDG = True

    # Set up valid_basins for water_for_ppl functions
    # Read all basins from the basin delineation file to avoid filtering
    from message_ix_models.util import package_data_path

    basin_file = f"basins_by_region_simpl_{test_context.regions}.csv"
    basin_path = package_data_path("water", "delineation", basin_file)
    df_basins = pd.read_csv(basin_path)
    test_context.valid_basins = set(df_basins["BCU_name"].astype(str))

    # Call the function to be tested
    result = add_e_flow(test_context)

    # Assert the results
    assert isinstance(result, dict)
    assert "bound_activity_lo" in result
    assert isinstance(result["bound_activity_lo"], pd.DataFrame)
