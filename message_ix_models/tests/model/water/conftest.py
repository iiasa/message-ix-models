"""Fixtures for water module tests."""

import pandas as pd
import pytest
from message_ix import Scenario

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.util import package_data_path


def setup_valid_basins(context, regions="R12"):
    """Set up valid_basins attribute for test contexts.

    This helper function ensures that test contexts have the valid_basins
    attribute that is normally set by the map_basin() function during
    model building. This is required for basin filtering functionality.

    Parameters
    ----------
    context : Context
        Test context object that needs valid_basins attribute
    regions : str, default "R12"
        Region code for basin delineation file
    """
    from message_ix_models.model.water.utils import filter_basins_by_region

    basin_file = f"basins_by_region_simpl_{regions}.csv"
    basin_path = package_data_path("water", "delineation", basin_file)
    df_basins = pd.read_csv(basin_path)

    # Apply basin filtering if enabled
    df_filtered = filter_basins_by_region(df_basins, context)

    # Set valid_basins as set of basin names
    context.valid_basins = set(df_filtered["BCU_name"].astype(str))

    return context


@pytest.fixture
def water_context(test_context, request):
    """Configure test_context with water module defaults.

    Use via indirect parametrization:
        @pytest.mark.parametrize("water_context", [
            {"regions": "R12", "RCP": "6p0", "nexus_set": "nexus"}
        ], indirect=True)
    """
    params = getattr(request, "param", {})

    # Apply defaults
    test_context.regions = params.get("regions", "R11")
    test_context.type_reg = params.get("type_reg", "global")
    test_context.time = params.get("time", "year")
    test_context.nexus_set = params.get("nexus_set", "nexus")

    # Optional attributes
    for attr in ["RCP", "REL", "SDG", "ssp"]:
        if attr in params:
            setattr(test_context, attr, params[attr])

    # Node mapping for country models
    if test_context.type_reg == "country":
        nodes = get_codes(f"node/{test_context.regions}")
        nodes = list(map(str, nodes[nodes.index("World")].child))
        test_context.map_ISO_c = {test_context.regions: nodes[0]}

    # Set up valid_basins for basin filtering
    setup_valid_basins(test_context, regions=test_context.regions)

    return test_context


@pytest.fixture
def water_scenario(request, test_context):
    """Create a basic scenario for water tests.

    Sets up a minimal scenario with horizon years and basic sets,
    then attaches it to test_context with ScenarioInfo.
    """
    mp = test_context.get_platform()
    s = Scenario(
        mp=mp,
        model=f"{request.node.name}/test water model",
        scenario=f"{request.node.name}/test water scenario",
        version="new",
    )
    s.add_horizon(year=[2020, 2030, 2040])
    s.add_set("year", [2020, 2030, 2040])
    s.add_set("technology", ["tech1", "tech2"])
    s.commit(comment="water test scenario")

    test_context.set_scenario(s)
    test_context["water build info"] = ScenarioInfo(s)

    return s


@pytest.fixture
def assert_message_params():
    """Validate MESSAGE-ix parameter DataFrames.

    Returns a function that validates common MESSAGE-ix parameter requirements:
    - No NaN in value columns
    - No duplicate rows
    - Time column not corrupted (single chars)
    - year_vtg <= year_act invariant
    """

    def _assert(result, expected_keys=None):
        """
        Validate result dict from water data functions.

        Parameters
        ----------
        result : dict
            Return value from add_water_supply, cool_tech, etc.
        expected_keys : list, optional
            Keys that must be present (e.g., ["input", "output"])
        """
        assert isinstance(result, dict), "Result must be dict"

        if expected_keys:
            for key in expected_keys:
                assert key in result, f"Missing key: {key}"

        for key, df in result.items():
            if not isinstance(df, pd.DataFrame):
                continue

            # No NaN in value column
            if "value" in df.columns:
                assert not df["value"].isna().any(), f"{key}: NaN in value"

            # No duplicates
            assert df.duplicated().sum() == 0, f"{key}: has duplicates"

            # Time column not corrupted (single chars)
            if "time" in df.columns:
                times = df["time"].unique()
                bad = [t for t in times if isinstance(t, str) and len(t) == 1]
                assert not bad, f"{key}: invalid time values {bad}"

            # year_vtg <= year_act
            if "year_vtg" in df.columns and "year_act" in df.columns:
                assert (df["year_vtg"] <= df["year_act"]).all(), (
                    f"{key}: year_vtg > year_act"
                )

    return _assert


@pytest.fixture
def assert_input_output_structure():
    """Validate input/output DataFrame columns per MESSAGE-ix spec.

    Returns a function that checks for required columns in input/output parameters.
    """

    INPUT_COLS = [
        "node_loc",
        "technology",
        "year_vtg",
        "year_act",
        "mode",
        "node_origin",
        "commodity",
        "level",
        "time",
        "time_origin",
        "value",
        "unit",
    ]
    OUTPUT_COLS = [
        "node_loc",
        "technology",
        "year_vtg",
        "year_act",
        "mode",
        "node_dest",
        "commodity",
        "level",
        "time",
        "time_dest",
        "value",
        "unit",
    ]

    def _assert(result):
        if "input" in result:
            missing = set(INPUT_COLS) - set(result["input"].columns)
            assert not missing, f"input missing columns: {missing}"

        if "output" in result:
            missing = set(OUTPUT_COLS) - set(result["output"].columns)
            assert not missing, f"output missing columns: {missing}"

    return _assert
