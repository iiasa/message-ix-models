"""Pytest fixtures and test helpers for water module tests."""

import json
from pathlib import Path

import pandas as pd
import pytest

from message_ix_models import ScenarioInfo
from message_ix_models.util import package_data_path

FIXTURE_DIR = Path(__file__).parent / "data" / "fixtures"


class MockScenario:
    """Mock scenario that returns fixture data for .par() calls.

    Provides minimal interface needed by water module functions:
    - par(name, filters) -> DataFrame
    - firstmodelyear -> int
    """

    def __init__(self, fixture_df: pd.DataFrame, firstmodelyear: int = 2030):
        self._data = {}
        for param in fixture_df["_param"].unique():
            df = fixture_df[fixture_df["_param"] == param].drop(columns=["_param"]).copy()
            df = df.dropna(axis=1, how="all")
            # Convert year columns to int
            for col in ["year_vtg", "year_act"]:
                if col in df.columns:
                    df[col] = df[col].astype(int)
            # Convert value column to float
            if "value" in df.columns:
                df["value"] = pd.to_numeric(df["value"], errors="coerce")
            self._data[param] = df
        self.firstmodelyear = firstmodelyear

    def par(self, name: str, filters: dict = None) -> pd.DataFrame:
        """Return parameter data, optionally filtered."""
        df = self._data.get(name, pd.DataFrame())
        if filters and not df.empty:
            for col, values in filters.items():
                if col in df.columns:
                    if not isinstance(values, (list, pd.Series)):
                        values = [values]
                    df = df[df[col].isin(values)]
        return df.copy()


def load_cool_tech_fixture():
    """Load the cool_tech test fixture data.

    Returns
    -------
    tuple[pd.DataFrame, dict]
        (fixture_df, metadata) where fixture_df contains all parameter data
        and metadata contains years, nodes, firstmodelyear, etc.
    """
    parquet_path = FIXTURE_DIR / "cool_tech_fixture.parquet"
    meta_path = FIXTURE_DIR / "cool_tech_meta.json"

    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Fixture not found at {parquet_path}. "
            "Run extract script to generate from ixmp_dev."
        )

    fixture_df = pd.read_parquet(parquet_path)
    with open(meta_path) as f:
        meta = json.load(f)

    return fixture_df, meta


def create_mock_scenario_info(meta: dict) -> ScenarioInfo:
    """Create a ScenarioInfo from fixture metadata without needing ixmp.

    Parameters
    ----------
    meta : dict
        Metadata dict with keys: years, nodes, firstmodelyear

    Returns
    -------
    ScenarioInfo
        Populated with year/node sets for use in water module functions.
    """
    info = ScenarioInfo()
    info.set["year"] = [int(y) for y in meta["years"]]
    info.set["node"] = meta["nodes"]
    info.y0 = int(meta["firstmodelyear"])
    return info


@pytest.fixture
def cool_tech_fixture():
    """Pytest fixture providing mock scenario and ScenarioInfo for cool_tech tests.

    Returns
    -------
    tuple[MockScenario, ScenarioInfo, dict]
        (mock_scenario, scenario_info, metadata)
    """
    fixture_df, meta = load_cool_tech_fixture()
    mock_scen = MockScenario(fixture_df, firstmodelyear=meta["firstmodelyear"])
    info = create_mock_scenario_info(meta)
    return mock_scen, info, meta


def setup_timeslices(context, scenario, n_time):
    """Set up sub-annual timeslices for test scenarios.

    This helper function creates timeslice sets for testing water module
    functionality with different temporal resolutions.

    Parameters
    ----------
    context : Context
        Test context object that needs time attribute set
    scenario : Scenario
        Test scenario to add timeslices to
    n_time : int
        Number of timeslices per year:
        - 1: Annual only (no sub-annual)
        - 2: Seasonal (h1, h2)
        - 12: Monthly (h1, h2, ..., h12)

    Returns
    -------
    context : Context
        Context with time attribute set to list of timeslice names
    """
    if n_time == 1:
        # Annual only - no sub-annual timeslices
        context.time = ["year"]
        return context

    # Generate timeslice names
    timeslices = [f"h{i+1}" for i in range(n_time)]

    # Add timeslices to scenario using transact context manager
    with scenario.transact(f"Add {n_time} timeslices for test"):
        existing_time = set(scenario.set("time"))
        for t in timeslices:
            if t not in existing_time:
                scenario.add_set("time", t)

    # Set context.time to list of timeslice names (excluding 'year')
    context.time = timeslices

    return context


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
    # Read basin delineation file to get all basins
    basin_file = f"basins_by_region_simpl_{regions}.csv"
    basin_path = package_data_path("water", "delineation", basin_file)
    df_basins = pd.read_csv(basin_path)

    # Apply basin filtering if enabled
    from message_ix_models.model.water.utils import filter_basins_by_region
    df_filtered = filter_basins_by_region(df_basins, context)

    # Set valid_basins as set of basin names
    context.valid_basins = set(df_filtered["BCU_name"].astype(str))

    return context