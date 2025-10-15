"""Pytest fixtures and test helpers for water module tests."""

import pandas as pd
from message_ix_models.util import package_data_path


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