import os
from pathlib import Path
from typing import Any, Literal, Optional, Union

import pandas as pd

from message_ix_models import Context, ScenarioInfo
from message_ix_models.util import load_package_data, package_data_path

# Configuration files
METADATA = [
    ("hydrogen", "set"),
]


def read_config() -> Context:
    """Read configuration from set.yaml.

    Returns
    -------
    message_ix_models.Context
        Context object holding information about MESSAGEix-Hydrogen structure
    """
    context = Context.get_instance(-1)

    if "hydrogen set" in context:
        # Already loaded
        return context

    # Load material configuration
    for parts in METADATA:
        # Key for storing in the context
        key = " ".join(parts)

        # Actual filename parts; ends with YAML
        _parts = list(parts)
        _parts[-1] += ".yaml"

        context[key] = load_package_data(*_parts)

    # Use a shorter name
    context["hydrogen"] = context["hydrogen set"]
    return context


def get_ssp_from_context(
    context: Context,
) -> Literal["SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"]:
    """Get selected SSP from context

    Parameters
    ----------
    context

    Returns
    -------
        SSP label
    """
    return "SSP2" if "ssp" not in context else context["ssp"]


def read_sector_data(filename: str) -> pd.DataFrame:
    """Read and clean sector techno-economic data from CSV.

    Parameters
    ----------
    filename : str
        Name of the CSV file to read from data/hydrogen directory

    Returns
    -------
    pd.DataFrame
        Cleaned data with proper column types
    """
    # Read the file
    data = pd.read_csv(package_data_path("hydrogen", filename), comment="#")

    # Drop rows where all values are NaN
    data = data.dropna(how="all")

    # Clean up any extra whitespace in string columns
    for col in data.select_dtypes(include=["object"]).columns:
        data[col] = data[col].str.strip() if data[col].dtype == "object" else data[col]

    return data


def read_timeseries(filename: str) -> pd.DataFrame:
    """Read and clean time-series data from CSV.

    Parameters
    ----------
    filename : str
        Name of the CSV file to read from data/hydrogen directory

    Returns
    -------
    pd.DataFrame
        Cleaned timeseries data
    """
    # Read the file
    data = pd.read_csv(package_data_path("hydrogen", filename), comment="#")

    # Drop rows where all values are NaN
    data = data.dropna(how="all")

    # Ensure year is integer type
    if "year" in data.columns:
        data["year"] = data["year"].astype(int)

    return data


def read_rel(filename: str) -> pd.DataFrame:
    """Read and clean relations/constraints data from CSV.

    Parameters
    ----------
    filename : str
        Name of the CSV file to read from data/hydrogen directory

    Returns
    -------
    pd.DataFrame
        Cleaned relations data
    """
    # Read the file
    data = pd.read_csv(package_data_path("hydrogen", filename), comment="#")

    # Drop rows where all values are NaN
    data = data.dropna(how="all")

    return data


def read_historical_data(filename: str) -> pd.DataFrame:
    """Read and clean historical calibration data from CSV.

    Parameters
    ----------
    filename : str
        Name of the CSV file to read from data/hydrogen directory

    Returns
    -------
    pd.DataFrame
        Cleaned historical data
    """
    # Read the file
    data = pd.read_csv(package_data_path("hydrogen", filename), comment="#")

    # Drop rows where all values are NaN
    data = data.dropna(how="all")

    # Convert year columns to appropriate types
    for col in ["year_act", "year_vtg"]:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

    return data
