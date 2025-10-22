import os
from pathlib import Path
from typing import Any, Literal, Optional, Union

import pandas as pd

from message_ix_models import Context, ScenarioInfo
from message_ix_models.util import load_package_data, package_data_path
from message_ix_models.util.transaction import transact

# Configuration files
METADATA = [
    ("hydrogen", "set"),
]


def load_hydrogen_parameters():
    """
    this method will load the hydrogen parameters for the technologies it
    finds in the data/hydrogen/set.yaml file.

    it looks inside the data/hydrogen/parameters folder for all the folder names
    that match the technology names, and then looks inside those folders for the
    parameter csv files. The CSV file names must match the parameter names in
    MESSAGEix.


    """

    # load the hydrogen techs to be added
    techs = get_requirements()["hydrogen"]["technology"]["add"]

    for tec in techs:
        # build the path to the parameter files
        param_path = package_data_path("hydrogen", "parameters", tec)

        # check if the path exists
        if not os.path.exists(param_path):
            print(
                f" WARNING: No parameter folder found for technology {tec} "
                f"at {param_path}. Skipping."
            )
            continue

        # loop over all csv files in the folder
        for file in os.listdir(param_path):
            if file.endswith(".csv"):
                param_name = file[:-4]  # remove .csv extension
                file_path = os.path.join(param_path, file)
                print(
                    f" Loading parameter {param_name} for technology {tec} from {file_path}"
                )

                # read the parameter data
                param_data = pd.read_csv(file_path)

                # Here you would add the code to insert the parameter data into the MESSAGEix scenario
                # This is a placeholder print statement
                print(f" Parameter data for {param_name}:\n{param_data.head()}")

    return param_data


def add_hydrogen_sets(scen, ssp="SSP2"):
    """
    Add new hydrogen related sets to a MESSAGEix scenario
    It takes as input a MESSAGEix scenario object,
    This method only adds the sets, not the parameters.
    The sets to be added are defined in data/hydrogen/set.yaml

    # Parameters:
    - scen: MESSAGEix Scenario Object
    - ssp: default is SSP2
    -----------
    """
    # TODO: we can actually take the keys from the set.yaml file
    # and check what are the sets that have the "add" key and loop over them.
    # I would do something like this:
    # sets = [k for k in req["hydrogen"].keys() if "add" in req["hydrogen"][k]]

    # load the hydrogen techs to be added
    reqs = get_requirements()

    # extract the technologies:
    tecs = reqs["technology"]["add"]
    techs_to_add = check_sets_in_scenario(scen, tecs, "technology")
    with scen.transact(commit_message="Adding hydrogen Techs"):
        for tec in techs_to_add:
            scen.add_set("technology", tec)

    # we also need to add the black carbon commodity
    commodities = reqs["commodity"]["add"]
    commodities_to_add = check_sets_in_scenario(scen, commodities, "commodity")

    with scen.transact(commit_message="Adding hydrogen Commodities"):
        for com in commodities_to_add:
            scen.add_set("commodity", com)

    # finally, we need to add the leakage emissions
    emissions = reqs["emission"]["add"]
    emissions_to_add = check_sets_in_scenario(scen, emissions, "emission")
    with scen.transact(commit_message="Adding hydrogen Emissions"):
        for em in emissions_to_add:
            scen.add_set("emission", em)


def check_sets_in_scenario(scen, tech_list: list[str], set_name: str) -> list[str]:
    """
    Check if hydrogen technologies are already in the scenario

    Parameters:
    -----------
    - scen: MESSAGEix Scenario Object

    Returns:
    --------
    - te: True if all hydrogen techs are in the scenario, False otherwise
    """

    existing_entries = scen.set(set_name)
    entries_to_add = []
    for tec in tech_list:
        if tec not in existing_entries:
            entries_to_add.append(tec)
        if tec in existing_entries:
            print(f" WARNING: Technology {tec} already exists in the scenario.")
    return entries_to_add


def get_requirements():
    """Get requirements from hydrogen set.yaml.

    Returns
    -------
    dict
        Requirements specified in hydrogen set.yaml
    """
    context = read_config()
    return context["hydrogen set"]


## first of all


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
