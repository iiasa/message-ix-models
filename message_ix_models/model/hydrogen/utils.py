import os

# from pathlib import Path
from typing import Literal

# from message_ix_models.util.transaction import transact
import message_ix
import pandas as pd

from message_ix_models import Context
from message_ix_models.util import load_package_data, package_data_path

# Configuration files
METADATA = [
    ("hydrogen", "set"),
]


def load_commodity_sets(scenario: message_ix.Scenario):
    """
    this method will load the commodity sets for the commodities it finds in
    data/hydrogen/set.yaml file.
    it looks inside the data/hydrogen/sets folder for all the folder names
    that match the commodity names, and then looks inside those folders for the
    set csv files. The CSV file names must match the set names in MESSAGEix.
    This method will automatically add the sets to the scenario.
    """
    commodities = get_requirements()["hydrogen"]["commodity"]["add"]
    for commodity in commodities:
        # build the path to the set files
        set_path = package_data_path("hydrogen", "sets", commodity)

        # check if the path exists
        if not os.path.exists(set_path):
            print(
                f" WARNING: No set folder found for commodity {commodity} "
                f"at {set_path}. Skipping."
            )
            continue

        # loop over all csv files in the folder
        for file in os.listdir(set_path):

            # skip files that start with is_ (automatically generated sets)
            if file.endswith(".csv") and not file.startswith("is_"):
                set_name = file[:-4]  # remove .csv extension
                file_path = os.path.join(set_path, file)
                print(f"Loading set {set_name} for commodity {commodity}")

                # read the set data
                if set_name in ["technology", "commodity", "emission"]:
                    set_data = pd.read_csv(file_path, header=None)
                    print(f"Adding {set_name} set for commodity {commodity}")
                    scenario.check_out()
                    set_values = set_data[0].to_list()
                    scenario.add_set(set_name, set_values)
                    scenario.commit(f"Add {commodity} commodity sets")
                    scenario.set_as_default()
                    # df_commodity = pd.DataFrame(scenario.set("commodity"))
                    # print(df_commodity)
                    # # Filter by the first column (index 0) instead of column name "0"
                    # df_commodity = df_commodity[df_commodity.iloc[:, 0] == commodity]
                    # print(f"Verification - {commodity} in commodity set:")
                    # print(df_commodity)
                else:
                    set_data = pd.read_csv(file_path)
                    print(f"Adding set {set_name}")
                    scenario.check_out()
                    scenario.add_set(set_name, set_data)
                    scenario.commit(f"Add {commodity} commodity sets")
                    scenario.set_as_default()


def load_emission_sets(scenario: message_ix.Scenario):
    """
    this method will load the emission sets for the emissions it finds in
    data/hydrogen/set.yaml file.
    it looks inside the data/hydrogen/sets folder for all the folder names
    that match the emission names, and then looks inside those folders for the
    set csv files. The CSV file names must match the set names in MESSAGEix.
    This method will automatically add the sets to the scenario.
    """
    emissions = get_requirements()["hydrogen"]["emission"]["add"]
    for emission in emissions:
        # build the path to the set files
        set_path = package_data_path("hydrogen", "sets", emission)

        # check if the path exists
        if not os.path.exists(set_path):
            print(
                f" WARNING: No set folder found for emission {emission} "
                f"at {set_path}. Skipping."
            )
            continue

        # loop over all csv files in the folder
        for file in os.listdir(set_path):

            # skip files that start with is_ (automatically generated sets)
            if file.endswith(".csv") and not file.startswith("is_"):
                set_name = file[:-4]  # remove .csv extension
                file_path = os.path.join(set_path, file)
                print(f"Loading set {set_name} for emission {emission}")

                # read the set data
                if set_name in ["technology", "commodity", "emission"]:
                    # print(f"SET NAME IS {set_name}")
                    set_data = pd.read_csv(file_path, header=None)
                    # print(f"Adding {set_name} set")
                    scenario.check_out()
                    set_values = set_data[0].to_list()
                    # print(f"SET VALUES ARE {set_values}")
                    scenario.add_set(set_name, set_values)
                    scenario.commit(f"Add {emission} emission sets")
                    scenario.set_as_default()
                    # df_emission = pd.DataFrame(scenario.set("emission"))
                    # print(df_emission)
                    # # Filter by the first column (index 0) instead of column name "0"
                    # df_emission = df_emission[df_emission.iloc[:, 0] == emission]
                    # print(f"Verification - {emission} in emission set:")
                    # print(df_emission)
                else:
                    set_data = pd.read_csv(file_path)
                    print(f"Adding set {set_name}")
                    scenario.check_out()
                    scenario.add_set(set_name, set_data)
                    scenario.commit(f"Add {emission} emission sets")
                    scenario.set_as_default()


def remove_deprecated_sets(scenario: message_ix.Scenario):
    """
    this method will remove the deprecated sets for the technologies it finds in
    data/hydrogen/set.yaml file.
    it looks inside the data/hydrogen/sets folder for all the folder names
    that match the technology names, and then looks inside those folders for the
    set yaml files. The YAML file names must match the set names in MESSAGEix.
    This method will automatically remove the sets from the scenario.
    """
    deprecated_sets = get_requirements()["hydrogen"]["technology"]["remove"]
    for set_name in deprecated_sets:
        scenario.check_out()
        scenario.remove_set("technology", set_name)
        scenario.commit(f"Remove deprecated set {set_name}")


def load_hydrogen_sets(scenario: message_ix.Scenario):
    """
    this method will load the hydrogen sets for the technologies it finds in
    data/hydrogen/set.yaml file.
    it looks inside the data/hydrogen/sets folder for all the folder names
    that match the technology names, and then looks inside those folders for the
    set yaml files. The YAML file names must match the set names in MESSAGEix.
    This method will automatically add the sets to the scenario.


    ## Parameters:ß
    - scenario: MESSAGEix Scenario Object
    """

    # load the hydrogen techs to be added:
    techs = get_requirements()["hydrogen"]["technology"]["add"]
    for tech in techs:
        # build the path to the set files
        set_path = package_data_path("hydrogen", "sets", tech)

        # check if the path exists
        if not os.path.exists(set_path):
            print(
                f" WARNING: No set folder found for technology {tech} "
                f"at {set_path}. Skipping."
            )
            continue

        # loop over all csv files in the folder
        for file in os.listdir(set_path):

            # we want to skip the files that start with is_
            # these are automatically generated sets.
            # We don't need to add them manually.
            if file.endswith(".csv") and not file.startswith("is_"):
                set_name = file[:-4]  # remove .csv extension
                file_path = os.path.join(set_path, file)
                print(f"Loading set {set_name}")

                # read the set data
                # Here you would add the code to insert the set data into the
                # MESSAGEix scenario This is a placeholder print statement
                if set_name in ["technology", "commodity", "emission"]:
                    set_data = pd.read_csv(file_path, header=None)
                    print(f"Adding {set_name} Set for {tech}")
                    scenario.check_out()
                    tech_name_set = set_data[0].to_list()
                    scenario.add_set(set_name, tech_name_set)
                    scenario.commit("Add hydrogen technology sets")
                    scenario.set_as_default()
                else:
                    set_data = pd.read_csv(file_path)
                    print(f"Adding {set_name} Set for {tech}")
                    scenario.check_out()
                    scenario.add_set(set_name, set_data)
                    scenario.commit("Add hydrogen sets")
                    scenario.set_as_default()

    # return statement is only for testing purposes
    return set_data


def load_hydrogen_parameters(scenario: message_ix.Scenario):
    """
    this method will load the hydrogen parameters for the technologies it
    finds in the data/hydrogen/set.yaml file.

    it looks inside the data/hydrogen/parameters folder for all the folder names
    that match the technology names, and then looks inside those folders for the
    parameter csv files. The CSV file names must match the parameter names in
    MESSAGEix.

    This method will automatically add the parameters to the scenario.

    ## Parameters:
    - scenario: MESSAGEix Scenario Object
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
                # print(f"Retrieving parameter {param_name}")

                # read the parameter data
                param_data = pd.read_csv(file_path)

                # Here you would add the code to insert the parameter data into the
                # MESSAGEix scenario This is a placeholder print statement
                print(f"Adding parameter {param_name} for {tec} to model")

                scenario.check_out()
                scenario.add_par(param_name, param_data)
                scenario.commit("Add hydrogen parameters")
                scenario.set_as_default()

    # return statement is only for testing purposes
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
