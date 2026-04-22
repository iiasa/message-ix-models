import pandas as pd
import plotly.express as px
from message_ix_models.util import package_data_path
from itertools import combinations
from pathlib import Path
import numpy as np
import os

from message_ix_models.project.alps_hhi.maps.base_regions import base_regions_map
from message_ix_models.tools.bilateralize.utils import load_config
from message_ix_models.tools.bilateralize.pull_gem import gem_region

# Function to set up pipeline dataframe
def import_gem(
    project_name: str | None = None,
    config_name: str | None = None,
    input_file: str | Path = "GEM-GGIT-Gas-Pipelines-2024-12.csv",
    ):
    """
    Import Global Energy Monitor data

    Args:
        input_file: Name of input file
        project_name: Name of project
        config_name: Name of config file
    """

    # Pull in configuration
    config, config_path = load_config(
        project_name=project_name, config_name=config_name
    )
    p_drive = config["p_drive_location"]

    # Data paths
    data_path = os.path.join(p_drive, "MESSAGE_trade")
    gem_path = os.path.join(data_path, "Global Energy Monitor")

    # df = pd.read_excel(os.path.join(gem_path, input_file), sheet_name=input_sheet)
    df = pd.read_csv(os.path.join(gem_path, input_file))

    df = df[df["StopYear"].isnull()]  # Only continuing projects

    df = df[
        [
            "StartYear1",
            "StartCountry",
            "EndCountry",
            "CapacityBOEd",
            "CostUSD",
            "LengthMergedKm",
        ]
    ].drop_duplicates()

    # Clean up country codes
    cw = pd.read_csv(os.path.join(gem_path, "country_crosswalk.csv"))
    for i in ["Start", "End"]:
        df = df.merge(cw, left_on=i + "Country", right_on="GEM Country", how="left")
        df = df.rename(columns={"ISO": i + "ISO"})

    # Add MESSAGE regions
    message_regions_list, message_regions = gem_region(project_name, config_name)
    df["EXPORTER"] = ""
    df["IMPORTER"] = ""
    for r in message_regions_list:
        df["EXPORTER"] = np.where(
            df["StartISO"].isin(message_regions[r]["child"]), r, df["EXPORTER"]
        )
        df["IMPORTER"] = np.where(
            df["EndISO"].isin(message_regions[r]["child"]), r, df["IMPORTER"]
        )

    df['Interregional'] = np.where(df['IMPORTER'] != df['EXPORTER'], 1, 0)

    df['CapacityBOEd'] = df['CapacityBOEd'].replace("--", "0", regex=True).astype(float)
    df = df.groupby(['StartISO', 'EndISO', 'Interregional', 'EXPORTER', 'IMPORTER']).agg({'CapacityBOEd': 'sum'}).reset_index()
    df['CapacityBOEd'] = round(df['CapacityBOEd'], 3)
    df = df[df['CapacityBOEd'] > 0]

    return df

# Read in interregional pipelines
pipelines = import_gem(project_name = "alps_hhi",
                       config_name = "config.yaml",
                       input_file = "GEM-GGIT-Gas-Pipelines-2024-12.csv")

pipelines.to_csv(package_data_path("alps_hhi", "maps", "pipelines.csv"), index=False)