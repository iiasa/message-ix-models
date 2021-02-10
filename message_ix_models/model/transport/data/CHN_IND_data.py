"""Retrieve transport activity data from China (Statistical Yearbook of the National
Bureau of Statistics) and from India (iTEM)."""
import numpy as np
import pandas as pd
from pycountry import countries
import yaml

from message_data.model.transport import read_config
from message_data.model.transport.data.ikarus import convert_units
from message_data.tools import make_df, same_node, set_info, Code, get_context

FILES = {"Passenger activity": ("Passenger-km.csv", 4),
         "Vehicle stock civil": ("Civil-Vehicles.csv", 5),
         "Vehicle stock private": ("Private-Vehicles.csv", 1),
         "Freight activity": ("Tonne-km.csv", 5),
         }


# TODO: import split_units from iea_eei.py and add extra argument for *pat* in .rsplit()
def split_units(s):
    """Split units (btw parentheses) from variable names of :class:`pandas.Series` *s*.

    Parameters
    ----------
    s : pandas.Series

    Returns
    -------
    DataFrame : pandas.DataFrame
        DataFrame with two columns: variable names and their respective units.
    """
    # Split str in *s* into variable name and units
    df = s.str.rsplit(pat="(", n=1, expand=True)

    # Remove remaining parentheses from units column and assign labels
    df[1] = df[1].str.replace(")", "").str.replace("/", " / ")
    df.columns = ["Variable", "Units"]
    return df


def get_pop():

    return


def get_chn_ind_data(context):
    """Read transport activity data from China and India.

    The data is read from from ``/China`` folder and imported from iTEM project,
    and the processed data is merged into IEA's EEI datasets for scenario calibration.

    Parameters
    ----------
    context : .Context
        Information about target Scenario.
    """
    # Load and process data from China
    df = pd.DataFrame()
    for file, skip_footer in FILES.values():
        # Read excel sheet
        df_aux = pd.read_csv(context.get_path("transport", "China", file),
                             skipfooter=skip_footer, header=2)
        df = pd.concat([df, df_aux], ignore_index=True)
    # Drop rows containing sub-categories of rail transport
    df.drop([2, 3, 4, 34, 35, 36], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df = pd.concat([df, split_units(df["Indicators"])], axis=1) \
           .drop(["Indicators"], axis=1)
    # Reach **tidy data** structure
    df = df.melt(id_vars=["Variable", "Units"], var_name="Year",
                 value_name="Value").sort_values("Year")
    df = df.pivot(index="Year", columns="Variable", values="Value")


    return df

