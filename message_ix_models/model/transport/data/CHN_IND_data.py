"""Retrieve transport activity data from China (Statistical Yearbook of the National
Bureau of Statistics) and from India (iTEM)."""
import numpy as np
import pandas as pd
from pycountry import countries
import yaml

from message_data.model.transport import read_config
from message_data.model.transport.data.ikarus import convert_units
from message_data.tools import make_df, same_node, set_info, Code, get_context


UNITS = dict(
    Population=(1.0e-6, None, None),
)

FILES = {"Passenger activity": ("Passenger-km.csv", 4),
         "Vehicle stock civil": ("Civil-Vehicles.csv", 5),
         "Vehicle stock private": ("Private-Vehicles.csv", 1),
         "Freight activity": ("Tonne-km.csv", 5),
         }

POP_FILE = "pop_CHN_IND.csv"


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


def get_chn_ind_pop(ctx):
    """Retrieve population data from for China and India.

    The dataset is a ``.csv`` file in */data* and was retrieved from `OECD
    <https://stats.oecd.org/Index.aspx?#>`_ website, filtering data for China and India.
    """
    pop = pd.read_csv(ctx.get_path("transport", POP_FILE), header=0)
    pop.rename(columns={"LOCATION": "ISO_code", "Time": "Year"}, inplace=True)
    pop.drop([x for x in pop.columns if x not in ["ISO_code", "Year", "Value"]],
             axis=1, inplace=True)
    # Add "Variable" name column
    pop["Variable"] = "Population"

    # TODO edit code below accordingly once *convert_units* will be migrated to /tools
    s = pop["Value"]
    factor, unit_in, unit_out = UNITS["Population"]
    # Convert the values to a pint.Quantity(array) with the input units
    qty = ctx.units.Quantity(factor * s.values, unit_in)
    # Convert to output units, then to a list of scalar Quantity
    pop["Value"] = pd.Series(qty.magnitude.tolist(), index=s.index)

    return pop


def get_chn_ind_data(ctx):
    """Read transport activity data from China and India.

    The data is read from from ``/China`` folder and imported from iTEM project,
    and the processed data is merged into IEA's EEI datasets for scenario calibration.

    Parameters
    ----------
    ctx : .Context
        Information about target Scenario.
    """
    # Load and process data from China
    df = pd.DataFrame()
    for file, skip_footer in FILES.values():
        # Read excel sheet
        df_aux = pd.read_csv(ctx.get_path("transport", "China", file),
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
    # Add "ISO_code" column to *df*, and move to first position
    df["ISO_code"] = "CHN"
    df.set_index("ISO_code", inplace=True)
    df.reset_index(inplace=True)
    df["Year"] = pd.to_numeric(df["Year"])

    # Drop 2019 values so it can concat with population values
    df = df.drop(df[df["Year"] == 2019].index)

    # Concat population values
    df = pd.concat([df, get_chn_ind_pop(ctx)], ignore_index=True) \
           .sort_values(["ISO_code", "Variable", "Year"], ignore_index=True)
    chn = df[df["ISO_code"] == "CHN"].pivot(index="Year", columns="Variable",
                                            values="Value")
    ind = df[df["ISO_code"] == "IND"].pivot(index="Year", columns="Variable",
                                            values="Value")

    return df

