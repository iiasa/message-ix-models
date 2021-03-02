"""Retrieve transport activity data from China (Statistical Yearbook of the National
Bureau of Statistics) and from India (iTEM)."""
import numpy as np
import pandas as pd

from item import historical
from message_data.tools import get_context
from message_data.tools.convert_units import convert_units
from message_data.tools.iea_eei import split_units


UNITS = {
    "Population": (1.0e-6, None, "dimensionless"),
    "Vehicle stock": (1.0e4, None, "vehicle"),
    "Passenger-Kilometers": (100, None, "megapassenger km"),
    "Ton-Kilometers": (100, None, "megaton km"),
}

FILES = {
    "Passenger activity": ("Passenger-km.csv", 4),
    "Vehicle stock civil": ("Civil-Vehicles.csv", 5),
    "Vehicle stock private": ("Private-Vehicles.csv", 1),
    "Freight activity": ("Tonne-km.csv", 5),
}

POP_FILE = "pop_CHN_IND.csv"


def split_variable(s):
    """Split strings in :class:`pandas.Series` *s* into Variable and Mode.

    Parameters
    ----------
    s : pandas.Series

    Returns
    -------
    DataFrame : pandas.DataFrame
        DataFrame with two columns: Variable names and their respective Mode.
    """
    # Split str in *s* into variable name and units
    df = s.str.rsplit(pat=" of ", n=1, expand=True)
    df[1].fillna("All", inplace=True)

    # Remove remaining parentheses from units column and assign labels
    df[1] = df[1].str.replace(")", "").str.replace("/", " / ")
    df.columns = ["Var", "Vehicle Type"]
    return df


def get_chn_ind_pop(ctx):
    """Retrieve population data for China and India.

    The dataset is a ``.csv`` file in */data* and was retrieved from `OECD
    <https://stats.oecd.org/Index.aspx?#>`_ website, filtering data for China and India.
    """
    # Read csv file
    pop = pd.read_csv(ctx.get_path("transport", POP_FILE), header=0)
    # Drop irrelevant columns and rename when necessary
    pop = pop.drop(
        [x for x in pop.columns if x not in ["LOCATION", "Time", "Value"]],
        axis=1,
    ).rename(columns={"LOCATION": "ISO Code", "Time": "Year"})
    # Add "Variable" name column
    pop["Variable"] = "Population"
    # Convert population units into millions using dict **UNITS** above
    pop["Value"] = convert_units(
        pop["Value"].rename("Population"), ctx, dict_units=UNITS
    ).apply(lambda qty: qty.magnitude)
    return pop


def get_chn_ind_data(ctx):
    """Read transport activity data from China and India.

    The data is read from from ``/China`` folder (data for China) and imported from
    iTEM project (data for India), and the processed data is merged into IEA's EEI
    datasets for scenario calibration.

    Parameters
    ----------
    ctx : .Context
        Information about target Scenario.
    """
    # Load and process data from China
    df = pd.DataFrame()
    for file, skip_footer in FILES.values():
        # Read excel sheet
        df_aux = pd.read_csv(
            ctx.get_path("transport", "China", file), skipfooter=skip_footer, header=2
        )
        df = pd.concat([df, df_aux], ignore_index=True)
    # Drop rows containing sub-categories of rail transport
    df.drop([2, 3, 4, 34, 35, 36], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df = pd.concat([df, split_units(df["Indicators"], pat="(")], axis=1).drop(
        ["Indicators"], axis=1
    )
    df["Variable"] = df["Variable"].str.replace("Possession", "Vehicle Stock")
    # Reach **tidy data** structure
    df = df.melt(
        id_vars=["Variable", "Units"], var_name="Year", value_name="Value"
    ).sort_values("Year")

    # Add "ISO Code" column to *df*, and move to first position
    df["ISO Code"] = "CHN"
    df = df.set_index("ISO Code").reset_index()

    df["Year"] = pd.to_numeric(df["Year"])

    # Drop 2019 values so it can concat with population values
    df.drop(df[df["Year"] == 2019].index, inplace=True)

    # Split Variable column into Variable and Mode
    df = (
        pd.concat([df, split_variable(df["Variable"])], axis=1)
        .drop(["Variable"], axis=1)
        .rename(columns={"Var": "Variable"})
    )

    # Reorder columns of *df*
    cols = df.columns.tolist()
    cols = [cols[0]] + cols[-2:] + cols[1:4]
    df = df[cols]

    # Concat population values
    df = pd.concat([df, get_chn_ind_pop(ctx)], ignore_index=True).sort_values(
        ["ISO Code", "Variable", "Year"], ignore_index=True
    )

    # Import data from iTEM database file T000.csv, including inland passenger
    # transport activity data
    df_raw = historical.process(0)
    df_raw.drop(
        columns=["Source", "Country", "Region", "Technology", "Fuel", "ID"],
        inplace=True,
    )
    # Filter values for CHN & IND between 2000-2018
    df_raw = df_raw[
        (df_raw["ISO Code"].isin(["IND", "CHN"]))
        & (df_raw["Year"].isin(list(np.arange(2000, 2019))))
    ].sort_values(["ISO Code", "Mode"], ignore_index=True)

    chn = df[df["ISO Code"] == "CHN"].pivot(
        index="Year", columns=["Variable", "Vehicle Type"], values="Value"
    )
    for var in list(df.columns.get_level_values("Variable")):
        df[var] = df[var].apply(convert_units, context=ctx, dict_units=UNITS)

    ind = (
        df[df["ISO Code"] == "IND"]
        .pivot(index="Year", columns="Variable", values="Value")
        .apply(convert_units, context=ctx, dict_units=UNITS)
    )

    return df

