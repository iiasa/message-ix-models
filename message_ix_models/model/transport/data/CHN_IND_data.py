"""Retrieve transport activity data from China (Statistical Yearbook of the National
Bureau of Statistics) and from India (iTEM)."""
import numpy as np
import pandas as pd

from item import historical
from message_data.tools.convert_units import convert_units
from message_data.tools.iea_eei import split_units
from message_ix_models.util import private_data_path

UNITS = {
    "Population": (1.0e-6, None, "dimensionless"),
    "Vehicle Stock": (1.0e4, "vehicle", "thousand vehicle"),
    "Passenger-Kilometers": (100, "megapkm", "gigapkm"),
    "Freight Ton-Kilometers": (100, "megatkm", "gigatkm"),
}


SDMX_MAP = {
    "REF_AREA": "ISO Code",
    "VARIABLE": "Variable",
    "VEHICLE": "Vehicle type",
    "MODE": "Mode",
    "UNIT": "Units",
    "TIME_PERIOD": "Year",
    "VALUE": "Value",
}
#: Files containing data for China ("name_of_file", rows_to_skip_from_the_bottom)
FILES = {
    "Passenger activity": ("Passenger-km.csv", 4),
    "Vehicle stock civil": ("Civil-Vehicles.csv", 5),
    "Vehicle stock private": ("Private-Vehicles.csv", 1),
    "Freight activity": ("Tonne-km.csv", 5),
}

POP_FILE = "pop_CHN_IND.csv"

# Rail sub-categories to be removed from Chinese dataset
RAIL_SUB_CAT = [
    "Freight Ton-Kilometers of National Railways(100 million ton-km)",
    "Freight Ton-Kilometers of Local Railways(100 million ton-km)",
    "Freight Ton-Kilometers of Joint-venture Railways(100 million ton-km)",
    "Passenger-Kilometers of National Railways(100 million passenger-km)",
    "Passenger-Kilometers of Local Railways(100 million passenger-km)",
    "Passenger-Kilometers of Joint-venture Railways(100 million passenger-km)",
]

# Mapping of variables to vehicle modes
FILL_VALUES = {
    "Passenger-Kilometers": "Total passenger transport",
    "Freight Ton-Kilometers": "Total freight transport",
}


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
    # Remove residual parentheses from variable column, still present in some entries
    for col in list(df.columns):
        df[col] = df[col].str.rsplit(pat="(", n=1, expand=True)[0]
    # Assign labels to columns
    df.columns = ["Var", "Mode/vehicle type"]
    # Use mapping FILL_VALUES to replace NaNs in "Mode/vehicle type" column
    for key, value in FILL_VALUES.items():
        df[df["Var"] == key] = df[df["Var"] == key].fillna(value)
    # Alternative not working:
    # def func(df):
    #     a_func = lambda group: group["target_col"].fillna(FILL_VALUES.get(group[0]))
    #     return df.assign(target_col=a_func)
    # df.groupby(0).pipe(func)
    return df


def get_chn_ind_pop():
    """Retrieve population data for China and India.

    The dataset is a ``.csv`` file in */data* and was retrieved from `OECD
    <https://stats.oecd.org/Index.aspx?#>`_ website, filtering data for China and India.

    Returns
    -------
    DataFrame : pandas.DataFrame
        DataFrame containing population data for China and India.
    """
    # Read csv file
    pop = pd.read_csv(private_data_path("transport", POP_FILE), header=0)
    # Drop irrelevant columns and rename when necessary
    pop = pop.drop(
        [x for x in pop.columns if x not in ["LOCATION", "Time", "Value"]],
        axis=1,
    ).rename(columns={"LOCATION": "ISO Code", "Time": "Year"})
    # Add "Variable" name column
    pop["Variable"] = "Population"
    return pop


def get_chn_ind_data():
    """Read transport activity data from China and India.

    The data is read from from ``/China`` folder (data for China) and imported from
    iTEM project (data for India), and the processed data is merged into IEA's EEI
    datasets for scenario calibration.
    """
    # Load and process data from China
    df = pd.DataFrame()
    for file, skip_footer in FILES.values():
        # Read excel sheet
        df_aux = pd.read_csv(
            private_data_path("transport", "China", file),
            skipfooter=skip_footer,
            header=2,
        )
        df = pd.concat([df, df_aux], ignore_index=True)
    # Drop rows containing sub-categories of rail transport
    df = df.drop(df[df["Indicators"].isin(RAIL_SUB_CAT)].index).reset_index(drop=True)
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

    # Drop 2019 values so it can be concatenated with population values
    df.drop(df[df["Year"] == 2019].index, inplace=True)

    # Split Variable column into Variable and Mode/vehicle type
    df = (
        pd.concat([df, split_variable(df["Variable"])], axis=1)
        # Drop "Variable" and then rename "Var" to "Variable", since "Variable" column
        # was previously returned with that label by split_units()
        .drop(["Variable"], axis=1).rename(columns={"Var": "Variable"})
    )

    # Reorder columns of *df*
    cols = df.columns.tolist()
    cols = [cols[0]] + cols[-2:] + cols[1:4]
    df = df[cols]

    # Concat population values
    df = pd.concat([df, get_chn_ind_pop()], ignore_index=True).sort_values(
        ["ISO Code", "Year", "Variable", "Mode/vehicle type"], ignore_index=True
    )

    # Import data from iTEM database file T000.csv, including inland passenger
    # transport activity data
    df_raw = historical.process(0)
    df_raw.drop(
        columns=[x for x in list(df_raw.columns) if x not in list(SDMX_MAP.keys())],
        inplace=True,
    )
    df_raw.columns = df_raw.columns.to_series().map(SDMX_MAP)
    # Filter values for CHN & IND between 2000-2018
    df_raw = df_raw[
        (df_raw["ISO Code"].isin(["IND", "CHN"]))
        & (df_raw["Year"].isin(list(np.arange(2000, 2019))))
    ].sort_values(["ISO Code", "Mode", "Vehicle type"], ignore_index=True)

    chn = df[df["ISO Code"] == "CHN"].pivot(
        index="Year", columns=["Mode/vehicle type", "Variable"], values="Value"
    )
    # Convert units using the mapping **UNITS** defined above
    idx_lvl_0 = list(set(chn.columns.get_level_values(0)))
    for mode in idx_lvl_0:
        chn[mode] = chn[mode].apply(convert_units, unit_info=UNITS)

    ind = (
        df[df["ISO Code"] == "IND"]
        .pivot(index="Year", columns="Variable", values="Value")
        .apply(convert_units, unit_info=UNITS)
    )

    return df

