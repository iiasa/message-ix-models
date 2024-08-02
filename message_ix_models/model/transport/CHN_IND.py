"""Retrieve transport activity data for China (Statistical Yearbook of the National
Bureau of Statistics) and for India (iTEM)."""

import numpy as np
import pandas as pd
from iam_units import registry
from item import historical  # type: ignore [import-not-found]

from message_ix_models.util import package_data_path

UNITS = {
    "Population": (1.0e-6, None, "dimensionless"),
    "Vehicle Stock": (1.0e4, "vehicle", "thousand vehicle"),
    "Passenger-Kilometers": (100, "megapkm", "gigapkm"),
    "Freight Ton-Kilometers": (100, "megatkm", "gigatkm"),
}


SDMX_MAP = {
    "REF_AREA": "ISO_code",
    "VARIABLE": "Variable",
    "VEHICLE": "Vehicle type",
    "MODE": "Mode/vehicle type",
    "UNIT": "Units",
    "TIME_PERIOD": "Year",
    "VALUE": "Value",
}
#: Files containing data for China ("name_of_file", rows_to_skip_from_the_bottom)
FILES = {
    "Passenger activity": ("CHN_activity-passenger.csv", 4),
    "Vehicle stock civil": ("CHN_stock-civil.csv", 5),
    "Vehicle stock private": ("CHN_stock-private.csv", 1),
    "Freight activity": ("CHN_activity-freight.csv", 5),
}

POP_FILE = "CHN_IND_population.csv"

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


# TODO: migrate to convert_units.py, as an extra argument: e.g. axis=1 being the
#  current way (converting per var names in columns) or axis=0 (converting per rows
#  as coded in _covert below).
def _convert(group_df):
    """ ""Convert units of a pandas.groupby object based on UNITS."""
    info = UNITS.get(group_df["Variable"].unique()[0], None)
    if not info:
        return group_df
    factor, unit_in, unit_out = info
    qty = registry.Quantity(factor * group_df["Value"].values, unit_in).to(
        unit_out or unit_in
    )
    return group_df.assign(Value=qty.magnitude, Units=qty.units)


def split_variable(s) -> pd.DataFrame:
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


def get_ind_item_data() -> pd.DataFrame:
    """Retrieve activity data for rail and road transport for India from iTEM.

    Data is obtained from iTEM database's file ``T000.csv`` and filtered for the
    period 2000-2018. Data for China is not retrieved since it comes from the NBSC.

    Returns
    -------
    DataFrame : pandas.DataFrame
        DataFrame with transport data for India.
    """
    # Import data from iTEM database historical files:
    all_data = pd.DataFrame()
    for x in range(0, 13, 1):
        # Exclude non-existent historical file 11
        if x != 11:
            df = historical.process(x)
            df.drop(
                columns=[x for x in list(df.columns) if x not in list(SDMX_MAP.keys())],
                inplace=True,
            )
            # Rename columns using SDMX_MAP dict, to match dataset format in
            # get_chn_ind_data()
            df.columns = df.columns.to_series().map(SDMX_MAP)
            # Filter values for IND between 2000-2018
            df = df[
                (df["ISO_code"].isin(["IND"]))
                & (df["Year"].isin(list(np.arange(2000, 2019))))
            ].sort_values(
                ["ISO_code", "Mode/vehicle type", "Vehicle type"], ignore_index=True
            )
            # Concat whenever data is retrieved
            all_data = pd.concat([all_data, df], ignore_index=True)

    return all_data


def get_chn_ind_pop() -> pd.DataFrame:
    """Retrieve population data for China and India.

    The dataset is a ``.csv`` file in */data* and was retrieved from `OECD
    <https://stats.oecd.org/Index.aspx?#>`_ website, filtering data for China and India.

    Returns
    -------
    DataFrame : pandas.DataFrame
        DataFrame containing population data for China and India.
    """
    # Read csv file
    pop = pd.read_csv(package_data_path("transport", POP_FILE), header=1)
    # Drop irrelevant columns and rename when necessary
    pop = pop.drop(
        [x for x in pop.columns if x not in ["LOCATION", "Time", "Value"]],
        axis=1,
    ).rename(columns={"LOCATION": "ISO_code", "Time": "Year"})
    # Add "Variable" name column
    pop["Variable"] = "Population"
    return pop


def get_chn_ind_data(private_vehicles=False) -> pd.DataFrame:
    """Read transport activity and vehicle stock data for China and India.

    The data is read from ``data/transport`` folder (data for China from NBSC) and
    imported from iTEM project (data for India). Then, it is processed into the same
    format as the IEA's EEI datasets -to be used for MESSAGEix-Transport calibration.

    Parameters
    ----------
    private_vehicles : bool, optional
        If ``True``, also data for private vehicles will be parsed. However,
        it is set by default to ``False`` since this category is included within the
        civil vehicles, which are read from another file. Therefore, in absolute
        terms, only the civil vehicles are relevant for top-level stock insights.

    Returns
    -------
    DataFrame : pandas.DataFrame
        DataFrame with processed transport data for China and India.
    """
    if not private_vehicles:
        del FILES["Vehicle stock private"]
    # Load and process data from China
    df = pd.DataFrame()
    for file, skip_footer in FILES.values():
        # Read excel sheet
        df_aux = pd.read_csv(
            package_data_path("transport", file),
            skipfooter=skip_footer,
            header=2,
        )
        df = pd.concat([df, df_aux], ignore_index=True)
    # Drop rows containing sub-categories of rail transport
    df = df.drop(df[df["Indicators"].isin(RAIL_SUB_CAT)].index).reset_index(drop=True)
    df = pd.concat(
        [
            df.drop("Indicators", axis=1),
            df["Indicators"].str.extract(r"(?P<variable>.*) \((?P<units>.*)\)"),
        ],
        axis=1,
    )
    df["Variable"] = df["Variable"].str.replace("Possession", "Vehicle Stock")
    # Reach **tidy data** structure
    df = df.melt(
        id_vars=["Variable", "Units"], var_name="Year", value_name="Value"
    ).sort_values("Year")

    # Add "ISO_code" column to *df*, and move to first position
    df["ISO_code"] = "CHN"
    df = df.set_index("ISO_code").reset_index()

    df["Year"] = pd.to_numeric(df["Year"])

    # Drop 2019 values so it can be concatenated with population values
    df.drop(df[df["Year"] == 2019].index, inplace=True)

    # Split Variable column into Variable and Mode/vehicle type
    df = (
        pd.concat([df, split_variable(df["Variable"])], axis=1)
        # Drop "Variable" and then rename "Var" to "Variable", since "Variable" column
        # was previously returned with that label by split_units()
        .drop(["Variable"], axis=1)
        .rename(columns={"Var": "Variable"})
    )

    # Reorder columns of *df*
    cols = df.columns.tolist()
    cols = [cols[0]] + cols[-2:] + cols[1:4]
    df = df[cols]

    # Concat population values
    df = pd.concat([df, get_chn_ind_pop()], ignore_index=True).sort_values(
        ["ISO_code", "Year", "Variable", "Mode/vehicle type"], ignore_index=True
    )

    # Import IND data from iTEM database
    df_item = get_ind_item_data()

    df_item.drop(columns="Vehicle type", inplace=True)

    # Concat CHN and IND data
    df = pd.concat([df, df_item], ignore_index=True)

    # Convert units
    df = df.groupby("Variable").apply(_convert).reset_index(drop=True)

    # NB: CHN data for *Waterways* includes *Ocean* and inland freight transport.
    # In IND, vehicle type *Container* would map to *Ocean* from CHN and *Inland* to
    # (Waterways minus Ocean) from CHN.

    return df
