"""Tools for IEA World Energy Balance (WEB) data."""
import logging
from pathlib import Path

import pandas as pd
import yaml
from iam_units import registry
from pycountry import countries

from message_ix_models.util import cached, package_data_path

log = logging.getLogger(__name__)

#: Subset of columns to load, mapped to returned values.
COLUMNS = {
    "Unit": "unit",
    "COUNTRY": "node",
    "PRODUCT": "commodity",
    "FLOW": "flow",
    "TIME": "year",
    "Flag Codes": "flag",
    "Value": "value",
}

#: File name containing data.
FILE = "WBAL_12052022124930839.csv"
FILE = "cac5fa90-en.zip"


@cached
def load_data(base_path=None) -> pd.DataFrame:
    """Load data from the IEA World Energy Balances.

    Parameters
    ----------
    base_path : os.Pathlike, optional
        Path containing :data:`.FILE`.

    Returns
    -------
    pandas.DataFrame
        The data frame has the following columns:

        - flow
        - commodity, i.e. “PRODUCT” in the raw data.
        - node, i.e. “COUNTRY” in the raw data.
        - year, i.e. “TIME” in the raw data. The IEA dimension ID is actually more
          precise here, but “year” is used because it aligns with the :mod:`message_ix`
          formulation
        - value
        - unit
        - flag
    """
    base_path = base_path or package_data_path("iea")
    path = base_path.joinpath(FILE)
    return pd.read_csv(path, usecols=COLUMNS.keys()).rename(columns=COLUMNS)


def generate_code_lists(base_path: Path = None) -> None:
    """Extract structure from the data itself."""
    base_path = base_path or package_data_path("iea")
    path = base_path.joinpath(FILE)

    log.info(f"Extract structure from {path}")

    # 'Peek' at the data to inspect the column headers
    peek = pd.read_csv(path, nrows=1)
    unit_id_column = peek.columns[0]

    # Country names that are already in pycountry
    def _check0(row):
        try:
            return countries.lookup(row["name"]).alpha_3 == row["id"]
        except LookupError:
            return False

    # Units that are understood by pint
    def _check1(value):
        try:
            registry(value)
            return True
        except ValueError:
            return False

    for (id, name) in [
        ("COUNTRY", "Country"),
        ("FLOW", "Flow"),
        ("PRODUCT", "Product"),
        ("TIME", "Time"),
        (unit_id_column, "Unit"),
        ("Flag Codes", "Flags"),
    ]:
        # - Re-read the data, only two columns; slower, but less overhead
        # - Drop empty rows and duplicates.
        # - Drop 'trivial' values, where the name and id are identical.
        df = (
            pd.read_csv(path, usecols=[id, name])
            .set_axis(["id", "name"], axis=1)
            .dropna(how="all")
            .drop_duplicates()
            .query("id != name")
        )

        # Mark more trivial values according to the concept
        if id == "COUNTRY":
            df["trivial"] = df.apply(_check0, axis=1)
        elif id == "UNIT":
            df["trivial"] = df["name"].apply(_check1)
        else:
            df["trivial"] = False

        # Drop trivial values
        df = df.query("not trivial").drop("trivial", axis=1)

        if not len(df):
            log.info(f"No non-trivial entries for code list {repr(id)}")
            continue

        # Store
        id = id.replace("MEASURE", "UNIT")  # If unit_id_column is "MEASURE"
        cl_path = (base_path or package_data_path("iea")).joinpath(
            f"{id.lower().replace(' ', '-')}.yaml"
        )

        log.info(f"Write {len(df)} codes to {cl_path}")
        data = {
            row["id"]: dict(name=row["name"])
            for _, row in df.sort_values("id").iterrows()
        }
        cl_path.write_text(yaml.dump(data))


