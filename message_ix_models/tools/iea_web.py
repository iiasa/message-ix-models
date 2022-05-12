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
