import sys
from typing import TYPE_CHECKING

import pandas as pd
from genno import Quantity

from message_ix_models.util import local_data_path

if TYPE_CHECKING:
    from genno.types import AnyQuantity

#: Dimensions of the PASTA activity data (= columns in CSV file in long format).
DIMS = ["Region", "Vehicle_type", "Data", "Sector", "Scope", "Scenario", "Year"]


def read_pasta_data() -> "AnyQuantity":
    """Read PASTA activity data from :file:`{local data}/edits/pasta.csv`.

    Returns
    -------
    .Quantity
       With dimensions :data:`.DIMS`.
    """
    path = local_data_path("edits", "pasta.csv")

    if not path.exists():
        # Create the directory
        path.parent.mkdir(parents=True, exist_ok=True)

        print(f"Not found: {path}")
        sys.exit(1)

    # - Read the data file.
    # - Rename "Value" to "value", as expected by genno.
    # - Set index.
    df = pd.read_csv(path).rename(columns={"Value": "value"}).set_index(DIMS)

    # Convert to genno.Quantity
    q = Quantity(df)

    # Show the dimensions and codes
    print(q.coords)

    return q
