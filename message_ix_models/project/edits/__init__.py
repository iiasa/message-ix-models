import sys
from typing import TYPE_CHECKING

import pandas as pd
import sdmx
from genno import Quantity
from genno.compat.sdmx.operator import quantity_to_message
from sdmx.message import StructureMessage
from sdmx.model.v21 import DataStructureDefinition

from message_ix_models.util import local_data_path

if TYPE_CHECKING:
    from genno.types import AnyQuantity

#: Dimensions of the PASTA activity data (= columns in CSV file in long format).
DIMS = ["Region", "Vehicle_type", "Data", "Sector", "Scope", "Scenario", "Year"]


def read_pasta_data() -> "AnyQuantity":
    """Read PASTA activity data from :file:`{[message_local_data]}/edits/pasta.csv`.

    The file :file:`{[message_local_data]}/edits/pasta-data.xml` is created with an
    SDMX-ML formatted version of the data set.

    Returns
    -------
    .Quantity
       with dimensions :data:`.DIMS`.

    See also
    --------
    generate_pasta_structures
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

    # Retrieve an SDMX structure message containing a data structure definition (DSD)
    sm = generate_pasta_structures()
    dsd = sm.structure["PASTA"]

    # Convert `q` to an SDMX data message
    msg = quantity_to_message(q, dsd)

    # Write to file
    with open(local_data_path("edits", "pasta-data.xml"), "wb") as f:
        f.write(sdmx.to_xml(msg, pretty_print=True))

    return q


def generate_pasta_structures() -> "StructureMessage":
    """Generate SDMX data structures for the PASTA activity data flows.

    The file :file:`{[message_local_data]}/edits/pasta-structures.xml` is created or
    updated.
    """
    # Create a structure message
    msg = StructureMessage()

    # Create a data structure definition (DSD) and add it to the message
    dsd = DataStructureDefinition(id="PASTA")
    msg.add(dsd)

    # Create dimensions within the DSD
    for dim in DIMS:
        dsd.dimensions.getdefault(id=dim)

    # Add the measure; currently the well-known SDMX "OBS_VALUE"
    # TODO Change to specific measure for each structure
    dsd.measures.getdefault(id="OBS_VALUE")

    # Write to file
    with open(local_data_path("edits", "pasta-structure.xml"), "wb") as f:
        f.write(sdmx.to_xml(msg, pretty_print=True))

    return msg
