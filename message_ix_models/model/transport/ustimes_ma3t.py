"""Legacy code for the US-TIMES–MA³T source for data on LDV technologies.

The code in :mod:`.ldv` is currently mostly agnostic to the upstream data source. This
module preserves code tailored to the specific data structure format used in
MESSAGE(V)-Transport.
"""

from collections import defaultdict
from collections.abc import Mapping
from typing import TYPE_CHECKING

import genno
import pandas as pd
from openpyxl import load_workbook

from message_ix_models.util import cached, package_data_path

if TYPE_CHECKING:
    from genno.types import AnyQuantity

#: Input file containing structured data about LDV technologies.
#:
#: For R11, this data is from the US-TIMES and MA³T models.
FILE = "ldv-cost-efficiency.xlsx"

#: (parameter name, cell range, units) for data to be read from multiple sheets in the
#: :data:`FILE`.
TABLES = {
    "fuel economy": (slice("B3", "Q15"), "Gv km / (GW year)"),
    "inv_cost": (slice("B33", "Q45"), "USD / vehicle"),
    "fix_cost": (slice("B62", "Q74"), "USD / vehicle"),
}


@cached
def read_USTIMES_MA3T(nodes: list[str], subdir=None) -> Mapping[str, "AnyQuantity"]:
    """Read the US-TIMES MA3T data from :data:`FILE`.

    No transformation is performed.

    **NB** this function takes only simple arguments (`nodes` and `subdir`) so that
    :func:`.cached` computes the same key every time to avoid the slow step of opening/
    reading the large spreadsheet. :func:`get_USTIMES_MA3T` then conforms the data to
    particular context settings.
    """
    # Open workbook
    path = package_data_path("transport", subdir or "", FILE)
    wb = load_workbook(path, read_only=True, data_only=True)

    # Tables
    data = defaultdict(list)

    # Iterate over regions/nodes
    for node in map(str, nodes):
        # Worksheet for this region
        sheet_node = node.split("_")[-1].lower()
        sheet = wb[f"MESSAGE_LDV_{sheet_node}"]

        # Read tables for efficiency, investment, and fixed O&M cost
        # NB fix_cost varies by distance driven, thus this is the value for average
        #    driving.
        # TODO calculate the values for modest and frequent driving
        for par_name, (cells, _) in TABLES.items():
            df = pd.DataFrame(list(sheet[cells])).map(lambda c: c.value)

            # - Make the first row the headers.
            # - Drop extra columns.
            # - Use 'MESSAGE name' as the technology name.
            # - Melt to long format.
            # - Year as integer.
            # - Assign "node" and "unit" columns.
            # - Drop NA values (e.g. ICE_L_ptrp after the first year).
            data[par_name].append(
                df.iloc[1:, :]
                .set_axis(df.loc[0, :], axis=1)
                .drop(["Technology", "Description"], axis=1)
                .rename(columns={"MESSAGE name": "t"})
                .melt(id_vars=["t"], var_name="y")
                .astype({"y": int})
                .assign(n=node)
                .dropna(subset=["value"])
            )

    # Combine data frames, convert to Quantity
    qty = {}
    for par_name, dfs in data.items():
        qty[par_name] = genno.Quantity(
            pd.concat(dfs, ignore_index=True).set_index(["n", "t", "y"]),
            units=TABLES[par_name][1],
            name=par_name,
        )

    return qty
