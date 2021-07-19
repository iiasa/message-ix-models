"""Freight transport data."""
from collections import defaultdict

import pandas as pd
from message_ix_models.util import broadcast, make_io, make_matched_dfs, same_node

from message_data.model.transport.utils import input_commodity_level


def get_freight_data(context):
    """Data for freight technologies.

    See also
    --------
    strip_emissions_data
    """
    codes = context["transport set"]["technology"]["add"]
    freight_truck = codes[codes.index("freight truck")]
    info = context["transport build info"]

    common = dict(
        year_vtg=info.Y,
        year_act=info.Y,
        mode="all",
        time="year",  # no subannual detail
        time_dest="year",
        time_origin="year",
    )

    data = defaultdict(list)
    for tech in freight_truck.child:
        i_o = make_io(
            src=(None, None, "GWa"),
            dest=("transport freight vehicle", "useful", "km"),
            efficiency=1.0,
            on="input",
            technology=tech.id,
            **common,
        )

        i_o["input"] = input_commodity_level(i_o["input"], "final")

        for par, df in i_o.items():
            data[par].append(df.pipe(broadcast, node_loc=info.N[1:]).pipe(same_node))

    data = {par: pd.concat(dfs) for par, dfs in data.items()}

    data.update(
        make_matched_dfs(
            base=data["input"],
            capacity_factor=1,
            technical_lifetime=10,
        )
    )

    # TODO re-add emissions data from file

    return data
