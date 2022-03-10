"""Data for transport modes and technologies outside of LDVs."""
import logging
from typing import Dict

import pandas as pd
from message_ix import make_df
from message_ix_models.util import broadcast, make_matched_dfs, merge_data, same_node

log = logging.getLogger(__name__)


def get_non_ldv_data(context) -> Dict[str, pd.DataFrame]:
    source = context["transport config"]["data source"].get("non-LDV", None)

    log.info(f"from {source}")

    if source == "IKARUS":
        from .ikarus import get_ikarus_data

        data = get_ikarus_data(context)
    elif source is None:
        data = dict()  # Don't add any data
    else:
        raise ValueError(f"invalid source for non-LDV data: {source}")

    # Merge in dummy/placeholder data for 2-wheelers (not present in IKARUS)
    merge_data(data, get_2w_dummies(context))

    return data


def get_2w_dummies(context) -> Dict[str, pd.DataFrame]:
    """Generate dummy, equal-cost output for 2-wheeler technologies.

    **NB** this is analogous to :func:`.ldv.get_dummy`.
    """
    # Information about the target structure
    info = context["transport build info"]

    # List of years to include
    years = list(filter(lambda y: y >= 2010, info.set["year"]))

    # List of 2-wheeler technologies
    all_techs = context["transport set"]["technology"]["add"]
    techs = list(map(str, all_techs[all_techs.index("2W")].child))

    # 'output' parameter values: all 1.0 (ACT units == output units)
    # - Broadcast across nodes.
    # - Broadcast across LDV technologies.
    # - Add commodity ID based on technology ID.
    output = (
        make_df(
            "output",
            value=1.0,
            commodity="transport pax 2w",
            year_act=years,
            year_vtg=years,
            unit="Gv km",
            level="useful",
            mode="all",
            time="year",
            time_dest="year",
        )
        .pipe(broadcast, node_loc=info.N[1:], technology=techs)
        .pipe(same_node)
    )

    # Add matching data for 'capacity_factor' and 'var_cost'
    data = make_matched_dfs(output, capacity_factor=1.0, var_cost=1.0)
    data["output"] = output

    return data
