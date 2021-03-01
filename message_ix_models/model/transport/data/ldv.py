import logging
from collections import defaultdict

import pandas as pd
from message_ix_models.util import private_data_path
from openpyxl import load_workbook

from message_data.model.transport.utils import add_commodity_and_level
from message_data.tools import (
    cached,
    check_support,
    ffill,
    make_df,
    make_io,
    make_matched_dfs,
)

log = logging.getLogger(__name__)


#: Input file containing data from US-TIMES and MA3T models.
FILE = "LDV_costs_efficiencies_US-TIMES_MA3T.xlsx"

#: (parameter name, cell range, units) for data to be read from multiple
#: sheets in the file.
TABLES = [
    ("efficiency", slice("B3", "Q15"), "10^9 v km / GWh / year"),
    ("inv_cost", slice("B33", "Q45"), "USD / vehicle"),
    ("fix_cost", slice("B62", "Q74"), "USD / vehicle"),
]


def get_ldv_data(context):
    source = context["transport config"]["data source"].get("LDV", None)

    if source == "US-TIMES MA3T":
        return get_USTIMES_MA3T(context)
    elif source is None:
        return {}  # Don't add any data
    else:
        raise ValueError(f"invalid source for non-LDV data: {source}")


@cached
def get_USTIMES_MA3T(context):
    """Read LDV cost and efficiency data from US-TIMES and MA3T.

    .. todo:: Some calculations are performed in the spreadsheet; transfer to code.
    """
    # Compatibility checks
    check_support(
        context,
        settings=dict(regions=frozenset(["R11"])),
        desc="US-TIMES and MA3T data available",
    )

    # Retrieve configuration and ScenarioInfo
    config = context["transport config"]
    info = context["transport build info"]

    # Open workbook
    path = private_data_path("transport", FILE)
    wb = load_workbook(path, read_only=True, data_only=True)

    # Tables
    data = defaultdict(list)

    # Iterate over regions/nodes
    for node in info.N:
        if node == "World":
            continue

        log.debug(node)

        # Worksheet for this region
        sheet_node = node.split("_")[-1].lower()
        sheet = wb[f"MESSAGE_LDV_{sheet_node}"]

        # Read tables for efficiency, investment, and fixed O&M cost
        # NB fix_cost varies by distance driven, thus this is the value for average
        #    driving.
        # TODO calculate the values for modest and frequent driving
        for par_name, cells, unit in TABLES:
            df = pd.DataFrame(list(sheet[cells])).applymap(lambda c: c.value)

            # - Make the first row the headers.
            # - Drop extra columns.
            # - Use 'MESSAGE name' as the technology name.
            # - Pivot to long format.
            # - Year as integer.
            # - Within the model horizon/time resolution.
            # - Assign values.
            # - Drop NA values (e.g. ICE_L_ptrp after the first year).
            data[par_name].append(
                df.iloc[1:, :]
                .set_axis(df.loc[0, :], axis=1)
                .drop(["Technology", "Description"], axis=1)
                .rename(columns={"MESSAGE name": "technology"})
                .melt(id_vars=["technology"], var_name="year")
                .astype({"year": int})
                .query(f"year in [{', '.join(map(str, info.Y))}]")
                .assign(node=node, unit=unit)
                .dropna(subset=["value"])
            )

    for par, dfs in data.items():
        # Dimension to forward fill along
        for col in ("year_vtg", "year"):
            if col in df.columns:
                break
        # - Concatenate data frames.
        # - Forward-fill over uncovered periods in the model horizon.
        data[par] = pd.concat(dfs, ignore_index=True).pipe(
            ffill,
            col,
            info.Y,
            expr="year_act = year_vtg" if col == "year_vtg" else None,
        )

    # Convert 'efficiency' into 'input' and 'output' parameter data
    base = data.pop("efficiency")
    i_o = make_io(
        src=(None, None, "GWa"),
        dest=("transport pax vehicle", "useful", "Gv km"),
        efficiency=1.0 / base["value"],
        on="input",
        # Other data
        node_loc=base["node"],
        node_origin=base["node"],
        node_dest=base["node"],
        technology=base["technology"],
        year_vtg=base["year"],
        year_act=base["year"],
        mode="all",
        # No subannual detail
        time="year",
        time_origin="year",
        time_dest="year",
    )

    i_o["input"] = add_commodity_and_level(i_o["input"], default_level="secondary")
    data.update(i_o)

    # Add technical lifetimes
    data.update(
        make_matched_dfs(
            base=i_o["output"],
            technical_lifetime=config["ldv lifetime"]["average"],
        )
    )

    # Transform costs
    for par in "fix_cost", "inv_cost":
        base = data.pop(par)
        # Rename 'node' and 'year' columns
        data[par] = make_df(
            par,
            node_loc=base["node"],
            technology=base["technology"],
            year_vtg=base["year"],
            year_act=base["year"],  # fix_cost only
            value=base["value"],
            unit=base["unit"],
        )

    # commented: incomplete / for debugging
    # Activity constraints
    # data.update(
    #     make_matched_dfs(
    #         base=i_o["output"],
    #         bound_new_capacity_up=1.,
    #         initial_activity_up=1.,
    #     )
    # )

    return data
