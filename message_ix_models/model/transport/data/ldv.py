import logging
from collections import defaultdict

import pandas as pd
from message_ix import make_df
from message_ix_models.util import (
    broadcast,
    ffill,
    make_io,
    make_matched_dfs,
    merge_data,
    private_data_path,
    same_node,
)
from openpyxl import load_workbook

from message_data.model.transport.utils import add_commodity_and_level
from message_data.tools import cached, check_support

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
        data = get_USTIMES_MA3T(context)
    elif source is None:
        data = get_dummy(context)
    else:
        raise ValueError(f"invalid source for non-LDV data: {source}")

    # Merge in constraint data
    merge_data(data, get_constraints(context))
    return data


@cached
def get_USTIMES_MA3T(context):
    """Read LDV cost and efficiency data from US-TIMES and MA3T.

    .. todo:: Some calculations are performed in the spreadsheet; transfer to code.
    .. todo:: Interpolate values for time periods e.g. 2025 not in the spreadsheet.
    """
    # Compatibility checks
    check_support(
        context,
        settings=dict(regions=frozenset(["R11"]), years=frozenset(["A"])),
        desc="US-TIMES and MA3T data available",
    )

    # Retrieve configuration and ScenarioInfo
    config = context["transport config"]
    info = context["transport build info"]

    # List of years to include
    years = list(filter(lambda y: y >= 2010, info.set["year"]))
    years_query = f"year in {repr(years)}"

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
                .query(years_query)
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
        dest=(None, "useful", "Gv km"),
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

    # Assign input commodity and level according to the technology
    i_o["input"] = add_commodity_and_level(i_o["input"], default_level="secondary")

    # Assign output commodity based on the technology name
    i_o["output"] = i_o["output"].assign(
        commodity=lambda df: "transport vehicle " + df["technology"]
    )

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


@cached
def get_dummy(context):
    """Generate dummy, equal-cost output for each LDV technology."""
    # Information about the target structure
    info = context["transport build info"]

    # List of years to include
    years = list(filter(lambda y: y >= 2010, info.set["year"]))

    # List of LDV technologies
    all_techs = context["transport set"]["technology"]["add"]
    ldv_techs = list(map(str, all_techs[all_techs.index("LDV")].child))

    # 'output' parameter values: all 1.0 (ACT units == output units)
    # - Broadcast across nodes.
    # - Broadcast across LDV technologies.
    # - Add commodity ID based on technology ID.
    output = (
        make_df(
            "output",
            value=1.0,
            year_act=years,
            year_vtg=years,
            unit="Gv km",
            level="useful",
            mode="all",
            time="year",
            time_dest="year",
        )
        .pipe(broadcast, node_loc=info.N[1:], technology=ldv_techs)
        .assign(commodity=lambda df: "transport vehicle " + df["technology"])
        .pipe(same_node)
    )

    # Discard rows for the historical LDV technology beyond 2010
    output = output[~output.eval("technology == 'ICE_L_ptrp' and year_vtg > 2010")]

    # Add matching data for 'capacity_factor' and 'var_cost'
    data = make_matched_dfs(output, capacity_factor=1.0, var_cost=1.0)
    data["output"] = output

    return data


def get_constraints(context):
    # Information about the target structure
    info = context["transport build info"]

    years = info.Y[1:]

    # List of LDV technologies
    all_techs = context["transport set"]["technology"]["add"]
    ldv_techs = list(map(str, all_techs[all_techs.index("LDV")].child))

    data = dict()

    # Constraint on activity growth: ± 10% every 5 years
    # TODO read this from config.yaml
    annual = (1.1 ** (1.0 / 5.0)) - 1.0

    for bound, factor in (("lo", -1.0), ("up", 1.0)):
        par = f"growth_activity_{bound}"
        data[par] = make_df(
            par, value=factor * annual, year_act=years, time="year", unit="-"
        ).pipe(broadcast, node_loc=info.N[1:], technology=ldv_techs)

    return data
