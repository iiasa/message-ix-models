"""Data input, processing, and parameter generation for the cement sector."""

from collections import defaultdict
from collections.abc import MutableMapping
from typing import TYPE_CHECKING

import pandas as pd
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.util import (
    broadcast,
    merge_data,
    nodes_ex_world,
    package_data_path,
    same_node,
)

from .data_util import (
    calculate_ini_new_cap,
    read_sector_data,
    read_timeseries,
)
from .material_demand.material_demand_calc import (
    derive_demand,
)
from .util import get_ssp_from_context, read_config

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models.types import MutableParameterData, ParameterData

FIXED = dict(time="year", time_origin="year", time_dest="year")


def gen_data_cement(scenario: "Scenario", dry_run: bool = False) -> "ParameterData":
    """Generate data for materials representation of cement industry.

    Parameters
    ----------
    scenario : message_ix.Scenario
    dry_run : bool

    Returns
    -------
    dict[str, pd.DataFrame]
    """
    # Load configuration
    context = read_config()
    config = context["material"]["cement"]
    ssp = get_ssp_from_context(context)

    # Information about `scenario`
    s_info = ScenarioInfo(scenario)
    yv_ya = s_info.yv_ya.query("year_vtg >= 1980")
    yv = sorted(yv_ya.year_vtg.unique())
    nodes = nodes_ex_world(s_info.N)  # Omit e.g. R12_GLB

    # Input data: techno-economic assumptions for each technology
    data_cement = read_sector_data(scenario, "cement", None, "cement_R12.csv")
    # Similar data for time-varying parameters
    data_cement_ts = read_timeseries(scenario, "cement", None, "timeseries_R12.csv")

    # List of data frames, to be concatenated together at end
    results: MutableMapping[str, list[pd.DataFrame]] = defaultdict(list)

    # Iterate over technologies
    for t in config["technology"]["add"]:
        # Retrieve the id if `t` is a Code instance; otherwise use str
        t = getattr(t, "id", t)

        # Subsets of `data_cement` and `data_cement_ts` related to `t`
        t_data = data_cement.query("technology == @t")
        t_data_ts = data_cement_ts.query("technology == @t")  # May be empty

        # Keyword arguments to make_df()
        kw = dict(technology=t, unit="t") | FIXED

        # Iterate over time-varying parameters, if any
        for par, par_data_ts in t_data_ts.groupby("parameter"):
            # More keyword arguments to make_df(). These go unused if they are not
            # dimensions of `par`.
            kw.update(
                node_loc=par_data_ts["region"],
                mode=par_data_ts["mode"],
                # units=par_data_ts["units"].values[0],
                value=par_data_ts["value"],
                # year_act == year_vtg by construction
                year_act=par_data_ts["year"],
                year_vtg=par_data_ts["year"],
            )

            # Keyword arguments to broadcast(): by default, do nothing
            bcast: dict[str, list[str]] = dict()

            if par == "var_cost":
                # Broadcast over all `nodes`
                kw.pop("node_loc")
                bcast.update(node_loc=nodes)

            # - Create parameter data.
            # - (Maybe) broadcast over nodes.
            # - Append to results.
            results[par].append(make_df(par, **kw).pipe(broadcast, **bcast))

        # Remove keywords specific to `par_data_ts`
        [kw.pop(dim, None) for dim in ("value", "year_act", "year_vtg")]

        # Iterate over parameters
        for par_info, par_data in t_data.groupby("parameter"):
            # read_sector_data() combines several dimensions (commodity, emission,
            # level, mode) into the "parameter" key. Split the parameter name and the
            # remainder.
            par, _, key = par_info.partition("|")

            # Vectors of values and nodes, which are of the same length
            kw.update(value=par_data["value"], node_loc=par_data["region"])

            # Keyword arguments to broadcast()
            # - If a parameter has both (year_vtg, year_act) dims, then use `yv_ya`, a
            #   data frame with valid combinations.
            # - Otherwise, use only `yv`.
            has_year_act = par not in ("inv_cost", "technical_lifetime")
            bcast = dict(labels=yv_ya) if has_year_act else dict(year_vtg=yv)

            if len(kw["node_loc"]) == 1:
                # Data only available for one node → use the same value for *all* nodes
                kw.update(node_loc=None)
                bcast.update(node_loc=nodes)

            # Unpack `key` into key values for other dimensions, as appropriate
            # FIXME This would not be needed if read_sector_data() did not collapse the
            #       dimensions. Adjust and remove.
            if par in ("input", "output"):
                c, l_, m = key.split("|")  # Key MUST be commodity|level|mode
                kw.update(commodity=c, level=l_, mode=m)
            elif par == "emission_factor":
                e, m = key.split("|")  # Key MUST be emission|mode
                kw.update(emission=e, mode=m)
            elif key:  # time-independent var_cost
                m = key.split("|")  # Key MUST be mode
                kw.update(mode=m)

            # - Create parameter data.
            # - (Maybe) broadcast over nodes.
            # - Use node_loc value for node_dest, node_origin, etc.
            # - Append to results.
            results[par].append(
                make_df(par, **kw).pipe(broadcast, **bcast).pipe(same_node)
            )

    # Create external demand param
    name = "demand"
    df_demand = pd.concat(
        [
            pd.read_csv(package_data_path("material", "cement", "demand_2025.csv")),
            derive_demand("cement", scenario, ssp=ssp).query("year != 2025"),
        ]
    )
    results[name].append(df_demand)

    name = "initial_new_capacity_up"
    for t in "clinker_dry_ccs_cement", "clinker_wet_ccs_cement":
        results[name].append(calculate_ini_new_cap(df_demand, t, "cement", ssp))

    # Concatenate to one data frame per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    # Merge data from other functions
    merge_data(
        results,
        gen_grow_cap_up(s_info, ssp),
        read_furnace_2020_bound(),
        gen_clinker_ratios(s_info),
        gen_addon_conv_ccs(nodes, s_info.Y),
    )

    results = drop_redundant_rows(results)
    return results


def drop_redundant_rows(results: "ParameterData") -> "MutableParameterData":
    """Drop duplicate row and those where :math:`y^A - y^V > 25` years.

    Parameters
    ----------
    results :
        A dictionary of dataframes with parameter names as keys.

    Returns
    -------
    ParameterData
    """
    reduced_pdict = {}
    for k, v in results.items():
        if {"year_act", "year_vtg"}.issubset(v.columns):
            v = v[(v["year_act"] - v["year_vtg"]) <= 25]
        reduced_pdict[k] = v.drop_duplicates().copy(deep=True)

    return reduced_pdict


def gen_addon_conv_ccs(nodes: list[str], years: list[int]) -> "ParameterData":
    """Generate addon conversion parameters for clinker CCS cement."""
    df = (
        make_df(
            "addon_conversion",
            mode="M1",
            technology=["clinker_dry_cement", "clinker_wet_cement"],
            type_addon=["dry_ccs_cement", "wet_ccs_cement"],
            value=1.0,
            unit="-",
            **FIXED,
        )
        .pipe(broadcast, node=nodes, year_act=years, year_vtg=years)
        .query("year_vtg <= year_act")
    )
    return {"addon_conversion": df}


def gen_grow_cap_up(s_info: "ScenarioInfo", ssp: str) -> "ParameterData":
    """Generate growth constraints for new clinker CCS capacity."""
    ssp_vals = {
        "LED": 0.05,
        "SSP1": 0.05,
        "SSP2": 0.1,
        "SSP3": 0.15,
        "SSP4": 0.15,
        "SSP5": 0.15,
    }

    df = make_df(
        "growth_new_capacity_up",
        technology=["clinker_dry_ccs_cement", "clinker_wet_ccs_cement"],
        value=ssp_vals[ssp],
        unit="???",
    ).pipe(broadcast, node_loc=nodes_ex_world(s_info.N), year_vtg=s_info.Y)
    return {"growth_new_capacity_up": df}


def read_furnace_2020_bound() -> "ParameterData":
    """Read the 2020 bound activity data for cement."""
    dir = package_data_path("material", "cement")
    df = pd.concat(
        [pd.read_csv(dir.joinpath(f"cement_bound_{y}.csv")) for y in (2020, 2025)]
    )
    return {"bound_activity_lo": df, "bound_activity_up": df}


def gen_clinker_ratios(s_info: "ScenarioInfo") -> "ParameterData":
    """Generate regionally differentiated clinker input for cement production.

    2020 ratios taken from `doi:10.1016/j.ijggc.2024.104280
    <https://doi.org/10.1016/j.ijggc.2024.104280>`_, Appendix B.
    """

    reg_map = {
        "R12_AFR": 0.75,
        "R12_CHN": 0.65,
        "R12_EEU": 0.82,
        "R12_FSU": 0.85,
        "R12_LAM": 0.71,
        "R12_MEA": 0.8,
        "R12_NAM": 0.87,
        "R12_PAO": 0.83,
        "R12_PAS": 0.78,
        "R12_RCPA": 0.78,
        "R12_SAS": 0.7,
        "R12_WEU": 0.74,
    }
    df = (
        make_df(
            "input",
            node_loc=reg_map.keys(),
            value=reg_map.values(),
            commodity="clinker_cement",
            level="tertiary_material",
            mode="M1",
            unit="???",
            **FIXED,
        )
        .pipe(
            broadcast,
            technology=["grinding_ballmill_cement", "grinding_vertmill_cement"],
            year_act=s_info.Y,
            year_vtg=s_info.yv_ya["year_vtg"].unique(),
        )
        .pipe(same_node)
        .query("0 <= year_act - year_vtg <= 25")
    )
    return {"input": df}
