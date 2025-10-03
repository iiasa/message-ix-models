"""
Data and parameter generation for the petrochemicals sector in MESSAGEix models.

This module provides functions to read, process, and generate parameter data
for petrochemical technologies, demand, trade, emissions, and related constraints.
"""

from collections import defaultdict
from typing import TYPE_CHECKING

import pandas as pd
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.data_util import (
    drop_redundant_rows,
    gen_chemicals_co2_ind_factors,
    gen_emi_rel_data,
    gen_ethanol_to_ethylene_emi_factor,
    gen_plastics_emission_factors,
    read_timeseries,
)
from message_ix_models.model.material.demand import gen_demand_petro
from message_ix_models.model.material.util import (
    get_ssp_from_context,
    read_config,
)
from message_ix_models.util import (
    broadcast,
    merge_data,
    nodes_ex_world,
    package_data_path,
    same_node,
)

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models.types import ParameterData
    from message_ix_models.util import Context

ssp_mode_map = {
    "SSP1": "CTS core",
    "SSP2": "RTS core",
    "SSP3": "RTS high",
    "SSP4": "CTS high",
    "SSP5": "RTS high",
    "LED": "CTS core",  # TODO: maybe move to OECD projection instead
}

iea_elasticity_map = {
    "CTS core": (0.75, 0.15),
    "CTS high": (0.9, 0.45),
    "RTS core": (0.75, 0.4),
    "RTS high": (0.95, 0.7),
}


def gen_data_petro_chemicals(
    scenario: "Scenario", dry_run: bool = False
) -> "ParameterData":
    """Generate all MESSAGEix parameter data for the petrochemicals sector.

    Parameters
    ----------
    scenario :
        Scenario instance to build petrochemicals model on.
    dry_run :
        *Not used, but kept for compatibility.*
    """
    # Load configuration
    context = read_config()
    config = context["material"]["petro_chemicals"]
    ssp = get_ssp_from_context(context)
    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    if "R12_CHN" in s_info.N:
        fname_suffix = "_R12"
    else:
        fname_suffix = "_R11"

    # Techno-economic assumptions
    data_petro = read_data_petrochemicals(fname_suffix)
    data_petro_ts = read_timeseries(
        scenario, "petrochemicals", None, f"timeseries{fname_suffix}.csv"
    )
    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    modelyears = s_info.Y  # s_info.Y is only for modeling years
    nodes = nodes_ex_world(s_info.N)
    global_region = [i for i in s_info.N if i.endswith("_GLB")][0]
    yv_ya = s_info.yv_ya

    format_par_data(
        config, data_petro, results, yv_ya, modelyears, nodes, global_region
    )

    share_dict = {
        "shares": "steam_cracker",
        "node_share": ["R12_MEA", "R12_NAM"],
        "technology": "steam_cracker_petro",
        "mode": "ethane",
        "year_act": "2020",
        "time": "year",
        "value": [0.4, 0.4],
        "unit": "-",
    }
    results["share_mode_lo"].append(make_df("share_mode_lo", **share_dict))

    default_gdp_elasticity_2020, default_gdp_elasticity_2030 = iea_elasticity_map[
        ssp_mode_map[ssp]
    ]
    df_demand = gen_demand_petro(
        scenario, "HVC", default_gdp_elasticity_2020, default_gdp_elasticity_2030
    )
    df_2025 = pd.read_csv(
        package_data_path("material", "petrochemicals", "demand_2025.csv")
    )
    df_demand = df_demand[df_demand["year"] != 2025]
    df_demand = pd.concat([df_2025, df_demand])
    results["demand"].append(df_demand)

    # Special treatment for time-varying params
    tec_ts = set(data_petro_ts.technology)  # set of tecs in timeseries sheet

    gen_data_petro_ts(data_petro_ts, results, tec_ts, nodes)

    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    # modify steam cracker hist data (naphtha -> gasoil) to make model feasible
    df_cap = pd.read_csv(
        package_data_path(
            "material", "petrochemicals", "steam_cracking_hist_new_cap.csv"
        )
    )
    df_act = pd.read_csv(
        package_data_path("material", "petrochemicals", "steam_cracking_hist_act.csv")
    )
    df_act.loc[df_act["mode"] == "naphtha", "mode"] = "vacuum_gasoil"
    df = results["historical_activity"]
    results["historical_activity"] = pd.concat(
        [df.loc[df["technology"] != "steam_cracker_petro"], df_act]
    )
    df = results["historical_new_capacity"]
    results["historical_new_capacity"] = pd.concat(
        [df.loc[df["technology"] != "steam_cracker_petro"], df_cap]
    )

    # remove growth constraint for R12_AFR to make trade constraints feasible
    df = results["growth_activity_up"]
    results["growth_activity_up"] = df[
        ~(
            (df["technology"] == "steam_cracker_petro")
            & (df["node_loc"] == "R12_AFR")
            & (df["year_act"] == 2020)
        )
    ]

    # add 25% total trade bound
    df_dem = results["demand"]
    df_dem = df_dem.groupby("year").sum(numeric_only=True) * 0.25
    df_dem = df_dem.reset_index()
    df_dem = df_dem.rename({"year": "year_act"}, axis=1)

    par_dict = {
        "node_loc": "R12_GLB",
        "technology": "trade_petro",
        "mode": "M1",
        "time": "year",
        "unit": "-",
    }
    results["bound_activity_up"] = pd.concat(
        [
            results["bound_activity_up"],
            make_df("bound_activity_up", **df_dem, **par_dict),
        ]
    )

    # TODO: move this to input xlsx file
    df = scenario.par(
        "relation_activity",
        filters={"relation": "h2_scrub_limit", "technology": "gas_bio"},
    )
    df["value"] = -(1.33181 * 0.482)  # gas input * emission factor of gas
    df["technology"] = "gas_processing_petro"
    results["relation_activity"] = df

    meth_downstream_emi_top_down = gen_plastics_emission_factors(s_info, "HVCs")
    meth_downstream_emi_bot_up = gen_chemicals_co2_ind_factors(s_info, "HVCs")
    meth_downstream_emi_eth = gen_ethanol_to_ethylene_emi_factor(s_info)

    merge_data(
        results,
        gen_emi_rel_data(s_info, "petrochemicals"),
        meth_downstream_emi_top_down,
        meth_downstream_emi_bot_up,
        meth_downstream_emi_eth,
    )

    # TODO: move this to input xlsx file
    df_gro = results["growth_activity_up"]
    drop_idx = df_gro[
        (df_gro["technology"] == "steam_cracker_petro")
        & (df_gro["node_loc"] == "R12_RCPA")
        & (df_gro["year_act"] == 2020)
    ].index
    results["growth_activity_up"] = results["growth_activity_up"].drop(drop_idx)

    drop_redundant_rows(scenario, results)
    return results


def read_data_petrochemicals(fname) -> pd.DataFrame:
    """Read and clean data from the petrochemicals techno-economic files.

    Returns
    -------
    pd.DataFrame
        Cleaned techno-economic data for petrochemicals.
    """
    # Read the file
    data_petro = pd.read_csv(
        package_data_path("material", "petrochemicals", f"data{fname}.csv")
    )
    # Clean the data
    data_petro = data_petro.drop(["Source", "Description"], axis=1)
    return data_petro


def gen_data_petro_ts(
    data_petro_ts: pd.DataFrame, results: dict[list], tec_ts: set[str], nodes: list[str]
) -> None:
    """Generate time-series parameter data for petrochemical technologies.

    Parameters
    ----------
    data_petro_ts :
        DataFrame with time-series parameter data.
    results :
        Dictionary to collect parameter DataFrames.
    tec_ts :
        Set of technology names.
    nodes :
        List of model nodes.
    """
    for t in tec_ts:
        common = dict(
            time="year",
            time_origin="year",
            time_dest="year",
        )

        param_name = data_petro_ts.loc[
            (data_petro_ts["technology"] == t), "parameter"
        ].unique()

        for p in set(param_name):
            val = data_petro_ts.loc[
                (data_petro_ts["technology"] == t) & (data_petro_ts["parameter"] == p),
                "value",
            ]
            mod = data_petro_ts.loc[
                (data_petro_ts["technology"] == t) & (data_petro_ts["parameter"] == p),
                "mode",
            ]
            yr = data_petro_ts.loc[
                (data_petro_ts["technology"] == t) & (data_petro_ts["parameter"] == p),
                "year",
            ]

            if p == "var_cost":
                df = make_df(
                    p,
                    technology=t,
                    value=val,
                    unit="t",
                    year_vtg=yr,
                    year_act=yr,
                    mode=mod,
                    **common,
                ).pipe(broadcast, node_loc=nodes)
            else:
                rg = data_petro_ts.loc[
                    (data_petro_ts["technology"] == t)
                    & (data_petro_ts["parameter"] == p),
                    "region",
                ]
                df = make_df(
                    p,
                    technology=t,
                    value=val,
                    unit="t",
                    year_vtg=yr,
                    year_act=yr,
                    mode=mod,
                    node_loc=rg,
                    **common,
                )

            results[p].append(df)


def format_par_data(
    config: "Context",
    data_petro: pd.DataFrame,
    results: dict,
    yv_ya: pd.DataFrame,
    modelyears: list[int],
    nodes: list[str],
    global_region: str,
):
    for t in config["technology"]["add"]:
        # Retrieve the id if `t` is a Code instance; otherwise use str
        t = getattr(t, "id", t)
        # years = s_info.Y
        params = data_petro.loc[(data_petro["technology"] == t), "parameter"].unique()

        # Availability year of the technology
        av = data_petro.loc[(data_petro["technology"] == t), "availability"].values[0]
        modelyears = [year for year in modelyears if year >= av]
        yva = yv_ya.loc[yv_ya.year_vtg >= av,]

        # Iterate over parameters
        for par in params:
            split = par.split("|")
            param_name = par.split("|")[0]

            val = data_petro.loc[
                ((data_petro["technology"] == t) & (data_petro["parameter"] == par)),
                "value",
            ]

            regions = data_petro.loc[
                ((data_petro["technology"] == t) & (data_petro["parameter"] == par)),
                "Region",
            ]

            # Common parameters for all input and output tables
            # node_dest and node_origin are the same as node_loc

            common = dict(
                year_vtg=yva.year_vtg,
                year_act=yva.year_act,
                time="year",
                time_origin="year",
                time_dest="year",
            )

            for rg in regions:
                if len(split) > 1:
                    if (param_name == "input") | (param_name == "output"):
                        df = assign_input_outpt(
                            split,
                            param_name,
                            regions,
                            val,
                            t,
                            rg,
                            global_region,
                            common,
                            nodes,
                        )
                    elif param_name == "emission_factor":
                        emi = split[1]
                        mod = split[2]

                        df = make_df(
                            param_name,
                            technology=t,
                            value=val[regions[regions == rg].index[0]],
                            emission=emi,
                            mode=mod,
                            unit="t",
                            node_loc=rg,
                            **common,
                        )

                    elif param_name == "var_cost":
                        mod = split[1]
                        if rg != global_region:
                            df = (
                                make_df(
                                    param_name,
                                    technology=t,
                                    mode=mod,
                                    value=val[regions[regions == rg].index[0]],
                                    unit="t",
                                    **common,
                                )
                                .pipe(broadcast, node_loc=nodes)
                                .pipe(same_node)
                            )
                        else:
                            df = make_df(
                                param_name,
                                technology=t,
                                mode=mod,
                                value=val[regions[regions == rg].index[0]],
                                node_loc=rg,
                                unit="t",
                                **common,
                            ).pipe(same_node)

                    elif param_name == "share_mode_up":
                        mod = split[1]

                        df = (
                            make_df(
                                param_name,
                                technology=t,
                                mode=mod,
                                shares="steam_cracker",
                                value=val[regions[regions == rg].index[0]],
                                unit="-",
                                **common,
                            )
                            .pipe(broadcast, node_share=nodes)
                            .pipe(same_node)
                        )

                # Rest of the parameters apart from input, output and emission_factor

                else:
                    df = make_df(
                        param_name,
                        technology=t,
                        value=val[regions[regions == rg].index[0]],
                        unit="t",
                        node_loc=rg,
                        **common,
                    )
                    df = df.drop_duplicates()

                # Copy parameters to all regions
                if (len(regions) == 1) and (rg != global_region):
                    df = broadcast_to_regions(df, global_region, nodes)

                results[param_name].append(df)


def assign_input_outpt(
    split: str,
    param_name: str,
    regions: pd.DataFrame,
    val: Union[float, int],
    t: str,
    rg: str,
    global_region: str,
    common: dict,
    nodes: list[str],
) -> pd.DataFrame:
    """Assign input/output parameters for petrochemical technologies.

    Parameters
    ----------
    split :
        Split parameter name.
    param_name :
        Parameter name.
    regions :
        Regions for the parameter.
    val :
        Parameter values.
    t :
        Technology name.
    rg :
        Region name
    global_region :
        Name of the global region.
    common :
        Common parameter dictionary.
    nodes :
        Model nodes.
    """
    com = split[1]
    lev = split[2]
    mod = split[3]

    if (param_name == "input") and (lev == "import"):
        df = make_df(
            param_name,
            technology=t,
            commodity=com,
            level=lev,
            mode=mod,
            value=val[regions[regions == rg].index[0]],
            unit="t",
            node_loc=rg,
            node_origin=global_region,
            **common,
        )
    elif (param_name == "output") and (lev == "export"):
        df = make_df(
            param_name,
            technology=t,
            commodity=com,
            level=lev,
            mode=mod,
            value=val[regions[regions == rg].index[0]],
            unit="t",
            node_loc=rg,
            node_dest=global_region,
            **common,
        )
    else:
        df = make_df(
            param_name,
            technology=t,
            commodity=com,
            level=lev,
            mode=mod,
            value=val[regions[regions == rg].index[0]],
            unit="t",
            node_loc=rg,
            **common,
        ).pipe(same_node)

    # Copy parameters to all regions, when node_loc is not GLB
    if (len(regions) == 1) and (rg != global_region):
        df["node_loc"] = None
        df = df.pipe(broadcast, node_loc=nodes)
        if (lev != "import") and (lev != "export"):
            df = df.pipe(same_node)
    return df


def broadcast_to_regions(df: pd.DataFrame, global_region: str, nodes: list[str]):
    """Broadcast a DataFrame to all regions if node_loc is not the global region.

    Parameters
    ----------
    df :
        DataFrame to broadcast regions.
    global_region :
        Name of the global region.
    nodes :
        List of model nodes.
    """
    if "node_loc" in df.columns:
        if (
            len(set(df["node_loc"])) == 1
            and list(set(df["node_loc"]))[0] != global_region
        ):
            df["node_loc"] = None
            df = df.pipe(broadcast, node_loc=nodes)
    return df
