from collections import defaultdict
from typing import Union

import message_ix
import pandas as pd
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.data_util import read_timeseries
from message_ix_models.model.material.material_demand import material_demand_calc
from message_ix_models.model.material.util import get_ssp_from_context, read_config
from message_ix_models.util import (
    broadcast,
    nodes_ex_world,
    package_data_path,
    same_node,
)

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


def read_data_petrochemicals(scenario: message_ix.Scenario) -> pd.DataFrame:
    """Read and clean data from :file:`petrochemicals_techno_economic.xlsx`."""

    # Ensure config is loaded, get the context
    s_info = ScenarioInfo(scenario)
    fname = "petrochemicals_techno_economic.xlsx"

    if "R12_CHN" in s_info.N:
        sheet_n = "data_R12"
    else:
        sheet_n = "data_R11"

    # Read the file
    data_petro = pd.read_excel(
        package_data_path("material", "petrochemicals", fname), sheet_name=sheet_n
    )
    # Clean the data

    data_petro = data_petro.drop(["Source", "Description"], axis=1)

    return data_petro


def gen_mock_demand_petro(
    scenario: message_ix.Scenario,
    gdp_elasticity_2020: float,
    gdp_elasticity_2030: float,
) -> pd.DataFrame:
    s_info = ScenarioInfo(scenario)
    modelyears = s_info.Y
    fy = scenario.firstmodelyear

    def get_demand_t1_with_income_elasticity(
        demand_t0, income_t0, income_t1, elasticity
    ):
        return (
            elasticity * demand_t0.mul(((income_t1 - income_t0) / income_t0), axis=0)
        ) + demand_t0

    gdp_mer = scenario.par("bound_activity_up", {"technology": "GDP"})
    mer_to_ppp = pd.read_csv(
        package_data_path("material", "other", "mer_to_ppp_default.csv")
    ).set_index(["node", "year"])
    # mer_to_ppp = scenario.par("MERtoPPP").set_index("node", "year")
    # TODO: might need to be re-activated for different SSPs
    gdp_mer = gdp_mer.merge(
        mer_to_ppp.reset_index()[["node", "year", "value"]],
        left_on=["node_loc", "year_act"],
        right_on=["node", "year"],
    )
    gdp_mer["gdp_ppp"] = gdp_mer["value_y"] * gdp_mer["value_x"]
    gdp_mer = gdp_mer[["year", "node_loc", "gdp_ppp"]].reset_index()
    gdp_mer["Region"] = gdp_mer["node_loc"]  # .str.replace("R12_", "")
    df_gdp_ts = gdp_mer.pivot(
        index="Region", columns="year", values="gdp_ppp"
    ).reset_index()
    num_cols = [i for i in df_gdp_ts.columns if isinstance(i, int)]
    hist_yrs = [i for i in num_cols if i < fy]
    df_gdp_ts = (
        df_gdp_ts.drop([i for i in hist_yrs if i in df_gdp_ts.columns], axis=1)
        .set_index("Region")
        .sort_index()
    )

    # 2018 production
    # Use as 2020
    # The Future of Petrochemicals Methodological Annex
    # Projections here do not show too much growth until 2050 for some regions.
    # For division of some regions assumptions made:
    # PAO, PAS, SAS, EEU,WEU
    # For R12: China and CPA demand divided by 0.1 and 0.9.
    # SSP2 R11 baseline GDP projection
    # The orders of the regions
    # r = ['R12_AFR', 'R12_RCPA', 'R12_EEU', 'R12_FSU', 'R12_LAM', 'R12_MEA',\
    #        'R12_NAM', 'R12_PAO', 'R12_PAS', 'R12_SAS', 'R12_WEU',"R12_CHN"]

    # if "R12_CHN" in nodes:
    #     nodes.remove("R12_GLB")
    #     dem_2020 = np.array([2.4, 0.44, 3, 5, 11, 40.3, 49.8, 11,
    #     37.5, 10.7, 29.2, 50.5])
    #     dem_2020 = pd.Series(dem_2020)
    #
    # else:
    #     nodes.remove("R11_GLB")
    #     dem_2020 = np.array([2, 75, 30, 4, 11, 42, 60, 32, 30, 29, 35])
    #     dem_2020 = pd.Series(dem_2020)

    from message_ix_models.model.material.material_demand.material_demand_calc import (
        read_base_demand,
    )

    df_demand_2020 = read_base_demand(
        package_data_path() / "material" / "petrochemicals/demand_petro.yaml"
    )
    df_demand_2020 = df_demand_2020.rename({"region": "Region"}, axis=1)
    df_demand = df_demand_2020.pivot(index="Region", columns="year", values="value")
    dem_next_yr = df_demand

    for i in range(len(modelyears) - 1):
        income_year1 = modelyears[i]
        income_year2 = modelyears[i + 1]

        if income_year2 >= 2030:
            dem_next_yr = get_demand_t1_with_income_elasticity(
                dem_next_yr,
                df_gdp_ts[income_year1],
                df_gdp_ts[income_year2],
                gdp_elasticity_2030,
            )
        else:
            dem_next_yr = get_demand_t1_with_income_elasticity(
                dem_next_yr,
                df_gdp_ts[income_year1],
                df_gdp_ts[income_year2],
                gdp_elasticity_2020,
            )
        df_demand[income_year2] = dem_next_yr

    df_melt = df_demand.melt(ignore_index=False).reset_index()

    return make_df(
        "demand",
        unit="t",
        level="demand",
        value=df_melt.value,
        time="year",
        commodity="HVC",
        year=df_melt.year,
        node=df_melt["Region"],
    )


def gen_data_petro_ts(
    data_petro_ts: pd.DataFrame, results: dict[list], tec_ts: set[str], nodes: list[str]
) -> None:
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
            # units = data_petro_ts.loc[
            #     (data_petro_ts["technology"] == t) &
            #     (data_petro_ts["parameter"] == p),
            #     "units",
            # ].values[0]
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
        # print("copying to all R11", rg, lev)
        df["node_loc"] = None
        df = df.pipe(broadcast, node_loc=nodes)  # .pipe(same_node)
        # Use same_node only for non-trade technologies
        if (lev != "import") and (lev != "export"):
            df = df.pipe(same_node)
    return df


def broadcast_to_regions(df: pd.DataFrame, global_region: str, nodes: list[str]):
    if "node_loc" in df.columns:
        if (
            len(set(df["node_loc"])) == 1
            and list(set(df["node_loc"]))[0] != global_region
        ):
            # print("Copying to all R11")
            df["node_loc"] = None
            df = df.pipe(broadcast, node_loc=nodes)
    return df


def gen_data_petro_chemicals(
    scenario: message_ix.Scenario, dry_run: bool = False
) -> dict[str, pd.DataFrame]:
    # Load configuration
    context = read_config()
    config = context["material"]["petro_chemicals"]
    ssp = get_ssp_from_context(context)
    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    data_petro = read_data_petrochemicals(scenario)
    data_petro_ts = read_timeseries(
        scenario, "petrochemicals", None, "petrochemicals_techno_economic.xlsx"
    )
    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    modelyears = s_info.Y  # s_info.Y is only for modeling years
    nodes = nodes_ex_world(s_info.N)
    global_region = [i for i in s_info.N if i.endswith("_GLB")][0]
    yv_ya = s_info.yv_ya

    for t in config["technology"]["add"]:
        t = t.id
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
    demand_hvc = material_demand_calc.gen_demand_petro(
        scenario, "HVC", default_gdp_elasticity_2020, default_gdp_elasticity_2030
    )
    results["demand"].append(demand_hvc)

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

    # TODO: move this to input xlsx file
    df_gro = results["growth_activity_up"]
    drop_idx = df_gro[
        (df_gro["technology"] == "steam_cracker_petro")
        & (df_gro["node_loc"] == "R12_RCPA")
        & (df_gro["year_act"] == 2020)
    ].index
    results["growth_activity_up"] = results["growth_activity_up"].drop(drop_idx)
    return results
