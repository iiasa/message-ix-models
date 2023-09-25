import pandas as pd
import numpy as np
from collections import defaultdict
from .data_util import read_timeseries, read_rel

import message_ix

from .util import read_config
from message_ix_models import ScenarioInfo
from message_ix import make_df
from message_ix_models.util import (
    broadcast,
    make_io,
    make_matched_dfs,
    same_node,
    add_par_data,
    package_data_path,
)


def read_data_petrochemicals(scenario):
    """Read and clean data from :file:`petrochemicals_techno_economic.xlsx`."""

    # Ensure config is loaded, get the context
    context = read_config()
    s_info = ScenarioInfo(scenario)
    fname = "petrochemicals_techno_economic.xlsx"

    if "R12_CHN" in s_info.N:
        sheet_n = "data_R12"
    else:
        sheet_n = "data_R11"

    # Read the file
    data_petro = pd.read_excel(package_data_path("material", "petrochemicals", fname),
    sheet_name=sheet_n)
    # Clean the data

    data_petro = data_petro.drop(["Source", "Description"], axis=1)

    return data_petro


def gen_mock_demand_petro(scenario, gdp_elasticity_2020, gdp_elasticity_2030):

    context = read_config()
    s_info = ScenarioInfo(scenario)
    modelyears = s_info.Y #s_info.Y is only for modeling years
    nodes = s_info.N

    def get_demand_t1_with_income_elasticity(
        demand_t0, income_t0, income_t1, elasticity
    ):
        return (
            elasticity * demand_t0 * ((income_t1 - income_t0) / income_t0)
        ) + demand_t0

    df_gdp = pd.read_excel(
        package_data_path("material", "methanol", "methanol demand.xlsx"),
        sheet_name="GDP_baseline",
    )

    df = df_gdp[(~df_gdp["Region"].isna()) & (df_gdp["Region"] != "World")]
    df = df.dropna(axis=1)

    df_demand = df.copy(deep=True)
    df_demand = df_demand.drop([2010, 2015, 2020], axis=1)

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

    if "R12_CHN" in nodes:
        nodes.remove("R12_GLB")
        region_set = 'R12_'
        dem_2020 = np.array([2.4, 0.44, 3, 5, 11, 40.3, 49.8, 11, 37.5, 10.7, 29.2, 50.5])
        dem_2020 = pd.Series(dem_2020)

    else:
        nodes.remove("R11_GLB")
        region_set = 'R11_'
        dem_2020 = np.array([2, 75, 30, 4, 11, 42, 60, 32, 30, 29, 35])
        dem_2020 = pd.Series(dem_2020)

    df_demand[2020] = dem_2020

    for i in range(len(modelyears) - 1):
        income_year1 = modelyears[i]
        income_year2 = modelyears[i + 1]

        if income_year2 >= 2030:
            dem_2020 = get_demand_t1_with_income_elasticity(
                dem_2020, df[income_year1], df[income_year2], gdp_elasticity_2030
            )
        else:
            dem_2020 = get_demand_t1_with_income_elasticity(
                dem_2020, df[income_year1], df[income_year2], gdp_elasticity_2020
            )
        df_demand[income_year2] = dem_2020

    df_melt = df_demand.melt(
        id_vars=["Region"], value_vars=df_demand.columns[5:], var_name="year"
    )

    return message_ix.make_df(
        "demand",
        unit="t",
        level="demand",
        value=df_melt.value,
        time="year",
        commodity="HVC",
        year=df_melt.year,
        node=(region_set + df_melt["Region"]),
    )


def gen_data_petro_chemicals(scenario, dry_run=False):
    # Load configuration

    config = read_config()["material"]["petro_chemicals"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    data_petro = read_data_petrochemicals(scenario)
    data_petro_ts = read_timeseries(scenario, "petrochemicals",
    "petrochemicals_techno_economic.xlsx")
    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    allyears = s_info.set["year"]
    modelyears = s_info.Y  # s_info.Y is only for modeling years
    nodes = s_info.N
    yv_ya = s_info.yv_ya
    fmy = s_info.y0
    nodes.remove("World")

    # Do not parametrize GLB region the same way
    if "R11_GLB" in nodes:
        nodes.remove("R11_GLB")
        global_region = "R11_GLB"
    if "R12_GLB" in nodes:
        nodes.remove("R12_GLB")
        global_region = "R12_GLB"

    for t in config["technology"]["add"]:

        # years = s_info.Y
        params = data_petro.loc[
            (data_petro["technology"] == t), "parameter"
        ].values.tolist()

        # Availability year of the technology
        av = data_petro.loc[(data_petro["technology"] == t), "availability"].values[0]
        modelyears = [year for year in modelyears if year >= av]
        yva = yv_ya.loc[
            yv_ya.year_vtg >= av,
        ]

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
                                **common
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
                                **common
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
                                **common
                            ).pipe(same_node)

                        # Copy parameters to all regions, when node_loc is not GLB
                        if (len(regions) == 1) and (rg != global_region):
                            # print("copying to all R11", rg, lev)
                            df["node_loc"] = None
                            df = df.pipe(broadcast, node_loc=nodes)  # .pipe(same_node)
                            # Use same_node only for non-trade technologies
                            if (lev != "import") and (lev != "export"):
                                df = df.pipe(same_node)

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
                            **common
                        )

                    elif param_name == "var_cost":
                        mod = split[1]
                        if rg != global_region:
                            df = (
                                make_df(
                                    param_name,
                                    technology=t,
                                    commodity=com,
                                    level=lev,
                                    mode=mod,
                                    value=val[regions[regions == rg].index[0]],
                                    unit="t",
                                    **common
                                )
                                .pipe(broadcast, node_loc=nodes)
                                .pipe(same_node)
                            )
                        else:
                            df = (
                                make_df(
                                    param_name,
                                    technology=t,
                                    commodity=com,
                                    level=lev,
                                    mode=mod,
                                    value=val[regions[regions == rg].index[0]],
                                    node_loc=rg,
                                    unit="t",
                                    **common
                                )
                                .pipe(same_node)
                            )

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
                                **common
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
                        **common
                    )
                    df = df.drop_duplicates()

                # Copy parameters to all regions
                if (len(regions) == 1) and (rg != global_region):
                    if "node_loc" in df.columns:
                        if (
                            len(set(df["node_loc"])) == 1
                            and list(set(df["node_loc"]))[0] != global_region
                        ):
                            # print("Copying to all R11")
                            df["node_loc"] = None
                            df = df.pipe(broadcast, node_loc=nodes)

                results[param_name].append(df)

    share_dict = {
        "shares": "steam_cracker",
        "node_share": ["R12_MEA", "R12_NAM"],
        "technology": "steam_cracker_petro",
        "mode": "ethane",
        "year_act": "2020",
        "time": "year",
        "value": [0.4, 0.4],
        "unit": "-"
    }
    results["share_mode_lo"].append(message_ix.make_df("share_mode_lo", **share_dict))

    # Add demand
    # Create external demand param

    # demand_HVC = derive_petro_demand(scenario)
    default_gdp_elasticity = float(0.93)
    context = read_config()

    df_pars = pd.read_excel(
        package_data_path(
            "material", "methanol", "methanol_sensitivity_pars.xlsx"
        ),
        sheet_name="Sheet1",
        dtype=object,
    )
    pars = df_pars.set_index("par").to_dict()["value"]
    default_gdp_elasticity_2020 = pars["hvc_elasticity_2020"]
    default_gdp_elasticity_2030 = pars["hvc_elasticity_2030"]
    demand_HVC = gen_mock_demand_petro(scenario, default_gdp_elasticity_2020, default_gdp_elasticity_2030)
    results["demand"].append(demand_HVC)

    # df_e = make_df(paramname, level='final_material', commodity="ethylene", \
    # value=demand_e.value, unit='t',year=demand_e.year, time='year', \
    # node=demand_e.node)#.pipe(broadcast, node=nodes)
    # results["demand"].append(df_e)
    #
    # df_p = make_df(paramname, level='final_material', commodity="propylene", \
    # value=demand_p.value, unit='t',year=demand_p.year, time='year', \
    # node=demand_p.node)#.pipe(broadcast, node=nodes)
    # results["demand"].append(df_p)
    #
    # df_BTX = make_df(paramname, level='final_material', commodity="BTX", \
    # value=demand_BTX.value, unit='t',year=demand_BTX.year, time='year', \
    # node=demand_BTX.node)#.pipe(broadcast, node=nodes)
    # results["demand"].append(df_BTX)

    # Special treatment for time-varying params

    tec_ts = set(data_petro_ts.technology)  # set of tecs in timeseries sheet

    for t in tec_ts:
        common = dict(
            time="year",
            time_origin="year",
            time_dest="year",
        )

        param_name = data_petro_ts.loc[(data_petro_ts["technology"] == t), "parameter"]

        for p in set(param_name):
            val = data_petro_ts.loc[
                (data_petro_ts["technology"] == t) & (data_petro_ts["parameter"] == p),
                "value",
            ]
            units = data_petro_ts.loc[
                (data_petro_ts["technology"] == t) & (data_petro_ts["parameter"] == p),
                "units",
            ].values[0]
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
                    **common
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
                    **common
                )

            results[p].append(df)

    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    # modify steam cracker hist data (naphtha -> gasoil) to make model feasible
    df_cap = pd.read_csv(package_data_path(
            "material", "petrochemicals", "steam_cracking_hist_new_cap.csv"
        ))
    df_act = pd.read_csv(package_data_path(
            "material", "petrochemicals", "steam_cracking_hist_act.csv"
        ))
    df_act.loc[df_act["mode"]=="naphtha", "mode"] = "vacuum_gasoil"
    df = results["historical_activity"]
    results["historical_activity"] = pd.concat([df.loc[df["technology"]!="steam_cracker_petro"], df_act])
    df = results["historical_new_capacity"]
    results["historical_new_capacity"] = pd.concat([df.loc[df["technology"]!="steam_cracker_petro"], df_cap])

    # remove growth constraint for R12_AFR to make trade constraints feasible
    df = results["growth_activity_up"]
    results["growth_activity_up"] = df[~((df["technology"]=="steam_cracker_petro") &
                                          (df["node_loc"]=="R12_AFR") &
                                          (df["year_act"]==2020))]

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
    results["bound_activity_up"] = pd.concat([results["bound_activity_up"],
                                              message_ix.make_df("bound_activity_up", **df_dem, **par_dict)])

    return results
