from collections import defaultdict

import message_ix
import pandas as pd
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.data_util import (
    calculate_ini_new_cap,
    read_sector_data,
    read_timeseries,
)
from message_ix_models.model.material.material_demand import material_demand_calc
from message_ix_models.model.material.util import (
    combine_df_dictionaries,
    get_ssp_from_context,
    read_config,
)
from message_ix_models.util import (
    broadcast,
    nodes_ex_world,
    package_data_path,
    same_node,
)


def gen_data_cement(
    scenario: message_ix.Scenario, dry_run: bool = False
) -> dict[str, pd.DataFrame]:
    """Generate data for materials representation of cement industry."""
    # Load configuration
    context = read_config()
    config = read_config()["material"]["cement"]
    ssp = get_ssp_from_context(context)
    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    data_cement = read_sector_data(scenario, "cement", None, "cement_R12.csv")
    # Special treatment for time-dependent Parameters
    data_cement_ts = read_timeseries(scenario, "cement", None, "timeseries_R12.csv")
    tec_ts = set(data_cement_ts.technology)  # set of tecs with var_cost

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are different input and output combinations
    # Iterate over technologies
    yv_ya = s_info.yv_ya
    yv_ya = yv_ya.loc[yv_ya.year_vtg >= 1980]
    # Do not parametrize GLB region the same way
    nodes = nodes_ex_world(s_info.N)

    for t in config["technology"]["add"]:
        t = t.id
        params = data_cement.loc[(data_cement["technology"] == t), "parameter"].unique()

        # Special treatment for time-varying params
        if t in tec_ts:
            common = dict(
                time="year",
                time_origin="year",
                time_dest="year",
            )

            param_name = data_cement_ts.loc[
                (data_cement_ts["technology"] == t), "parameter"
            ]

            for p in set(param_name):
                val = data_cement_ts.loc[
                    (data_cement_ts["technology"] == t)
                    & (data_cement_ts["parameter"] == p),
                    "value",
                ]
                # units = data_cement_ts.loc[
                #     (data_cement_ts["technology"] == t)
                #     & (data_cement_ts["parameter"] == p),
                #     "units",
                # ].values[0]
                mod = data_cement_ts.loc[
                    (data_cement_ts["technology"] == t)
                    & (data_cement_ts["parameter"] == p),
                    "mode",
                ]
                yr = data_cement_ts.loc[
                    (data_cement_ts["technology"] == t)
                    & (data_cement_ts["parameter"] == p),
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
                    rg = data_cement_ts.loc[
                        (data_cement_ts["technology"] == t)
                        & (data_cement_ts["parameter"] == p),
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

        # Iterate over parameters
        for par in params:
            # Obtain the parameter names, commodity,level,emission
            split = par.split("|")
            param_name = split[0]
            # Obtain the scalar value for the parameter
            val = data_cement.loc[
                ((data_cement["technology"] == t) & (data_cement["parameter"] == par)),
                "value",
            ]  # .values
            regions = data_cement.loc[
                ((data_cement["technology"] == t) & (data_cement["parameter"] == par)),
                "region",
            ]  # .values

            common = dict(
                year_vtg=yv_ya.year_vtg,
                year_act=yv_ya.year_act,
                # mode="M1",
                time="year",
                time_origin="year",
                time_dest="year",
            )

            for rg in regions:
                # For the parameters which inlcudes index names
                if len(split) > 1:
                    if (param_name == "input") | (param_name == "output"):
                        # Assign commodity and level names
                        com = split[1]
                        lev = split[2]
                        mod = split[3]

                        df = make_df(
                            param_name,
                            technology=t,
                            commodity=com,
                            level=lev,
                            value=val[regions[regions == rg].index[0]],
                            mode=mod,
                            unit="t",
                            node_loc=rg,
                            **common,
                        ).pipe(same_node)

                    elif param_name == "emission_factor":
                        # Assign the emisson type
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
                        )  # .pipe(broadcast, \
                        # node_loc=nodes))

                    else:  # time-independent var_cost
                        mod = split[1]
                        df = make_df(
                            param_name,
                            technology=t,
                            value=val[regions[regions == rg].index[0]],
                            mode=mod,
                            unit="t",
                            node_loc=rg,
                            **common,
                        )  # .pipe(broadcast, node_loc=nodes))

                # Parameters with only parameter name
                else:
                    df = make_df(
                        param_name,
                        technology=t,
                        value=val[regions[regions == rg].index[0]],
                        unit="t",
                        node_loc=rg,
                        **common,
                    )  # .pipe(broadcast, node_loc=nodes))

                if len(regions) == 1:
                    df["node_loc"] = None
                    df = df.pipe(broadcast, node_loc=nodes).pipe(same_node)

                results[param_name].append(df)

    # Create external demand param
    parname = "demand"
    df_2025 = pd.read_csv(package_data_path("material", "cement", "demand_2025.csv"))
    df_demand = material_demand_calc.derive_demand("cement", scenario, ssp=ssp)
    df_demand = df_demand[df_demand["year"] != 2025]
    df_demand = pd.concat([df_2025, df_demand])
    results[parname].append(df_demand)

    # Add CCS as addon
    parname = "addon_conversion"

    technology_1 = ["clinker_dry_cement"]
    df_1 = make_df(
        parname, mode="M1", type_addon="dry_ccs_cement", value=1, unit="-", **common
    ).pipe(broadcast, node=nodes, technology=technology_1)

    technology_2 = ["clinker_wet_cement"]
    df_2 = make_df(
        parname, mode="M1", type_addon="wet_ccs_cement", value=1, unit="-", **common
    ).pipe(broadcast, node=nodes, technology=technology_2)

    results[parname].append(df_1)
    results[parname].append(df_2)

    # Concatenate to one data frame per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    results["initial_new_capacity_up"] = pd.concat(
        [
            calculate_ini_new_cap(
                df_demand=df_demand.copy(deep=True),
                technology="clinker_dry_ccs_cement",
                material="cement",
                ssp=ssp,
            ),
            calculate_ini_new_cap(
                df_demand=df_demand.copy(deep=True),
                technology="clinker_wet_ccs_cement",
                material="cement",
                ssp=ssp,
            ),
        ]
    )
    results = combine_df_dictionaries(
        results,
        gen_grow_cap_up(s_info, ssp),
        read_furnace_2020_bound(),
        gen_clinker_ratios(s_info),
    )

    reduced_pdict = {}
    for k, v in results.items():
        if set(["year_act", "year_vtg"]).issubset(v.columns):
            v = v[(v["year_act"] - v["year_vtg"]) <= 25]
        reduced_pdict[k] = v.drop_duplicates().copy(deep=True)

    return reduced_pdict


def gen_grow_cap_up(s_info, ssp):
    ssp_vals = {
        "LED": 0.0009,
        "SSP1": 0.0009,
        "SSP2": 0.0009,
        "SSP3": 0.0007,
        "SSP4": 0.0010,
        "SSP5": 0.0010,
    }

    df = (
        make_df(
            "growth_new_capacity_up",
            technology=["clinker_dry_ccs_cement", "clinker_wet_ccs_cement"],
            value=ssp_vals[ssp],
            unit="???",
        )
        .pipe(broadcast, node_loc=nodes_ex_world(s_info.N))
        .pipe(broadcast, year_vtg=s_info.Y)
    )
    return {"growth_new_capacity_up": df}


def read_furnace_2020_bound():
    df = pd.read_csv(package_data_path("material", "cement", "cement_bound_2020.csv"))
    return {"bound_activity_lo": df}


def gen_clinker_ratios(s_info):
    # 2020 ratios from
    # https://www.sciencedirect.com/science/article/pii/S1750583624002238#bib0071
    # Appendix B
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
    df = make_df(
        "input",
        node_loc=reg_map.keys(),
        node_origin=reg_map.keys(),
        value=reg_map.values(),
        commodity="clinker_cement",
        level="tertiary_material",
        mode="M1",
        time="year",
        time_origin="year",
        unit="???",
    )
    df = (
        df.pipe(
            broadcast,
            technology=["grinding_ballmill_cement", "grinding_vertmill_cement"],
        )
        .pipe(broadcast, year_act=s_info.Y)
        .pipe(broadcast, year_vtg=s_info.yv_ya["year_vtg"].unique())
    )
    df = df[df["year_act"] >= df["year_vtg"]]
    df = df[df["year_act"] - df["year_vtg"] < 25]
    return {"input": df}
