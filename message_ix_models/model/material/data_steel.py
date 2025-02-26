from collections import defaultdict
from collections.abc import Iterable
from typing import Literal

import message_ix
import pandas as pd
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.data_util import (
    calculate_ini_new_cap,
    read_rel,
    read_sector_data,
    read_timeseries,
)
from message_ix_models.model.material.material_demand import material_demand_calc
from message_ix_models.model.material.util import (
    combine_df_dictionaries,
    get_ssp_from_context,
    maybe_remove_water_tec,
    read_config,
    remove_from_list_if_exists,
)
from message_ix_models.util import (
    broadcast,
    nodes_ex_world,
    package_data_path,
    same_node,
)


def gen_mock_demand_steel(scenario: message_ix.Scenario) -> pd.DataFrame:
    s_info = ScenarioInfo(scenario)
    nodes = s_info.N
    nodes.remove("World")

    # The order:
    # r = ['R12_AFR', 'R12_RCPA', 'R12_EEU', 'R12_FSU', 'R12_LAM', 'R12_MEA',\
    # 'R12_NAM', 'R12_PAO', 'R12_PAS', 'R12_SAS', 'R12_WEU',"R12_CHN"]

    # Finished steel demand from:
    # https://www.oecd.org/industry/ind/Item_4b_Worldsteel.pdf
    # For region definitions:
    # https://worldsteel.org/wp-content/uploads/2021-World-Steel-in-Figures.pdf
    # For detailed assumptions and calculation see: steel_demand_calculation.xlsx
    # under
    # https://iiasahub.sharepoint.com/
    # sites/eceprog?cid=75ea8244-8757-44f1-83fd-d34f94ffd06a

    if "R12_CHN" in nodes:
        nodes.remove("R12_GLB")
        sheet_n = "data_R12"
        region_set = "R12_"
        d = [
            20.04,
            12.08,
            56.55,
            56.5,
            64.94,
            54.26,
            97.76,
            91.3,
            65.2,
            164.28,
            131.95,
            980.1,
        ]

    else:
        nodes.remove("R11_GLB")
        sheet_n = "data_R11"
        region_set = "R11_"
        d = [
            20.04,
            992.18,
            56.55,
            56.5,
            64.94,
            54.26,
            97.76,
            91.3,
            65.2,
            164.28,
            131.95,
        ]
        # MEA change from 39 to 9 to make it feasible (coal supply bound)

    # SSP2 R11 baseline GDP projection
    gdp_growth = pd.read_excel(
        package_data_path("material", "other", "iamc_db ENGAGE baseline GDP PPP.xlsx"),
        sheet_name=sheet_n,
    )

    gdp_growth = gdp_growth.loc[
        (gdp_growth["Scenario"] == "baseline") & (gdp_growth["Region"] != "World")
    ].drop(["Model", "Variable", "Unit", "Notes", 2000, 2005], axis=1)

    gdp_growth["Region"] = region_set + gdp_growth["Region"]

    demand2020_steel = (
        pd.DataFrame({"Region": nodes, "Val": d})
        .join(gdp_growth.set_index("Region"), on="Region")
        .rename(columns={"Region": "node"})
    )

    demand2020_steel.iloc[:, 3:] = (
        demand2020_steel.iloc[:, 3:]
        .div(demand2020_steel[2020], axis=0)
        .multiply(demand2020_steel["Val"], axis=0)
    )

    demand2020_steel = pd.melt(
        demand2020_steel.drop(["Val", "Scenario"], axis=1),
        id_vars=["node"],
        var_name="year",
        value_name="value",
    )

    return demand2020_steel


def gen_data_steel_ts(
    data_steel_ts: pd.DataFrame, results: dict[str, list], t: str, nodes: list[str]
):
    common = dict(
        time="year",
        time_origin="year",
        time_dest="year",
    )

    param_name = data_steel_ts.loc[
        (data_steel_ts["technology"] == t), "parameter"
    ].unique()

    for p in set(param_name):
        val = data_steel_ts.loc[
            (data_steel_ts["technology"] == t) & (data_steel_ts["parameter"] == p),
            "value",
        ]
        # units = data_steel_ts.loc[
        #     (data_steel_ts["technology"] == t)
        #     & (data_steel_ts["parameter"] == p),
        #     "units",
        # ].values[0]
        mod = data_steel_ts.loc[
            (data_steel_ts["technology"] == t) & (data_steel_ts["parameter"] == p),
            "mode",
        ]
        yr = data_steel_ts.loc[
            (data_steel_ts["technology"] == t) & (data_steel_ts["parameter"] == p),
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
        if p == "output":
            comm = data_steel_ts.loc[
                (data_steel_ts["technology"] == t) & (data_steel_ts["parameter"] == p),
                "commodity",
            ]

            lev = data_steel_ts.loc[
                (data_steel_ts["technology"] == t) & (data_steel_ts["parameter"] == p),
                "level",
            ]

            rg = data_steel_ts.loc[
                (data_steel_ts["technology"] == t) & (data_steel_ts["parameter"] == p),
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
                node_dest=rg,
                commodity=comm,
                level=lev,
                **common,
            )
        else:
            rg = data_steel_ts.loc[
                (data_steel_ts["technology"] == t) & (data_steel_ts["parameter"] == p),
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
    return


def get_data_steel_const(
    data_steel: pd.DataFrame,
    results: dict[str, list],
    params: Iterable,
    t: str,
    yv_ya: pd.DataFrame,
    nodes: list[str],
    global_region: str,
):
    for par in params:
        # Obtain the parameter names, commodity,level,emission
        split = par.split("|")
        param_name = split[0]
        # Obtain the scalar value for the parameter
        val = data_steel.loc[
            ((data_steel["technology"] == t) & (data_steel["parameter"] == par)),
            "value",
        ]  # .values
        regions = data_steel.loc[
            ((data_steel["technology"] == t) & (data_steel["parameter"] == par)),
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
                    if (param_name == "input") and (lev == "import"):
                        df = make_df(
                            param_name,
                            technology=t,
                            commodity=com,
                            level=lev,
                            value=val[regions[regions == rg].index[0]],
                            mode=mod,
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
                            value=val[regions[regions == rg].index[0]],
                            mode=mod,
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
                            value=val[regions[regions == rg].index[0]],
                            mode=mod,
                            unit="t",
                            node_loc=rg,
                            **common,
                        ).pipe(same_node)

                    # Copy parameters to all regions, when node_loc is not GLB
                    if (len(regions) == 1) and (rg != global_region):
                        df["node_loc"] = None
                        df = df.pipe(broadcast, node_loc=nodes)
                        # Use same_node only for non-trade technologies
                        if (lev != "import") and (lev != "export"):
                            df = df.pipe(same_node)

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
                    )

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
                    )

            # Parameters with only parameter name
            else:
                df = make_df(
                    param_name,
                    technology=t,
                    value=val[regions[regions == rg].index[0]],
                    unit="t",
                    node_loc=rg,
                    **common,
                )

            # Copy parameters to all regions
            if (
                len(set(df["node_loc"])) == 1
                and list(set(df["node_loc"]))[0] != global_region
            ):
                df["node_loc"] = None
                df = df.pipe(broadcast, node_loc=nodes)

            results[param_name].append(df)
    return


def gen_data_steel_rel(data_steel_rel, results, regions, modelyears):
    for reg in regions:
        for r in data_steel_rel["relation"].unique():
            model_years_rel = modelyears.copy()
            if r is None:
                break
            if r == "max_global_recycling_steel":
                continue
            if r in ["minimum_recycling_steel", "max_regional_recycling_steel"]:
                # Do not implement the minimum recycling rate for the year 2020
                remove_from_list_if_exists(2020, model_years_rel)

            params = set(
                data_steel_rel.loc[
                    (data_steel_rel["relation"] == r), "parameter"
                ].values
            )

            common_rel = dict(
                year_rel=model_years_rel,
                year_act=model_years_rel,
                mode="M1",
                relation=r,
            )

            for par_name in params:
                if par_name == "relation_activity":
                    tec_list = data_steel_rel.loc[
                        (
                            (data_steel_rel["relation"] == r)
                            & (data_steel_rel["parameter"] == par_name)
                        ),
                        "technology",
                    ]

                    for tec in tec_list.unique():
                        val = data_steel_rel.loc[
                            (
                                (data_steel_rel["relation"] == r)
                                & (data_steel_rel["parameter"] == par_name)
                                & (data_steel_rel["technology"] == tec)
                                & (data_steel_rel["Region"] == reg)
                            ),
                            "value",
                        ].values[0]

                        df = make_df(
                            par_name,
                            technology=tec,
                            value=val,
                            unit="-",
                            node_loc=reg,
                            node_rel=reg,
                            **common_rel,
                        ).pipe(same_node)

                        results[par_name].append(df)

                elif (par_name == "relation_upper") | (par_name == "relation_lower"):
                    val = data_steel_rel.loc[
                        (
                            (data_steel_rel["relation"] == r)
                            & (data_steel_rel["parameter"] == par_name)
                            & (data_steel_rel["Region"] == reg)
                        ),
                        "value",
                    ].values[0]

                    df = make_df(
                        par_name, value=val, unit="-", node_rel=reg, **common_rel
                    )

                    results[par_name].append(df)
    return


def gen_data_steel(scenario: message_ix.Scenario, dry_run: bool = False):
    """Generate data for materials representation of steel industry."""
    # Load configuration
    context = read_config()
    config = context["material"]["steel"]
    ssp = get_ssp_from_context(context)
    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    # TEMP: now add cement sector as well
    # => Need to separate those since now I have get_data_steel and cement
    data_steel = read_sector_data(scenario, "steel", None, "steel_R12.csv")
    # Special treatment for time-dependent Parameters
    data_steel_ts = read_timeseries(scenario, "steel", None, "timeseries_R12.csv")
    data_steel_rel = read_rel(scenario, "steel", None, "relations_R12.csv")

    tec_ts = set(data_steel_ts.technology)  # set of tecs with var_cost

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    modelyears = s_info.Y  # s_info.Y is only for modeling years
    nodes = nodes_ex_world(s_info.N)
    global_region = [i for i in s_info.N if i.endswith("_GLB")][0]
    yv_ya = s_info.yv_ya
    yv_ya = yv_ya.loc[yv_ya.year_vtg >= 1970]

    df = yv_ya.loc[yv_ya.year_act == 2020]
    df["year_act"] = None
    pre_model = df.pipe(broadcast, year_act=df.year_vtg)
    pre_model = pre_model[
        (pre_model["year_act"].ge(pre_model["year_vtg"]))
        & (pre_model["year_act"].lt(2020))
    ]
    yv_ya = pd.concat([pre_model, yv_ya])

    # For each technology there are differnet input and output combinations
    # Iterate over technologies
    for t in config["technology"]["add"]:
        t = t.id
        params = data_steel.loc[(data_steel["technology"] == t), "parameter"].unique()

        # Special treatment for time-varying params
        if t in tec_ts:
            gen_data_steel_ts(data_steel_ts, results, t, nodes)

        # Iterate over parameters
        get_data_steel_const(
            data_steel, results, params, t, yv_ya, nodes, global_region
        )

    # Add relation for the maximum global scrap use in 2020
    for t in [
        "scrap_recovery_steel_1",
        "scrap_recovery_steel_2",
        "scrap_recovery_steel_3",
    ]:
        df_max_recycling = pd.DataFrame(
            {
                "relation": "max_global_recycling_steel",
                "node_rel": "R12_GLB",
                "year_rel": 2020,
                "year_act": 2020,
                "node_loc": nodes,
                "technology": t,
                "mode": "M1",
                "unit": "???",
                "value": data_steel_rel.loc[
                    (
                        (data_steel_rel["relation"] == "max_global_recycling_steel")
                        & (data_steel_rel["parameter"] == "relation_activity")
                    ),
                    "value",
                ].values[0],
            }
        )

        results["relation_activity"].append(df_max_recycling)

    df_max_recycling_upper = pd.DataFrame(
        {
            "relation": "max_global_recycling_steel",
            "node_rel": "R12_GLB",
            "year_rel": 2020,
            "unit": "???",
            "value": data_steel_rel.loc[
                (
                    (data_steel_rel["relation"] == "max_global_recycling_steel")
                    & (data_steel_rel["parameter"] == "relation_upper")
                ),
                "value",
            ].values[0],
        },
        index=[0],
    )
    df_max_recycling_lower = pd.DataFrame(
        {
            "relation": "max_global_recycling_steel",
            "node_rel": "R12_GLB",
            "year_rel": 2020,
            "unit": "???",
            "value": data_steel_rel.loc[
                (
                    (data_steel_rel["relation"] == "max_global_recycling_steel")
                    & (data_steel_rel["parameter"] == "relation_lower")
                ),
                "value",
            ].values[0],
        },
        index=[0],
    )
    results["relation_activity"].append(df_max_recycling)
    results["relation_upper"].append(df_max_recycling_upper)
    results["relation_lower"].append(df_max_recycling_lower)
    # Add relations for scrap grades and availability
    regions = set(data_steel_rel["Region"].values)
    gen_data_steel_rel(data_steel_rel, results, regions, modelyears)

    # Create external demand param
    parname = "demand"
    df_2025 = pd.read_csv(package_data_path("material", "steel", "demand_2025.csv"))
    df_demand = material_demand_calc.derive_demand("steel", scenario, ssp=ssp)
    df_demand = df_demand[df_demand["year"] != 2025]
    df_demand = pd.concat([df_2025, df_demand])
    results[parname].append(df_demand)

    new_scrap_ratio = {
        "R12_AFR": 0.92,
        "R12_CHN": 0.9,
        "R12_EEU": 0.9,
        "R12_FSU": 0.9,
        "R12_LAM": 0.9,
        "R12_MEA": 0.92,
        "R12_NAM": 0.85,
        "R12_PAO": 0.85,
        "R12_PAS": 0.9,
        "R12_RCPA": 0.92,
        "R12_SAS": 0.92,
        "R12_WEU": 0.85,
    }

    scale_fse_demand(df_demand, new_scrap_ratio)

    common = dict(
        year_vtg=yv_ya.year_vtg,
        year_act=yv_ya.year_act,
        time="year",
        time_origin="year",
        time_dest="year",
    )

    # Add CCS as addon
    parname = "addon_conversion"
    bf_tec = ["bf_steel"]
    df = make_df(
        parname, mode="M2", type_addon="bf_ccs_steel_addon", value=1, unit="-", **common
    ).pipe(broadcast, node=nodes, technology=bf_tec)
    results[parname].append(df)

    dri_gas_tec = ["dri_gas_steel"]
    df = make_df(
        parname,
        mode="M1",
        type_addon="dri_gas_ccs_steel_addon",
        value=1,
        unit="-",
        **common,
    ).pipe(broadcast, node=nodes, technology=dri_gas_tec)
    results[parname].append(df)

    dri_tec = ["dri_steel"]
    df = make_df(
        parname, mode="M1", type_addon="dri_steel_addon", value=1, unit="-", **common
    ).pipe(broadcast, node=nodes, technology=dri_tec)
    results[parname].append(df)

    # Concatenate to one data frame per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    results["initial_new_capacity_up"] = pd.concat(
        [
            calculate_ini_new_cap(
                df_demand=df_demand.copy(deep=True),
                technology="dri_gas_ccs_steel",
                material="steel",
                ssp=ssp,
            ),
            calculate_ini_new_cap(
                df_demand=df_demand.copy(deep=True),
                technology="bf_ccs_steel",
                material="steel",
                ssp=ssp,
            ),
        ]
    )

    results["relation_activity"] = pd.concat(
        [results["relation_activity"], gen_cokeoven_co2_cc(s_info)]
    )

    results = combine_df_dictionaries(
        results,
        gen_dri_act_bound(),
        gen_dri_cap_calibration(),
        gen_dri_coal_model(s_info),
    )

    maybe_remove_water_tec(scenario, results)

    results = combine_df_dictionaries(
        results,
        get_scrap_prep_cost(s_info, ssp),
        gen_max_recycling_rel(s_info, ssp),
        gen_grow_cap_up(s_info, ssp),
    )

    results = combine_df_dictionaries(
        results,
        gen_2020_calibration_relation(s_info, "eaf"),
        gen_2020_calibration_relation(s_info, "bof"),
        gen_2020_calibration_relation(s_info, "bf"),
        gen_bof_pig_input(s_info),
        gen_finishing_steel_io(s_info),
        gen_manuf_steel_io(new_scrap_ratio, s_info),
    )

    if ssp == "SSP1":
        df_tmp = results["relation_activity"]
        df_tmp = df_tmp[
            (df_tmp["relation"] == "minimum_recycling_steel")
            & (df_tmp["technology"] == "total_EOL_steel")
        ]
        df_tmp = df_tmp[df_tmp["year_rel"] >= 2030]
        df_tmp["value"] = -0.7

    reduced_pdict = {}
    for k, v in results.items():
        if set(["year_act", "year_vtg"]).issubset(v.columns):
            v = v[(v["year_act"] - v["year_vtg"]) <= 60]
        reduced_pdict[k] = v.drop_duplicates().copy(deep=True)

    return reduced_pdict


def gen_cokeoven_co2_cc(s_info):
    emi_dict = {
        "unit": "Mt C/yr",
        "technology": "cokeoven_steel",
        "mode": "M1",
        "value": 0.814 * 0.2,
        # coal coeffient * difference between coal "final" and "dummy_emission" input
        "relation": "CO2_cc",
    }
    df = (
        make_df("relation_activity", **emi_dict)
        .pipe(broadcast, node_loc=nodes_ex_world(s_info.N), year_act=s_info.Y)
        .pipe(same_node)
    )
    df["year_rel"] = df["year_act"]
    return df


def gen_dri_act_bound():
    df_act = pd.read_csv(
        package_data_path("material", "steel", "dri_activity_2020.csv")
    )
    df_hist = pd.read_csv(
        package_data_path("material", "steel", "dri_activity_hist.csv")
    )
    return {
        "bound_activity_up": df_act,
        "bound_activity_lo": df_act,
        "historical_activity": df_hist,
    }


def gen_dri_cap_calibration():
    df_cap_2020 = pd.read_csv(
        package_data_path("material", "steel", "dri_capacity_2020.csv")
    )
    df_cap_hist = pd.read_csv(
        package_data_path("material", "steel", "dri_capacity_hist.csv")
    )
    return {
        "bound_new_capacity_up": df_cap_2020,
        "bound_new_capacity_lo": df_cap_2020,
        "historical_new_capacity": df_cap_hist,
    }


def gen_dri_coal_model(s_info):
    model_years = s_info.Y
    df = pd.read_csv(package_data_path("material", "steel", "dri_coal.csv"))
    common = {
        "time": "year",
        "time_origin": "year",
        "time_dest": "year",
    }
    par_dict = {}
    for par in df.parameter.unique():
        df_tmp = (
            make_df(par, **df[df["parameter"] == par], **common)
            .pipe(broadcast, year_act=model_years)
            .pipe(same_node)
        )
        if "year_rel" in df_tmp.columns:
            df_tmp["year_rel"] = df_tmp["year_act"]
        if "year_vtg" in df_tmp.columns:
            df_tmp["year_vtg"] = df_tmp["year_act"]
        par_dict[par] = df_tmp

    tec_lt_hist = make_df(
        "technical_lifetime",
        technology="dri_steel",
        year_vtg=[i for i in range(1970, 2020, 5)],
        value=[i for i in range(60, 30, -5)] + [30] * 4,
        unit="???",
        **common,
    ).pipe(
        broadcast,
        node_loc=nodes_ex_world(s_info.N),
    )
    tec_lt = make_df(
        "technical_lifetime",
        technology="dri_steel",
        value=30,
        year_vtg=model_years,
        unit="???",
        **common,
    ).pipe(
        broadcast,
        node_loc=nodes_ex_world(s_info.N),
    )
    par_dict["technical_lifetime"] = pd.concat([tec_lt_hist, tec_lt])
    return par_dict


def gen_2020_calibration_relation(s_info, tech: Literal["eaf", "bof", "bf"]):
    modes = {
        "eaf": ["M1", "M2", "M3"],
        "bof": ["M1", "M2"],
        "bf": ["M1", "M2", "M3", "M4"],
    }
    df = (
        make_df(
            "relation_activity",
            relation=f"{tech}_bound_2020",
            year_rel=2020,
            value=1,
            technology=f"{tech}_steel",
            year_act=2020,
            unit="???",
        )
        .pipe(broadcast, mode=modes[tech])
        .pipe(broadcast, node_loc=nodes_ex_world(s_info.N))
        .pipe(same_node)
        .assign(node_rel=lambda x: x["node_loc"])
    )

    rel_up = pd.read_csv(
        package_data_path("material", "steel", f"{tech}_bound_2020.csv")
    )

    return {"relation_activity": df, "relation_upper": rel_up, "relation_lower": rel_up}


def gen_bof_2020_calibration():
    bound = pd.read_csv(package_data_path("material", "steel", "bof_bound_2020.csv"))

    return {"bound_activity_up": bound, "bound_activity_lo": bound}


def get_scrap_prep_cost(s_info, ssp):
    years1 = [i for i in range(2020, 2065, 5)]
    years2 = [i for i in range(2070, 2115, 10)]
    ref_tec_ssp = {
        "LED": "prep_secondary_steel_1",
        "SSP1": "prep_secondary_steel_1",
        "SSP3": "prep_secondary_steel_3",
        "SSP5": "prep_secondary_steel_3",
    }
    start_val = {
        "prep_secondary_steel_1": 70,
        "prep_secondary_steel_2": 100,
        "prep_secondary_steel_3": 130,
    }
    common = {
        "mode": "M1",
        "time": "year",
        "unit": "???",
    }

    if ssp not in ref_tec_ssp.keys():
        ref_cost1 = [
            start_val[list(start_val.keys())[0]] * 1.025**i
            for i, _ in enumerate(years1)
        ]
        ref_cost2 = [ref_cost1[-1] * 1.05 ** (i + 1) for i, _ in enumerate(years2)]
        ref_cost = ref_cost1 + ref_cost2

        tec2_1 = [
            start_val[list(start_val.keys())[1]] * 1.025**i
            for i, _ in enumerate(years1)
        ]
        tec2_2 = [tec2_1[-1] * 1.05 ** (i + 1) for i, _ in enumerate(years2)]
        tec2 = tec2_1 + tec2_2

        tec3_1 = [
            start_val[list(start_val.keys())[2]] * 1.025**i
            for i, _ in enumerate(years1)
        ]
        tec3_2 = [tec3_1[-1] * 1.05 ** (i + 1) for i, _ in enumerate(years2)]
        tec3 = tec3_1 + tec3_2

        df1 = make_df(
            "var_cost",
            technology=list(start_val.keys())[0],
            year_act=years1 + years2,
            value=ref_cost,
            **common,
        )
        df2 = make_df(
            "var_cost",
            technology=list(start_val.keys())[1],
            year_act=years1 + years2,
            value=tec2,
            **common,
        )
        df3 = make_df(
            "var_cost",
            technology=list(start_val.keys())[2],
            year_act=years1 + years2,
            value=tec3,
            **common,
        )
    else:
        years = years1 + years2
        ref_cost1 = [
            start_val[ref_tec_ssp[ssp]] * 1.025**i for i, _ in enumerate(years1)
        ]
        ref_cost2 = [ref_cost1[-1] * 1.05 ** (i + 1) for i, _ in enumerate(years2)]
        ref_cost = ref_cost1 + ref_cost2
        other_tecs = start_val.keys() - [ref_tec_ssp[ssp]]
        tec2 = [
            start_val[list(other_tecs)[0]]
            + ((ref_cost[-1] - start_val[list(other_tecs)[0]]) / (len(years) - 1)) * i
            for i, _ in enumerate(years)
        ]
        tec3 = [
            start_val[list(other_tecs)[1]]
            + ((ref_cost[-1] - start_val[list(other_tecs)[1]]) / (len(years) - 1)) * i
            for i, _ in enumerate(years)
        ]

        df1 = make_df(
            "var_cost",
            technology=ref_tec_ssp[ssp],
            year_act=years,
            value=ref_cost,
            **common,
        )
        df2 = make_df(
            "var_cost",
            technology=list(other_tecs)[0],
            year_act=years,
            value=tec2,
            **common,
        )
        df3 = make_df(
            "var_cost",
            technology=list(other_tecs)[1],
            year_act=years,
            value=tec3,
            **common,
        )
    df = (
        pd.concat([df1, df2, df3])
        .pipe(broadcast, node_loc=nodes_ex_world(s_info.N))
        .assign(year_vtg=lambda x: x.year_act)
    )
    return {"var_cost": df}


def gen_max_recycling_rel(s_info, ssp):
    ssp_vals = {
        "LED": -0.98,
        "SSP1": -0.98,
        "SSP2": -0.85,
        "SSP3": -0.85,
        "SSP4": -0.85,
        "SSP5": -0.85,
    }
    df = (
        make_df(
            "relation_activity",
            technology="total_EOL_steel",
            value=ssp_vals[ssp],
            mode="M1",
            relation="max_regional_recycling_steel",
            time="year",
            time_origin="year",
            unit="???",
        )
        .pipe(broadcast, node_loc=nodes_ex_world(s_info.N))
        .pipe(broadcast, year_act=[i for i in s_info.Y if i > 2020])
        .pipe(same_node)
        .assign(year_rel=lambda x: x.year_act)
    )
    return {"relation_activity": df}


def gen_grow_cap_up(s_info, ssp):
    ssp_vals = {
        "LED": 0.0010,
        "SSP1": 0.0010,
        "SSP2": 0.0010,
        "SSP3": 0.0009,
        "SSP4": 0.0020,
        "SSP5": 0.0020,
    }

    df = (
        make_df(
            "growth_new_capacity_up",
            technology=["bf_ccs_steel", "dri_gas_ccs_steel"],
            value=ssp_vals[ssp],
            unit="???",
        )
        .pipe(broadcast, node_loc=nodes_ex_world(s_info.N))
        .pipe(broadcast, year_vtg=s_info.Y)
    )
    return {"growth_new_capacity_up": df}


def gen_bof_pig_input(s_info):
    """Generate BOF feed input coefficients.

    Assume 20% scrap share for regions (except CHN and EEU until 2030).
    EEU needs higher share in 2020 to be feasible with calibrated pig availability.
    CHN uses less scrap and more pig iron due to high BF activity.

    Parameters
    ----------
    s_info

    Returns
    -------

    """
    special_regions = ["R12_EEU"]  # , "R12_NAM"]
    other_regions = [
        i for i in nodes_ex_world(s_info.N) if i not in special_regions + ["R12_CHN"]
    ]
    years = [i for i in range(1970, 2060, 5)] + [i for i in range(2060, 2115, 10)]
    dimensions = {
        "technology": "bof_steel",
        "mode": ["M1", "M2"],
        "commodity": "pig_iron",
        "level": "tertiary_material",
        "time": "year",
        "time_origin": "year",
        "unit": "???",
    }
    pig_bof_conversion_eff = 0.95
    df_other = (
        make_df("input", value=0.8 / pig_bof_conversion_eff, **dimensions)
        .pipe(broadcast, node_loc=other_regions)
        .pipe(same_node)
        .pipe(broadcast, year_act=years)
        .pipe(broadcast, year_vtg=years)
    )
    df_chn = (
        make_df("input", value=0.83 / pig_bof_conversion_eff, **dimensions)
        .pipe(broadcast, node_loc="R12_CHN")
        .pipe(same_node)
        .pipe(broadcast, year_act=[2020, 2025])
        .pipe(broadcast, year_vtg=years)
    )
    df_eeu = (
        make_df("input", value=0.79 / pig_bof_conversion_eff, **dimensions)
        .pipe(broadcast, node_loc=special_regions)
        .pipe(same_node)
        .pipe(broadcast, year_act=[2020, 2025])
        .pipe(broadcast, year_vtg=years)
    )
    df3 = (
        make_df("input", value=0.8 / pig_bof_conversion_eff, **dimensions)
        .pipe(broadcast, node_loc=special_regions + ["R12_CHN"])
        .pipe(same_node)
        .pipe(broadcast, year_act=[i for i in years if i > 2025])
        .pipe(broadcast, year_vtg=years)
    )
    df = pd.concat([df_other, df_chn, df_eeu, df3])
    dimensions = {
        "technology": "bof_steel",
        "mode": ["M1", "M2"],
        "commodity": "steel",
        "level": "new_scrap",
        "time": "year",
        "time_origin": "year",
        "unit": "???",
    }
    scrap_bof_conversion_eff = 0.99
    df_other = (
        make_df("input", value=(1 - 0.8) / scrap_bof_conversion_eff, **dimensions)
        .pipe(broadcast, node_loc=other_regions)
        .pipe(same_node)
        .pipe(broadcast, year_act=years)
        .pipe(broadcast, year_vtg=years)
    )
    df_chn = (
        make_df("input", value=(1 - 0.83) / scrap_bof_conversion_eff, **dimensions)
        .pipe(broadcast, node_loc="R12_CHN")
        .pipe(same_node)
        .pipe(broadcast, year_act=[2020, 2025])
        .pipe(broadcast, year_vtg=years)
    )
    df_eeu = (
        make_df("input", value=(1 - 0.79) / scrap_bof_conversion_eff, **dimensions)
        .pipe(broadcast, node_loc=special_regions)
        .pipe(same_node)
        .pipe(broadcast, year_act=[2020, 2025])
        .pipe(broadcast, year_vtg=years)
    )
    df3 = (
        make_df("input", value=(1 - 0.8) / scrap_bof_conversion_eff, **dimensions)
        .pipe(broadcast, node_loc=special_regions + ["R12_CHN"])
        .pipe(same_node)
        .pipe(broadcast, year_act=[i for i in years if i > 2025])
        .pipe(broadcast, year_vtg=years)
    )
    df_all = pd.concat([df, df_other, df_chn, df_eeu, df3])
    df_all = df_all[df_all["year_act"] - df_all["year_vtg"] < 30]
    return {"input": df_all}


def scale_fse_demand(demand, new_scrap_ratio):
    demand["value"] = demand.apply(
        lambda x: x["value"] * (new_scrap_ratio[x["node"]]), axis=1
    )


def gen_finishing_steel_io(s_info: ScenarioInfo):
    dimensions = {
        "technology": "finishing_steel",
        "mode": "M1",
        "commodity": "steel",
        "time": "year",
        "time_dest": "year",
        "unit": "???",
    }
    df = pd.read_csv(package_data_path("material", "steel", "finishing_loss_ratio.csv"))
    df_out_steel = (
        make_df("output", **df, **dimensions, level="useful_material")
        .pipe(same_node)
        .pipe(broadcast, year_act=s_info.Y)
        .assign(year_vtg=lambda x: x["year_act"])
    )
    df_out_steel = df_out_steel[df_out_steel["year_act"].le(df_out_steel["year_vtg"])]
    df_out_scrap = (
        make_df("output", **df, **dimensions, level="new_scrap")
        .pipe(same_node)
        .pipe(broadcast, year_act=s_info.Y)
        .assign(year_vtg=lambda x: x["year_act"])
    )
    df_out_scrap["value"] = df_out_scrap["value"].sub(1).abs().round(4)
    df_out_scrap = df_out_scrap[df_out_scrap["year_act"].le(df_out_scrap["year_vtg"])]
    return {"output": pd.concat([df_out_steel, df_out_scrap])}


def gen_manuf_steel_io(ratio, s_info: ScenarioInfo):
    dimensions = {
        "technology": "manuf_steel",
        "mode": "M1",
        "commodity": "steel",
        "time": "year",
        "time_dest": "year",
        "unit": "???",
    }
    df = (
        pd.Series(ratio)
        .to_frame()
        .reset_index()
        .rename(columns={"index": "node_loc", 0: "value"})
    )
    df_out_steel = (
        make_df("output", **df, **dimensions, level="product")
        .pipe(same_node)
        .pipe(broadcast, year_act=s_info.Y)
        .assign(year_vtg=lambda x: x["year_act"])
    )
    df_out_steel = df_out_steel[df_out_steel["year_act"].le(df_out_steel["year_vtg"])]
    df_out_scrap = (
        make_df("output", **df, **dimensions, level="new_scrap")
        .pipe(same_node)
        .pipe(broadcast, year_act=s_info.Y)
        .assign(year_vtg=lambda x: x["year_act"])
    )
    df_out_scrap["value"] = df_out_scrap["value"].sub(1).abs().round(4)
    df_out_scrap = df_out_scrap[df_out_scrap["year_act"].le(df_out_scrap["year_vtg"])]
    return {"output": pd.concat([df_out_steel, df_out_scrap])}
