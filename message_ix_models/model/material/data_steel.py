from collections import defaultdict
from collections.abc import Iterable

import message_ix
import pandas as pd
from message_ix import make_df

# Get endogenous material demand from buildings interface
from message_ix_models import ScenarioInfo
from message_ix_models.model.material.data_util import (
    calculate_ini_new_cap,
    read_rel,
    read_sector_data,
    read_timeseries,
)
from message_ix_models.model.material.material_demand import material_demand_calc
from message_ix_models.model.material.util import (
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
    data_steel = read_sector_data(scenario, "steel", "", "Global_steel_MESSAGE.xlsx")
    # Special treatment for time-dependent Parameters
    data_steel_ts = read_timeseries(scenario, "steel", "", "Global_steel_MESSAGE.xlsx")
    data_steel_rel = read_rel(scenario, "steel", "", "Global_steel_MESSAGE.xlsx")

    tec_ts = set(data_steel_ts.technology)  # set of tecs with var_cost

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    modelyears = s_info.Y  # s_info.Y is only for modeling years
    nodes = nodes_ex_world(s_info.N)
    global_region = [i for i in s_info.N if i.endswith("_GLB")][0]
    yv_ya = s_info.yv_ya
    yv_ya = yv_ya.loc[yv_ya.year_vtg >= 1990]

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
    df_max_recycling = pd.DataFrame(
        {
            "relation": "max_global_recycling_steel",
            "node_rel": "R12_GLB",
            "year_rel": 2020,
            "year_act": 2020,
            "node_loc": nodes,
            "technology": "scrap_recovery_steel",
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
    df_demand = material_demand_calc.derive_demand("steel", scenario, ssp=ssp)
    results[parname].append(df_demand)

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
                ssp="FIXME",
            ),
            calculate_ini_new_cap(
                df_demand=df_demand.copy(deep=True),
                technology="bf_ccs_steel",
                material="steel",
                ssp="FIXME",
            ),
        ]
    )

    maybe_remove_water_tec(scenario, results)

    return results
