"""Data and parameter generation for the steel sector in MESSAGEix models.

This module provides functions to read, process, and generate parameter data for steel
technologies, demand, recycling, CCS, trade and related constraints.
"""

from collections import defaultdict
from collections.abc import Iterable
from typing import TYPE_CHECKING, Literal

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
from message_ix_models.model.material.material_demand.material_demand_calc import (
    read_base_demand,
)
from message_ix_models.model.material.util import (
    get_ssp_from_context,
    maybe_remove_water_tec,
    read_config,
    remove_from_list_if_exists,
)
from message_ix_models.util import (
    broadcast,
    merge_data,
    nodes_ex_world,
    package_data_path,
    same_node,
)

if TYPE_CHECKING:
    from message_ix_models.types import ParameterData


def gen_mock_demand_steel(scenario: message_ix.Scenario) -> pd.DataFrame:
    """Generate mock steel demand time series for MESSAGEix regions.
    ** Not used anymore **

    Parameters
    ----------
    scenario : message_ix.Scenario
        Scenario instance to build steel demand on.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns ['node', 'year', 'value'] for steel demand.
    """
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
    # https://iiasahub.sharepoint.com/sites/eceprog?cid=75ea8244-8757-44f1-83fd-d34f94ffd06a

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
    """Generate time-series parameter data for steel technologies.

    Parameters
    ----------
    data_steel_ts : pd.DataFrame
        DataFrame with time-series parameter data.
    results : dict[str, list]
        Dictionary to collect parameter DataFrames.
    t : str
        Technology name.
    nodes : list[str]
        List of model nodes.

    Returns
    -------
    None
    """
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
    """Generate time-independent (constant) parameter data for steel technologies.

    Parameters
    ----------
    data_steel : pd.DataFrame
        DataFrame with constant parameter data.
    results : dict[str, list]
        Dictionary to collect parameter DataFrames.
    params : Iterable
        Iterable of parameter names.
    t : str
        Technology name.
    yv_ya : pd.DataFrame
        DataFrame with year vintage and year active combinations.
    nodes : list[str]
        List of model nodes.
    global_region : str
        Name of the global region.

    Returns
    -------
    None
    """
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


def gen_data_steel_rel(
    data_steel_rel: pd.DataFrame,
    results: dict,
    regions: set[str],
    modelyears: list[int],
):
    """Generate relation parameter data for steel sector.

    Parameters
    ----------
    data_steel_rel : pd.DataFrame
        DataFrame with relation parameter data.
    results : dict
        Dictionary to collect parameter DataFrames.
    regions : iterable
        Iterable of region names.
    modelyears : list
        List of model years.

    Returns
    -------
    None
    """
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
    """Generate all MESSAGEix parameter data for the steel sector.

    Parameters
    ----------
    scenario : message_ix.Scenario
        Scenario instance to build steel model on.
    dry_run : bool, optional
        If True, do not perform any file writing or scenario modification.

    Returns
    -------
    dict
        Dictionary with MESSAGEix parameters as keys and parametrization as values.
    """
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
        # Retrieve the id if `t` is a Code instance; otherwise use str
        t = getattr(t, "id", t)
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
    df_demand = gen_demand(ssp)
    results["demand"].append(df_demand)

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

    merge_data(
        results,
        gen_dri_act_bound(),
        gen_dri_cap_calibration(),
        gen_dri_coal_model(s_info),
        get_scrap_prep_cost(s_info, ssp),
        gen_max_recycling_rel(s_info, ssp),
        gen_grow_cap_up(s_info, ssp),
        gen_2020_calibration_relation(s_info, "eaf"),
        gen_2020_calibration_relation(s_info, "bof"),
        gen_2020_calibration_relation(s_info, "bf"),
        gen_bof_pig_input(s_info),
        gen_finishing_steel_io(s_info),
        gen_manuf_steel_io(new_scrap_ratio, s_info),
        gen_iron_ore_cost(s_info, ssp),
        gen_charcoal_bf_bound(s_info),
        read_hist_cap("eaf"),
        read_hist_cap("bof"),
        read_hist_cap("bf"),
    )
    maybe_remove_water_tec(scenario, results)

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


def gen_cokeoven_co2_cc(s_info: ScenarioInfo):
    """Generate relation_activity for CO2 emissions from coke ovens.

    Parameters
    ----------
    s_info : ScenarioInfo
        Scenario information object.

    Returns
    -------
    pd.DataFrame
        DataFrame with relation_activity for CO2.
    """
    emi_dict = {
        "unit": "-",
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


def gen_dri_act_bound() -> dict[str, pd.DataFrame]:
    """Read historical DRI activity data of R12 regions
     for historical_activity and bound activity parameter.

    Returns
    -------

    """
    df_act = pd.read_csv(
        package_data_path(
            "material", "steel", "baseyear_calibration", "dri_activity_2020.csv"
        )
    )
    df_hist = pd.read_csv(
        package_data_path(
            "material", "steel", "baseyear_calibration", "dri_activity_hist.csv"
        )
    )
    return {
        "bound_activity_up": df_act,
        "bound_activity_lo": df_act,
        "historical_activity": df_hist,
    }


def gen_dri_cap_calibration() -> dict[str, pd.DataFrame]:
    """Read historical new DRI capacity data of R12 regions
     for historical_activity and bound activity parameter.

    Returns
    -------

    """
    df_cap_2020 = pd.read_csv(
        package_data_path(
            "material", "steel", "baseyear_calibration", "dri_capacity_2020.csv"
        )
    )
    df_cap_hist = pd.read_csv(
        package_data_path(
            "material", "steel", "baseyear_calibration", "dri_capacity_hist.csv"
        )
    )
    return {
        "bound_new_capacity_up": df_cap_2020,
        "bound_new_capacity_lo": df_cap_2020,
        "historical_new_capacity": df_cap_hist,
    }


def gen_dri_coal_model(s_info: ScenarioInfo):
    """Generate techno-economic coal based DRI technology model parameters.

    Parameters
    ----------
    s_info : ScenarioInfo

    Returns
    -------

    """
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


def gen_2020_calibration_relation(
    s_info: ScenarioInfo, tech: Literal["eaf", "bof", "bf"]
) -> dict[str, pd.DataFrame]:
    """Generate generic relation data to calibrate 2020 steel production by process.

    Parameters
    ----------
    s_info : ScenarioInfo
    tech : Literal["eaf", "bof", "bf"]

    Returns
    -------

    """
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
        package_data_path(
            "material", "steel", "baseyear_calibration", f"{tech}_bound_2020.csv"
        )
    )

    return {"relation_activity": df, "relation_upper": rel_up, "relation_lower": rel_up}


def get_scrap_prep_cost(s_info: ScenarioInfo, ssp: str) -> "ParameterData":
    """Generate variable cost parameter for steel scrap preparation technologies.

    Parameters
    ----------
    s_info : ScenarioInfo
        Scenario information object.
    ssp : str
        Shared Socioeconomic Pathway.

    Returns
    -------
    dict
        Dictionary with 'var_cost' parameter DataFrame.
    """
    # TODO Retrieve these from `s_info` instead of hard-coding
    years1 = [i for i in range(2020, 2065, 5)]
    years2 = [i for i in range(2070, 2115, 10)]
    years = years1 + years2

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
    common = {"mode": "M1", "time": "year", "unit": "???", "year_act": years}

    dfs = []
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

        # Create 3 data frames with different var_cost values
        for idx, value in ((0, ref_cost), (1, tec2), (2, tec3)):
            t = list(start_val.keys())[idx]
            dfs.append(make_df("var_cost", technology=t, value=value, **common))
    else:
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

        # Create 3 data frames with different var_cost values
        for t, value in (
            (ref_tec_ssp[ssp], ref_cost),
            (list(other_tecs)[0], tec2),
            (list(other_tecs)[1], tec3),
        ):
            dfs.append(make_df("var_cost", technology=t, value=value, **common))

    df = (
        pd.concat(dfs)
        .pipe(broadcast, node_loc=nodes_ex_world(s_info.N))
        .assign(year_vtg=lambda x: x.year_act)
    )
    return {"var_cost": df}


def gen_max_recycling_rel(s_info: ScenarioInfo, ssp):
    """Generate maximum recycling relation activity for steel.

    Parameters
    ----------
    s_info : ScenarioInfo
        Scenario information object.
    ssp : str
        Shared Socioeconomic Pathway.

    Returns
    -------
    dict
        Dictionary with 'relation_activity' DataFrame.
    """
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


def gen_grow_cap_up(s_info: ScenarioInfo, ssp):
    """Generate growth constraints for new steel CCS capacity.

    Parameters
    ----------
    s_info : ScenarioInfo
        Scenario information object.
    ssp : str
        Shared Socioeconomic Pathway.

    Returns
    -------
    dict
        Dictionary with 'growth_new_capacity_up' parameter DataFrame.
    """
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


def gen_bof_pig_input(s_info: ScenarioInfo) -> "ParameterData":
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

    common = dict(
        mode=["M1", "M2"],
        technology="bof_steel",
        time="year",
        time_origin="year",
        unit="???",
    )
    pig = common | dict(commodity="pig_iron", level="tertiary_material")
    scrap = common | dict(commodity="steel", level="new_scrap")
    conversion_eff = dict(pig=0.95, scrap=0.99)

    # Accumulate data frames with distinct node_loc, year_act, and values
    dfs = []
    for node_loc, year_act, x in (
        (other_regions, years, 0.8),
        (["R12_CHN"], [2020, 2025], 0.83),
        (special_regions, [2020, 2025], 0.79),
        (special_regions + ["R12_CHN"], [i for i in years if i > 2025], 0.8),
    ):
        dfs.append(
            pd.concat(
                [
                    make_df("input", value=x / conversion_eff["pig"], **pig),
                    make_df("input", value=(1 - x) / conversion_eff["scrap"], **scrap),
                ]
            ).pipe(broadcast, node_loc=node_loc, year_act=year_act)
        )

    # - Concatenate all data.
    # - Broadcast over all year_vtg, then filter excess (yV, yA) combinations.
    # - Fill node_origin from node_loc.
    return {
        "input": pd.concat(dfs)
        .pipe(broadcast, year_vtg=years)
        .query("year_act - year_vtg <= 30")
        .pipe(same_node)
    }


def scale_fse_demand(demand: pd.DataFrame, new_scrap_ratio: dict[str, float]):
    """Helper function to convert crude steel demand to
    finished steel demand by scaling with new scrap ratio.

    Parameters
    ----------
    demand : pd.DataFrame
    new_scrap_ratio : dict[str, float]
    """
    demand["value"] = demand.apply(
        lambda x: x["value"] * (new_scrap_ratio[x["node"]]), axis=1
    )


def gen_finishing_steel_io(s_info: ScenarioInfo) -> dict[str, pd.DataFrame]:
    """Generate output parameters for steel finishing process.

    The output ratios are derived from worldsteel statistics of 2020.

    Parameters
    ----------
    s_info : ScenarioInfo

    Returns
    -------

    """
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


def gen_manuf_steel_io(
    ratio: dict[str, float], s_info: ScenarioInfo
) -> dict[str, pd.DataFrame]:
    """Generate output parameters for steel manufacturing process.

    Parameters
    ----------
    ratio : dict[str, float]
    s_info : ScenarioInfo

    Returns
    -------

    """
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


def gen_iron_ore_cost(s_info: ScenarioInfo, ssp: str) -> dict[str, pd.DataFrame]:
    """Generate variable cost parameter for iron ore supply based on SSP narrative.

    Parameters
    ----------
    s_info : ScenarioInfo
    ssp : str

    Returns
    -------

    """
    years1 = [i for i in range(2020, 2030, 5)]
    years2 = [i for i in range(2030, 2060, 5)] + [i for i in range(2060, 2115, 10)]
    start_val = 100
    end_val = {
        "LED": 130,
        "SSP1": 130,
        "SSP2": 80,
        "SSP3": 100,
        "SSP4": 60,
        "SSP5": 80,
    }
    common = {
        "technology": "DUMMY_ore_supply",
        "mode": "M1",
        "time": "year",
        "unit": "???",
    }

    df1 = make_df(
        "var_cost",
        year_act=years1,
        value=start_val,
        **common,
    )
    df2 = make_df(
        "var_cost",
        year_act=years2,
        value=end_val[ssp],
        **common,
    )
    df = (
        pd.concat([df1, df2])
        .pipe(broadcast, node_loc=nodes_ex_world(s_info.N))
        .assign(year_vtg=lambda x: x.year_act)
    )
    return {"var_cost": df}


def gen_charcoal_bf_bound(s_info: ScenarioInfo) -> dict[str, pd.DataFrame]:
    """Generate bound activity for blast furnace modes with
    charcoal input of 0 to calibrate to statistics.

    Parameters
    ----------
    s_info : ScenarioInfo

    Returns
    -------

    """
    dimensions = {
        "technology": "bf_steel",
        "mode": ["M3", "M4"],
        "time": "year",
        "unit": "???",
        "value": 0,
    }
    df = (
        make_df("bound_activity_up", **dimensions)
        .pipe(broadcast, node_loc=nodes_ex_world(s_info.N))
        .pipe(broadcast, year_act=[2020, 2025])
    )
    return {"bound_activity_up": df}


def gen_ssp_demand(ssp: str) -> pd.DataFrame:
    """Generate steel demand projections based on SSP scenarios.

    Timeseries is currently read from pre-computed data file.

    Parameters
    ----------
    ssp : str

    Returns
    -------

    """
    mapping = {
        "SSP1": {"phi": 5, "mu": 0.1, "q": 0.01},
        "SSP2": {"phi": 5, "mu": 0.1, "q": 0.1},
        "SSP3": {"phi": 5, "mu": 0.05, "q": 0.25},
        "SSP4": {"phi": 5, "mu": 0.05, "q": 0.1},
        "SSP5": {"phi": 5, "mu": 0.1, "q": 0.5},
    }
    if ssp == "LED":
        ssp = "SSP1"
    df = pd.read_parquet(
        package_data_path("material", "steel", "demand_projections.parquet")
    ).drop_duplicates()
    df = df[
        (df["SSP"] == int(ssp.replace("SSP", "")))
        & (df["quantile"] == mapping[ssp]["q"])
        & (df["mu"] == mapping[ssp]["mu"])
    ]
    df = df.rename(columns={"region": "node", "total_demand": "value"})
    df[["commodity", "level", "time", "unit"]] = ["steel", "demand", "year", "t"]
    df = make_df("demand", **df).round(2)
    return df


def gen_demand(ssp: str) -> pd.DataFrame:
    """Generate steel demand DataFrame for the given SSP scenario.

    Combines calibrated 2020 and 2025 demand with SSP
    dependent demand projection.

    Parameters
    ----------
    ssp : str

    Returns
    -------

    """
    df_2025 = pd.read_csv(package_data_path("material", "steel", "demand_2025.csv"))
    df_2020 = (
        read_base_demand(package_data_path("material", "steel", "demand_steel.yaml"))
        .rename(columns={"region": "node"})
        .assign(commodity="steel", level="demand", unit="t", time="year")
    )
    df_demand = gen_ssp_demand(ssp)
    df_demand = df_demand[df_demand["year"] != 2025]
    df_demand = pd.concat([df_2020, df_2025, df_demand])
    return df_demand


def read_hist_cap(tec: Literal["eaf", "bof", "bf"]) -> dict[str, pd.DataFrame]:
    """Read historical new capacity data for a given technology.

    Parameters
    ----------
    tec : str

    Returns
    -------

    """
    df = pd.read_csv(
        package_data_path(
            "material", "steel", "baseyear_calibration", f"{tec}_capacity.csv"
        )
    )
    df = df[df["year_vtg"] < 2020]
    return {"historical_new_capacity": df}
