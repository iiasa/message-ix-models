"""
Data and parameter generation for the aluminum sector in MESSAGEix-Materials model.

This module provides functions to read, process, and generate parameter data
for aluminum technologies, demand, trade, and related constraints.
"""

import os
from collections import defaultdict
from collections.abc import Iterable
from typing import Literal

import message_ix
import pandas as pd
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.data_util import read_rel, read_timeseries
from message_ix_models.model.material.material_demand import material_demand_calc
from message_ix_models.model.material.util import (
    add_R12_column,
    combine_df_dictionaries,
    get_pycountry_iso,
    get_ssp_from_context,
    invert_dictionary,
    read_config,
)
from message_ix_models.util import (
    broadcast,
    make_io,
    nodes_ex_world,
    package_data_path,
    same_node,
)


def read_data_aluminum(
    scenario: message_ix.Scenario,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Read and clean data from aluminum techno-economic and timeseries files.

    Parameters
    ----------
    scenario : message_ix.Scenario
        Scenario instance to build aluminum on.

    Returns
    -------
    tuple of three pd.DataFrames
        Aluminum data in three separate groups:
        - time independent parameters,
        - relation parameters,
        - time dependent parameters.
    """

    # Read the file
    data_alu = pd.read_csv(package_data_path("material", "aluminum", "data_R12.csv"))

    # Drop columns that don't contain useful information
    data_alu = data_alu.drop(["Source", "Description"], axis=1)

    data_alu_rel = read_rel(scenario, "aluminum", None, "relations_R12.csv")

    data_aluminum_ts = read_timeseries(scenario, "aluminum", None, "timeseries_R12.csv")

    # Unit conversion

    # At the moment this is done in the excel file, can be also done here
    # To make sure we use the same units

    return data_alu, data_alu_rel, data_aluminum_ts


def gen_data_alu_ts(data: pd.DataFrame, nodes: list) -> dict[str, pd.DataFrame]:
    """Generate time-variable parameter data for the aluminum sector.

    Parameters
    ----------
    data : pd.DataFrame
        Time-variable data from input file.
    nodes : list
        Regions of the model.

    Returns
    -------
    dict[str, pd.DataFrame]
        Key-value pairs of parameter names and parameter data.
    """
    tec_ts = set(data.technology)  # set of tecs in timeseries sheet
    common = dict(
        time="year",
        time_origin="year",
        time_dest="year",
    )
    par_dict = defaultdict(list)
    for t in tec_ts:
        param_name = data.loc[(data["technology"] == t), "parameter"].unique()
        for p in set(param_name):
            val = data.loc[
                (data["technology"] == t) & (data["parameter"] == p),
                "value",
            ]
            # units = data.loc[
            #     (data["technology"] == t)
            #     & (data["parameter"] == p),
            #     "units",
            # ].values[0]
            mod = data.loc[
                (data["technology"] == t) & (data["parameter"] == p),
                "mode",
            ]
            yr = data.loc[
                (data["technology"] == t) & (data["parameter"] == p),
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
                rg = data.loc[
                    (data["technology"] == t) & (data["parameter"] == p),
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

            par_dict[p].append(df)
    return {par_name: pd.concat(dfs) for par_name, dfs in par_dict.items()}


def gen_data_alu_rel(data: pd.DataFrame, years: list) -> dict[str, pd.DataFrame]:
    """Generate relation parameter data for the aluminum sector.

    Parameters
    ----------
    data : pd.DataFrame
        Relation data from input file.
    years : list
        Model years.

    Returns
    -------
    dict[str, pd.DataFrame]
        Key-value pairs of relation parameter names and data.
    """
    par_dict = defaultdict(list)
    regions = set(data["Region"].values)
    for reg in regions:
        for r in data["relation"].unique():
            if r is None:
                break

            params = set(data.loc[(data["relation"] == r), "parameter"].values)

            # This relation should start from 2020...
            if r == "minimum_recycling_aluminum":
                modelyears_copy = years[:]
                if 2020 in modelyears_copy:
                    modelyears_copy.remove(2020)

                common_rel = dict(
                    year_rel=modelyears_copy,
                    year_act=modelyears_copy,
                    mode="M1",
                    relation=r,
                )
            else:
                # Use all the model years for other relations...
                common_rel = dict(
                    year_rel=years,
                    year_act=years,
                    mode="M1",
                    relation=r,
                )

            for par_name in params:
                if par_name == "relation_activity":
                    tec_list = data.loc[
                        ((data["relation"] == r) & (data["parameter"] == par_name)),
                        "technology",
                    ]

                    for tec in tec_list.unique():
                        val = data.loc[
                            (
                                (data["relation"] == r)
                                & (data["parameter"] == par_name)
                                & (data["technology"] == tec)
                                & (data["Region"] == reg)
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

                        par_dict[par_name].append(df)

                elif (par_name == "relation_upper") | (par_name == "relation_lower"):
                    val = data.loc[
                        (
                            (data["relation"] == r)
                            & (data["parameter"] == par_name)
                            & (data["Region"] == reg)
                        ),
                        "value",
                    ].values[0]

                    df = make_df(
                        par_name, value=val, unit="-", node_rel=reg, **common_rel
                    )

                    par_dict[par_name].append(df)
    return {par_name: pd.concat(dfs) for par_name, dfs in par_dict.items()}


def assign_input_outpt(
    split,
    param_name: str,
    regions: pd.DataFrame,
    val,
    t: str,
    rg: str,
    glb_reg: str,
    common: dict,
    yv_ya: pd.DataFrame,
    nodes,
):
    """Assign input/output or emission_factor parameters for aluminum technologies.

    Parameters
    ----------
    split : list
        Split parameter name.
    param_name : str
        Parameter name.
    regions : pd.DataFrame
        Regions for the parameter.
    val : pd.Series
        Parameter values.
    t : str
        Technology name.
    rg : str
        Region.
    glb_reg : str
        Global region.
    common : dict
        Common parameter dictionary.
    yv_ya : pd.DataFrame
        Year vintage/active combinations.
    nodes : list
        Model nodes.

    Returns
    -------
    pd.DataFrame
        Parameter DataFrame.
    """
    # Assign commodity and level names
    # Later mod can be added
    com = split[1]
    lev = split[2]

    if (param_name == "input") and (lev == "import"):
        df = make_df(
            param_name,
            technology=t,
            commodity=com,
            level=lev,
            value=val[regions[regions == rg].index[0]],
            unit="t",
            node_loc=rg,
            node_origin=glb_reg,
            **common,
        )

    elif (param_name == "output") and (lev == "export"):
        df = make_df(
            param_name,
            technology=t,
            commodity=com,
            level=lev,
            value=val[regions[regions == rg].index[0]],
            unit="t",
            node_loc=rg,
            node_dest=glb_reg,
            **common,
        )

    # Assign higher efficiency to younger plants
    elif (
        ((t == "soderberg_aluminum") or (t == "prebake_aluminum"))
        & (com == "electr")
        & (param_name == "input")
    ):
        # All the vıntage years
        year_vtg = sorted(set(yv_ya.year_vtg.values))
        # Collect the values for the combination of vintage and
        # active years.
        input_values_all: list[float] = []
        for yr_v in year_vtg:
            # The initial year efficiency value
            input_values_temp = [val[regions[regions == rg].index[0]]]
            # Reduction after the vintage year
            year_vtg_filtered = list(filter(lambda op: op >= yr_v, year_vtg))
            # Filter the active model years
            year_act = yv_ya.loc[yv_ya["year_vtg"] == yr_v, "year_act"].values
            for i in range(len(year_vtg_filtered) - 1):
                input_values_temp.append(input_values_temp[i] * 1.1)

            act_year_no = len(year_act)
            input_values_temp = input_values_temp[-act_year_no:]
            input_values_all = input_values_all + input_values_temp

        df = make_df(
            param_name,
            technology=t,
            commodity=com,
            level=lev,
            value=input_values_all,
            unit="t",
            node_loc=rg,
            **common,
        ).pipe(same_node)

    else:
        df = make_df(
            param_name,
            technology=t,
            commodity=com,
            level=lev,
            value=val[regions[regions == rg].index[0]],
            unit="t",
            node_loc=rg,
            **common,
        ).pipe(same_node)

    # Copy parameters to all regions, when node_loc is not GLB
    if (len(regions) == 1) and (rg != glb_reg):
        df["node_loc"] = None
        df = df.pipe(broadcast, node_loc=nodes)  # .pipe(same_node)
        # Use same_node only for non-trade technologies
        if (lev != "import") and (lev != "export"):
            df = df.pipe(same_node)
    return df


def gen_data_alu_const(
    data: pd.DataFrame,
    config: dict,
    glb_reg: str,
    years: Iterable,
    yv_ya: pd.DataFrame,
    nodes: list[str],
):
    """Generate time-independent (constant) parameter data for aluminum technologies.

    Parameters
    ----------
    data : pd.DataFrame
        Constant parameter data.
    config : dict
        Technology configuration.
    glb_reg : str
        Global region.
    years : Iterable
        Model years.
    yv_ya : pd.DataFrame
        Year vintage/active combinations.
    nodes : list[str]
        Model nodes.

    Returns
    -------
    dict[str, pd.DataFrame]
        Key-value pairs of parameter names and parameter data.
    """
    results = defaultdict(list)
    for t in config["technology"]["add"]:
        t = t.id
        params = data.loc[(data["technology"] == t), "parameter"].unique()
        # Obtain the active and vintage years
        if not len(params):
            continue
        av = data.loc[(data["technology"] == t), "availability"].values[0]
        years = [year for year in years if year >= av]
        yv_ya = yv_ya.loc[yv_ya.year_vtg >= av]
        common = dict(
            year_vtg=yv_ya.year_vtg,
            year_act=yv_ya.year_act,
            mode="M1",
            time="year",
            time_origin="year",
            time_dest="year",
        )
        # Iterate over parameters
        for par in params:
            # Obtain the parameter names, commodity,level,emission
            split = par.split("|")
            param_name = split[0]

            # Obtain the scalar value for the parameter
            val = data.loc[
                ((data["technology"] == t) & (data["parameter"] == par)),
                "value",
            ]

            regions = data.loc[
                ((data["technology"] == t) & (data["parameter"] == par)),
                "region",
            ]

            for rg in regions:
                # For the parameters which includes index names
                if len(split) > 1:
                    if (param_name == "input") | (param_name == "output"):
                        df = assign_input_outpt(
                            split,
                            param_name,
                            regions,
                            val,
                            t,
                            rg,
                            glb_reg,
                            common,
                            yv_ya,
                            nodes,
                        )

                    elif param_name == "emission_factor":
                        # Assign the emisson type
                        emi = split[1]

                        df = make_df(
                            param_name,
                            technology=t,
                            value=val[regions[regions == rg].index[0]],
                            emission=emi,
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
                    (len(regions) == 1)
                    and len(set(df["node_loc"])) == 1
                    and list(set(df["node_loc"]))[0] != glb_reg
                ):
                    df["node_loc"] = None
                    df = df.pipe(broadcast, node_loc=nodes)

                results[param_name].append(df)
    return {par_name: pd.concat(dfs) for par_name, dfs in results.items()}


def gen_data_aluminum(
    scenario: message_ix.Scenario, dry_run: bool = False
) -> dict[str, pd.DataFrame]:
    """Generate all MESSAGEix parameter data for the aluminum sector.

    Parameters
    ----------
    scenario : message_ix.Scenario
        Scenario instance to build aluminum model on.
    dry_run : bool
        *Not implemented.*

    Returns
    -------
    dict[str, pd.DataFrame]
        Dictionary with MESSAGEix parameters as keys and parametrization as values.
    """
    context = read_config()
    config = context["material"]["aluminum"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)
    ssp = get_ssp_from_context(context)
    # Techno-economic assumptions
    data_aluminum, data_aluminum_rel, data_aluminum_ts = read_data_aluminum(scenario)
    # List of data frames, to be concatenated together at end

    modelyears = s_info.Y
    yv_ya = s_info.yv_ya
    nodes = nodes_ex_world(s_info.N)
    global_region = [i for i in s_info.N if i.endswith("_GLB")][0]

    const_dict = gen_data_alu_const(
        data_aluminum, config, global_region, modelyears, yv_ya, nodes
    )

    demand_dict = gen_demand(scenario, ssp)

    ts_dict = gen_data_alu_ts(data_aluminum_ts, nodes)
    ts_dict.update(gen_hist_new_cap(s_info))
    ts_dict = combine_df_dictionaries(
        ts_dict, gen_smelting_hist_act(), gen_refining_hist_act()
    )

    rel_dict = gen_data_alu_rel(data_aluminum_rel, modelyears)

    trade_dict = gen_data_alu_trade(scenario)
    alumina_trd = gen_alumina_trade_tecs(s_info)
    growth_constr_dict = gen_2020_growth_constraints(s_info)
    ref_heat_input = gen_refining_input(s_info)
    ref_hist_act = calibrate_2020_furnaces(s_info)
    scrap_cost = get_scrap_prep_cost(s_info, ssp)
    max_recyc = gen_max_recycling_rel(s_info, ssp)
    scrap_heat = gen_scrap_prep_heat(s_info, ssp)
    results_aluminum = combine_df_dictionaries(
        const_dict,
        ts_dict,
        rel_dict,
        demand_dict,
        trade_dict,
        growth_constr_dict,
        alumina_trd,
        ref_heat_input,
        ref_hist_act,
        scrap_cost,
        max_recyc,
        scrap_heat,
    )
    reduced_pdict = {}
    for k, v in results_aluminum.items():
        if set(["year_act", "year_vtg"]).issubset(v.columns):
            v = v[(v["year_act"] - v["year_vtg"]) <= 30]
        reduced_pdict[k] = v.drop_duplicates().copy(deep=True)

    return reduced_pdict


def gen_demand(scenario, ssp):
    """Generate aluminum demand parameter data.

    Parameters
    ----------
    scenario : message_ix.Scenario
        Scenario instance.
    ssp : str
        Shared Socioeconomic Pathway.

    Returns
    -------
    dict
        Dictionary with 'demand' parameter DataFrame.
    """
    parname = "demand"
    demand_dict = {}
    df_2025 = pd.read_csv(package_data_path("material", "aluminum", "demand_2025.csv"))
    df = material_demand_calc.derive_demand("aluminum", scenario, ssp=ssp)
    df = df[df["year"] != 2025]
    df = pd.concat([df_2025, df])
    demand_dict[parname] = df
    return demand_dict


def gen_data_alu_trade(scenario: message_ix.Scenario) -> dict[str, pd.DataFrame]:
    """Generate trade-related parameter data for aluminum.

    Parameters
    ----------
    scenario : message_ix.Scenario
        Scenario instance.

    Returns
    -------
    dict[str, pd.DataFrame]
        Key-value pairs of trade parameter names and data.
    """
    results = defaultdict(list)

    data_trade = pd.read_csv(
        package_data_path("material", "aluminum", "aluminum_trade.csv")
    )

    data_trade.drop_duplicates()

    s_info = ScenarioInfo(scenario)

    yv_ya = s_info.yv_ya
    year_all = yv_ya["year_vtg"].unique()

    data_trade = data_trade[data_trade["Year"].isin(year_all)]

    # Divide R12_WEU as 0.7 WEU, 0.3 EEU.
    data_trade.loc[(data_trade["Region"] == "Europe"), "Value"] *= 0.7
    data_trade.loc[(data_trade["Region"] == "Europe"), "Region"] = "West Europe"

    data_trade_eeu = data_trade.loc[data_trade["Region"] == "West Europe"].copy(
        deep=True
    )
    data_trade_eeu["Value"] *= 0.3 / 0.7
    data_trade_eeu["Region"] = "East Europe"

    data_trade = pd.concat([data_trade, data_trade_eeu])

    # Sum Japan and Oceania as PAO

    condition = (data_trade["Region"] == "Japan") | (data_trade["Region"] == "Oceania")
    data_trade_pao = data_trade.loc[condition]
    data_trade_pao = (
        data_trade_pao.groupby(["Variable", "Year"])["Value"].sum().reset_index()
    )

    data_trade_pao["Region"] = "Pacific OECD"
    data_trade = pd.concat([data_trade, data_trade_pao])
    condition_updated = (data_trade["Region"] == "Japan") | (
        data_trade["Region"] == "Oceania"
    )
    data_trade = data_trade.drop(data_trade[condition_updated].index)

    data_trade.reset_index(drop=True, inplace=True)

    # Divide Other Asia 50-50 to SAS and PAS

    data_trade.loc[(data_trade["Region"] == "Other Asia"), "Value"] *= 0.5
    data_trade.loc[(data_trade["Region"] == "Other Asia"), "Region"] = "South Asia"

    data_trade_pas = data_trade[data_trade["Region"] == "South Asia"].copy(deep=True)
    data_trade_pas["Region"] = "Other Pacific Asia"

    data_trade = pd.concat([data_trade, data_trade_pas])

    # Divide Other Producing Regions 50-50s as Africa and FSU

    data_trade.loc[(data_trade["Region"] == "Other Producers"), "Value"] *= 0.5
    data_trade.loc[(data_trade["Region"] == "Other Producers"), "Region"] = "Africa"

    data_trade_fsu = data_trade[data_trade["Region"] == "Africa"].copy(deep=True)
    data_trade_fsu["Region"] = "Former Soviet Union"

    data_trade = pd.concat([data_trade, data_trade_fsu])

    # Drop non-producers

    condition = data_trade["Region"] == "Non Producers"
    data_trade = data_trade.drop(data_trade[condition].index)

    s_info = ScenarioInfo(scenario)

    region_tag = "R12_" if "R12_CHN" in s_info.N else "R11_"
    china_mapping = "R12_CHN" if "R12_CHN" in s_info.N else "R11_CPA"

    region_mapping = {
        "China": china_mapping,
        "West Europe": region_tag + "WEU",
        "East Europe": region_tag + "EEU",
        "Pacific OECD": region_tag + "PAO",
        "South Asia": region_tag + "SAS",
        "Other Pacific Asia": region_tag + "PAS",
        "Africa": region_tag + "AFR",
        "Former Soviet Union": region_tag + "FSU",
        "Middle East": region_tag + "MEA",
        "North America": region_tag + "NAM",
        "South America": region_tag + "LAM",
    }

    # Add the data as historical_activity

    data_trade = data_trade.replace(region_mapping)
    data_trade.rename(
        columns={"Region": "node_loc", "Year": "year_act", "Value": "value"},
        inplace=True,
    )

    # Trade is at the product level.
    # For imports this corresponds to: USE|Inputs|Imports

    data_import = data_trade[data_trade["Variable"] == "USE|Inputs|Imports"]
    data_import_hist = data_import[data_import["year_act"] <= 2015].copy(deep=True)
    data_import_hist["technology"] = "import_aluminum"
    data_import_hist["mode"] = "M1"
    data_import_hist["time"] = "year"
    data_import_hist["unit"] = "-"
    data_import_hist.drop(["Variable"], axis=1, inplace=True)
    data_import_hist.reset_index(drop=True)

    # For exports this corresponds to: MANUFACTURING|Outputs|Exports

    data_export = data_trade[data_trade["Variable"] == "MANUFACTURING|Outputs|Exports"]
    data_export_hist = data_export[data_export["year_act"] <= 2015].copy(deep=True)
    data_export_hist["technology"] = "export_aluminum"
    data_export_hist["mode"] = "M1"
    data_export_hist["time"] = "year"
    data_export_hist["unit"] = "-"
    data_export_hist.drop(["Variable"], axis=1, inplace=True)
    data_export_hist.reset_index(drop=True)

    results["historical_activity"].append(data_export_hist)
    results["historical_activity"].append(data_import_hist)

    # Add data as historical_new_capacity for export

    for r in data_export_hist["node_loc"].unique():
        df_hist_cap = data_export_hist[data_export_hist["node_loc"] == r]
        df_hist_cap = df_hist_cap.sort_values(by="year_act")
        df_hist_cap["value_difference"] = df_hist_cap["value"].diff()
        df_hist_cap["value_difference"] = df_hist_cap["value_difference"].fillna(
            df_hist_cap["value"]
        )
        df_hist_cap["historical_new_capacity"] = df_hist_cap["value_difference"] / 5

        df_hist_cap = df_hist_cap.drop(
            columns=["mode", "time", "value", "value_difference"], axis=1
        )
        df_hist_cap.rename(
            columns={"historical_new_capacity": "value", "year_act": "year_vtg"},
            inplace=True,
        )

        df_hist_cap["value"] = df_hist_cap["value"].apply(lambda x: 0 if x < 0 else x)
        df_hist_cap["unit"] = "-"
        results["historical_new_capacity"].append(df_hist_cap)

    # For China fixing 2020 and 2025 values

    import_chn = data_import[
        (data_import["year_act"] == 2020) & (data_import["node_loc"] == "R12_CHN")
    ]

    export_chn = data_export[
        (data_export["year_act"] == 2020) & (data_export["node_loc"] == "R12_CHN")
    ]

    # Merge the DataFrames on 'node_loc' and 'year'
    merged_df = pd.merge(
        import_chn,
        export_chn,
        on=["node_loc", "year_act"],
        suffixes=("_import", "_export"),
    )

    # Subtract the 'value_import' from 'value_export' to get net export value
    merged_df["value"] = merged_df["value_export"] - merged_df["value_import"]

    # Select relevant columns for the final DataFrame
    bound_act_net_export_chn = merged_df[["node_loc", "year_act", "value"]].copy(
        deep=True
    )

    bound_act_net_export_chn["technology"] = "export_aluminum"
    bound_act_net_export_chn["mode"] = "M1"
    bound_act_net_export_chn["time"] = "year"
    bound_act_net_export_chn["unit"] = "-"

    bound_act_net_export_chn_2025 = bound_act_net_export_chn.replace({2020: 2025})

    results["bound_activity_up"].append(bound_act_net_export_chn)
    results["bound_activity_lo"].append(bound_act_net_export_chn)
    results["bound_activity_up"].append(bound_act_net_export_chn_2025)
    results["bound_activity_lo"].append(bound_act_net_export_chn_2025)

    return {par_name: pd.concat(dfs) for par_name, dfs in results.items()}


def gen_hist_new_cap(s_info):
    """Generate historical new capacity data for aluminum smelters.

    Parameters
    ----------
    s_info : ScenarioInfo
        Scenario information object.

    Returns
    -------
    dict
        Dictionary with 'historical_new_capacity' and 'fixed_new_capacity' DataFrames.
    """
    # NB Because this is (older) .xls and not .xlsx, the 'xlrd' package is required
    df_cap = pd.read_excel(
        package_data_path(
            "material", "aluminum", "raw", "smelters-with 2022 projection.xls"
        ),
        sheet_name="Sheet1",
        skipfooter=23,
    ).rename(columns={"Unnamed: 0": "Country", "Unnamed: 1": "Region"})
    df_cap.Technology = df_cap.Technology.fillna("unknown")
    df_cap = df_cap[~df_cap[1995].isna()]
    df_cap.Country = df_cap["Country"].ffill()
    df_cap["ISO"] = df_cap.Country.apply(
        lambda c: get_pycountry_iso(
            c,
            {
                "Surinam": "SUR",
                "Trinidad": "TTO",
                "Quatar": "QAT",
                "Turkey": "TUR",
                "UAE": "ARE",
                "Gernamy": "DEU",
                "Azerbaydzhan": "AZE",
                "Russia": "RUS",
                "Tadzhikistan": "TJK",
                "UK": "GBR",
                "Total": "World",
                "Bosnia": "BIH",
            },
        )
    )
    df_cap = add_R12_column(
        df_cap, file_path=package_data_path("node", "R12.yaml"), iso_column="ISO"
    )

    # generate historical_new_capacity for soderberg
    df_cap_ss = df_cap[
        (df_cap.Technology.str.contains("SS") & ~(df_cap.Technology.str.contains("PB")))
        & ~(df_cap.Technology.str.contains("HAL"))
        & ~(df_cap.Technology.str.contains("P"))
    ]
    df_cap_ss_r12 = df_cap_ss.groupby("R12").sum(numeric_only=True)
    sample = df_cap_ss_r12[df_cap_ss_r12[df_cap_ss_r12.columns[-1]] != 0][
        [i for i in range(1995, 2020, 5)] + [2019]
    ]
    hist_new_cap_ss = (
        compute_differences(sample, 1995) / 10**6 / 5
    )  # convert to Mt and divide by year intervall
    hist_new_cap_ss = hist_new_cap_ss.rename(columns={2019: 2020})
    hist_new_cap_ss = (
        hist_new_cap_ss.reset_index()
        .melt(id_vars="R12", var_name="year_vtg")
        .assign(unit="Mt", technology="soderberg_aluminum")
        .rename(columns={"R12": "node_loc"})
    )

    # generate historical_new_capacity for prebake
    df_cap_pb = df_cap.loc[df_cap.index.difference(df_cap_ss.index)]
    df_cap_pb_r12 = df_cap_pb.groupby("R12").sum(numeric_only=True)
    sample = df_cap_pb_r12[[i for i in range(1995, 2020, 5)] + [2019]]
    hist_new_cap_pb = compute_differences(sample, 1995) / 10**6 / 5
    hist_new_cap_pb = hist_new_cap_pb.rename(columns={2019: 2020})
    hist_new_cap_pb = (
        hist_new_cap_pb.reset_index()
        .melt(id_vars="R12", var_name="year_vtg")
        .assign(unit="Mt", technology="prebake_aluminum")
        .rename(columns={"R12": "node_loc"})
    )
    # only allow new soderberg capacity in FSU region
    # (the only region where soderberg is still developed)
    cols = {
        "technology": "soderberg_aluminum",
        "value": 0.05,
        "unit": "GW",
        "node_loc": [i for i in nodes_ex_world(s_info.N) if i not in ["R12_FSU"]],
    }
    df_soder_up = make_df("fixed_new_capacity", **cols).pipe(
        broadcast, year_vtg=s_info.Y[1:]
    )
    return {
        "historical_new_capacity": pd.concat([hist_new_cap_ss, hist_new_cap_pb]),
        "fixed_new_capacity": df_soder_up,
    }


def compute_differences(df, ref_col):
    """Compute positive differences between columns and a reference column.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with columns to compare.
    ref_col : int or str
        Reference column.

    Returns
    -------
    pd.DataFrame
        DataFrame of positive differences.
    """
    # Initialize a DataFrame to store differences
    differences = df[ref_col].to_frame()

    # Start with the reference column
    ref_values = df[ref_col].copy()

    for col in df.columns:
        if col == ref_col:
            continue  # Skip the reference column

        # Compute differences
        diff = df[col] - ref_values
        diff[diff <= 0] = 0  # Keep only positive differences

        # Store differences
        differences[col] = diff

        # Update the reference column where the current column is greater
        ref_values = ref_values.where(df[col] <= ref_values, df[col])

    return differences


def load_bgs_data(commodity: Literal["aluminum", "alumina"]):
    """Load and format BGS production data for aluminum or alumina.

    Parameters
    ----------
    commodity : Literal["aluminum", "alumina"]
        Commodity to load.

    Returns
    -------
    pd.DataFrame
        Formatted production data.
    """
    bgs_data_path = package_data_path(
        "material", "aluminum", "raw", "bgs_production", commodity
    )

    dfs = []

    for fname in os.listdir(bgs_data_path):
        if not fname.endswith(".xlsx"):
            continue
        # read and format BGS data
        df_prim = pd.read_excel(bgs_data_path.joinpath(fname), skipfooter=9, skiprows=1)
        year_cols = df_prim.columns[2::2]
        df_prim = df_prim[
            [df_prim.columns.tolist()[0]] + df_prim.columns[3::2].tolist()
        ]
        df_prim.columns = ["Country"] + [int(i) for i in year_cols]
        df_prim["ISO"] = df_prim["Country"].apply(
            lambda x: get_pycountry_iso(
                x,
                {
                    "Turkey": "TUR",
                    "Russia": "RUS",
                    "Bosnia & Herzegovina": "BIH",
                    "Ireland, Republic of": "IRL",
                },
            )
        )
        df_prim.drop("Country", axis=1, inplace=True)
        for year in [i for i in df_prim.columns if isinstance(i, int)]:
            df_prim[year] = pd.to_numeric(df_prim[year], errors="coerce")
        dfs.append(df_prim)

    df_prim = dfs[0].groupby("ISO").sum()
    for _df in dfs[1:]:
        df_prim = _df.groupby("ISO").sum().join(df_prim, how="outer")
    df_prim = df_prim.dropna(how="all")
    df_prim = df_prim[sorted(df_prim.columns)]

    df_prim.reset_index(inplace=True)

    # add R12 column
    df_prim = add_R12_column(
        df_prim.rename(columns={"ISO": "COUNTRY"}),
        package_data_path("node", "R12.yaml"),
    )
    df_prim.rename(columns={"COUNTRY": "ISO"}, inplace=True)

    return df_prim


def gen_smelting_hist_act():
    """Generate historical activity and bounds for aluminum smelting technologies.

    Returns
    -------
    dict
        Dict with 'historical_activity', 'bound_activity_up', and 'bound_activity_lo'.
    """
    df_prim = load_bgs_data("aluminum")
    df_prim_r12 = df_prim.groupby("R12").sum(numeric_only=True).div(10**6)

    # Soderberg
    df_ss_act = df_prim_r12[[2015, 2020]].copy(deep=True)
    # calculate historical production with soderberg electrodes in the only 3 regions
    #   that still have soderberg capacity (based on capacity data from genisim)
    df_ss_act.loc["R12_WEU"] *= 0.025
    df_ss_act.loc["R12_LAM"] *= 0.25
    df_ss_act.loc["R12_FSU"] *= 0.65
    df_ss_act = df_ss_act.loc[["R12_WEU", "R12_LAM", "R12_FSU"]]
    df_ss_act = (
        df_ss_act.reset_index()
        .rename(columns={"R12": "node_loc"})
        .melt(id_vars="node_loc", var_name="year_act")
    )
    df_ss_act = df_ss_act.assign(
        technology="soderberg_aluminum", mode="M1", time="year", unit="Mt/yr"
    )
    df_ss_act = make_df("historical_activity", **df_ss_act)

    # Prebake
    df_pb_act = df_prim_r12[[2015, 2020]].copy(deep=True)
    # deduct historical production with soderberg electrodes in the only 3 regions that
    #   still have soderberg capacity (based on capacity data from genisim) to get
    #   production with prebaked electrodes
    df_pb_act.loc["R12_WEU"] *= 1 - 0.025
    df_pb_act.loc["R12_LAM"] *= 1 - 0.25
    df_pb_act.loc["R12_FSU"] *= 1 - 0.65
    df_pb_act = (
        df_pb_act.reset_index()
        .rename(columns={"R12": "node_loc"})
        .melt(id_vars="node_loc", var_name="year_act")
    )
    df_pb_act = df_pb_act.assign(
        technology="prebake_aluminum", mode="M1", time="year", unit="Mt/yr"
    )
    df_pb_act = make_df("historical_activity", **df_pb_act)

    par_dict = {}
    par_dict["historical_activity"] = pd.concat(
        [
            df_pb_act[df_pb_act["year_act"] == 2015],
            df_ss_act[df_ss_act["year_act"] == 2015],
        ]
    )
    par_dict["bound_activity_up"] = pd.concat(
        [
            df_pb_act[df_pb_act["year_act"] == 2020],
            df_ss_act[df_ss_act["year_act"] == 2020],
        ]
    )
    par_dict["bound_activity_lo"] = par_dict["bound_activity_up"].copy(deep=True)
    return par_dict


def gen_refining_hist_act():
    """Generate historical activity and 2020 bounds for alumina refining technologies.

    Returns
    -------
    dict
        Dict with 'historical_activity', 'bound_activity_lo', and 'bound_activity_up'.
    """
    df_ref = load_bgs_data("alumina")
    df_ref_r12 = df_ref.groupby("R12").sum(numeric_only=True).div(10**6)

    df_ref_act = df_ref_r12[[2015, 2020]].copy(deep=True)
    df_ref_act.loc["R12_LAM", 2020] -= 10
    df_ref_act = (
        df_ref_act.reset_index()
        .rename(columns={"R12": "node_loc"})
        .melt(id_vars="node_loc", var_name="year_act")
    )
    df_ref_act = df_ref_act.assign(
        technology="refining_aluminum", mode="M1", time="year", unit="Mt/yr"
    )
    hist_act = make_df(
        "historical_activity", **df_ref_act[df_ref_act["year_act"] == 2015]
    )
    bound_act = pd.concat(
        [make_df("bound_activity_up", **df_ref_act[df_ref_act["year_act"] == 2020])]
    )
    par_dict = {
        "historical_activity": hist_act,
        "bound_activity_lo": bound_act,
        "bound_activity_up": bound_act,
    }
    return par_dict


def gen_alumina_trade_tecs(s_info):
    """Generate trade technology parameter data for alumina.

    Parameters
    ----------
    s_info : ScenarioInfo
        Scenario information object.

    Returns
    -------
    dict
        Dictionary of trade technology parameter DataFrames.
    """
    modelyears = s_info.Y
    nodes = nodes_ex_world(s_info.N)
    global_region = [i for i in s_info.N if i.endswith("_GLB")][0]

    common = {
        "time": "year",
        "time_origin": "year",
        "time_dest": "year",
        "mode": "M1",
        "node_loc": nodes,
    }
    exp_dict = make_io(
        src=("alumina", "secondary_material", "Mt"),
        dest=("alumina", "export", "Mt"),
        efficiency=1.0,
        technology="export_alumina",
        node_dest=global_region,
        node_origin=nodes,
        **common,
    )
    imp_dict = make_io(
        src=("alumina", "import", "Mt"),
        dest=("alumina", "secondary_material", "Mt"),
        efficiency=1.0,
        technology="import_alumina",
        node_origin=global_region,
        node_dest=nodes,
        **common,
    )

    common = {
        "time": "year",
        "time_origin": "year",
        "time_dest": "year",
        "mode": "M1",
        "node_loc": global_region,
    }
    trd_dict = make_io(
        src=("alumina", "export", "Mt"),
        dest=("alumina", "import", "Mt"),
        efficiency=1.0,
        technology="trade_alumina",
        node_dest=global_region,
        node_origin=global_region,
        **common,
    )

    trade_dict = combine_df_dictionaries(imp_dict, trd_dict, exp_dict)
    trade_dict = {
        k: v.pipe(broadcast, year_act=modelyears).assign(year_vtg=lambda x: x.year_act)
        for k, v in trade_dict.items()
    }
    return trade_dict


def gen_2020_growth_constraints(s_info):
    """Generate 2020 growth constraints for soderberg aluminum smelters.

    Parameters
    ----------
    s_info : ScenarioInfo
        Scenario information object.

    Returns
    -------
    dict
        Dictionary with 'growth_activity_up' DataFrame.
    """
    common = {
        "technology": "soderberg_aluminum",
        "time": "year",
        "unit": "???",
        "value": 0.5,
        "year_act": 2020,
    }
    df = make_df("growth_activity_up", **common).pipe(
        broadcast, node_loc=nodes_ex_world(s_info.N)
    )
    return {"growth_activity_up": df}


def calibrate_2020_furnaces(s_info):
    """Calibrate 2020 furnace activity for aluminum refining by fuel.

    Parameters
    ----------
    s_info : ScenarioInfo
        Scenario information object.

    Returns
    -------
    dict
        Dictionary with 'bound_activity_lo' and 'bound_activity_up' DataFrames.
    """
    fname = "MetallurgicalAluminaRefiningFuelConsumption_1985-2023.csv"
    iai_ref_map = {
        "Africa & Asia (ex China)": [
            "Azerbaijan",
            "Azerbaijan",
            "Azerbaijan",
            "Guinea",
            "India",
            "Iran",
            "Kazakhstan",
            "Kazakhstan",
            "Turkey",
        ],
        "North America": [
            "Canada",
            "United States of America",
            "US Virgin Islands",
            "US Virgin Islands",
        ],
        "South America": [
            "Brazil",
            "Guyana",
            "Jamaica",
            "Suriname",
            "US Virgin Islands",
            "Venezuela",
        ],
        "Oceania": ["Australia"],
        "Europe": [
            "Bosnia and Herzegovina",
            "France",
            "German Democratic Republic",
            "Germany",
            "Greece",
            "Hungary",
            "Hungary",
            "Hungary",
            "Ireland",
            "Italy",
            "Montenegro",
            "Romania",
            "Russian Federation",
            "Russian Federation",
            "Serbia and Montenegro",
            "Serbia and Montenegro",
            "Slovakia",
            "Slovenia",
            "Spain",
            "Ukraine",
            "Ukraine",
            "United Kingdom",
        ],
        "China": ["China"],
    }
    iai_mis_dict = {
        "Turkey": "TUR",
        "Serbia and Montenegro": "SRB",
        "US Virgin Islands": "VIR",
        "German Democratic Republic": "DDR",
    }
    fuel_tec_map = {
        "Coal": "furnace_coal_aluminum",
        "Gas": "furnace_gas_aluminum",
        "Oil": "furnace_foil_aluminum",
        "Electricity": "furnace_elec_aluminum",
    }
    iai_ref_map = {k: v[0] for k, v in invert_dictionary(iai_ref_map).items()}
    df_iai_cmap = pd.Series(iai_ref_map).to_frame().reset_index()

    df_iai_cmap["ISO"] = df_iai_cmap["index"].apply(
        lambda x: get_pycountry_iso(
            x,
            iai_mis_dict,
        )
    )
    df_iai_cmap.drop(columns="index", inplace=True)

    df_ref = load_bgs_data("alumina")
    df_ref = df_ref.merge(df_iai_cmap, on="ISO", how="left")
    df_ref["IAI"] = df_ref[0].fillna("Estimated Unreported")
    df_ref = df_ref.drop(columns=0)

    df_ref_en = pd.read_csv(
        package_data_path("material", "aluminum", "raw", fname), sep=";"
    )
    df_ref_en["Year"] = (
        df_ref_en["Period from"].str.split("-", expand=True)[0].astype(int)
    )
    df_ref_en = df_ref_en.melt(
        id_vars=["Year", "Row"], value_vars=df_ref_en.columns[4:-1], var_name="Region"
    ).rename(columns={"Row": "Variable"})

    df_ref_iai = df_ref.set_index(["IAI", "ISO"]).drop(columns="R12")
    test = (
        df_ref_en.rename(columns={"Region": "IAI"})
        .set_index(["Year", "Variable", "IAI"])
        .join((df_ref_iai / df_ref_iai.groupby("IAI").sum())[2019])
    )
    test["value"] = test["value"] * test[2019]
    test = test.drop_duplicates().dropna().loc[2020].drop(2019, axis=1)

    tec_map_df = pd.Series(fuel_tec_map).to_frame().reset_index()
    tec_map_df.columns = ["Variable", "technology"]

    test_r12 = add_R12_column(
        test.reset_index(), package_data_path("node", "R12.yaml"), "ISO"
    )
    test_r12 = test_r12.groupby(["Variable", "R12"]).sum(numeric_only=True)
    test_r12 = (
        test_r12.loc[["Coal", "Gas", "Oil"]]
        .div(10**6)
        .mul(31.7)
        .round(5)
        .reset_index()
        .merge(tec_map_df, on="Variable")
        .drop("Variable", axis=1)
        .rename(columns={"R12": "node_loc"})
        .assign(unit="GWa", time="year", mode="high_temp", year_act=2020)
    )
    zero_furnaces = {
        "unit": "GWa",
        "time": "year",
        "mode": "high_temp",
        "technology": [
            "furnace_biomass_aluminum",
            "furnace_elec_aluminum",
            "furnace_ethanol_aluminum",
            "furnace_methanol_aluminum",
            "furnace_h2_aluminum",
        ],
        "year_act": 2020,
        "value": 0,
    }
    zbounds = make_df("bound_activity_up", **zero_furnaces).pipe(
        broadcast, node_loc=nodes_ex_world(s_info.N)
    )

    return {"bound_activity_lo": test_r12, "bound_activity_up": zbounds}


def gen_refining_input(s_info):
    """Generate input parameter for aluminum refining technology.

    Parameters
    ----------
    s_info : ScenarioInfo
        Scenario information object.

    Returns
    -------
    dict
        Dictionary with 'input' parameter DataFrame.
    """
    # read IAI refining statistics and format
    path = package_data_path("material", "aluminum")
    df_ref_int = pd.read_csv(path.joinpath("alu_ref_int_1985_2023.csv"), sep=";")
    df_ref_int = df_ref_int.melt(
        id_vars=["Period"], value_vars=df_ref_int.columns[1:], var_name="Region"
    ).rename(columns={"Period": "Year"})
    df_ref_int.value = pd.to_numeric(df_ref_int.value, errors="coerce")

    # map from IAI regions to R12 regions based on country grouping listed in
    # https://international-aluminium.org/statistics/metallurgical-alumina-refining-fuel-consumption/
    # in the -Areas dropdown menu
    iai_int_r12_map = {
        "Africa & Asia (ex China)": [
            "R12_AFR",
            "R12_PAS",
            "R12_SAS",
            "R12_RCPA",
            "R12_MEA",
        ],
        "North America": ["R12_NAM"],
        "Europe": ["R12_WEU", "R12_EEU", "R12_FSU"],
        "Oceania": ["R12_PAO"],
        "South America": ["R12_LAM"],
        "China": ["R12_CHN"],
    }
    iai_int_r12_map = {k: v[0] for k, v in invert_dictionary(iai_int_r12_map).items()}

    df_act = (
        pd.Series(iai_int_r12_map)
        .to_frame()
        .reset_index()
        .set_index(0)
        .merge(df_ref_int, left_on=0, right_on="Region")
        .drop(columns=["key_0"])
        .drop_duplicates()
    )

    # select 2020 statistics as input for refing_aluminum technology
    # format to "input" parameter dataframe
    df_msg = (
        df_act[df_act["Year"] == 2020]
        .rename(columns={"index": "node_loc"})
        .drop(columns="Region")
    )
    df_msg.value /= 3.6 * 8760
    df_msg.value = df_msg.value.round(5)
    df_msg = df_msg.assign(
        technology="refining_aluminum",
        mode="M1",
        commodity="ht_heat",
        level="useful_aluminum",
        time="year",
        time_origin="year",
        unit="GWa",
    )
    df_msg = (
        make_df("input", **df_msg)
        .pipe(same_node)
        .pipe(broadcast, year_act=s_info.yv_ya.year_act.unique())
    )
    df_msg["year_vtg"] = df_msg["year_act"]
    return {"input": df_msg}


def gen_trade_growth_constraints(s_info):
    """Generate growth and initial activity constraints for aluminum and alumina trade.

    Parameters
    ----------
    s_info : ScenarioInfo
        Scenario information object.

    Returns
    -------
    dict
        Dictionary of growth and initial activity constraints.
    """
    par_dict1 = {}
    par_dict2 = {}
    aluminum_tecs = ["export_aluminum"]
    alumina_tecs = ["export_alumina", "import_alumina"]
    for par in ["growth_activity_up", "initial_activity_up"]:
        par_dict2[par] = (
            make_df(par, value=0.1, technology=aluminum_tecs, time="year", unit="???")
            .pipe(broadcast, node_loc=nodes_ex_world(s_info.N))
            .pipe(
                broadcast,
                year_act=[i for i in range(2020, 2060, 5)]
                + [i for i in range(2060, 2115, 10)],
            )
        )
    for par in ["growth_activity_up", "initial_activity_up"]:
        par_dict1[par] = (
            make_df(par, value=0.1, technology=alumina_tecs, time="year", unit="???")
            .pipe(broadcast, node_loc=nodes_ex_world(s_info.N))
            .pipe(
                broadcast,
                year_act=[i for i in range(2025, 2060, 5)]
                + [i for i in range(2060, 2115, 10)],
            )
        )
    return combine_df_dictionaries(par_dict1, par_dict2)


def gen_max_recycling_rel(s_info, ssp):
    """Generate parametrization for maximum recycling relation.

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
        "SSP2": -0.90,
        "SSP3": -0.80,
        "SSP4": -0.90,
        "SSP5": -0.80,
    }
    df = (
        make_df(
            "relation_activity",
            technology="total_EOL_aluminum",
            value=ssp_vals[ssp],
            mode="M1",
            relation="maximum_recycling_aluminum",
            time="year",
            time_origin="year",
            unit="???",
        )
        .pipe(broadcast, node_loc=nodes_ex_world(s_info.N))
        .pipe(broadcast, year_act=s_info.Y)
        .pipe(same_node)
        .assign(year_rel=lambda x: x.year_act)
    )
    return {"relation_activity": df}


def gen_scrap_prep_heat(s_info, ssp):
    """Generate heat input parametrization for aluminum scrap preparation.

    Parameters
    ----------
    s_info : ScenarioInfo
        Scenario information object.
    ssp : str
        Shared Socioeconomic Pathway.

    Returns
    -------
    dict
        Dictionary with 'input' parameter DataFrame.
    """
    # Converted from 1.4 GJ/t,https://publications.jrc.ec.europa.eu/repository/handle/JRC96680
    ssp_vals = {
        "LED": [0.044394, 0.044394, 0.044394],
        "SSP1": [0.044394, 0.044394, 0.044394],
        "SSP2": [0.044394, 0.046, 0.048],
        "SSP3": [0.048, 0.048, 0.048],
        "SSP4": [0.044394, 0.046, 0.048],
        "SSP5": [0.048, 0.048, 0.048],
    }

    df = (
        make_df(
            "input",
            technology=[
                "prep_secondary_aluminum_1",
                "prep_secondary_aluminum_2",
                "prep_secondary_aluminum_3",
            ],
            value=ssp_vals[ssp],
            mode="M1",
            commodity="lt_heat",
            level="useful_aluminum",
            time="year",
            time_origin="year",
            unit="GWa",
        )
        .pipe(broadcast, node_loc=nodes_ex_world(s_info.N))
        .pipe(broadcast, year_act=s_info.Y)
        .pipe(same_node)
        .assign(year_vtg=lambda x: x.year_act)
    )
    return {"input": df}


def get_scrap_prep_cost(s_info, ssp):
    """Generate variable cost parametrization for aluminum scrap preparation.

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
    years = s_info.Y
    ref_tec_ssp = {
        "LED": "prep_secondary_aluminum_1",
        "SSP1": "prep_secondary_aluminum_1",
        "SSP3": "prep_secondary_aluminum_3",
        "SSP5": "prep_secondary_aluminum_3",
    }
    start_val = {
        "prep_secondary_aluminum_1": 500,
        "prep_secondary_aluminum_2": 1000,
        "prep_secondary_aluminum_3": 1500,
    }
    common = {
        "mode": "M1",
        "time": "year",
        "unit": "???",
    }

    if ssp not in ref_tec_ssp.keys():
        ref_cost = [
            start_val[list(start_val.keys())[0]] * 1.05**i for i, _ in enumerate(years)
        ]
        tec2 = [
            start_val[list(start_val.keys())[1]] * 1.05**i for i, _ in enumerate(years)
        ]
        tec3 = [
            start_val[list(start_val.keys())[2]] * 1.05**i for i, _ in enumerate(years)
        ]

        df1 = make_df(
            "var_cost",
            technology=list(start_val.keys())[0],
            year_act=years,
            value=ref_cost,
            **common,
        )
        df2 = make_df(
            "var_cost",
            technology=list(start_val.keys())[1],
            year_act=years,
            value=tec2,
            **common,
        )
        df3 = make_df(
            "var_cost",
            technology=list(start_val.keys())[2],
            year_act=years,
            value=tec3,
            **common,
        )
    else:
        ref_cost = [start_val[ref_tec_ssp[ssp]] * 1.05**i for i, _ in enumerate(years)]
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
