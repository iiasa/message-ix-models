from collections import defaultdict

import message_ix
import pandas as pd
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.util import (
    broadcast,
    nodes_ex_world,
    package_data_path,
    same_node,
)

from .data_util import read_rel, read_timeseries
from .material_demand import material_demand_calc
from .util import combine_df_dictionaries, get_ssp_from_context, read_config


def read_data_aluminum(
    scenario: message_ix.Scenario,
) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """Read and clean data from :file:`aluminum_techno_economic.xlsx`.

    Parameters
    ----------
    scenario: message_ix.Scenario
        Scenario instance to build aluminum on
    Returns
    -------
    tuple of three pd.DataFrames
        returns aluminum data in three separate groups
        time indepenendent parameters, relation parameters and time dependent parameters
    """

    # Ensure config is loaded, get the context
    s_info = ScenarioInfo(scenario)

    # Shorter access to sets configuration
    # sets = context["material"]["generic"]

    fname = "aluminum_techno_economic.xlsx"

    if "R12_CHN" in s_info.N:
        sheet_n = "data_R12"
    else:
        sheet_n = "data_R11"

    # Read the file
    data_alu = pd.read_excel(
        package_data_path("material", "aluminum", fname), sheet_name=sheet_n
    )

    # Drop columns that don't contain useful information
    data_alu = data_alu.drop(["Source", "Description"], axis=1)

    data_alu_rel = read_rel(scenario, "aluminum", "aluminum_techno_economic.xlsx")

    data_aluminum_ts = read_timeseries(
        scenario, "aluminum", "aluminum_techno_economic.xlsx"
    )

    # Unit conversion

    # At the moment this is done in the excel file, can be also done here
    # To make sure we use the same units

    return data_alu, data_alu_rel, data_aluminum_ts


def gen_data_alu_ts(data: pd.DataFrame, nodes: list) -> dict[str, pd.DataFrame]:
    """
    Generates time variable parameter data for aluminum sector
    Parameters
    ----------
    data: pd.DataFrame
        time variable data from input file
    nodes: list
        regions of model

    Returns
    -------
    pd.DataFrame
        key-value pairs of parameter names and parameter data
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
    split, param_name, regions, val, t, rg, glb_reg, common, yv_ya, nodes
):
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
        input_values_all = []
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


def gen_data_alu_const(data, config, glb_reg, years, yv_ya, nodes):
    results = defaultdict(list)
    for t in config["technology"]["add"]:
        params = data.loc[(data["technology"] == t), "parameter"].unique()
        # Obtain the active and vintage years
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
    """

    Parameters
    ----------
    scenario: message_ix.Scenario
        Scenario instance to build aluminum model on
    dry_run: bool
        *not implemented*
    Returns
    -------
    dict[pd.DataFrame]
        dict with MESSAGEix parameters as keys and parametrization as values
        stored in pd.DataFrame
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

    parname = "demand"
    demand_dict = {}
    df = material_demand_calc.derive_demand(
        "aluminum", scenario, old_gdp=False, ssp=ssp
    )
    demand_dict[parname] = df

    ts_dict = gen_data_alu_ts(data_aluminum_ts, nodes)
    rel_dict = gen_data_alu_rel(data_aluminum_rel, modelyears)

    results_aluminum = combine_df_dictionaries(
        const_dict, ts_dict, rel_dict, demand_dict
    )
    return results_aluminum


def gen_mock_demand_aluminum(scenario: message_ix.Scenario) -> pd.DataFrame:
    s_info = ScenarioInfo(scenario)
    nodes = s_info.N
    nodes.remove("World")

    # Demand at product level (IAI Global Aluminum Cycle 2018)
    # Globally: 82.4 Mt
    # Domestic production + Import
    # AFR: No Data
    # CPA - China: 28.2 Mt
    # EEU / 2 + WEU / 2 = Europe 12.5 Mt
    # FSU: No data
    # LAM: South America: 2.5 Mt
    # MEA: Middle East: 2
    # NAM: North America: 14.1
    # PAO: Japan: 3
    # PAS/2 + SAS /2: Other Asia: 11.5 Mt
    # Remaining 8.612 Mt shared between AFR and FSU
    # This is used as 2020 data.

    # For R12: China and CPA demand divided by 0.1 and 0.9.

    # The order:
    # r = ['R12_AFR', 'R12_RCPA', 'R12_EEU', 'R12_FSU', 'R12_LAM', 'R12_MEA',\
    # 'R12_NAM', 'R12_PAO', 'R12_PAS', 'R12_SAS', 'R12_WEU',"R12_CHN"]

    if "R12_CHN" in nodes:
        nodes.remove("R12_GLB")
        sheet_n = "data_R12"
        region_set = "R12_"
        d = [3, 2, 6, 5, 2.5, 2, 13.6, 3, 4.8, 4.8, 6, 26]

    else:
        nodes.remove("R11_GLB")
        sheet_n = "data_R11"
        region_set = "R11_"
        d = [3, 28, 6, 5, 2.5, 2, 13.6, 3, 4.8, 4.8, 6]

    # SSP2 R11 baseline GDP projection
    gdp_growth = pd.read_excel(
        package_data_path("material", "other", "iamc_db ENGAGE baseline GDP PPP.xlsx"),
        sheet_name=sheet_n,
    )

    gdp_growth = gdp_growth.loc[
        (gdp_growth["Scenario"] == "baseline") & (gdp_growth["Region"] != "World")
    ].drop(["Model", "Variable", "Unit", "Notes", 2000, 2005], axis=1)

    gdp_growth["Region"] = region_set + gdp_growth["Region"]

    demand2020_al = (
        pd.DataFrame({"Region": nodes, "Val": d})
        .join(gdp_growth.set_index("Region"), on="Region")
        .rename(columns={"Region": "node"})
    )

    demand2020_al.iloc[:, 3:] = (
        demand2020_al.iloc[:, 3:]
        .div(demand2020_al[2020], axis=0)
        .multiply(demand2020_al["Val"], axis=0)
    )

    demand2020_al = pd.melt(
        demand2020_al.drop(["Val", "Scenario"], axis=1),
        id_vars=["node"],
        var_name="year",
        value_name="value",
    )

    return demand2020_al
