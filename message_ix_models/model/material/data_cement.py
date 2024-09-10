from collections import defaultdict
import math

import pandas as pd
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.data_util import read_sector_data, read_timeseries, calculate_ini_new_cap, read_rel
from message_ix_models.model.material.material_demand import material_demand_calc
from message_ix_models.model.material.util import get_ssp_from_context, read_config
from message_ix_models.util import (
    broadcast,
    nodes_ex_world,
    package_data_path,
    same_node,
)

def gen_mock_demand_cement(scenario):
    s_info = ScenarioInfo(scenario)
    nodes = s_info.N
    nodes.remove("World")

    # 2019 production by country (USGS)
    # p43 of https://pubs.usgs.gov/periodicals/mcs2020/mcs2020-cement.pdf

    # For R12: China and CPA demand divided by 0.1 and 0.9.

    # The order:
    # r = ['R12_AFR', 'R12_RCPA', 'R12_EEU', 'R12_FSU', 'R12_LAM', 'R12_MEA',\
    # 'R12_NAM', 'R12_PAO', 'R12_PAS', 'R12_SAS', 'R12_WEU',"R12_CHN"]

    if "R12_CHN" in nodes:
        nodes.remove("R12_GLB")
        sheet_n = "data_R12"
        region_set = "R12_"

        demand2020_top = [76, 229.5, 0, 57, 55, 60, 89, 54, 129, 320, 51, 2065.5]
        # the rest (~900 Mt) allocated by % values in http://www.cembureau.eu/media/clkdda45/activity-report-2019.pdf
        demand2020_rest = [
            (4100 * 0.051 - 76),
            ((4100 * 0.14 - 155) * 0.2 * 0.1),
            (4100 * 0.064 * 0.5),
            (4100 * 0.026 - 57),
            (4100 * 0.046 * 0.5 - 55),
            ((4100 * 0.14 - 155) * 0.2),
            (4100 * 0.046 * 0.5),
            12,
            (4100 * 0.003),
            ((4100 * 0.14 - 155) * 0.6),
            (4100 * 0.064 * 0.5 - 51),
            ((4100 * 0.14 - 155) * 0.2 * 0.9),
        ]
    else:
        nodes.remove("R11_GLB")
        sheet_n = "data_R11"
        region_set = "R11_"

        demand2020_top = [76, 2295, 0, 57, 55, 60, 89, 54, 129, 320, 51]
        # the rest (~900 Mt) allocated by % values in http://www.cembureau.eu/media/clkdda45/activity-report-2019.pdf
        demand2020_rest = [
            (4100 * 0.051 - 76),
            ((4100 * 0.14 - 155) * 0.2),
            (4100 * 0.064 * 0.5),
            (4100 * 0.026 - 57),
            (4100 * 0.046 * 0.5 - 55),
            ((4100 * 0.14 - 155) * 0.2),
            (4100 * 0.046 * 0.5),
            12,
            (4100 * 0.003),
            ((4100 * 0.14 - 155) * 0.6),
            (4100 * 0.064 * 0.5 - 51),
        ]

    # SSP2 R11 baseline GDP projection
    gdp_growth = pd.read_excel(
        package_data_path("material", "other", "iamc_db ENGAGE baseline GDP PPP.xlsx"),
        sheet_name=sheet_n,
    )

    gdp_growth = gdp_growth.loc[
        (gdp_growth["Scenario"] == "baseline") & (gdp_growth["Region"] != "World")
    ].drop(["Model", "Variable", "Unit", "Notes", 2000, 2005], axis=1)

    d = [a + b for a, b in zip(demand2020_top, demand2020_rest)]
    gdp_growth["Region"] = region_set + gdp_growth["Region"]

    # # Regions setting for IMAGE
    # region_cement = pd.read_excel(
    #     package_data_path("material",  "CEMENT.BvR2010.xlsx"),
    #     sheet_name="Timer_Regions", skiprows=range(0,3))[['Region #', 'Name']]\
    #     .drop_duplicates().sort_values(by='Region #')
    #
    # region_cement = region_cement.loc[region_cement['Region #'] < 999]
    # region_cement['node'] = \
    #     ['R11_NAM', 'R11_NAM',
    #      'R11_LAM', 'R11_LAM',
    #      'R11_LAM', 'R11_LAM',
    #      'R11_AFR', 'R11_AFR',
    #      'R11_AFR', 'R11_AFR',
    #      'R11_WEU', 'R11_EEU',
    #      'R11_EEU', 'R11_FSU',
    #      'R11_FSU', 'R11_FSU',
    #      'R11_MEA', 'R11_SAS',
    #      'R11_PAS', 'R11_CPA',
    #      'R11_PAS', 'R11_PAS',
    #      'R11_PAO', 'R11_PAO',
    #      'R11_SAS', 'R11_AFR']
    #
    # # Cement demand 2010 [Mt/year] (IMAGE)
    # demand2010_cement = pd.read_excel(
    #     package_data_path("material",  "CEMENT.BvR2010.xlsx"),
    #     sheet_name="Domestic Consumption", skiprows=range(0,3)).\
    #     groupby(by=["Region #"]).sum()[[2010]].\
    #     join(region_cement.set_index('Region #'), on='Region #').\
    #     rename(columns={2010:'value'})
    #
    # demand2010_cement = demand2010_cement.groupby(by=['node']).sum().reset_index()
    # demand2010_cement['value'] = demand2010_cement['value'] / 1e9 # kg to Mt

    # Directly assigned countries from the table on p43

    demand2020_cement = (
        pd.DataFrame({"Region": nodes, "value": d})
        .join(gdp_growth.set_index("Region"), on="Region")
        .rename(columns={"Region": "node"})
    )

    # demand2010_cement = demand2010_cement.\
    #    join(gdp_growth.rename(columns={'Region':'node'}).set_index('node'), on='node')

    demand2020_cement.iloc[:, 3:] = (
        demand2020_cement.iloc[:, 3:]
        .div(demand2020_cement[2020], axis=0)
        .multiply(demand2020_cement["value"], axis=0)
    )

    # Do this if we have 2020 demand values for buildings
    # sp = get_spec()
    # if 'buildings' in sp['add'].set['technology']:
    #     val = get_scen_mat_demand("cement",scenario) # Mt in 2020
    #     print("Base year demand of {}:".format("cement"), val)
    #     # demand2020_cement['value'] = demand2020_cement['value'] - val['value']
    #     # Scale down all years' demand values by the 2020 ratio
    #     demand2020_cement.iloc[:,3:] =  demand2020_cement.iloc[:,3:].\
    #         multiply(demand2020_cement[2020]- val['value'], axis=0).\
    #         div(demand2020_cement[2020], axis=0)
    #     print("UPDATE {} demand for 2020!".format("cement"))
    #
    demand2020_cement = pd.melt(
        demand2020_cement.drop(["value", "Scenario"], axis=1),
        id_vars=["node"],
        var_name="year",
        value_name="value",
    )

    return demand2020_cement


def gen_data_cement(scenario, dry_run=False):
    """Generate data for materials representation of cement industry."""
    # Load configuration
    context = read_config()
    config = read_config()["material"]["cement"]
    ssp = get_ssp_from_context(context)
    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)
    context.datafile = "Global_steel_cement_MESSAGE.xlsx"

    # Techno-economic assumptions
    # TEMP: now add cement sector as well
    data_cement = read_sector_data(scenario, "cement", "Global_cement_MESSAGE.xlsx" )
    # Special treatment for time-dependent Parameters
    data_cement_ts = read_timeseries(scenario, "cement", "Global_cement_MESSAGE.xlsx")
    data_cement_rel = read_rel(scenario, "cement", "Global_cement_MESSAGE.xlsx")
    tec_ts = set(data_cement_ts.technology)  # set of tecs with var_cost

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    yv_ya = s_info.yv_ya
    yv_ya = yv_ya.loc[yv_ya.year_vtg >= 1980]
    # Do not parametrize GLB region the same way
    nodes = nodes_ex_world(s_info.N)

    # for t in s_info.set['technology']:
    for t in config["technology"]["add"]:
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

    # Add relations for the maximum recycling

    modelyears = s_info.Y  # s_info.Y is only for modeling years
    regions = set(data_cement_rel["Region"].values)
    for reg in regions:
        for r in data_cement_rel["relation"]:
            model_years_rel = modelyears.copy()
            if r is None:
                break
            params = set(
                data_cement_rel.loc[
                    (data_cement_rel["relation"] == r), "parameter"
                ].values
            )

            common_rel = dict(
                year_rel=model_years_rel,
                year_act=model_years_rel,
                relation=r,
            )

            for par_name in params:
                if par_name == "relation_activity":

                    tec_list = data_cement_rel.loc[
                        (
                            (data_cement_rel["relation"] == r)
                            & (data_cement_rel["parameter"] == par_name)
                        ),
                        "technology",
                    ]

                    for tec in tec_list.unique():

                        mode = data_cement_rel.loc[
                            (
                                (data_cement_rel["relation"] == r)
                                & (data_cement_rel["parameter"] == par_name)
                                & (data_cement_rel["technology"] == tec)
                                & (data_cement_rel["Region"] == reg)
                            ),
                            "mode",
                        ].values

                        for m in mode:

                            val = data_cement_rel.loc[
                                (
                                    (data_cement_rel["relation"] == r)
                                    & (data_cement_rel["parameter"] == par_name)
                                    & (data_cement_rel["technology"] == tec)
                                    & (data_cement_rel["Region"] == reg)
                                    & (data_cement_rel["mode"] == m)
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
                                mode = m,
                                **common_rel
                            ).pipe(same_node)

                            results[par_name].append(df)

                elif (par_name == "relation_upper") | (par_name == "relation_lower"):
                    val = data_cement_rel.loc[
                        (
                            (data_cement_rel["relation"] == r)
                            & (data_cement_rel["parameter"] == par_name)
                            & (data_cement_rel["Region"] == reg)
                        ),
                        "value",
                    ].values[0]

                    df = make_df(
                        par_name, value=val, unit="-", node_rel=reg, **common_rel
                    )

                    results[par_name].append(df)

    # Create external demand param
    parname = "demand"
    # demand = gen_mock_demand_cement(scenario)
    # Converte to concrete demand by dividing to 0.15.
    df_demand = material_demand_calc.derive_demand("cement", scenario, old_gdp=False, ssp=ssp)
    df_demand['value'] = df_demand['value'] / 0.15
    df_demand['commodity'] = 'concrete'
    results[parname].append(df_demand)

    # Add CCS as addon
    parname = "addon_conversion"

    # technology_1 = ["clinker_dry_cement"]
    # df_1 = make_df(
    #     parname, mode="M1", type_addon="dry_ccs_cement", value=1, unit="-", **common
    # ).pipe(broadcast, node=nodes, technology=technology_1)

    technology_2 = ["clinker_wet_cement"]
    df_2 = make_df(
        parname, mode="M1", type_addon="wet_ccs_cement", value=1, unit="-", **common
    ).pipe(broadcast, node=nodes, technology= technology_2)

    technology_3 = ["rotary_kiln_wet_cement"]
    df_3 = make_df(
        parname, mode="M1", type_addon="rotary_kiln_wet_addons", value=1, unit="-", **common
    ).pipe(broadcast, node=nodes, technology= technology_3)

    # technology_4 = ["rotary_kiln_dry_cement"]
    # df_4 = make_df(
    #     parname, mode="M1", type_addon="rotary_kiln_dry_addons", value=1, unit="-", **common
    # ).pipe(broadcast, node=nodes, technology= technology_4)

    results[parname].append(df_2)
    results[parname].append(df_3)

    # Adding fly_ash as waste product from coal technologies
    coal_technologies_modes = {"feedstock": ["meth_coal","meth_coal_ccs"],
                               "fuel":      ["meth_coal","meth_coal_ccs"],
                               "M1":        ["coal_i","sp_coal_I","coal_NH3",
                                             "coal_NH3_ccs","coal_adv",
                                             "coal_adv_ccs","coal_ppl",
                                             "coal_ppl_u","coal_gas",
                                             "coal_hpl", "h2_coal",
                                             "h2_coal_ccs"],
                               "high_temp": ["furnace_coal_steel",
                                             "furnace_coal_aluminum",
                                             "furnace_coal_cement",
                                             "furnace_coal_petro",
                                             "furnace_coal_refining",
                                             "furnace_coal_resins",],
                               "low_temp":  ["furnace_coal_steel",
                                             "furnace_coal_aluminum",
                                             "furnace_coal_cement",
                                             "furnace_coal_petro",
                                             "furnace_coal_refining",
                                             "furnace_coal_resins",]
                               }

    # value = (Mt coal/GWa) * (input_coal) * (fly ash as mass % of coal)
    # * (% of fly ash in total ash)
    # Mt coal/GWa = 1kg --> 25 MJ --> (25*2.778*10^-7)/8760 GWa
    # Mt coal/GWa = 1kg --> 7.928*10^-10 GWa, 1 Mt --> 0.7928 GWa
    # Mt coal/GWa = 1 GWa --> 1.261 Mt coal
    # fly ash as mass % of coal = 0.125 (Shah et al., 2022)
    # % of fly ash in total ash = 0.9   (Shah et al., 2022)
    modes = coal_technologies_modes.keys()
    conversion_factor = 1.261 * 0.125 * 0.9
    df_input = scenario.par('input')

    for n in nodes:
        for m in modes:
            for t in coal_technologies_modes[m]:
                df_output = df_input[(df_input['node_loc']==n) & (df_input['technology']==t)
                & (df_input['mode']==m)]
                if df_output.empty:
                    print('Technology {} not found in the input table'.format(t))
                    continue
                else:
                    df_output["updated_value"] = df_output['value']*conversion_factor
                    df_output.drop(["value"], axis=1, inplace = True)
                    df_output.rename(columns={"node_origin":"node_dest",
                                                         'time_origin':'time_dest',
                                                         'updated_value':'value'}, inplace = True)
                    df_output['level']= 'waste_material'
                    df_output['commodity']= 'fly_ash'
                    df_output['unit']= 'Mt'

                    results["output"].append(df_output)

    # Concatenate to one data frame per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    results["initial_new_capacity_up"] = pd.concat(
        [
            calculate_ini_new_cap(
                df_demand=df_demand.copy(deep=True), technology="clinker_dry_ccs_cement",
                material = "cement"
            ),
            calculate_ini_new_cap(
                df_demand=df_demand.copy(deep=True), technology="clinker_wet_ccs_cement",
                material = "cement"
            ),
            calculate_ini_new_cap(
                df_demand=df_demand.copy(deep=True), technology="clay_wet_cement",
                material = "cement"
            ),
            # calculate_ini_new_cap(
            #     df_demand=df_demand.copy(deep=True), technology="clay_dry_cement",
            #     material = "cement"
            # ),
            calculate_ini_new_cap(
                df_demand=df_demand.copy(deep=True), technology="flash_calciner_cement",
                material = "cement"
            ),
        ]
    )

    return results
