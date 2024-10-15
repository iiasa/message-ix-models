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
from message_ix_models.model.material.util import get_ssp_from_context, read_config
from message_ix_models.util import (
    broadcast,
    nodes_ex_world,
    package_data_path,
    same_node,
)


def gen_mock_demand_cement(scenario: message_ix.Scenario) -> pd.DataFrame:
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
            4100 * 0.051 - 76,
            (4100 * 0.14 - 155) * 0.2 * 0.1,
            4100 * 0.064 * 0.5,
            4100 * 0.026 - 57,
            4100 * 0.046 * 0.5 - 55,
            (4100 * 0.14 - 155) * 0.2,
            4100 * 0.046 * 0.5,
            12,
            4100 * 0.003,
            (4100 * 0.14 - 155) * 0.6,
            4100 * 0.064 * 0.5 - 51,
            (4100 * 0.14 - 155) * 0.2 * 0.9,
        ]
    else:
        nodes.remove("R11_GLB")
        sheet_n = "data_R11"
        region_set = "R11_"

        demand2020_top = [76, 2295, 0, 57, 55, 60, 89, 54, 129, 320, 51]
        # the rest (~900 Mt) allocated by % values in http://www.cembureau.eu/media/clkdda45/activity-report-2019.pdf
        demand2020_rest = [
            4100 * 0.051 - 76,
            (4100 * 0.14 - 155) * 0.2,
            4100 * 0.064 * 0.5,
            4100 * 0.026 - 57,
            4100 * 0.046 * 0.5 - 55,
            (4100 * 0.14 - 155) * 0.2,
            4100 * 0.046 * 0.5,
            12,
            4100 * 0.003,
            (4100 * 0.14 - 155) * 0.6,
            4100 * 0.064 * 0.5 - 51,
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
    context.datafile = "Global_steel_cement_MESSAGE.xlsx"

    # Techno-economic assumptions
    data_cement = read_sector_data(scenario, "cement", "Global_cement_MESSAGE.xlsx")
    # Special treatment for time-dependent Parameters
    data_cement_ts = read_timeseries(scenario, "cement", "Global_cement_MESSAGE.xlsx")
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
    df_demand = material_demand_calc.derive_demand("cement", scenario, ssp=ssp)
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
            ),
            calculate_ini_new_cap(
                df_demand=df_demand.copy(deep=True),
                technology="clinker_wet_ccs_cement",
                material="cement",
            ),
        ]
    )

    return results
