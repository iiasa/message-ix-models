from collections import defaultdict

import numpy as np
import pandas as pd
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.util import read_config
from message_ix_models.util import (
    copy_column,
    package_data_path,
    same_node,
)

CASE_SENS = "mean"
INFRA_SCEN = "baseline"
INPUTFILE = "stocks_forecast_MESSAGE.csv"

print('Adding infrastructure demand with:')
print(CASE_SENS)
print(INFRA_SCEN)


def read_timeseries_infrastructure(filename, case=CASE_SENS, infra_scenario = INFRA_SCEN):

    # Read the file and filter the given sensitivity case
    inf_input_raw = pd.read_csv(package_data_path("material", "infrastructure", filename))
    inf_input_raw = inf_input_raw.loc[(inf_input_raw.Sensitivity == CASE_SENS) & (inf_input_raw.Scenario == INFRA_SCEN)]
    inf_input_raw["Region"] = "R12_" + inf_input_raw["Region"]

    # Area
    inf_input_area = inf_input_raw[
    inf_input_raw["Variable"
    ].str.contains( "Area")]

    inf_input_area = inf_input_area.groupby(['Model','Scenario','Unit','Region']).sum(numeric_only=True).reset_index()
    inf_input_area['Variable'] = 'Infrastructure|Area'

    # Demand
    inf_input_demand = inf_input_raw[
        inf_input_raw[
            "Variable"
        ].str.contains(
            "Material Demand"
        )]

    inf_input_demand['Material'] = inf_input_demand['Variable'].str.split('|').str[3]
    inf_input_demand = inf_input_demand.groupby(['Model','Scenario','Unit','Region','Material']).sum(numeric_only=True).reset_index()
    inf_input_demand['Variable'] = 'Material Demand|Infrastructure|' + inf_input_demand['Material']
    inf_input_demand = inf_input_demand.drop(['Material'], axis = 1)

    # Release

    inf_input_rel = inf_input_raw[
        inf_input_raw[
            "Variable"
        ].str.contains(  # "Floor Space|Aluminum|Cement|Steel|Final Energy"
            "Material Release"
        )]

    inf_input_rel['Material'] = inf_input_rel['Variable'].str.split('|').str[3]
    inf_input_rel = inf_input_rel.groupby(['Model','Scenario','Unit','Region','Material']).sum(numeric_only=True).reset_index()
    inf_input_rel['Variable'] = 'Material Release|Infrastructure|' + inf_input_rel['Material']
    inf_input_rel = inf_input_rel.drop(['Material'], axis = 1)

    # Merge

    inf_input = pd.concat([inf_input_area, inf_input_demand, inf_input_rel])

    inf_input.columns = inf_input.columns.map(str)

    inf_input_pivot = (
        inf_input.melt(
            id_vars=["Region", "Variable"],
            var_name="Year",
            value_vars=list(map(str, range(2020, 2111, 5))),
        )
        .set_index(["Region", "Year", "Variable"])
        .squeeze()
        .unstack()
        .reset_index()
    )

    # Divide by area to get material intensities

    inf_intensity_mat = inf_input_pivot.iloc[:, 2:].div(
        inf_input_pivot["Infrastructure|Area"], axis=0
    )
    inf_intensity_mat.columns = [
        s + "|Intensity" for s in inf_intensity_mat.columns
    ]
    inf_intensity_mat = pd.concat(
        [
            inf_input_pivot[["Region", "Year"]],
            inf_intensity_mat.reindex(inf_input_pivot.index),
        ],
        axis=1,
    ).drop(columns=["Infrastructure|Area|Intensity"])

    inf_intensity_mat["Infrastructure|Area"] = inf_input_pivot[
        "Infrastructure|Area"
    ]

    # Material intensities are in kt/m2 (Mt/billion m2)

    inf_data_long = inf_intensity_mat.melt(
        id_vars=["Region", "Year"], var_name="Variable"
    ).rename(columns={"Region": "node", "Year": "year"})

    inf_intensity_long = inf_data_long[
        inf_data_long["Variable"].str.contains("Intensity")
    ].reset_index(drop=True)
    inf_area_long = inf_data_long[
        inf_data_long["Variable"] == "Infrastructure|Area"
    ].reset_index(drop=True)

    tmp = inf_intensity_long.Variable.str.split("|", expand=True)

    inf_intensity_long["commodity"] = tmp[2].str.lower()  # Material type
    inf_intensity_long["type"] = tmp[0]  # 'Material Demand' or 'Scrap Release'
    inf_intensity_long["unit"] = "kt/m2"

    inf_intensity_long = inf_intensity_long.drop(columns="Variable")
    inf_area_long = inf_area_long.drop(columns="Variable")

    inf_intensity_long = inf_intensity_long.drop(
        inf_intensity_long[np.isnan(inf_intensity_long.value)].index
    )

    # Obtain the material demand (Mt/year in 2020)

    inf_demand_long = inf_input_pivot.melt(
        id_vars=["Region", "Year"], var_name="Variable"
    ).rename(columns={"Region": "node", "Year": "year"})
    tmp = inf_demand_long.Variable.str.split("|", expand=True)
    inf_demand_long["commodity"] = tmp[2].str.lower()  # Material type
    # bld_demand_long = bld_demand_long[bld_demand_long['year']=="2020"].\
    #     dropna(how='any')
    inf_demand_long = inf_demand_long.dropna(how="any")
    inf_demand_long = inf_demand_long[
        inf_demand_long["Variable"].str.contains("Material Demand")
    ].drop(columns="Variable")

    return inf_intensity_long, inf_area_long, inf_demand_long

def get_inf_mat_demand(
    commod, year="2020", inputfile=INPUTFILE, case=CASE_SENS, infra_scenario = INFRA_SCEN
):
    a, b, c = read_timeseries_infrastructure(inputfile, case, infra_scenario)
    print('Outputs from read_timeseries_infrastructure function')
    print(a)
    print(b)
    print(c)
    if not year == "all":  # specific year
        cc = c[(c.commodity == commod) & (c.year == year)].reset_index(drop=True)
        print('cc')
        print(cc)
    else:  # all years
        cc = c[(c.commodity == commod)].reset_index(drop=True)
    return cc

def adjust_demand_param(scen):
    scen_mat_demand = scen.par(
        "demand", {"level": "demand"}
    )

    scen.check_out()
    comms = ["steel", "concrete", "aluminum"]
    INPUTFILE = package_data_path("material", "infrastructure", "stocks_forecast_MESSAGE.csv")
    for c in comms:
        print('adjsut demand commodity')
        print(c)
        mat_inf_all = get_inf_mat_demand(
            c,
            inputfile=INPUTFILE,
            year="all",
            case='mean',
            infra_scenario='baseline'
        ).rename(columns={"value": "inf_demand"})

        print('mat_inf_all')
        print(mat_inf_all)
        mat_inf_all.to_excel('mat_inf_nonasphlt.xlsx')

        mat_inf_all["year"] = mat_inf_all["year"].astype(int)

        sub_mat_demand = scen_mat_demand.loc[scen_mat_demand.commodity == c]

        print('scenario material demand')
        print(sub_mat_demand)
        sub_mat_demand.to_excel('sub_mat_demand.xlsx')
        # print("old", sub_mat_demand.loc[sub_mat_demand.year >=2025])


        sub_mat_demand = sub_mat_demand.join(
            mat_inf_all.set_index(["node", "year", "commodity"]),
            on=["node", "year", "commodity"],
            how="left",
        )

        print('joined table')
        print(sub_mat_demand)
        sub_mat_demand.to_excel('joined_table.xlsx')

        sub_mat_demand["value"] = sub_mat_demand["value"] + sub_mat_demand["inf_demand"]
        print('the sum')
        print(sub_mat_demand)

        sub_mat_demand = sub_mat_demand.drop(columns=["inf_demand"]).dropna(how="any")

        print('the sum')
        print(sub_mat_demand)
        sub_mat_demand.to_excel('sub_mat_demand_final.xlsx')

        scen.add_par("demand", sub_mat_demand.loc[sub_mat_demand.year >= 2025])

    # Add asphalt demand

    mat_inf_asphalt = get_inf_mat_demand(
                'asphalt',
                inputfile=INPUTFILE,
                year="all",
                case='mean',
                infra_scenario='baseline')
    mat_inf_asphalt["year"] = mat_inf_asphalt["year"].astype(int)
    mat_inf_asphalt['level'] = 'demand'
    mat_inf_asphalt["time"] = "year"
    mat_inf_asphalt["unit"] = "t"

    print('asphalt demand')
    print(mat_inf_asphalt)
    mat_inf_asphalt.to_excel('mat_inf_asphalt.xlsx')

    mat_inf_asphalt = mat_inf_asphalt[~mat_inf_asphalt['year'].isin([2065, 2075, 2085, 2095, 2105])]

    # Only replace for year >= 2025

    scen.add_par("demand", mat_inf_asphalt.loc[mat_inf_asphalt.year >= 2025])

    # print("new", sub_mat_demand.loc[sub_mat_demand.year >=2025])

    scen.commit("Building material demand subtracted")

def gen_data_infrastructure(scenario, dry_run=False):

    # Load configuration
    context = read_config()
    config = context["material"]["infrastructure"]

    # New element names for infrastructure integrations
    lev_new = config["level"]["add"][0]
    comm_new = config["commodity"]["add"][0]
    tec_new = config["technology"]["add"][0]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Infrastructure raw data
    (
        data_infrastructure,
        data_infrastructure_demand,
        data_infrastructure_mat_demand,
    ) = read_timeseries_infrastructure(INPUTFILE, CASE_SENS, INFRA_SCEN)

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are different input and output combinations
    # Iterate over technologies

    # allyears = s_info.set['year'] #s_info.Y is only for modeling years
    modelyears = s_info.Y  # s_info.Y is only for modeling years
    nodes = s_info.N
    # fmy = s_info.y0
    nodes.remove("World")

    # Read field values from the infrastructure input data
    regions = list(set(data_infrastructure.node))
    comms = list(set(data_infrastructure.commodity))
    types = ["Material Demand", "Material Release"]  # Order matters

    common = dict(time="year", time_origin="year", time_dest="year", mode="M1")

    # Filter only the years in the base scenario
    data_infrastructure["year"] = data_infrastructure["year"].astype(int)
    data_infrastructure_demand["year"] = data_infrastructure_demand["year"].astype(int)
    data_infrastructure = data_infrastructure[data_infrastructure["year"].isin(modelyears)]
    data_infrastructure_demand = data_infrastructure_demand[
        data_infrastructure_demand["year"].isin(modelyears)
    ]

    # historical demands

    for rg in regions:
        for comm in comms:
            # for typ in types:

            val_mat = data_infrastructure.loc[
                (data_infrastructure["type"] == types[0])
                & (data_infrastructure["commodity"] == comm)
                & (data_infrastructure["node"] == rg),
            ]
            val_rls = data_infrastructure.loc[
                (data_infrastructure["type"] == types[1])
                & (data_infrastructure["commodity"] == comm)
                & (data_infrastructure["node"] == rg),
            ]

            # Material input to infrastructure
            df = (
                make_df(
                    "input",
                    technology=tec_new,
                    commodity=comm,
                    level="demand",
                    year_vtg=val_mat.year,
                    value=val_mat.value,
                    unit="t",
                    node_loc=rg,
                    **common,
                )
                .pipe(same_node)
                .assign(year_act=copy_column("year_vtg"))
            )
            results["input"].append(df)

            # Material release from infrastructure
            df = (
                make_df(
                    "output",
                    technology=tec_new,
                    commodity=comm,
                    level="end_of_life",
                    year_vtg=val_rls.year,
                    value=val_rls.value,
                    unit="t",
                    node_loc=rg,
                    **common,
                )
                .pipe(same_node)
                .assign(year_act=copy_column("year_vtg"))
            )
            results["output"].append(df)

        # Service output as infrastructure demand
        df = (
            make_df(
                "output",
                technology=tec_new,
                commodity=comm_new,
                level="demand",
                year_vtg=val_mat.year,
                value=1,
                unit="t",
                node_loc=rg,
                **common,
            )
            .pipe(same_node)
            .assign(year_act=copy_column("year_vtg"))
        )
        results["output"].append(df)

    # Create external demand param
    parname = "demand"
    demand = data_infrastructure_demand
    df = make_df(
        parname,
        level="demand",
        commodity=comm_new,
        value=demand.value,
        unit="t",
        year=demand.year,
        time="year",
        node=demand.node,
    )
    results[parname].append(df)

    # Concatenate to one data frame per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    return results
