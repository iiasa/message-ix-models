from .data_util import read_sector_data

import numpy as np
from collections import defaultdict
import logging

import pandas as pd

from .util import read_config
from .data_util import read_rel
from message_ix_models import ScenarioInfo
from message_ix import make_df
from message_ix_models.util import (
    broadcast,
    make_io,
    make_matched_dfs,
    same_node,
    copy_column,
    add_par_data,
    package_data_path,
)

CASE_SENS = "ref"  # 'min', 'max'
INPUTFILE = "LED_LED_report_IAMC_sensitivity_R12.csv"
# INPUTFILE = 'LED_LED_report_IAMC_sensitivity_R11.csv'


def read_timeseries_buildings(filename, scenario, case=CASE_SENS):

    import numpy as np

    # Ensure config is loaded, get the context
    context = read_config()
    s_info = ScenarioInfo(scenario)
    nodes = s_info.N

    # Read the file and filter the given sensitivity case
    bld_input_raw = pd.read_csv(package_data_path("material", filename))
    bld_input_raw = bld_input_raw.loc[bld_input_raw.Sensitivity == case]

    bld_input_mat = bld_input_raw[
        bld_input_raw[
            "Variable"
        ].str.contains(  # str.contains("Floor Space|Aluminum|Cement|Steel|Final Energy")]
            "Floor Space|Aluminum|Cement|Steel"
        )
    ]  # Final Energy - Later. Need to figure out carving out
    bld_input_mat["Region"] = "R11_" + bld_input_mat["Region"]
    print("Check the year values")
    print(bld_input_mat)

    bld_input_pivot = (
        bld_input_mat.melt(
            id_vars=["Region", "Variable"],
            var_name="Year",
            value_vars=list(map(str, range(2015, 2101, 5))),
        )
        .set_index(["Region", "Year", "Variable"])
        .squeeze()
        .unstack()
        .reset_index()
    )

    # Divide by floor area to get energy/material intensities
    bld_intensity_ene_mat = bld_input_pivot.iloc[:, 2:].div(
        bld_input_pivot["Energy Service|Residential|Floor Space"], axis=0
    )
    bld_intensity_ene_mat.columns = [
        s + "|Intensity" for s in bld_intensity_ene_mat.columns
    ]
    bld_intensity_ene_mat = pd.concat(
        [
            bld_input_pivot[["Region", "Year"]],
            bld_intensity_ene_mat.reindex(bld_input_pivot.index),
        ],
        axis=1,
    ).drop(columns=["Energy Service|Residential|Floor Space|Intensity"])

    bld_intensity_ene_mat["Energy Service|Residential|Floor Space"] = bld_input_pivot[
        "Energy Service|Residential|Floor Space"
    ]

    # Material intensities are in kg/m2
    bld_data_long = bld_intensity_ene_mat.melt(
        id_vars=["Region", "Year"], var_name="Variable"
    ).rename(columns={"Region": "node", "Year": "year"})
    # Both for energy and material
    bld_intensity_long = bld_data_long[
        bld_data_long["Variable"].str.contains("Intensity")
    ].reset_index(drop=True)
    bld_area_long = bld_data_long[
        bld_data_long["Variable"] == "Energy Service|Residential|Floor Space"
    ].reset_index(drop=True)

    tmp = bld_intensity_long.Variable.str.split("|", expand=True)

    bld_intensity_long["commodity"] = tmp[3].str.lower()  # Material type
    bld_intensity_long["type"] = tmp[0]  # 'Material Demand' or 'Scrap Release'
    bld_intensity_long["unit"] = "kg/m2"

    bld_intensity_long = bld_intensity_long.drop(columns="Variable")
    bld_area_long = bld_area_long.drop(columns="Variable")

    bld_intensity_long = bld_intensity_long.drop(
        bld_intensity_long[np.isnan(bld_intensity_long.value)].index
    )

    # Derive baseyear material demand (Mt/year in 2020)
    bld_demand_long = bld_input_pivot.melt(
        id_vars=["Region", "Year"], var_name="Variable"
    ).rename(columns={"Region": "node", "Year": "year"})
    tmp = bld_demand_long.Variable.str.split("|", expand=True)
    bld_demand_long["commodity"] = tmp[3].str.lower()  # Material type
    # bld_demand_long = bld_demand_long[bld_demand_long['year']=="2020"].\
    #     dropna(how='any')
    bld_demand_long = bld_demand_long.dropna(how="any")
    bld_demand_long = bld_demand_long[
        bld_demand_long["Variable"].str.contains("Material Demand")
    ].drop(columns="Variable")

    return bld_intensity_long, bld_area_long, bld_demand_long


def get_scen_mat_demand(
    commod, scenario, year="2020", inputfile=INPUTFILE, case=CASE_SENS
):
    a, b, c = read_timeseries_buildings(inputfile, scenario, case)
    if not year == "all":  # specific year
        cc = c[(c.commodity == commod) & (c.year == year)].reset_index(drop=True)
    else:  # all years
        cc = c[(c.commodity == commod)].reset_index(drop=True)
    return cc


def adjust_demand_param(scen):

    s_info = ScenarioInfo(scen)
    modelyears = s_info.Y  # s_info.Y is only for modeling years

    # scen.clone(model=scen.model, scenario=scen.scenario+"_building")

    scen_mat_demand = scen.par(
        "demand", {"level": "demand"}
    )  # mat demand without buildings considered

    scen.check_out()
    comms = ["steel", "cement", "aluminum"]
    for c in comms:
        mat_building = get_scen_mat_demand(c, scen, year="all").rename(
            columns={"value": "bld_demand"}
        )  # mat demand (timeseries) from buildings model (Alessio)
        mat_building["year"] = mat_building["year"].astype(int)

        sub_mat_demand = scen_mat_demand.loc[scen_mat_demand.commodity == c]
        # print("old", sub_mat_demand.loc[sub_mat_demand.year >=2025])
        sub_mat_demand = sub_mat_demand.join(
            mat_building.set_index(["node", "year", "commodity"]),
            on=["node", "year", "commodity"],
            how="left",
        )
        sub_mat_demand["value"] = sub_mat_demand["value"] - sub_mat_demand["bld_demand"]
        sub_mat_demand = sub_mat_demand.drop(columns=["bld_demand"]).dropna(how="any")

        # Only replace for year >= 2025
        scen.add_par("demand", sub_mat_demand.loc[sub_mat_demand.year >= 2025])
        # print("new", sub_mat_demand.loc[sub_mat_demand.year >=2025])
    scen.commit("Building material demand subtracted")


def gen_data_buildings(scenario, dry_run=False):
    """Generate data for materials representation of steel industry."""
    # Load configuration
    context = read_config()
    config = context["material"]["buildings"]

    # New element names for buildings integrations
    lev_new = config["level"]["add"][0]
    comm_new = config["commodity"]["add"][0]
    tec_new = config["technology"]["add"][0]  # "buildings"

    print(lev_new, comm_new, tec_new, type(tec_new))

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Buildings raw data (from Alessio)
    (
        data_buildings,
        data_buildings_demand,
        data_buildings_mat_demand,
    ) = read_timeseries_buildings(INPUTFILE, scenario, CASE_SENS)

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    # allyears = s_info.set['year'] #s_info.Y is only for modeling years
    modelyears = s_info.Y  # s_info.Y is only for modeling years
    nodes = s_info.N
    yv_ya = s_info.yv_ya
    # fmy = s_info.y0
    nodes.remove("World")
    nodes.remove("R11_RCPA")

    # Read field values from the buildings input data
    regions = list(set(data_buildings.node))
    comms = list(set(data_buildings.commodity))
    # types = list(set(data_buildings.type))
    types = ["Material Demand", "Scrap Release"]  # Order matters

    common = dict(time="year", time_origin="year", time_dest="year", mode="M1")

    # Filter only the years in the base scenario
    data_buildings["year"] = data_buildings["year"].astype(int)
    data_buildings_demand["year"] = data_buildings_demand["year"].astype(int)
    data_buildings = data_buildings[data_buildings["year"].isin(modelyears)]
    data_buildings_demand = data_buildings_demand[
        data_buildings_demand["year"].isin(modelyears)
    ]

    # historical demands

    for rg in regions:
        for comm in comms:
            # for typ in types:

            val_mat = data_buildings.loc[
                (data_buildings["type"] == types[0])
                & (data_buildings["commodity"] == comm)
                & (data_buildings["node"] == rg),
            ]
            val_scr = data_buildings.loc[
                (data_buildings["type"] == types[1])
                & (data_buildings["commodity"] == comm)
                & (data_buildings["node"] == rg),
            ]

            # Material input to buildings
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
                    **common
                )
                .pipe(same_node)
                .assign(year_act=copy_column("year_vtg"))
            )
            results["input"].append(df)

            # Scrap output back to industry
            df = (
                make_df(
                    "output",
                    technology=tec_new,
                    commodity=comm,
                    level="end_of_life",
                    year_vtg=val_scr.year,
                    value=val_scr.value,
                    unit="t",
                    node_loc=rg,
                    **common
                )
                .pipe(same_node)
                .assign(year_act=copy_column("year_vtg"))
            )
            results["output"].append(df)

        # Service output to buildings demand
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
                **common
            )
            .pipe(same_node)
            .assign(year_act=copy_column("year_vtg"))
        )
        results["output"].append(df)

    # Create external demand param
    parname = "demand"
    demand = data_buildings_demand
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

    # TODO: check the starting model/scenario, if not ENGAGE, call adjust_demand_param
    if scenario.scenario == "LEDXXXX":
        adjust_demand_param(scenario)

    return results
