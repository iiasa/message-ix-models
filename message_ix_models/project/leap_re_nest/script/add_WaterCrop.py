# -*- coding: utf-8 -*-
"""
Importing pre-formtted irrgation data from Water Crop
Creating irrigation technologies

@author: vinca
"""
import numpy as np
import pandas as pd
from message_ix import make_df

from message_ix_models.model.water.utils import map_yv_ya_lt
from message_ix_models.util import broadcast, package_data_path, same_node, same_time


def add_irri_demand(sc, ss):
    """Load and add water demand"""

    try:
        dem_irr = sc.par("demand", {"commodity": "urban_mw"})
    except KeyError:
        print(
            "No urban water demand existing, "
            "probably you need to run the water module first"
        )

    # change values
    # dem_irr
    file = "ww_scen_km3_main_crops_NEST_BCU.csv"
    path_csv = package_data_path("projects", "leap_re_nest", "crops", file)
    # in km3
    dem_csv = pd.read_csv(path_csv)
    # filter the right scenario
    dem_df = dem_csv[dem_csv["scenario"] == ss]
    crops = list(dem_df.crop.unique())
    # map basin to numbers
    ba = np.array(sc.set("node"))
    ba = list(ba[["|" in x for x in ba]])
    map_ba = pd.DataFrame({"node": ba, "BCU": list(range(1, 25))})
    # shape output file
    dem_out = dem_df.merge(map_ba, how="left").assign(commodity="irr_water_basin")
    dem_out["level"] = "irr_" + dem_out["crop"]
    # select onlt the needed columns
    dem_out = dem_out[["node", "commodity", "level", "year", "time", "value"]]
    # load on scenario
    sc.check_out()
    levels = ["irr_" + x for x in crops]
    new_com = "irr_water_basin"
    sc.add_set("level", levels)
    sc.add_set("commodity", new_com)
    sc.add_par("demand", dem_out)
    sc.commit("Adding irrigation demand")
    print("Irrigation water demand added")


def irri_tecs(sc):
    """Create and populate irriagion technologies"""

    # irrigation efficiency
    file = "crop_params_NEST.csv"
    path_csv = package_data_path("projects", "leap_re_nest", "crops", file)
    # in km3
    crop_par_csv = pd.read_csv(path_csv)
    # irr par from Indus NEST
    file = "irr_params_NEST.csv"
    path_csv = package_data_path("projects", "leap_re_nest", "crops", file)
    irr_par_csv = pd.read_csv(path_csv)
    # sub-time
    ti = list(sc.set("time"))
    ti.remove("year")
    # sub-nodes, basins
    ba = np.array(sc.set("node"))
    ba = ba[["|" in x for x in ba]]
    first_year = sc.firstmodelyear
    years = sc.set("year")

    # km3/ha average ratio from Watercrop and SMAP2017 area data
    ww_ha = 1.691995e-06
    # 0.015
    # https://www.researchgate.net/figure/rrigation-water-requirements-for-1-ha-of-land-for-different-types-of-crops-modified_tbl2_259140691

    irr_inp = pd.DataFrame()
    irr_out = pd.DataFrame()
    irr_lt = pd.DataFrame()
    irr_inv = pd.DataFrame()
    irr_fix = pd.DataFrame()
    irr_var = pd.DataFrame()
    sc.check_out()

    cl = sc.set("level")
    cl = cl[cl.str.contains("irr_")]
    cl = [x for x in cl if x not in ["irr_cereal", "irr_sugarcrops", "irr_oilcrops"]]
    cl = [x.split("_")[1] for x in cl]
    if len(cl) == 0:
        print(
            "ERROR: Your demand input file does not contain crops for the scenario",
            sc.scenario,
        )
        return

    for cr in cl:
        print("Adding crop: ", cr)
        irr_tec = crop_par_csv.loc[crop_par_csv["crop"] == cr, "irr_type"].values[0]
        irr_eff = crop_par_csv.loc[crop_par_csv["crop"] == cr, "eta_irr"].values[0]
        tec_name = "irr_" + cr + "_" + irr_tec
        sc.add_set("technology", tec_name)
        lvl = "irr_" + cr
        # lifetime of offgrid technologies
        lt = irr_par_csv.loc[irr_par_csv["irr_type"] == irr_tec, "lifetime"].values[0]
        yv_ya_gr = map_yv_ya_lt(years[years >= first_year - lt], lt, first_year)
        # covert from kWh/m3 to GWa/km3
        en_inp = (
            irr_par_csv.loc[irr_par_csv["irr_type"] == irr_tec, "en_inp_kWh_m3"].values[
                0
            ]
            * 1000
            / 8760
        )
        # costs still in USD/ha
        inv_cost = irr_par_csv.loc[
            irr_par_csv["irr_type"] == irr_tec, "inv_USD_ha"
        ].values[0]
        var_cost = irr_par_csv.loc[
            irr_par_csv["irr_type"] == irr_tec, "var_USD_ha"
        ].values[0]
        fix_cost = irr_par_csv.loc[
            irr_par_csv["irr_type"] == irr_tec, "fix_USD_ha"
        ].values[0]
        irr_inp = pd.concat(
            [
                irr_inp,
                (
                    make_df(
                        "input",
                        technology=tec_name,
                        value=1,
                        unit="km3/month",
                        level="water_supply_basin",
                        commodity="freshwater_basin",
                        mode="M1",
                        time=ti,
                    )
                    .pipe(
                        broadcast,
                        yv_ya_gr,
                        node_loc=ba,
                    )
                    .pipe(same_time)
                    .pipe(same_node)
                ),
            ]
        )
        # electricity input
        irr_inp = pd.concat(
            [
                irr_inp,
                (
                    make_df(
                        "input",
                        technology=tec_name,
                        value=en_inp,
                        unit="GWa/month",
                        level="final_rur",  # maybe do a dedicated irrigation level
                        commodity="electr",
                        mode="M1",
                        time=ti,
                        time_origin="year",
                    )
                    .pipe(
                        broadcast,
                        yv_ya_gr,
                        node_loc=ba,
                    )
                    .pipe(same_time)
                    .pipe(same_node)
                ),
            ]
        )

        irr_out = pd.concat(
            [
                irr_out,
                (
                    make_df(
                        "output",
                        technology=tec_name,
                        value=irr_eff,
                        unit="km3/month",
                        level=lvl,
                        commodity="irr_water_basin",
                        mode="M1",
                        time=ti,
                        time_dest="year",
                    )
                    .pipe(
                        broadcast,
                        yv_ya_gr,
                        node_loc=ba,
                    )
                    .pipe(same_time)
                    .pipe(same_node)
                ),
            ]
        )
        # lifetime maybe to be changwes
        irr_lt = pd.concat(
            [
                irr_lt,
                make_df(
                    "technical_lifetime",
                    technology=tec_name,
                    year_vtg=yv_ya_gr["year_vtg"].unique(),
                    unit="year",
                    value=lt,
                ).pipe(broadcast, node_loc=ba),
            ]
        )
        # investment
        irr_inv = pd.concat(
            [
                irr_inv,
                make_df(
                    "inv_cost",
                    technology=tec_name,
                    year_vtg=yv_ya_gr["year_vtg"].unique(),
                    unit="USD/kW",
                    value=inv_cost / ww_ha * 1e-6,  # million USD/km3
                ).pipe(broadcast, node_loc=ba),
            ]
        )
        # fix costs
        irr_fix = pd.concat(
            [
                irr_fix,
                make_df(
                    "fix_cost",
                    technology=tec_name,
                    unit="USD/kW",
                    node_loc=ba,
                    value=fix_cost / ww_ha * 1e-6,  # million USD/km3
                ).pipe(broadcast, yv_ya_gr),
            ]
        )
        # var costs
        irr_var = pd.concat(
            [
                irr_var,
                make_df(
                    "var_cost",
                    technology=tec_name,
                    unit="USD/kW",
                    node_loc=ba,
                    mode="M1",
                    value=var_cost / ww_ha * 1e-6,  # million USD/km3
                ).pipe(broadcast, yv_ya_gr, time=ti),
            ]
        )
        # loop end

    # add paramenters
    sc.add_par("input", irr_inp)
    sc.add_par("output", irr_out)
    sc.add_par("technical_lifetime", irr_lt)
    sc.add_par("inv_cost", irr_inv)
    sc.add_par("fix_cost", irr_fix)
    sc.add_par("var_cost", irr_var)
    sc.commit("adding irrigation tehnologies")
    print("Ïrrigation technologies added for the scenario", sc.scenario)


def main(sc, ss):
    """This script removes old electricity demand from MESSAGE and
    replaces it with estimated demand from MLED, with sub-annual timestep
    (by adding the technologies...)
    """

    # add irrigation demand
    add_irri_demand(sc, ss)
    irri_tecs(sc)


if __name__ == "__main__":
    # parse sys.argv[1:] using optparse or argparse or what have you
    main("test")
