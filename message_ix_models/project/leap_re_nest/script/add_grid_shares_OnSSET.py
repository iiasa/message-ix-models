# -*- coding: utf-8 -*-
"""
Created on Mon Nov 14 16:38:39 2022

@author: vinca
"""

import numpy as np
import pandas as pd
from message_ix import make_df

from message_ix_models.model.water.utils import map_yv_ya_lt

# add OnSSET shares of grid vs off grid electricity
from message_ix_models.util import broadcast, package_data_path, same_node, same_time


def add_trsm_dist_basin(sc, scen_name):
    # sub-time
    ti = list(sc.set("time"))
    ti.remove("year")
    # sub-nodes, basins
    ba = np.array(sc.set("node"))
    ba = ba[["|" in x for x in ba]]
    # modes for sub-basins
    mo = np.array(sc.set("mode"))
    mo = mo[["|" in x for x in mo]]

    last_vtg_year = 2010
    file = "OnSSET_cost_paramters.csv"
    path_csv = package_data_path("projects", "leap_re_nest", file)
    onsset_pars = pd.read_csv(path_csv)
    # TODO
    file = "grid_cost_bcu_" + scen_name + ".csv"
    # file = "grid_cost_bcu.csv"
    path_csv = package_data_path("projects", "leap_re_nest", file)
    grid_cost = pd.read_csv(path_csv)[["BCU", "urb_rur", "tot_cost_usd2010_kW"]]
    grid_loss = onsset_pars["Value"][onsset_pars["Variable"] == "grid_losses"]
    sc.check_out()

    for ur in ["_urb", "_rur"]:
        # load elec_t_d and make a elec_t_basin with outputs to the basin by
        # mode
        inp_t = sc.par("input", {"technology": "elec_t_d"})
        inp_t["mode"] = np.nan
        inp_t["node_loc"] = np.nan
        inp_t["time"] = np.nan
        map_mode_node_t = pd.DataFrame({"mode": mo, "node_loc": ba})
        inp_t = inp_t.pipe(broadcast, map_mode_node_t, time=ti)
        inp_t["technology"] = "elec_t_d_basin" + ur

        sc.add_set(
            "technology",
            ["elec_t_d_basin" + ur, "sp_el_I_basin" + ur, "sp_el_RC_basin" + ur],
        )
        sc.add_set("level", ["sec_urb", "sec_rur", "final_urb", "final_rur"])
        # no ned for parameter before 2010, save space
        inp_t = inp_t[inp_t["year_vtg"] >= last_vtg_year]
        sc.add_par("input", inp_t)

        # from country to sub-basins
        out_t = sc.par("output", {"technology": "elec_t_d"})
        out_t["time_dest"] = np.nan
        out_t["time"] = np.nan
        out_t["mode"] = np.nan
        out_t["node_dest"] = np.nan
        out_t["node_loc"] = np.nan
        map_mode_node_t = pd.DataFrame({"mode": mo, "node_loc": ba})
        out_t = (
            out_t.pipe(broadcast, map_mode_node_t, time=ti)
            .pipe(same_time)
            .pipe(same_node)
        )
        out_t["technology"] = "elec_t_d_basin" + ur
        out_t["level"] = "sec" + ur
        # Apply grid losses from OnSET
        out_t["value"] = out_t["value"] - float(grid_loss)
        # no ned for parameter before 2010, save space
        out_t = out_t[out_t["year_vtg"] >= last_vtg_year]
        sc.add_par("output", out_t)

        # tec lifetime
        tecl_t = sc.par("technical_lifetime", {"technology": "elec_t_d"})
        tecl_t["technology"] = "elec_t_d_basin" + ur
        tecl_t["node_loc"] = np.nan
        tecl_t = tecl_t.pipe(broadcast, node_loc=ba)
        tecl_t = tecl_t[tecl_t["year_vtg"] >= last_vtg_year]
        sc.add_par("technical_lifetime", tecl_t)

        # add inv cotst
        grid_cost
        inv_t = sc.par("inv_cost", {"technology": "elec_t_d"})
        inv_t["technology"] = "elec_t_d_basin" + ur
        inv_t["node_loc"] = np.nan
        inv_t = inv_t.pipe(broadcast, node_loc=ba)
        inv_t["BCU"] = [int(x.split("|")[0][1:]) for x in inv_t.node_loc]
        inv_t = inv_t.merge(grid_cost[grid_cost["urb_rur"] == ur], how="left")
        inv_t["value"] = inv_t["tot_cost_usd2010_kW"]
        inv_t = inv_t[
            [
                "node_loc",
                "technology",
                "year_vtg",
                "value",
                "unit",
            ]
        ]
        inv_t = inv_t[inv_t["year_vtg"] >= last_vtg_year]

        sc.add_par("inv_cost", inv_t)
        # fix cost
        fix_t = sc.par("fix_cost", {"technology": "elec_t_d"})
        fix_t["technology"] = "elec_t_d_basin" + ur
        fix_t["node_loc"] = np.nan
        fix_t = fix_t.pipe(broadcast, node_loc=ba)
        fix_t = fix_t[fix_t["year_vtg"] >= last_vtg_year]
        sc.add_par("fix_cost", fix_t)
        # add var cost
        var_t = sc.par("var_cost", {"technology": "elec_t_d"})
        # add the mode dimension when creating heterogenous cost mapping
        var_t["mode"] = np.nan
        var_t["node_loc"] = np.nan
        var_t["time"] = np.nan
        var_t = var_t.pipe(broadcast, map_mode_node_t, time=ti)
        var_t["technology"] = "elec_t_d_basin" + ur
        var_t = var_t[var_t["year_vtg"] >= last_vtg_year]
        sc.add_par("var_cost", var_t)

        # add sp_el_I and sp_el_RC to link the new transmission technologies to the new
        # demands
        # between transmission and final user there is a splitter that takes some energy
        # form rural generation

        inp = sc.par("input", {"technology": "sp_el_I"})
        inp["time"] = np.nan
        inp["node_loc"] = np.nan
        inp = inp.pipe(broadcast, time=ti, node_loc=ba)
        inp["level"] = "final" + ur
        inp["time_origin"] = inp["time"]
        inp["node_origin"] = inp["node_loc"]

        inp_I = inp.copy(deep=True)
        inp_I["technology"] = "sp_el_I_basin" + ur
        inp_RC = inp.copy(deep=True)
        inp_RC["technology"] = "sp_el_RC_basin" + ur
        inp_RC = inp_RC[inp_RC["year_vtg"] >= last_vtg_year]
        sc.add_par("input", pd.concat([inp_I, inp_RC]))
        # output, need to be split into basins
        out = sc.par("output", {"technology": "sp_el_I"})
        out["time"] = np.nan
        out["node_loc"] = np.nan
        out = out.pipe(broadcast, time=ti, node_loc=ba)
        out["time_dest"] = out["time"]
        out["node_dest"] = out["node_loc"]

        out_I = out.copy(deep=True)
        out_I["technology"] = "sp_el_I_basin" + ur
        out_I["commodity"] = "ind_man" + ur
        out_RC = out.copy(deep=True)
        out_RC["technology"] = "sp_el_RC_basin" + ur
        out_RC["commodity"] = "res_com" + ur

        # existing capacity should be added
        # 1.  first save a model output where the model builds needed capacity in 2020
        # 2. that can be used as calibration and to be compared with OnSSET data
        # This is done externlly to this function, in calib_energy()
        out_RC = out_RC[out_RC["year_vtg"] >= last_vtg_year]
        sc.add_par("output", pd.concat([out_I, out_RC]))
        # end of loop

    # add sp_el_crop link the new transmission technologies to the crop
    # processing demands, only in the rural generation
    sc.add_set("technology", ["sp_el_crop_basin_rur"])
    ur = "_rur"
    inp = sc.par("input", {"technology": "sp_el_I"})
    inp["time"] = np.nan
    inp["node_loc"] = np.nan
    inp = inp.pipe(broadcast, time=ti, node_loc=ba)
    inp["level"] = "final" + ur
    inp["time_origin"] = inp["time"]
    inp["node_origin"] = inp["node_loc"]

    inp_crop = inp.copy(deep=True)
    inp_crop["technology"] = "sp_el_crop_basin" + ur
    sc.add_par("input", inp_crop)
    # output, need to be split into basins
    out = sc.par("output", {"technology": "sp_el_I"})
    out["time"] = np.nan
    out["node_loc"] = np.nan
    out = out.pipe(broadcast, time=ti, node_loc=ba)
    out["time_dest"] = out["time"]
    out["node_dest"] = out["node_loc"]

    out_crop = out.copy(deep=True)
    out_crop["technology"] = "sp_el_crop_basin" + ur
    out_crop["commodity"] = "crop" + ur
    sc.add_par("output", out_crop)

    sc.commit("new electricity transmission")
    print("New distribution and linkaged to basin's demand added")


def add_offgrid_gen(sc, scen_name):
    """This function adds off-grid electricity generation technologies

    Parameters
    ----------
    sc : :class:`message_ix.Scenario`
        Scenario to which changes should be applied.
    scen_name : string
        name of the specific LEAP-RE policy scenario

    Returns
    -------
    None.

    """

    # get some info needed
    first_year = sc.firstmodelyear
    years = sc.set("year")

    ti = list(sc.set("time"))
    ti.remove("year")
    # lifetime of offgrid technologies
    lt = 30
    yv_ya_gr = map_yv_ya_lt(years[years >= first_year - lt], lt, first_year)

    # add needed sets
    sc.check_out()
    sc.add_set("technology", ["offgrid_urb", "offgrid_rur"])
    sc.add_set("level", ["offgrid_final_urb", "offgrid_final_rur"])

    file = "energy_allocation_results_for_nest.csv"
    path_csv = package_data_path("projects", "leap_re_nest", file)
    onsset_en = pd.read_csv(path_csv)
    onsset_en = onsset_en[onsset_en["scenario"].str.contains(scen_name)]
    # interpolate missing years IT MIGHT CHANGE in FUTURE VERSION
    onsset_en_int = (
        onsset_en.pivot(
            index="year",
            columns=["BCU", "tec", "urb_rur", "scenario", "unit"],
            values="value",
        )
        .reindex(range(onsset_en["year"].min(), onsset_en["year"].max() + 1, 10))
        .interpolate("index")
        .unstack(-1)
        .reset_index(name="value")
    )

    strings = ["B" + str(x) for x in onsset_en_int["BCU"]]
    onsset_en_int["BCU"] = strings

    nodes = list(sc.set("node"))
    nodes_df = pd.DataFrame({"node": nodes, "BCU": [x.split("|")[0] for x in nodes]})

    # Calculate the share of grid vs offgrid generation and use it to make a
    # share constraints
    df = onsset_en_int.merge(nodes_df, how="left")
    df_grid = df.copy()
    # exclude unelectrified
    df_grid = df_grid[df_grid["tec"] != "unelectrified"]
    df_grid["tec"] = [
        "offgrid" if x not in ["grid_existing", "grid_new"] else "grid"
        for x in df_grid["tec"]
    ]
    df_grid = df_grid.groupby(["node", "year", "urb_rur", "tec"], as_index=False).sum(
        "value"
    )
    df_grid = (
        df_grid.groupby(["node", "year", "urb_rur"])
        .apply(lambda grp: grp.assign(share=lambda x: x.value / x.value.sum()))
        .fillna(0)
    )
    df_grid["urb_rur"] = "_" + df_grid["urb_rur"]

    # also calculate the share of different off-grid technologies within
    # the off-grid generation. This is used to assign costs to the single
    # off-grid technology
    df_off = df.copy()
    df_off = df_off[~df_off.tec.isin(["grid_existing", "grid_new", "unelectrified"])]
    df_off = (
        df_off.groupby(["node", "year", "urb_rur"])
        .apply(lambda grp: grp.assign(share=lambda x: x.value / x.value.sum()))
        .fillna(0)
    )

    # load file with investment cost assumption from OnSSET
    file = "OnSSET_cost_paramters.xlsx"
    path_xls = package_data_path("projects", "leap_re_nest", file)
    tec_cost = pd.read_excel(path_xls, sheet_name="for_NEST")
    avg_cost = int(tec_cost[tec_cost["tec"] == "average"]["inv_cost"])

    df_offc = df_off.merge(tec_cost, how="left")
    df_offc["tot_cost"] = df_offc["share"] * df_offc["inv_cost"]
    df_offc = df_offc.groupby(["node", "year", "urb_rur"], as_index=False).sum(
        "tot_cost"
    )

    df_offc["urb_rur"] = "_" + df_offc["urb_rur"]

    for ur in ["_urb", "_rur"]:
        # only one off-grid technology
        out_t = (
            make_df(
                "output",
                technology="offgrid" + ur,
                commodity="electr",
                level="offgrid_final" + ur,
                time=ti,
                mode="M1",
                unit="GWa",
                value=1,
            )
            .pipe(
                broadcast,
                yv_ya_gr,
                node_loc=df.node.unique(),
            )
            .pipe(same_node)
            .pipe(same_time)
        )

        # lifetime
        lt_t = make_df(
            "technical_lifetime",
            technology="offgrid" + ur,
            year_vtg=out_t.year_vtg.unique(),
            unit="year",
            value=lt,
        ).pipe(broadcast, node_loc=df.node.unique())

        # capital cost
        inv_df = df_offc[df_offc["urb_rur"].str.contains(ur)]
        # Calculate the average value for each group excluding zeros
        avg_values = (
            inv_df.loc[inv_df["tot_cost"] != 0]
            .groupby(["node", "urb_rur"])["tot_cost"]
            .mean()
        )
        # Replace 0 values in 'tot_cost' with the average value of each group
        inv_df.loc[inv_df["tot_cost"] == 0, "tot_cost"] = (
            inv_df.loc[inv_df["tot_cost"] == 0]
            .set_index(["node", "urb_rur"])
            .index.map(avg_values.get)
        )
        # if inv_df has na values replace with avg_cost
        inv_df["tot_cost"] = inv_df["tot_cost"].fillna(avg_cost)

        inv_t = make_df(
            "inv_cost",
            technology="offgrid" + ur,
            year_vtg=inv_df["year"],
            unit="USD/kW",
            node_loc=inv_df["node"],
            value=avg_cost if inv_df.empty else inv_df["tot_cost"],
        )

        # fixed costs for off-grid 2% of inv costs (as solar pv and wind)
        fix_t = make_df(
            "fix_cost",
            technology="offgrid" + ur,
            unit="USD/kW",
            node_loc=inv_df["node"],
            value=avg_cost * 0.02 if inv_df.empty else inv_df["tot_cost"] * 0.02,
        ).pipe(broadcast, yv_ya_gr)

        # add paramenters
        sc.add_par("output", out_t)
        sc.add_par("technical_lifetime", lt_t)
        sc.add_par("inv_cost", inv_t)
        sc.add_par("fix_cost", fix_t)

        # add technology with share constraint
        sc.add_set("technology", ["elec_fin" + ur])
        map_lvl_mode = pd.DataFrame(
            {"level": ["sec" + ur, "offgrid_final" + ur], "mode": ["M1", "M2"]}
        )
        inp_f = (
            make_df(
                "input",
                technology="elec_fin" + ur,
                commodity="electr",
                year_vtg=out_t.year_vtg.unique(),
                year_act=out_t.year_vtg.unique(),
                unit="GWa",
                value=1,
            )
            .pipe(broadcast, map_lvl_mode, time=ti, node_loc=df.node.unique())
            .pipe(same_node)
            .pipe(same_time)
        )

        out_f = (
            make_df(
                "output",
                technology="elec_fin" + ur,
                commodity="electr",
                level="final" + ur,
                year_vtg=out_t.year_vtg.unique(),
                year_act=out_t.year_vtg.unique(),
                unit="GWa",
                value=1,
            )
            .pipe(broadcast, mode=["M1", "M2"], time=ti, node_loc=df.node.unique())
            .pipe(same_node)
            .pipe(same_time)
        )

        sc.add_par("input", inp_f)
        sc.add_par("output", out_f)

        # add share for the electricity use from df_gridd
        # TODO simplyfy decimals
        sc.add_set("shares", ["share_grid"])
        share_df = df_grid[df_grid["urb_rur"].str.contains(ur)]
        share_df["share"][share_df["share"].isna()] = 1
        share_g = share_df[share_df["tec"] == "grid"]
        share_og = share_g.copy()
        share_og["tec"] = "offgrid"
        share_og["share"] = 1 - share_og["share"]
        share_df = pd.concat([share_g, share_og])
        share_df["share"] = round(share_df["share"], 4)

        share_df["mode"] = np.where(share_df["tec"] == "grid", "M1", "M2")

        share_f = make_df(
            "share_mode_up",
            shares="share_grid",
            technology="elec_fin" + ur,
            mode=share_df["mode"],
            node_share=share_df["node"],
            value=share_df["share"],
            unit="%",
            year_act=share_df["year"],
        ).pipe(broadcast, time=ti)

        sc.add_par("share_mode_up", share_f)
        sc.add_par("share_mode_lo", share_f)

    sc.commit("adding off-grid generation and share constraints")
    print("Adding off-grid generation and share constraints")


def calib_energy(sc, sc_cali):
    """calibrating the historical capacity of different energy technologies
    including transmission lines
    """
    calib_cap = sc_cali.var("CAP_NEW")
    calib_cap = calib_cap[(calib_cap["year_vtg"] == 2020) & (calib_cap["lvl"] > 0)]
    calib_cap["value"] = calib_cap["lvl"]
    # if set to 2020 it will be inglored by the model (not consider as historical)
    calib_cap["year_vtg"] = 2015
    calib_cap.drop(columns={"lvl", "mrg"}, inplace=True)
    calib_cap.reset_index(drop=True, inplace=True)
    calib_cap["unit"] = "GWa/a"

    # add it as historical new capacity to the main scenario
    sc.check_out()
    sc.add_par("historical_new_capacity", calib_cap)
    sc.commit("calibrating historical new capcity")
    print("Calibrated historical new caapcity added to the model")


def main(sc, scen_name):
    """Adding transmission technologies in the sub-nodes and off-grid generation"""

    # load previous total demand in firstmodelyear

    add_trsm_dist_basin(sc, scen_name)
    add_offgrid_gen(sc, scen_name)

    # calibration
    sc_cali = sc.clone(sc.model, sc.scenario + "_cali", keep_solution=False)
    sc_cali.check_out()
    ytr = sc.set("year")
    ytr = ytr[ytr > sc.firstmodelyear]
    print("Removing future years for calibration")
    sc_cali.remove_set("year", ytr)
    sc_cali.commit("removing future years for calibration...")
    print("Running the calibration...")
    sc_cali.solve(solve_options={"lpmethod": "4"}, model="MESSAGE")
    sc_cali.set_as_default()
    print("calibration solved")

    calib_energy(sc, sc_cali)


if __name__ == "__main__":
    # parse sys.argv[1:] using optparse or argparse or what have you
    main("test")
