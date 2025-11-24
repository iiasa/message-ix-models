# -*- coding: utf-8 -*-
"""
This script receives a scenario with only 'year' as time slice, and an Excel
file for the input data for a number of sub-annual time slices.
Then the script does the following:
    - it reads data of timeslices and their duration from Excel
    - it adds timeslices to relevant parameters, e.g., "output", "input",
    "capacity_factor", "var_cost" etc., for those technologies that their
    electricity generation must be at the sub-annual level.
    - adds year-equivalent of all technologies with timeslices, by relations
    for summing up each technology to 'year'.
    - modifies parameters as needed
    Als it can do the following (some are optional):
    - remove extra vintage years, and exctra technologies
    - correct capacity factor of zero for VRE
    - copying historical data to "ref" parameters
"""

import os
from datetime import datetime
from itertools import product
from timeit import default_timer as timer

import pandas as pd

# path_files = r"C:\Users\zakeri\Documents\Github\python_scripts"
# os.chdir(path_files)


# 1.1) A function for reading input data from excel and formatting as needed
def xls_to_df(xls, n_time, nodes, exclude_sheets=["peak_demand"]):
    # Converting time series based on the number of time slices
    df_time = xls.parse("time_steps", converters={"time": str})
    df_time = df_time.loc[df_time["lvl_temporal"] != "Sum"]
    n_xls = len(df_time["time"])
    dur = df_time["duration_time"]
    length = int(n_xls / n_time)
    par_list = [x for x in xls.sheet_names if x not in ["time_steps"] + exclude_sheets]

    dict_xls = {}
    for p in par_list:
        df_xls = xls.parse(p)
        idx = df_xls.columns[0]
        df_xls = df_xls.set_index(idx)

        new = pd.DataFrame(index=range(1, n_time + 1), columns=df_xls.columns)
        for col, rate in df_xls.loc["rate", :].to_dict().items():
            df_xls = df_xls.loc[df_xls.index != "rate"]
            z = 1
            for i in list(range(0, n_xls, length)):
                if rate == "Yes":
                    new.loc[z, col] = float(df_xls.iloc[i : i + length, :][col].sum())
                elif rate == "No":
                    new.loc[z, col] = float(df_xls.iloc[i : i + length, :][col].mean())
                z = z + 1
            new.loc["rate", col] = rate

        # populating "all" nodes
        for col in new.columns:
            if col.split(",")[0] == "all":
                for node in nodes:
                    c = node + "," + col.split(",")[1]
                    new[c] = new.loc[:, col]
                new = new.drop(col, axis=1)

        dict_xls[p] = new

    duration = [round(x, 8) for x in dur.groupby(dur.index // length).sum()]
    df_time = df_time.loc[0 : n_time - 1, :]
    df_time["duration_time"] = duration
    return duration, df_time, dict_xls


# 1.2) A function for updating sets and duration time after adding time slices
def time_setup(sc, df, last_year=False, remove_old_time_lvl=False):
    sc.check_out()
    # Changing the last year if needed
    if last_year:
        print("- Removing extra model years...")
        for y in [x for x in sc.set("year").tolist() if int(x) > last_year]:
            sc.remove_set("year", y)
        # sc.commit('')
        # sc.check_out()
        print("- Extra model years bigger than {} removed.".format(str(last_year)))

    # Adding sub-annual time slices to the set 'time'
    for t in df["time"].tolist():
        sc.add_set("time", t)

    # Adding sub-annual time levels to the set 'lvl_temporal'
    for l in set(df["lvl_temporal"]):
        sc.add_set("lvl_temporal", l)

    # Adding temporal hierarchy
    df_ref = sc.set("map_temporal_hierarchy")
    d = pd.DataFrame(columns=df_ref.columns, index=[0])

    for i in df.index:
        d["lvl_temporal"] = df.loc[i, "lvl_temporal"]
        d["time"] = df.loc[i, "time"]
        d["time_parent"] = df.loc[i, "parent_time"]
        # df = df.append(df_new, ignore_index=True)
        sc.add_set("map_temporal_hierarchy", d)

    if remove_old_time_lvl:
        sc.remove_set("map_temporal_hierarchy", df_ref)
    sc.commit("")
    print("- All sets modified for the new time slices.")


# 1.3) A function for modifying duration time
def duration_time(sc, df):
    sc.check_out()
    df_ref = sc.par("duration_time")
    d = df_ref.copy()
    sc.remove_par("duration_time", df_ref)

    for i in df.index:
        d["time"] = df.loc[i, "time"]
        d["unit"] = "%"
        df["value"] = df.loc[i, "duration_time"]
        sc.add_par("duration_time", df)

    check = sc.par("duration_time")
    check.loc[0, "value"] = check.loc[0, "value"] + 1.00 - check["value"].sum()
    sc.add_par("duration_time", check.loc[[0]])
    sc.commit("duration time modified")
    print('- Parameter "duration_time" updated for new values.')


# 1.4) Setting up for parameters
def par_setup(sc, dict_xls, par_update):
    sc.check_out()
    xls_pars = [x for x in dict_xls.keys() if x in sc.par_list()]

    index_cols = {}
    for parname in set(list(par_update.keys()) + xls_pars):
        if "technology" in sc.idx_names(parname):
            index_cols[parname] = "technology"
        elif "commodity" in sc.idx_names(parname):
            index_cols[parname] = "commodity"
        elif "relation" in sc.idx_names(parname):
            index_cols[parname] = "relation"

        if parname in xls_pars:
            df_xls = dict_xls[parname]

            for item in df_xls.columns:
                if df_xls.loc["rate", item] == "No":
                    if "," in item:
                        node_col = [x for x in sc.idx_names(parname) if "node" in x]
                        df_item = sc.par(
                            parname,
                            {
                                index_cols[parname]: [item.split(",")[1]],
                                node_col[0]: [item.split(",")[0]],
                            },
                        )
                    else:
                        df_item = sc.par(parname, {index_cols[parname]: [item]})
                    df_item["value"] = 1
                    sc.add_par(parname, df_item)

                if parname not in par_update.keys():
                    par_update[parname] = {}

                par_update[parname][item] = df_xls[item].tolist()
    sc.commit("")
    print("- Input data of parameters are set up.")
    return par_update, index_cols


# 1.5) The main function for adding timeslices to a scenario
def par_time_update(sc, parname, data_dict, index_col, item_list, n1, nn, sc_ref=None):
    sc.check_out()
    if not sc_ref:
        sc_ref = sc

    node_tec = [
        (k.split(",")[0], k.split(",")[1]) for k in data_dict.keys() if "," in k
    ]
    node_col = [
        x for x in sc.idx_names(parname) if x in ["node", "node_loc", "node_rel"]
    ][0]
    for key, ratio in data_dict.items():
        df = []

        if key == "all":
            if item_list:
                df_ref = sc_ref.par(parname, {index_col: item_list, "time": "year"})
            else:
                df_ref = sc_ref.par(parname, {"time": "year"})

            # Excluding those technologies that have explicit CF through Excel
            df_ref = df_ref.loc[~df_ref[index_col].isin(data_dict.keys())]

            # also for "node,technology" pairs
            for x in node_tec:
                df_ref = df_ref.loc[
                    (df_ref[index_col] != x[1]) & (df_ref[node_col] != x[1])
                ]

        # This part is for those parameters defined per node (key='KAZ,i_spec')
        elif "," in key:
            df_ref = sc_ref.par(
                parname,
                {
                    index_col: key.split(",")[1],
                    node_col: key.split(",")[0],
                    "time": "year",
                },
            )

        else:
            df_ref = sc_ref.par(parname, {index_col: key, "time": "year"})

        if not df_ref.empty:
            if nn >= n_time:
                sc.remove_par(parname, df_ref)
            time_cols = [
                x
                for x in sc.idx_names(parname)
                if x in ["time", "time_dest", "time_origin"]
            ]

            for ti in df_time["time"][n1:nn].tolist():
                df_new = df_ref.copy()
                for col in time_cols:
                    if col == "time_origin":
                        df_new.loc[df_new["technology"].isin(tec_inp), col] = ti
                    elif col == "time_dest":
                        df_new.loc[~df_new["technology"].isin(tec_only_inp), col] = ti
                    else:
                        df_new[col] = ti
                df_new["value"] *= ratio[df_time["time"].tolist().index(ti)]
                df.append(df_new)

            df = pd.concat(df, ignore_index=True)
            sc.add_par(parname, df)
    sc.commit("")
    df = df_new = []


# 1.6) A function for minor adjustments in some parameters
def par_adjust(parname, dict_data, multiplier=0, plus=0):
    df = sc.par(parname, dict_data)
    df["value"] *= multiplier
    df["value"] += plus
    sc.add_par(parname, df)


# 1.7) Removing cooling technologies from a list
def remove_cooling(sc, tec_list):
    print("- Removing cooling technologies...")
    # Both commodity and technology must be removed
    cool_terms = ["__air", "__cl_fresh", "__ot_fresh", "__ot_saline"]
    tec_cool = [
        x
        for x in set(sc.set("technology"))
        if x in [y + z for (y, z) in product(tec_list, cool_terms)]
    ]
    # tec_cool = sc2.set('cat_tec', {'type_tec': 'power_plant_cooling'})
    com_cool = [
        x
        for x in sc.set("commodity").tolist()
        if any([y in x for y in ["cooling", "freshwater"]])
    ]
    lvl_cool = ["cooling", "water_supply"]

    sc.check_out()
    for x in tec_cool:
        sc.remove_set("technology", x)
    for x in com_cool:
        sc.remove_set("commodity", x)
    for x in lvl_cool:
        if x in sc.set("level"):
            sc.remove_set("level", x)
    tec_list = [x for x in tec_list if x not in tec_cool]
    tec_rem = list(set([x.split("__")[0] for x in tec_cool]))
    print(
        "- Cooling technologies for {}, and cooling commodities and"
        " levels were removed.".format(tec_rem)
    )
    sc.commit("cooling removed")
    return sorted(tec_list)


# 1.8) Removing data from a parameter if value iz zero
def remove_data_zero(sc, parname, filter_dict, value=0):
    sc.check_out()
    df = sc.par(parname, filter_dict)
    df = df.loc[df["value"] == value]
    sc.remove_par(parname, df)
    sc.commit("zero removed")
    return df


# 1.9) removing some parameters if needed
def par_remove(sc, par_dict, verb=True):
    for parname, filters in par_dict.items():
        df = sc.par(parname, filters)
        sc.remove_par(parname, df)
        if verb:
            if not filters:
                filters = "All"
            print('> {} data was removed from parmeter "{}".'.format(filters, parname))


# 1.10) Adding year-equivalent of technologies at the timeslice level
def rel_year_equivalent(
    sc,
    tec_list,
    rel_ref="res_marg",
    tec_ref="elec_t_d",
    rel_bound="CH4_Emission",
    remove_existing=False,
):
    """
    sc: object, MESSAGEix scenario
    tec_list: list, technologies for which to add a 'year' equivalent relaiton
    rel_ref: string, a relation to copy data of relation activity from
    tec_ref: string or list, a technology to copy a relation from, if list
        the technology and nodes are mapped as ['tec', ['node1', 'node2']]
    rel_bound: string, a relation to copy data of relation upper and lower from
    remove_existing: boolean, if True all data for (rel_new) is removed

    """
    parname = "relation_activity_time"
    for tec in tec_list:
        rel_new = tec + "_year"

        # adding sets
        if remove_existing and rel_new in set(sc.set("relation")):
            sc.remove_set("relation", rel_new)

        sc.add_set("relation", rel_new)

        # The part for technologies in 'time' slices'
        df = sc.par(parname, {"relation": rel_ref, "technology": tec_ref})
        df["relation"] = rel_new
        df["technology"] = tec
        df["value"] = -1

        # mapping with technical_lifetime
        df_lt = sc.par("technical_lifetime", {"technology": tec})
        df_lt = df_lt.set_index(["node_loc", "technology", "year_vtg"])
        df = df.set_index(["node_loc", "technology", "year_act"])

        if not df_lt.empty:
            df = df.loc[df.index.isin(set(df_lt.index))]
        df = df.reset_index()

        # The part for equivalent technology at 'year' level
        d = df.loc[df["time"] == df["time"][0]].copy()
        d["value"] = 1
        d["time"] = "year"

        df = df.append(d, ignore_index=True)
        sc.add_par(parname, df)

        # relation upper and lower
        df = df.set_index(["node_rel", "year_rel"])
        for par in ["relation_lower_time", "relation_upper_time"]:
            df_b = sc.par(par, {"relation": rel_bound})
            df_b = df_b.set_index(["node_rel", "year_rel"])
            df_b = df_b.loc[df_b.index.isin(set(df.index))]
            df_b = df_b.reset_index()

            df_b["relation"] = rel_new
            sc.add_par(par, df_b)


# 1.11) Sorting relations based on their applicability to time slices
def relation_tec(
    sc,
    tec_list,
    times,
    rel_yr_ti=["res_marg"],
    rel_remain=["CO2_cc", "CO2_trp", "CO2_ind", "CO2_r_c"],
):
    sc.check_out()
    print("- Removing extra relations...")
    # removing extra relations
    for r in ["RES_variable_limt", "gas_prod"]:
        if r in set(sc.set("relation")):
            sc.remove_set("relation", r)
            print(r)

    rel_list = [x for x in sc.set("relation").to_list() if x not in rel_remain]
    for r in rel_list:
        if (
            sc.par("relation_lower", {"relation": r}).empty
            and (sc.par("relation_upper", {"relation": r}).empty)
            and (sc.par("relation_cost", {"relation": r}).empty)
        ):
            sc.remove_set("relation", r)
            print(r)

    rel_list = sc.set("relation").to_list()
    tec_in = list(set(sc.par("input")["technology"]))
    tec_out = list(set(sc.par("output")["technology"]))
    tec_in_out = set(tec_in + tec_out)

    rel_time = []  # relations at time level (including their lower/upper)
    account = {}  # most of accounting relations (will be at year level)
    rel_year = []  # relations must be at year (but time related tecs at time)
    rel_pot = []  # potential relations (must be at year level)
    tecs_free = []
    yr_list = [x for x in sc.set("year") if x >= sc.firstmodelyear]

    for r in rel_list:
        d = sc.par("relation_activity", {"relation": r, "year_act": yr_list})

        # free technologies (have no input/output)
        tec_free = [x for x in set(d["technology"]) if x not in tec_in_out]
        tecs_free = tecs_free + tec_free

        # technologies in this relation that must be at time level
        tec_time = [x for x in set(d["technology"]) if x in tec_list]
        tec_time = tec_time + tec_free

        # if all tecs at time slices, then the relation at time level
        if len(tec_time) == len(set(d["technology"])):
            rel_time.append(r)

        # if all tecs at time slice but one not, collected as accounting for now
        elif len(tec_time) + 1 == len(set(d["technology"])) and len(tec_time) > 0:
            account[r] = [x for x in set(d["technology"]) if x not in tec_time][0]

        # all other relations (mix of time and non-time tecs, at year level)
        else:
            rel_year.append(r)

        d2 = sc.par("relation_upper", {"relation": r, "year_rel": yr_list})
        if not d2.empty:
            if d2["value"].mode()[0] != 0:
                rel_pot.append(r)
        d3 = sc.par("relation_lower", {"relation": r, "year_rel": yr_list})
        if not d3.empty:
            if d3["value"].mode()[0] != 0:
                rel_pot.append(r)

    # potential and accounting relations will be accounted at the year level
    rel_year2 = [x for x in rel_pot if x not in rel_year]
    rel_year3 = [x for x in set(account.keys()) if x not in rel_year]

    # here we exclude all relations that can be with time index but not urgent
    rel_year4 = [x for x in rel_time if x[0].isupper()]

    # min/max and 'res_marg' at the 'year' level
    rel_year5 = [
        x
        for x in rel_time
        if any(
            [y in x for y in ["min", "max", "lim", "unabated", "weight", "res_marg"]]
        )
    ]

    # final set of year and time relations
    rel_yr = sorted(rel_year + rel_year2 + rel_year3 + rel_year4 + rel_year5)
    rel_ti = [x for x in rel_time if x not in rel_yr]

    # 1) adding relation for year level
    df = sc.par("relation_activity", {"relation": rel_yr, "year_act": yr_list})

    # Not adding 'time' for relations at 'year' (except 'res_marg')
    df_yr = df.loc[~df["relation"].isin(rel_yr_ti)].copy()
    df_yr = df_yr.append(
        df.loc[(df["relation"].isin(rel_yr_ti)) & (~df["technology"].isin(tec_list))],
        ignore_index=True,
    )
    df_yr.loc[:, "time"] = "year"
    sc.add_par("relation_activity_time", df_yr)
    print('- Relation activity for "year" related relations added.')

    # adding relation for time-related technologies in year level
    df_ti = df.loc[
        (df["relation"].isin(rel_yr_ti)) & (df["technology"].isin(tec_list))
    ].copy()

    for ti in times:
        df_ti.loc[:, "time"] = ti
        sc.add_par("relation_activity_time", df_ti)

    # 2) adding relations for time level
    df = sc.par("relation_activity", {"relation": rel_ti, "year_act": yr_list})
    for ti in times:
        df.loc[:, "time"] = ti
        sc.add_par("relation_activity_time", df)
    print("- Relation activity for relations at time slice level added.")

    for relname in ["relation_upper", "relation_lower"]:
        df = sc.par(relname)
        sc.remove_par(relname, df)
        df = df.loc[df["year_rel"].isin(yr_list)]
        df_yr = df.loc[df["relation"].isin(rel_yr)].copy()

        df_yr.loc[:, "time"] = "year"
        sc.add_par(relname + "_time", df_yr)

        df_ti = df.loc[df["relation"].isin(rel_ti)].copy()
        for ti in times:
            df_ti.loc[:, "time"] = ti
            sc.add_par(relname + "_time", df_ti)

    print("- Relation upper and lower updated for time slices.")

    # 3) Conversion of rel_total_capcaity to rel_activity for curtailment
    rel_curt = [x for x in rel_list if "curtailment" in x]
    df = sc.par("relation_total_capacity", {"relation": rel_curt})
    if not df.empty:
        sc.remove_par("relation_total_capacity", df)

        tecs = list(set(df["technology"]))
        df = df.loc[df["year_rel"].isin(yr_list)]
        df = df.set_index(["node_rel", "technology", "year_rel"]).sort_index()

        # first, dividing by capacity factor of the respective technologies
        cf = (
            sc.par("capacity_factor", {"technology": tecs})
            .groupby(["node_loc", "technology", "year_act"])
            .mean()
            .sort_index()
        )

        for i in set(df.index):
            df.loc[i, "value"] /= cf.loc[i, "value"]

        # then, converting to relation_activity
        df = df.reset_index()
        df["year_act"] = df["year_rel"]
        df["node_loc"] = df["node_rel"]
        df["mode"] = "M1"
        for ti in times:
            df.loc[:, "time"] = ti
            sc.add_par("relation_activity_time", df)
        print(
            "- Relation total capacity converted to activity for"
            " curtailment technologies."
        )

    # 4) Adding year equivalent relation for technologies at 'time' level
    print(
        "- Adding year-equivalent relations for technologies"
        " active at time slice level..."
    )
    rel_year_equivalent(sc, tec_list)

    # 5) Removing relation_activity for all
    sc.remove_par("relation_activity", sc.par("relation_activity"))
    sc.commit("relations updated")
    print("- Relations were successfully set up for time slices.")
    return rel_yr, rel_ti


# 1.12) Adding mapping parameters
def mapping_sets(sc, par_list):
    sc.check_out()
    for parname in par_list:
        setname = "is_" + parname

        # initiating the sets
        idx_s = sc.idx_sets(parname)
        idx_n = sc.idx_names(parname)
        try:
            sc.set(setname)
        except:
            sc.init_set(setname, idx_sets=idx_s, idx_names=idx_n)
            print("- Set {} was initiated.".format(setname))

        # emptying old data in sets
        df = sc.set(setname)
        sc.remove_set(setname, df)

        # adding data to the mapping sets
        df = sc.par(parname)
        if not df.empty:
            # for i in df.index:
            #     d = df.loc[i, :].copy().drop(['value', 'unit'])
            df = df.drop(["value", "unit"], axis=1)
            sc.add_set(setname, df)

            print('- Mapping sets updated for "{}"'.format(setname))
    sc.commit("")


# 1.13) Initialization of new parameters
def init_par_time(sc, par_list=[]):
    sc.check_out()
    if not par_list:
        par_list = [
            "emission_factor",
            "relation_activity",
            "relation_upper",
            "relation_lower",
        ]
    for parname in par_list:
        idx_s = sc.idx_sets(parname) + ["time"]
        idx_n = sc.idx_names(parname) + ["time"]
        try:
            sc.par(parname + "_time")
        except:
            sc.init_par(parname + "_time", idx_sets=idx_s, idx_names=idx_n)
            print("- Parameter {} was initiated.".format(parname + "_time"))
    for setname in ["is_relation_lower_time", "is_relation_upper_time"]:
        p = setname.split("is_")[1]
        idx_s = sc.idx_sets(p)
        idx_n = sc.idx_names(p)
        try:
            sc.set(setname)
        except:
            sc.init_set(setname, idx_sets=idx_s, idx_names=idx_n)
            print("- Set {} was initiated.".format(setname))

    sc.commit("")


# 1.14) Updating reserve margin
def update_res_marg(
    sc,
    path_xls,
    sc_ref=[],  # based on a reference scenario
    parname="relation_activity_time",
    reserve_margin=0.2,  # on top of peak load for reserves
    xls_sheet="peak_demand",
    nodes="all",
):
    # Notice: if based on sc_ref, double check if should reserve_margin = 0

    # 1) Loading Excel data (peak demand per region in GW)
    xls = pd.ExcelFile(path_xls)
    df = xls.parse("peak_demand").set_index("year")
    peak = df.loc["peak", :]
    peak_growth = df.loc[df.index != "peak"]

    resm_old = sc.par(parname, {"relation": "res_marg", "technology": "elec_t_d"})
    if sc_ref:
        resm_ref = sc_ref.par(
            "relation_activity", {"relation": "res_marg", "technology": "elec_t_d"}
        )
        dem_ref = sc_ref.par("demand", {"commodity": ["i_spec", "rc_spec"]})
        dem_ref = dem_ref.groupby(["node", "year", "time"]).sum()

    if nodes == "all":
        nodes = list(set(resm_old["node_loc"]))

    if "time" not in sc.idx_names(parname):
        resm_old["time"] = "year"
        if sc_ref:
            resm_ref["time"] = "year"
            resm_ref = resm_ref.set_index(["node_loc", "year_act", "time"]).sort_index()

    resm = resm_old.set_index(["node_loc", "year_act", "time"]).sort_index()

    # Total electricity demand
    dur = sc.par("duration_time")
    dem = sc.par("demand", {"commodity": ["i_spec", "rc_spec"]})
    dem = dem.groupby(["node", "year", "time"]).sum().reset_index()
    d = dem.pivot_table(values="value", columns="year", index=["node", "time"])
    dem_growth = d.copy()
    yr_first = sc.firstmodelyear
    dem_growth[yr_first] = 0
    for yr in [x for x in dem_growth.columns if x > yr_first]:
        dem_growth[yr] = (d[yr] - d[yr_first]) / d[yr_first]

    # Finding max demand based on duration time
    dem["actual"] = 0
    for t in set(sc.set("time")):
        dem.loc[dem["time"] == t, "actual"] = dem.loc[
            dem["time"] == t, "value"
        ] / float(dur.loc[dur["time"] == t, "value"])
    dem_max = dem.loc[dem.groupby(["node", "year"])["actual"].idxmax()].set_index(
        ["node", "year", "time"]
    )

    # udating res_marg only for timeslices that maximum demand (GW) happens
    resm = resm.loc[resm.index.isin(dem_max.index)]

    for i in resm.index:
        if sc_ref:
            ref_res = float(resm_ref.loc[(i[0], i[1]), "value"])
            ref_dem = float(dem_ref.loc[(i[0], i[1]), "value"])
            peak_dem = -ref_res * ref_dem
        else:
            growth_demand = 1 + dem_growth.loc[(i[0], i[2]), i[1]]
            growth_peak = 1 + peak_growth.loc[i[1], i[0]]
            peak_dem = float(peak[i[0]]) * growth_demand * growth_peak

        # duration = float(dur.loc[dur['time'] == i[2], 'value'])
        # max_dem = dem.loc[i, 'value'] / duration
        max_dem = dem_max.loc[i, "actual"]

        if (peak_dem / max_dem) < 1:
            new_res = -(1.0 + reserve_margin)
        else:
            new_res = -(peak_dem / max_dem) * (1.0 + reserve_margin)

        resm.loc[i, "value"] = new_res

    if "time" not in sc.idx_names(parname):
        resm = resm.reset_index().drop(["time"], axis=1)

    sc.check_out()
    sc.remove_par(parname, resm_old)
    sc.add_par(parname, resm.reset_index())
    sc.commit("res_marg updated")
    print("- Reserve margin updated based on the new peak demand data.")


# 1.15) Copying historical data to "ref" parameters
def historical_to_ref(sc, year_list=[], sc_ref=None, regions=all, remove_ref=True):
    if regions == all:
        regions = list(sc.set("node"))

    sc.check_out()

    if not year_list:
        year_list = [x for x in set(sc.set("year")) if x < sc.firstmodelyear]

    par_list = [
        "historical_activity",
        "historical_new_capacity",
        "historical_extraction",
    ]
    par_list_ref = [x.replace("historical", "ref") for x in par_list]

    # If no reference scenario is given, uses the same scenario
    if not sc_ref:
        sc_ref = sc

    # Remove the existing "ref" data
    if remove_ref:
        for parname in par_list_ref:
            node_col = [x for x in sc.par(parname).columns if "node" in x][0]
            year_col = [x for x in sc.par(parname).columns if "year" in x][0]
            df_ref = sc.par(parname, {node_col: regions, year_col: year_list})
            sc.remove_par(parname, df_ref)

    # Add new "ref" data
    for parname in par_list:
        node_col = [x for x in sc.par(parname).columns if "node" in x][0]
        year_col = [x for x in sc.par(parname).columns if "year" in x][0]
        df_par = sc_ref.par(parname, {node_col: regions, year_col: year_list})
        if not df_par.empty:
            sc.add_par(par_list_ref[par_list.index(parname)], df_par)

            print(
                '> The content of parameter "{}" was copied to "{}" to the scenario.'.format(
                    parname, par_list_ref[par_list.index(parname)]
                )
            )

    sc.commit(" ")


# %% 2) Input data and specifications
if __name__ == "__main__":
    import ixmp as ix
    import message_ix

    mp = ix.Platform(name="ene_ixmp", jvmargs=["-Xms2g", "-Xmx14g"])
    start = timer()
    # model, scenario, version, suffix of Excel file
    # Country model: 'MESSAGE_China', 'baseline', 7, '48'  (cleaned up, VRE new, unit, 2110)
    model_ref = "MESSAGEix_China"
    scenario_ref = "baseline"
    version_ref = None
    model_family = "CHN"
    n_time = 48  # number of time slices <= file ID
    file_id = "48"

    path_files = r"C:\Users\zakeri\Documents\Github\time_clustering"
    path_xls = path_files + "\\scenario work\\China\\data"
    xls_file = "input_data_" + file_id + "_" + model_family + ".xlsx"

    os.chdir(path_files)

    clone = True  # if True, clones a new scenario
    model_new = model_ref
    scenario_new = scenario_ref + "_t" + str(n_time)
    version_new = 1  # if clone False will be used

    interval = 50  # intervals for commiting to the DB (no impact on results)
    set_update = True  # if True, adds time slices and set adjustments
    last_year = None  # either int (year) or None (removes extra years)
    node_exlude = ["World"]  # nodes to be excluded from the timeslicing

    # 2) Adjustment related to general cleanup
    vre_cleanup = False  # do it if not yet, cleanup VRE and new implementation
    unit_correct = False  # if True, corrects the units
    cleaning = False  # cleanup of technologies and relations

    # 3) Adjustment related to cleanup before adding time slices
    remove_old_time_lvl = True  # if True, removes old temporal mapping
    remove_cool_tec = True  # if True, removes all cooling technologies
    remove_extra_vintage = False  # removes vintaging before 2020 (not safe)

    # 3) Adjustment after adding time slices
    growth_act_adjust = True  # if true, does some adjustments for growth rates

    # a relation to connect collectors and their input tecs (not needed)
    add_relation_collector = False

    # adding index of 'time' to parameters like rel_activity (removes old data)
    add_time_index = []  # ['emission_factor']       # a list of parameters or []

    # if True, finds relations for tecs at time slice level and addes times
    relation_modification = True

    # if True, the content of historical parameters will be copied to ref
    history_to_ref = False

    # if a list, removes those parameters like relation_activity # can be []
    par_old_removal = [  # 'relation_activity', 'relation_upper', 'relation_lower',
        # 'relation_cost',
    ]
    # updating reserve margin
    update_reserve_marg = True  # if True, based on Excel sheet: peak_demand
    # 2.1) Loading/clonning scenarios
    print("- Loading reference scenario and cloning...")
    sc_ref = message_ix.Scenario(
        mp, model=model_ref, scenario=scenario_ref, version=version_ref
    )
    if clone:
        sc = sc_ref.clone(model=model_new, scenario=scenario_new, keep_solution=False)
    else:
        sc = message_ix.Scenario(
            mp, model=model_new, scenario=scenario_new, version=version_new
        )
    # sc_ref = None
    version_cloned = sc.version

    if sc.has_solution():
        sc.remove_solution()

    # try:
    #     sc.par("storage_initial")
    # except:
    #     addNewParameter(sc, path_xls, remove_par=True)

    nodes = [x for x in sc.set("node") if x not in ["World"] + node_exlude]

    # 2.2) Loading Excel data (time series)
    xls = pd.ExcelFile("//".join([path_xls, xls_file]))

    # -----------------------------------------------------------------------------
    # 3) Updating sets related to time
    # Adding subannual time slices to the relevant sets
    duration, df_time, dict_xls = xls_to_df(xls, n_time, nodes)
    times = df_time["time"].tolist()

    if set_update:
        time_setup(sc, df_time, last_year)
        duration_time(sc, df_time)
        if last_year:
            df = sc.par("bound_activity_up")
            assert max(set(df["year_act"])) <= last_year

    # -----------------------------------------------------------------------------
    # 4) Modifying some parameters before adding sub-annual time slices
    # List of technologies that will have output to time slices
    # sectors/commodities represented at time slice level
    # Representaiton model (input= to, ta ... output: ta, td)

    print("- Identifying technologies to be represented at subannual level...")
    sectors = ["rc_spec", "i_spec", "i_therm", "rc_therm", "transport"]
    coms = ["electr", "d_heat"]

    tec_out = list(set(sc.par("output", {"commodity": coms + sectors})["technology"]))
    extra_com = ["Trans", "Itherm", "RCtherm", "back_"]
    tec_out = sorted([x for x in tec_out if not any([y in x for y in extra_com])])
    len(tec_out)

    # List of technologies that must have input from time slices
    tec_inp = sorted(set(sc.par("input", {"commodity": coms})["technology"]))
    len(tec_inp)

    # Excluding technologies with zero output from time slices
    df_rem_out = set(
        remove_data_zero(sc, "output", {"technology": tec_out})["technology"]
    )
    tec_out = sorted([x for x in tec_out if x not in df_rem_out])

    df_rem_in = set(
        remove_data_zero(sc, "input", {"technology": tec_inp})["technology"]
    )
    tec_inp = sorted([x for x in tec_inp if x not in df_rem_in])
    len(tec_inp)

    # technologies with output to 'useful' level (already timeslices in demand)
    use_tec = set(
        sc.par("output", {"commodity": sectors, "level": "useful"})["technology"]
    )

    # Technologies with only input needed at timeslice (output: 'year'/nothing)
    # representaiton mode (to='1', ta='1' ... ta='1', td='year')
    tec_only_inp = [x for x in tec_inp if x not in tec_out and x not in use_tec]
    len(tec_only_inp)

    # All technologies relevant to time slices
    tec_list = tec_out + tec_inp

    if remove_cool_tec:
        tec_list = remove_cooling(sc, tec_list)
    tec_list = list(set(tec_list))

    if remove_extra_vintage:
        extra_y = [int(y) for y in sc.set("year") if int(y) < sc.firstmodelyear]
        for parname in [
            p
            for p in sc.par_list()
            if len([y for y in set(sc.idx_names(p)) if "year" in y]) == 2
            and "year_act" in set(sc.idx_names(p))
        ]:
            df_rem = sc.par(parname, {"year_act": extra_y})
            sc.remove_par(parname, df_rem)

    # first initialization of parameters with time slice
    init_par_time(sc)

    # second, converting and updating relation parameters
    if relation_modification:
        relation_tec(sc, tec_list, times)

    # finding relevant parameters
    par_list = [
        x
        for x in sc.par_list()
        if "time" in sc.idx_sets(x) and "technology" in sc.idx_sets(x)
    ]

    # parameters that should get the same value as 'year'
    par_equal = [
        "input",
        "output",
        "capacity_factor",
        "var_cost",
        # 'growth_activity_up', 'growth_activity_lo',
        # 'level_cost_activity_soft_up',
        # 'level_cost_activity_soft_lo',
        # 'soft_activity_lo', 'soft_activity_up',
    ]

    # parameters that should divide values of 'year' between time slices
    par_divide = [  # 'demand'          # demand will be treated from EXcel
        #  'historical_activity',
        # 'bound_activity_lo', 'bound_activity_up',
        # 'initial_activity_up', 'initial_activity_lo',
        # 'abs_cost_activity_soft_up', 'abs_cost_activity_soft_lo',
    ]

    # parameters that should be neglected for time slices (removed)
    par_ignore = []

    sc.check_out()
    par_update = {}  # a dictionary for updating parameters with time index
    for parname in par_list:
        if parname in par_equal:
            par_update[parname] = {"all": [1] * n_time}
        elif parname in par_divide:
            par_update[parname] = {"all": duration}
        elif parname in par_ignore:
            par_remove(sc, {parname: {"technology": tec_list}}, verb=False)
    sc.commit("")

    # Moving parameters with no 'time' index to the new ones with 'time'
    for parname in add_time_index:
        df = sc.par(parname)
        df_new = df.copy()
        df_new["time"] = "year"
        sc.remove_par(parname, df)
        sc.add_par(parname + "_time", df_new)
        par_update[parname + "_time"] = {"all": [1] * n_time}

    # Setting up the data and values of parameters
    par_update, index_cols = par_setup(sc, dict_xls, par_update)

    print("- Parameters are being modified for time slices...")
    for parname, data_dict in par_update.items():
        index_col = index_cols[parname]
        if index_col == "technology":
            item_list = tec_list
        else:
            item_list = None
        n1 = 0
        while n1 < n_time:
            nn = min([n_time, n1 + interval - 1])
            par_time_update(sc, parname, data_dict, index_col, item_list, n1, nn)
            print("    ", end="\r")
            print(str(nn + 1), end="\r")
            n1 = n1 + interval
        print('- Time slices added to "{}".'.format(parname))
    print("- All parameters sucessfully modified for time slices.")

    # -------------------------------------------------------------------------
    # 5) Doing some adjustments and solving
    sc.check_out()

    # Finding other commodity/levels that must be at the time slice level
    # 1) Correcting input of tec_inp technologies
    com_div = []
    for t in tec_inp:
        # technologies with electr input that have other input commodities too
        df = sc.par("input", {"technology": t})
        df = df.loc[~df["commodity"].isin(coms + sectors)].set_index(
            ["commodity", "level"]
        )
        com_div = com_div + list(set(df.index))
    com_div = set(com_div)
    print(
        "- Divider technologies being added for commodity/levels of: {}".format(com_div)
    )

    for x in com_div:
        tec = x[0] + "_divider"
        sc.add_set("technology", tec)

        yr_list = [int(x) for x in sc.set("year") if int(x) >= sc.firstmodelyear]
        df = sc.par(
            "input",
            {
                "commodity": x[0],
                "level": x[1],
                "time": "year",
                "year_act": yr_list,
                "year_vtg": yr_list,
            },
        )
        df = df.loc[(df["year_act"] == df["year_vtg"])].copy()

        df["technology"] = tec
        df = df.drop_duplicates()
        df["value"] = 1

        for ti in df_time["time"].tolist():
            df["time"] = ti
            sc.add_par("input", df)

            df2 = df.copy()
            df2 = df2.rename(
                {"time_origin": "time_dest", "node_origin": "node_dest"}, axis=1
            )
            df2["time_dest"] = ti
            sc.add_par("output", df2)

    # 2) Correcting output of tec_out technologies
    com_coll = []
    for t in tec_out:
        df = sc.par("output", {"technology": t})
        df = df.loc[~df["commodity"].isin(coms + sectors)].set_index(
            ["commodity", "level"]
        )
        com_coll = com_coll + list(set(df.index))
    com_coll = set(com_coll)
    print(
        "- Collector technologies being added for commodity/levels of: {}".format(
            com_coll
        )
    )

    for x in com_coll:
        tec = x[0] + "_collector"
        sc.add_set("technology", tec)

        df = sc.par(
            "output",
            {
                "commodity": x[0],
                "level": x[1],
                "time": times,
                "year_act": yr_list,
                "year_vtg": yr_list,
            },
        )
        t_list = list(set(df["technology"]))
        df = df.loc[(df["year_act"] == df["year_vtg"])].copy()

        df["technology"] = tec
        df = df.drop_duplicates()
        df["value"] = 1
        df["time_dest"] = "year"

        for ti in times:
            df["time"] = ti
            sc.add_par("output", df)

            # adding input of collector
            inp = df.copy()
            inp = inp.rename(
                {"time_dest": "time_origin", "node_dest": "node_origin"}, axis=1
            )
            inp["node_origin"] = inp["node_loc"]
            inp["time_origin"] = ti
            sc.add_par("input", inp)

        # adding a relation for collectors
        if add_relation_collector:
            rel = x[0] + "_aggregation"
            sc.add_set("relation", rel)
            rac = df.copy()
            rac = rac.rename(
                {
                    "commodity": "relation",
                    "year_vtg": "year_rel",
                    "node_dest": "node_rel",
                },
                axis=1,
            ).drop(["time_dest", "level"], axis=1)
            rac["relation"] = rel
            rac["value"] = -1
            sc.add_par("relation_activity_time", rac)
            dlo = rac.copy().drop(
                ["node_loc", "year_act", "technology", "mode"], axis=1
            )
            dlo["value"] = 0
            sc.add_par("relation_lower_time", dlo)

            rac["value"] = 1
            for tec, ti in product(t_list, times):
                rac["technology"] = tec
                rac["time"] = ti
                sc.add_par("relation_activity_time", rac)

    if par_old_removal:
        for par in par_old_removal:
            par_remove(sc, {par: {}})

    # Saving tec_list as a tec_type
    for t in tec_list:
        sc.add_cat("technology", "time_slice", t)
    print(
        "- Technologies active at time slice level saved with a type_tec"
        ' of "time_slice" in this scenario.'
    )

    # Adjusting of dynamic act for some technologies
    # (if growth and initial parameters were at the "time" level)
    if growth_act_adjust:
        if "R11_WEU" in set(sc.set("node")):  # for t=12
            par_adjust(
                "relation_activity_time",
                {
                    "node_loc": "R11_WEU",
                    "relation": "UE_res_comm_th_biomass",
                    "technology": ["useful_res_comm_th"],
                },
                multiplier=0.70,
            )

        if "R11_CPA" in set(sc.set("node")):  # for t=24
            par_adjust(
                "bound_activity_up",
                {"node_loc": "R11_CPA", "technology": ["hydro_lc"], "year_act": 2020},
                multiplier=1.05,
            )

        if "R11_SAS" in set(sc.set("node")):
            df = sc.par(
                "bound_new_capacity_up",
                {
                    "node_loc": "R11_SAS",
                    "technology": "hydro_lc",
                    "year_vtg": [
                        x for x in set(sc.set("year")) if x >= sc.firstmodelyear
                    ],
                },
            )
            sc.remove_par("bound_new_capacity_up", df)
            df["value"] = 41.7
            df = df.rename({"year_vtg": "year_act"}, axis=1)
            sc.add_par("bound_total_capacity_up", df)

    # TODO MESSAGE bug: changing capacity factor of zero
    tecs = ["wind_ppl", "wind_ppf", "solar_pv_ppl", "hydro_lc", "hydro_hc"]
    df = sc.par("capacity_factor", {"technology": tecs, "time": times})
    df = df.loc[df["value"] == 0].copy()
    df["value"] = 0.000001
    sc.add_par("capacity_factor", df)

    sc.commit("")

    # Adjustment of reserve margins (in relation_activity_time)
    if update_reserve_marg:
        update_res_marg(sc, "//".join([path_xls, xls_file]))

    # copying historical activity to ref_activity
    if history_to_ref:
        historical_to_ref(sc)

    # At the end, updating mapping sets based on new relation_act_time
    mapping_sets(sc, par_list=["relation_upper_time", "relation_lower_time"])
    end = timer()

    print(
        "Elapsed time for adding time slices and modifying scenario:",
        int((end - start) / 60),
        "min and",
        round((end - start) % 60, 2),
        "sec.",
    )

    casename = sc.model + "__" + sc.scenario + "__v" + str(sc.version)
    print(
        'Solving scenario "{}", started at {}, please wait...!'.format(
            casename, datetime.now().strftime("%H:%M:%S")
        )
    )
    start = timer()
    sc.solve(case=casename, solve_options={"lpmethod": "4"})
    end = timer()
    print(
        "Elapsed time solving scenario:",
        int((end - start) / 60),
        "min and",
        round((end - start) % 60, 2),
        "sec.",
    )

    # %% Some specific adjustments for some scenarios
    # 1) Correcting historical activity of some technologies from Excel
    parname = "historical_activity"
    xls = pd.ExcelFile("//".join([path_xls, xls_file]))
    df_xls = xls.parse(parname, converters={"time": str})
    df_xls = df_xls.set_index(df_xls.columns[0])

    sc.check_out()
    for tec in df_xls.columns:
        df = sc.par(parname, {"technology": tec})
        df_yr = df.copy().groupby(["node_loc", "year_act"]).sum()
        df_yr = df_yr.loc[df_yr["value"] > 0]

        df = df.set_index(["node_loc", "year_act", "time"])
        for i in df_yr.index:
            for ti in df_time["time"].tolist():
                df.loc[(i[0], i[1], ti), "value"] = (
                    df_xls.loc[ti, tec] * df_yr.loc[i, "value"]
                )
        sc.add_par(parname, df.reset_index())

    # 2) Removing initial_act_up and _lo for time slices
    sc.check_out()
    par_list = [
        "initial_activity_up",
        "initial_activity_lo",
        "growth_activity_up",
        "growth_activity_lo",
    ]
    for parname in par_list:
        df = sc.par(parname)
        df = df.loc[df["time"] != "year"]
        sc.remove_par(parname, df)
    sc.commit("")

    # 3) Copying relation parameters back
    for p in par_old_removal:
        df = sc_ref.par(p)
        df = df.loc[df["relation"].isin(set(sc.set("relation")))]
        if "technology" in df.columns:
            df = df.loc[df["technology"].isin(set(sc.set("technology")))]
        sc.add_par(p, df)

    # 4) testing the balance of rc_therm
    ti = "1"
    dur = float(sc.par("duration_time", {"time": ti})["value"])
    for com, lvl in com_coll:
        tec = com + "_collector"
        tecs = list(
            set(sc.par("output", {"commodity": com, "level": lvl})["technology"])
        )
        df = sc.var("ACT", {"technology": tecs, "year_act": yr_list})
        coll = df.loc[df["technology"] == tec].groupby(["year_act"]).sum()
        if not coll.empty:
            rest = df.loc[df["technology"] != tec].groupby(["year_act"]).sum()
            ratio = (rest[rest["lvl"] > 0] / coll[coll["lvl"] > 0])["lvl"]
            print(
                "- Results for commodity {} at level {} is {}.".format(com, lvl, ratio)
            )
    # df=sc.par('input',{'technology':'rc_therm_collector'})
