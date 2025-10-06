# -*- coding: utf-8 -*-
"""
Subannual timeslice module for MESSAGEix models.

This module provides functionality to add subannual time slices to MESSAGEix scenarios,
enabling representation of seasonal and monthly variations in energy and water systems.

The implementation is based on the original add_timeslice.py script, refactored for
modular use within the message_ix_models framework.
"""

import logging
from itertools import product
from pathlib import Path

import pandas as pd

from message_ix_models import Context

log = logging.getLogger(__name__)


# 1.1) A function for reading input data from excel and formatting as needed
def xls_to_df(xls, n_time, nodes, exclude_sheets=["peak_demand"]):
    """Read and format timeslice data from Excel file.

    Parameters
    ----------
    xls : pd.ExcelFile
        Excel file containing timeslice data
    n_time : int
        Number of time slices to create
    nodes : list
        List of node names
    exclude_sheets : list, optional
        Sheet names to exclude from processing

    Returns
    -------
    duration : list
        Duration of each timeslice as fraction of year
    df_time : pd.DataFrame
        Time step definitions
    dict_xls : dict
        Dictionary of parameter data by sheet name
    """
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
    """Add subannual time slices to scenario sets.

    Parameters
    ----------
    sc : message_ix.Scenario
        Scenario to modify
    df : pd.DataFrame
        Time step definitions from xls_to_df
    last_year : int, optional
        Remove model years beyond this year
    remove_old_time_lvl : bool, optional
        Remove existing temporal hierarchy
    """
    sc.check_out()
    # Changing the last year if needed
    if last_year:
        log.info(f"Removing extra model years beyond {last_year}...")
        for y in [x for x in sc.set("year").tolist() if int(x) > last_year]:
            sc.remove_set("year", y)
        log.info(f"Extra model years removed.")

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
        sc.add_set("map_temporal_hierarchy", d)

    if remove_old_time_lvl:
        sc.remove_set("map_temporal_hierarchy", df_ref)
    sc.commit("")
    log.info("All sets modified for the new time slices.")


# 1.3) A function for modifying duration time
def duration_time(sc, df):
    """Update duration_time parameter for new timeslices.

    Parameters
    ----------
    sc : message_ix.Scenario
        Scenario to modify
    df : pd.DataFrame
        Time step definitions with duration_time column
    """
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
    log.info('Parameter "duration_time" updated for new values.')


# 1.4) Setting up for parameters
def par_setup(sc, dict_xls, par_update):
    """Set up parameter data structure for timeslice addition.

    Parameters
    ----------
    sc : message_ix.Scenario
        Scenario to modify
    dict_xls : dict
        Parameter data from Excel
    par_update : dict
        Dictionary to update with parameter configurations

    Returns
    -------
    par_update : dict
        Updated parameter configurations
    index_cols : dict
        Index column names for each parameter
    """
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
    log.info("Input data of parameters are set up.")
    return par_update, index_cols


# 1.5) The main function for adding timeslices to a scenario
def par_time_update(
    sc, parname, data_dict, index_col, item_list, n1, nn, n_time, df_time,
    tec_inp, tec_only_inp, sc_ref=None
):
    """Add timeslice dimension to parameters.

    Parameters
    ----------
    sc : message_ix.Scenario
        Scenario to modify
    parname : str
        Parameter name
    data_dict : dict
        Data ratios for timeslices
    index_col : str
        Index column name (technology, commodity, or relation)
    item_list : list
        List of items to update
    n1, nn : int
        Range of timeslices to process
    n_time : int
        Total number of timeslices
    df_time : pd.DataFrame
        Time definitions
    tec_inp : list
        Technologies with input at timeslice level
    tec_only_inp : list
        Technologies with only input at timeslice level
    sc_ref : message_ix.Scenario, optional
        Reference scenario
    """
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


# 1.6) Removing cooling technologies from a list
def remove_cooling(sc, tec_list):
    """Remove cooling technologies from scenario.

    Parameters
    ----------
    sc : message_ix.Scenario
        Scenario to modify
    tec_list : list
        List of technologies

    Returns
    -------
    tec_list : list
        Updated technology list without cooling technologies
    """
    log.info("Removing cooling technologies...")
    # Both commodity and technology must be removed
    cool_terms = ["__air", "__cl_fresh", "__ot_fresh", "__ot_saline"]
    tec_cool = [
        x
        for x in set(sc.set("technology"))
        if x in [y + z for (y, z) in product(tec_list, cool_terms)]
    ]
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
    log.info(f"Cooling technologies for {tec_rem}, and cooling commodities/levels removed.")
    sc.commit("cooling removed")
    return sorted(tec_list)


# 1.7) Removing data from a parameter if value is zero
def remove_data_zero(sc, parname, filter_dict, value=0):
    """Remove parameter data where value equals specified value.

    Parameters
    ----------
    sc : message_ix.Scenario
        Scenario to modify
    parname : str
        Parameter name
    filter_dict : dict
        Filters for parameter query
    value : float, optional
        Value to match for removal

    Returns
    -------
    df : pd.DataFrame
        Removed data
    """
    sc.check_out()
    df = sc.par(parname, filter_dict)
    df = df.loc[df["value"] == value]
    sc.remove_par(parname, df)
    sc.commit("zero removed")
    return df


# 1.8) Initialization of new parameters
def init_par_time(sc, par_list=[]):
    """Initialize parameters with time dimension.

    Parameters
    ----------
    sc : message_ix.Scenario
        Scenario to modify
    par_list : list, optional
        Parameters to initialize
    """
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
            log.info(f"Parameter {parname}_time initialized.")
    for setname in ["is_relation_lower_time", "is_relation_upper_time"]:
        p = setname.split("is_")[1]
        idx_s = sc.idx_sets(p)
        idx_n = sc.idx_names(p)
        try:
            sc.set(setname)
        except:
            sc.init_set(setname, idx_sets=idx_s, idx_names=idx_n)
            log.info(f"Set {setname} initialized.")

    sc.commit("")


# 1.9) Adding year-equivalent of technologies at the timeslice level
def rel_year_equivalent(
    sc,
    tec_list,
    rel_ref="res_marg",
    tec_ref="elec_t_d",
    rel_bound="CH4_Emission",
    remove_existing=False,
):
    """Add year-equivalent relations for timeslice technologies.

    Parameters
    ----------
    sc : message_ix.Scenario
        Scenario to modify
    tec_list : list
        Technologies for which to add year-equivalent relations
    rel_ref : str, optional
        Reference relation to copy structure from
    tec_ref : str, optional
        Reference technology
    rel_bound : str, optional
        Relation for bounds
    remove_existing : bool, optional
        Remove existing relations before adding
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


def add_timeslices(
    scenario,
    context: Context,
    n_time: int = 12,
    excel_path: str = None,
    regions: str = None,
    remove_cooling_tec: bool = True,
    update_reserve_margin: bool = False,
    interval: int = 50,
):
    """Add subannual timeslices to a MESSAGEix scenario.

    This is the main entry point for adding timeslices to a scenario.

    Parameters
    ----------
    scenario : message_ix.Scenario
        Scenario to modify
    context : Context
        Model context
    n_time : int, optional
        Number of timeslices (default: 12 for monthly)
    excel_path : str, optional
        Path to Excel file with timeslice data
    regions : str, optional
        Region specification (e.g., 'R12')
    remove_cooling_tec : bool, optional
        Remove cooling technologies before adding timeslices
    update_reserve_margin : bool, optional
        Update reserve margin after adding timeslices
    interval : int, optional
        Commit interval for large operations

    Returns
    -------
    message_ix.Scenario
        Modified scenario with timeslices
    """
    log.info(f"Adding {n_time} timeslices to scenario {scenario.scenario}")

    sc = scenario

    # Determine regions from scenario if not provided
    if not regions:
        regions = context.regions if hasattr(context, 'regions') else 'R12'

    # Determine Excel path
    if not excel_path:
        from message_ix_models.util import package_data_path
        excel_path = package_data_path(
            "project", "alps", "data", "timeslice_data",
            f"input_data_{n_time}_{regions}.xlsx"
        )

    log.info(f"Loading timeslice data from {excel_path}")
    xls = pd.ExcelFile(excel_path)

    # Get nodes from scenario
    nodes = [x for x in sc.set("node") if x not in ["World"]]

    # Load and process timeslice data
    duration, df_time, dict_xls = xls_to_df(xls, n_time, nodes)
    times = df_time["time"].tolist()

    # Update sets related to time
    time_setup(sc, df_time)
    duration_time(sc, df_time)

    # Identify technologies to be represented at subannual level
    log.info("Identifying technologies for subannual representation...")
    sectors = ["rc_spec", "i_spec", "i_therm", "rc_therm", "transport"]
    coms = ["electr", "d_heat"]

    tec_out = list(set(sc.par("output", {"commodity": coms + sectors})["technology"]))
    extra_com = ["Trans", "Itherm", "RCtherm", "back_"]
    tec_out = sorted([x for x in tec_out if not any([y in x for y in extra_com])])

    tec_inp = sorted(set(sc.par("input", {"commodity": coms})["technology"]))

    # Exclude technologies with zero output
    df_rem_out = set(remove_data_zero(sc, "output", {"technology": tec_out})["technology"])
    tec_out = sorted([x for x in tec_out if x not in df_rem_out])

    df_rem_in = set(remove_data_zero(sc, "input", {"technology": tec_inp})["technology"])
    tec_inp = sorted([x for x in tec_inp if x not in df_rem_in])

    # Technologies with output to 'useful' level
    use_tec = set(sc.par("output", {"commodity": sectors, "level": "useful"})["technology"])

    # Technologies with only input needed at timeslice
    tec_only_inp = [x for x in tec_inp if x not in tec_out and x not in use_tec]

    # All technologies relevant to time slices
    tec_list = list(set(tec_out + tec_inp))

    if remove_cooling_tec:
        tec_list = remove_cooling(sc, tec_list)

    # Initialize parameters with time dimension
    init_par_time(sc)

    # Prepare parameter updates
    par_equal = ["input", "output", "capacity_factor", "var_cost"]

    sc.check_out()
    par_update = {}
    for parname in par_equal:
        par_update[parname] = {"all": [1] * n_time}
    sc.commit("")

    # Set up parameter data
    par_update, index_cols = par_setup(sc, dict_xls, par_update)

    # Add timeslices to parameters
    log.info("Adding timeslices to parameters...")
    for parname, data_dict in par_update.items():
        index_col = index_cols[parname]
        if index_col == "technology":
            item_list = tec_list
        else:
            item_list = None
        n1 = 0
        while n1 < n_time:
            nn = min([n_time, n1 + interval - 1])
            par_time_update(
                sc, parname, data_dict, index_col, item_list, n1, nn,
                n_time, df_time, tec_inp, tec_only_inp
            )
            n1 = n1 + interval
        log.info(f'Time slices added to "{parname}".')

    # Save tec_list as a tec_type
    for t in tec_list:
        sc.add_cat("technology", "time_slice", t)
    log.info('Technologies active at time slice level saved with type_tec "time_slice".')

    # Fix zero capacity factors for VRE
    tecs = ["wind_ppl", "wind_ppf", "solar_pv_ppl", "hydro_lc", "hydro_hc"]
    df = sc.par("capacity_factor", {"technology": tecs, "time": times})
    df = df.loc[df["value"] == 0].copy()
    if not df.empty:
        df["value"] = 0.000001
        sc.add_par("capacity_factor", df)

    sc.commit("Subannual timeslices added")
    log.info(f"Successfully added {n_time} timeslices to scenario.")

    return sc
