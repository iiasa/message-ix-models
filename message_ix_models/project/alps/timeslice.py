# -*- coding: utf-8 -*-
"""
Subannual timeslice module for MESSAGEix models.

This module provides functionality to add subannual time slices to MESSAGEix scenarios,
enabling representation of seasonal and monthly variations in energy and water systems.
"""

import logging
from itertools import product

import pandas as pd

from message_ix_models import Context

log = logging.getLogger(__name__)


def map_months_to_timeslices(months, timeslice_months: list[set]) -> list:
    """Map month numbers to timeslice names.

    Parameters
    ----------
    months : array-like
        Month numbers (1-12)
    timeslice_months : list of set
        Months belonging to each timeslice, e.g. [{1,6,7,8,9,10,11,12}, {2,3,4,5}]

    Returns
    -------
    list of str
        Timeslice names (h1, h2, ...)
    """
    month_to_slice = {}
    for i, ts_months in enumerate(timeslice_months):
        for m in ts_months:
            month_to_slice[m] = f"h{i+1}"
    return [month_to_slice[m] for m in months]


def generate_timeslices(n_time, split=None):
    """Generate timeslice structure with specified durations.

    Parameters
    ----------
    n_time : int
        Number of timeslices to create
    split : list of int, optional
        Number of months per timeslice. Must sum to 12.
        If None, uses uniform split (12/n_time months each).
        Example: [10, 2] for 10-month h1, 2-month h2.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: time, lvl_temporal, parent_time, duration_time
    """
    times = [f"h{i + 1}" for i in range(n_time)]

    if split is None:
        durations = [1.0 / n_time] * n_time
    else:
        if len(split) != n_time:
            raise ValueError(f"split length {len(split)} != n_time {n_time}")
        if sum(split) != 12:
            raise ValueError(f"split must sum to 12, got {sum(split)}")
        durations = [s / 12 for s in split]

    df_time = pd.DataFrame(
        {
            "time": times,
            "lvl_temporal": ["subannual"] * n_time,
            "parent_time": ["year"] * n_time,
            "duration_time": durations,
        }
    )

    return df_time


def time_setup(sc, df, last_year=False, remove_old_time_lvl=False):
    """Add subannual time slices to scenario sets.

    Parameters
    ----------
    sc : message_ix.Scenario
        Scenario to modify
    df : pd.DataFrame
        Time step definitions
    last_year : int, optional
        Remove model years beyond this year
    remove_old_time_lvl : bool, optional
        Remove existing temporal hierarchy
    """
    with sc.transact("Time setup for subannual slices"):
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
    log.info("All sets modified for the new time slices.")


def duration_time(sc, df):
    """Update duration_time parameter for new timeslices.

    Parameters
    ----------
    sc : message_ix.Scenario
        Scenario to modify
    df : pd.DataFrame
        Time step definitions with duration_time column
    """
    with sc.transact("duration time modified"):
        df_ref = sc.par("duration_time")
        d = df_ref.copy()
        sc.remove_par("duration_time", df_ref)

        for i in df.index:
            d["time"] = df.loc[i, "time"]
            d["unit"] = "%"
            d["value"] = df.loc[i, "duration_time"]
            sc.add_par("duration_time", d)

        check = sc.par("duration_time")
        check.loc[0, "value"] = check.loc[0, "value"] + 1.00 - check["value"].sum()
        sc.add_par("duration_time", check.loc[[0]])
    log.info('Parameter "duration_time" updated for new values.')


def par_setup(sc, par_update):
    """Set up parameter data structure for timeslice addition.

    Parameters
    ----------
    sc : message_ix.Scenario
        Scenario to modify
    par_update : dict
        Dictionary to update with parameter configurations

    Returns
    -------
    index_cols : dict
        Index column names for each parameter
    """
    index_cols = {}
    for parname in par_update.keys():
        if "technology" in sc.idx_names(parname):
            index_cols[parname] = "technology"
        elif "commodity" in sc.idx_names(parname):
            index_cols[parname] = "commodity"
        elif "relation" in sc.idx_names(parname):
            index_cols[parname] = "relation"

    log.info("Parameter structure set up.")
    return index_cols


def par_time_update(
    sc,
    parname,
    data_dict,
    index_col,
    item_list,
    n1,
    nn,
    n_time,
    df_time,
    tec_inp,
    tec_only_inp,
    sc_ref=None,
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
    with sc.transact(f"Update {parname} for timeslices"):
        if not sc_ref:
            sc_ref = sc

        node_col = [
            x for x in sc.idx_names(parname) if x in ["node", "node_loc", "node_rel"]
        ]
        if node_col:
            node_col = node_col[0]
        else:
            node_col = None

        for key, ratio in data_dict.items():
            df = []

            if key == "all":
                if item_list:
                    df_ref = sc_ref.par(parname, {index_col: item_list, "time": "year"})
                else:
                    df_ref = sc_ref.par(parname, {"time": "year"})
            else:
                continue

            if not df_ref.empty:
                if nn >= n_time:
                    # Remove annual-level entries after timeslicing to avoid dual representation.
                    # MESSAGE GAMS constraints (ADDON_ACTIVITY_*, ACTIVITY_CONSTRAINT_*) now aggregate
                    # timesliced activity at annual level via map_temporal_hierarchy, making annual entries
                    # unnecessary and problematic (would cause triple-counting in commodity balance).
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
                            df_new.loc[
                                ~df_new["technology"].isin(tec_only_inp), col
                            ] = ti
                        else:
                            df_new[col] = ti
                    df_new["value"] *= ratio[df_time["time"].tolist().index(ti)]
                    df.append(df_new)

                df = pd.concat(df, ignore_index=True)
                sc.add_par(parname, df)


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

    with sc.transact("cooling removed"):
        for x in tec_cool:
            sc.remove_set("technology", x)
        for x in com_cool:
            sc.remove_set("commodity", x)
        for x in lvl_cool:
            if x in sc.set("level"):
                sc.remove_set("level", x)
    tec_list = [x for x in tec_list if x not in tec_cool]
    tec_rem = list(set([x.split("__")[0] for x in tec_cool]))
    log.info(
        f"Cooling technologies for {tec_rem}, and cooling commodities/levels removed."
    )
    return sorted(tec_list)


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
    with sc.transact("zero removed"):
        df = sc.par(parname, filter_dict)
        df = df.loc[df["value"] == value]
        sc.remove_par(parname, df)
    return df


def init_par_time(sc, par_list=[]):
    """Initialize parameters with time dimension.

    Parameters
    ----------
    sc : message_ix.Scenario
        Scenario to modify
    par_list : list, optional
        Parameters to initialize
    """
    with sc.transact("Initialize time parameters"):
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


def add_timeslices(
    scenario,
    context: Context = None,
    n_time: int = 12,
    remove_cooling_tec: bool = True,
    interval: int = 50,
):
    """Add subannual timeslices to a MESSAGEix scenario.

    This is the main entry point for adding timeslices to a scenario.
    Creates uniform timeslices programmatically without external data files.

    Parameters
    ----------
    scenario : message_ix.Scenario
        Scenario to modify
    context : Context, optional
        Model context (not currently used)
    n_time : int, optional
        Number of timeslices (can be any positive integer)
    remove_cooling_tec : bool, optional
        Remove cooling technologies before adding timeslices
    interval : int, optional
        Commit interval for large operations

    Returns
    -------
    message_ix.Scenario
        Modified scenario with timeslices
    """
    log.info(f"Adding {n_time} timeslices to scenario {scenario.scenario}")

    sc = scenario

    # Generate uniform timeslice structure
    df_time = generate_timeslices(n_time)
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
    df_rem_out = set(
        remove_data_zero(sc, "output", {"technology": tec_out})["technology"]
    )
    tec_out = sorted([x for x in tec_out if x not in df_rem_out])

    df_rem_in = set(
        remove_data_zero(sc, "input", {"technology": tec_inp})["technology"]
    )
    tec_inp = sorted([x for x in tec_inp if x not in df_rem_in])

    # Technologies with output to 'useful' level
    use_tec = set(
        sc.par("output", {"commodity": sectors, "level": "useful"})["technology"]
    )

    # Technologies with only input needed at timeslice
    tec_only_inp = [x for x in tec_inp if x not in tec_out and x not in use_tec]

    # All technologies relevant to time slices
    tec_list = list(set(tec_out + tec_inp))

    if remove_cooling_tec:
        tec_list = remove_cooling(sc, tec_list)

    # Initialize parameters with time dimension
    init_par_time(sc)

    log.info(f"Demand will be timesliced for commodities: {coms + sectors}")

    # Prepare parameter updates - demand added here
    par_equal = ["input", "output", "capacity_factor", "var_cost", "demand"]

    with sc.transact("Prepare parameter update structure"):
        par_update = {}
        for parname in par_equal:
            par_update[parname] = {"all": [1] * n_time}

    # Set up parameter data
    index_cols = par_setup(sc, par_update)

    # Add timeslices to parameters
    log.info("Adding timeslices to parameters...")
    for parname, data_dict in par_update.items():
        index_col = index_cols[parname]
        if index_col == "technology":
            item_list = tec_list
        elif index_col == "commodity" and parname == "demand":
            item_list = coms + sectors
        else:
            item_list = None
        n1 = 0
        while n1 < n_time:
            nn = min([n_time, n1 + interval - 1])
            par_time_update(
                sc,
                parname,
                data_dict,
                index_col,
                item_list,
                n1,
                nn,
                n_time,
                df_time,
                tec_inp,
                tec_only_inp,
            )
            n1 = n1 + interval
        log.info(f'Time slices added to "{parname}".')

    # Save tec_list as a tec_type
    with sc.transact("Subannual timeslices added"):
        for t in tec_list:
            sc.add_cat("technology", "time_slice", t)
        log.info(
            'Technologies active at time slice level saved with type_tec "time_slice".'
        )

        # Fix zero capacity factors for VRE
        tecs = ["wind_ppl", "wind_ppf", "solar_pv_ppl", "hydro_lc", "hydro_hc"]
        df = sc.par("capacity_factor", {"technology": tecs, "time": times})
        df = df.loc[df["value"] == 0].copy()
        if not df.empty:
            df["value"] = 0.000001
            sc.add_par("capacity_factor", df)
    log.info(f"Successfully added {n_time} timeslices to scenario.")

    return sc


def setup_timeslices(scenario, n_time: int, context):
    """Add basic time structure to scenario without modifying existing parameters.

    Creates time set elements, temporal hierarchy, and duration_time parameters
    for subannual resolution. Does not modify any existing technology parameters.

    Updates context.time to reflect the new timeslices. This is critical for
    water module to load monthly data instead of annual data.

    Parameters
    ----------
    scenario : message_ix.Scenario
        Scenario to modify
    n_time : int
        Number of timeslices to create
    context : Context
        Model context to update with new time structure

    Returns
    -------
    message_ix.Scenario
        Modified scenario with time structure
    """
    log.info(f"Setting up {n_time} timeslices (basic structure only)")

    # Generate uniform timeslice structure
    df_time = generate_timeslices(n_time)

    # Add time sets and hierarchy
    time_setup(scenario, df_time)

    # Add duration_time parameter
    duration_time(scenario, df_time)

    log.info(f"Basic time structure with {n_time} timeslices added successfully")

    # Update context.time to reflect new timeslices
    time_set = scenario.set("time")
    sub_time = list(time_set[time_set != "year"])
    context.time = sub_time
    log.info(f"Updated context.time to: {context.time}")

    return scenario


def add_electricity_router(
    scenario, n_time: int, commodity: str = "electr", router_name: str = "electr_router"
):
    """Add router technology to bridge annual electricity to subannual timeslices.

    Creates a pass-through technology that consumes electricity at annual level
    and outputs to each timeslice, enabling annual power plants to serve
    subannual demands.

    Parameters
    ----------
    scenario : message_ix.Scenario
        Scenario with timeslices already added via setup_timeslices()
    n_time : int
        Number of timeslices
    commodity : str, optional
        Commodity to route (default: 'electr')
    router_name : str, optional
        Name for router technology (default: 'electr_router')

    Returns
    -------
    message_ix.Scenario
        Modified scenario with router technology
    """
    log.info(f"Adding electricity router: {router_name} for {n_time} timeslices")

    # Get nodes, years, and time elements from scenario
    nodes = [x for x in scenario.set("node") if x not in ["World"]]
    years = scenario.set("year").tolist()

    # Get actual time elements from scenario (excluding 'year')
    time_set = scenario.set("time")
    times = [t for t in time_set if t != "year"]

    if len(times) != n_time:
        raise ValueError(
            f"Expected {n_time} timeslices in scenario, found {len(times)}: {times}"
        )

    with scenario.transact(f"Add {router_name} technology"):
        # Add router to technology set
        scenario.add_set("technology", router_name)

        # For each node
        for node in nodes:
            # Input: consume electricity at annual level
            input_data = pd.DataFrame(
                {
                    "node_loc": node,
                    "technology": router_name,
                    "year_vtg": years,
                    "year_act": years,
                    "mode": "M1",
                    "node_origin": node,
                    "commodity": commodity,
                    "level": "secondary",
                    "time": "year",
                    "time_origin": "year",
                    "value": 1.0,
                    "unit": "GWa",
                }
            )
            scenario.add_par("input", input_data)

            # Output: produce electricity to each timeslice
            for t in times:
                output_data = pd.DataFrame(
                    {
                        "node_loc": node,
                        "technology": router_name,
                        "year_vtg": years,
                        "year_act": years,
                        "mode": "M1",
                        "node_dest": node,
                        "commodity": commodity,
                        "level": "secondary",
                        "time": "year",
                        "time_dest": t,
                        "value": 1.0,
                        "unit": "GWa",
                    }
                )
                scenario.add_par("output", output_data)

            # Technical lifetime
            lifetime_data = pd.DataFrame(
                {
                    "node_loc": node,
                    "technology": router_name,
                    "year_vtg": years,
                    "value": 1,  # 1 year lifetime (instantly available)
                    "unit": "y",
                }
            )
            scenario.add_par("technical_lifetime", lifetime_data)

            # Capacity factor (always available)
            for t in times:
                cf_data = pd.DataFrame(
                    {
                        "node_loc": node,
                        "technology": router_name,
                        "year_vtg": years,
                        "year_act": years,
                        "time": t,
                        "value": 1.0,
                        "unit": "-",
                    }
                )
                scenario.add_par("capacity_factor", cf_data)

    log.info(f"Router technology {router_name} added successfully")
    return scenario
