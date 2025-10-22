"""Data and parameter generation for hydrogen technologies in MESSAGE-IX model.

This module provides functions to read, process, and generate parameter data
for hydrogen production, storage, and utilization technologies.
"""

from collections import defaultdict
from typing import TYPE_CHECKING

import pandas as pd
from message_ix import Scenario, make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.hydrogen.utils import (
    get_ssp_from_context,
    read_config,
    read_historical_data,
    read_rel,
    read_sector_data,
    read_timeseries,
)
from message_ix_models.util import (
    broadcast,
    merge_data,
    nodes_ex_world,
    same_node,
)

if TYPE_CHECKING:
    from message_ix_models.types import ParameterData


"""
What I want to do here:
read a bunch of csv files with techno-economic data for hydrogen technologies 
and add them to message_ix default workflow.
"""


def add_hydrogen_techs(scenario: Scenario):
    """
    this method simply calls the two methods from message_ix_models.model.hydrogen.utils
    """
    from message_ix_models.model.hydrogen.utils import (
        load_hydrogen_parameters,
        load_hydrogen_sets,
    )

    load_hydrogen_sets(scenario)
    load_hydrogen_parameters(scenario)


def read_data_hydrogen(
    scenario: "Scenario",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Read and clean hydrogen techno-economic data.

    Parameters
    ----------
    scenario
        Scenario instance to build hydrogen technologies on.

    Returns
    -------
    tuple of three pd.DataFrames
        Hydrogen data in three separate groups:
        - time independent parameters,
        - relation parameters,
        - time dependent parameters.
    """
    # Read the techno-economic data
    data_hydrogen = read_sector_data("hydrogen_techno_economic.csv")

    # Read relations/constraints data
    data_hydrogen_rel = read_rel("relations_hydrogen.csv")

    # Read time-series data
    data_hydrogen_ts = read_timeseries("timeseries_hydrogen.csv")

    return data_hydrogen, data_hydrogen_rel, data_hydrogen_ts


def gen_data_hydrogen_const(
    data: pd.DataFrame,
    config: dict,
    glb_reg: str,
    years: list[int],
    yv_ya: pd.DataFrame,
    nodes: list[str],
) -> dict[str, pd.DataFrame]:
    """Generate time-independent (constant) parameter data for hydrogen technologies.

    Parameters
    ----------
    data
        Constant parameter data from CSV file.
    config
        Technology configuration from set.yaml.
    glb_reg
        Global region identifier.
    years
        Model years.
    yv_ya
        Year vintage/active combinations.
    nodes
        Model nodes.

    Returns
    -------
    dict[str, pd.DataFrame]
        Key-value pairs of parameter names and parameter data.
    """
    results = defaultdict(list)

    # Get list of technologies to process
    technologies = [
        t.id if hasattr(t, "id") else t for t in config["technology"]["add"]
    ]
    # Also include required technologies
    technologies.extend(
        [
            t.id if hasattr(t, "id") else t
            for t in config["technology"].get("require", [])
        ]
    )

    for t in technologies:
        # Get data for this technology
        t_data = data[data["technology"] == t]

        if t_data.empty:
            continue

        # Get availability year for this technology
        av = (
            t_data["availability"].iloc[0]
            if "availability" in t_data.columns
            else min(years)
        )
        av = int(av) if pd.notna(av) else min(years)

        # Filter years and yv_ya based on availability
        t_years = [year for year in years if year >= av]
        t_yv_ya = yv_ya[yv_ya.year_vtg >= av]

        # Common parameters for make_df
        common = dict(
            year_vtg=t_yv_ya.year_vtg,
            year_act=t_yv_ya.year_act,
            time="year",
            time_origin="year",
            time_dest="year",
        )

        # Get unique parameters for this technology
        params = t_data["parameter"].unique()

        for par_full in params:
            # Split parameter name to get type and dimensions
            split = par_full.split("|")
            param_name = split[0]

            # Get the row(s) for this parameter
            par_data = t_data[t_data["parameter"] == par_full]

            # Get node_loc (use "default" if not specified)
            node_loc = (
                par_data["node_loc"].iloc[0]
                if "node_loc" in par_data.columns
                else "default"
            )
            node_loc = node_loc if pd.notna(node_loc) else "default"

            # Get value
            value = par_data["value"].iloc[0]

            # Get unit
            unit = par_data["unit"].iloc[0] if "unit" in par_data.columns else "t"
            unit = unit if pd.notna(unit) else "t"

            # Handle different parameter types
            if param_name in ["technical_lifetime", "construction_time"]:
                # Parameters without year dimensions
                df = make_df(
                    param_name,
                    technology=t,
                    value=value,
                    unit=unit,
                    node_loc=node_loc,
                )

            elif param_name == "capacity_factor":
                # Capacity factor has mode
                mode = par_data["mode"].iloc[0] if "mode" in par_data.columns else "M1"
                df = make_df(
                    param_name,
                    technology=t,
                    value=value,
                    unit=unit,
                    node_loc=node_loc,
                    mode=mode,
                    **{
                        k: v
                        for k, v in common.items()
                        if k not in ["time_origin", "time_dest"]
                    },
                )

            elif param_name in ["input", "output"]:
                # Input/output parameters have commodity, level, mode
                commodity = split[1] if len(split) > 1 else ""
                level = split[2] if len(split) > 2 else ""
                mode = par_data["mode"].iloc[0] if "mode" in par_data.columns else "M1"

                df = make_df(
                    param_name,
                    technology=t,
                    commodity=commodity,
                    level=level,
                    mode=mode,
                    value=value,
                    unit=unit,
                    node_loc=node_loc,
                    **common,
                )

            elif param_name == "emission_factor":
                # Emission factor has emission type and mode
                emission = (
                    par_data["emission"].iloc[0]
                    if "emission" in par_data.columns
                    else split[1] if len(split) > 1 else "CO2"
                )
                mode = par_data["mode"].iloc[0] if "mode" in par_data.columns else "M1"

                df = make_df(
                    param_name,
                    technology=t,
                    emission=emission,
                    mode=mode,
                    value=value,
                    unit=unit,
                    node_loc=node_loc,
                    **common,
                )

            else:
                # Generic parameter
                df = make_df(
                    param_name,
                    technology=t,
                    value=value,
                    unit=unit,
                    node_loc=node_loc,
                    **common,
                )

            # Broadcast to all nodes if node_loc is "default"
            if node_loc == "default":
                df["node_loc"] = None
                df = df.pipe(broadcast, node_loc=nodes)

            # Apply same_node for input/output parameters
            if param_name in ["input", "output"] and node_loc != glb_reg:
                df = df.pipe(same_node)

            results[param_name].append(df)

    return {par_name: pd.concat(dfs) for par_name, dfs in results.items() if dfs}


def gen_data_hydrogen_ts(
    data_ts: pd.DataFrame, nodes: list[str]
) -> dict[str, pd.DataFrame]:
    """Generate time-varying parameter data for hydrogen technologies.

    Parameters
    ----------
    data_ts
        Time-variable data from CSV file.
    nodes
        Regions of the model.

    Returns
    -------
    dict[str, pd.DataFrame]
        Key-value pairs of parameter names and parameter data.
    """
    results = defaultdict(list)

    # Get unique technologies
    technologies = data_ts["technology"].unique()

    common = dict(
        time="year",
        time_origin="year",
        time_dest="year",
    )

    for t in technologies:
        t_data = data_ts[data_ts["technology"] == t]

        # Get unique parameters for this technology
        params = t_data["parameter"].unique()

        for p in params:
            p_data = t_data[t_data["parameter"] == p]

            # Get values, years, modes, regions
            values = p_data["value"].values
            years = p_data["year"].values
            modes = (
                p_data["mode"].values
                if "mode" in p_data.columns
                else ["M1"] * len(values)
            )
            regions = (
                p_data["region"].values
                if "region" in p_data.columns
                else ["default"] * len(values)
            )
            units = p_data["unit"].iloc[0] if "unit" in p_data.columns else "t"

            # Create dataframe
            if p == "var_cost":
                df = make_df(
                    p,
                    technology=t,
                    value=values,
                    unit=units,
                    year_vtg=years,
                    year_act=years,
                    mode=modes,
                    **common,
                )

                # Broadcast to all regions if "default"
                if regions[0] == "default":
                    df = df.pipe(broadcast, node_loc=nodes)

            else:
                # Other time-varying parameters
                df = make_df(
                    p,
                    technology=t,
                    value=values,
                    unit=units,
                    year_vtg=years,
                    year_act=years,
                    mode=modes,
                    node_loc=regions,
                    **common,
                )

                # Broadcast to all regions if "default"
                if regions[0] == "default":
                    df["node_loc"] = None
                    df = df.pipe(broadcast, node_loc=nodes)

            results[p].append(df)

    return {par_name: pd.concat(dfs) for par_name, dfs in results.items() if dfs}


def gen_data_hydrogen_rel(
    data_rel: pd.DataFrame, years: list[int]
) -> dict[str, pd.DataFrame]:
    """Generate relation parameter data for hydrogen technologies.

    Parameters
    ----------
    data_rel
        Relation data from CSV file.
    years
        Model years.

    Returns
    -------
    dict[str, pd.DataFrame]
        Key-value pairs of relation parameter names and data.
    """
    results = defaultdict(list)

    # Get unique relations
    relations = data_rel["relation"].unique()

    for rel in relations:
        rel_data = data_rel[data_rel["relation"] == rel]

        # Get unique parameters for this relation
        params = rel_data["parameter"].unique()

        common_rel = dict(
            year_rel=years,
            year_act=years,
            mode="M1",
            relation=rel,
        )

        for par_name in params:
            par_data = rel_data[rel_data["parameter"] == par_name]

            if par_name == "relation_activity":
                # Has technology
                for _, row in par_data.iterrows():
                    tec = row["technology"]
                    reg = row["Region"]
                    val = row["value"]

                    df = make_df(
                        par_name,
                        technology=tec,
                        value=val,
                        unit="-",
                        node_loc=reg,
                        node_rel=reg,
                        **common_rel,
                    ).pipe(same_node)

                    results[par_name].append(df)

            elif par_name in ["relation_upper", "relation_lower"]:
                # No technology, just bounds
                for _, row in par_data.iterrows():
                    reg = row["Region"]
                    val = row["value"]

                    df = make_df(
                        par_name,
                        value=val,
                        unit="-",
                        node_rel=reg,
                        **common_rel,
                    )

                    results[par_name].append(df)

    return {par_name: pd.concat(dfs) for par_name, dfs in results.items() if dfs}


def gen_historical_data(s_info: ScenarioInfo) -> dict[str, pd.DataFrame]:
    """Generate historical calibration data for hydrogen technologies.

    Parameters
    ----------
    s_info
        Scenario information object.

    Returns
    -------
    dict[str, pd.DataFrame]
        Dict with historical_activity, historical_new_capacity, and bounds.
    """
    # Read historical data
    hist_data = read_historical_data("historical_data.csv")

    results = defaultdict(list)

    # Group by parameter
    for par_name in hist_data["parameter"].unique():
        par_data = hist_data[hist_data["parameter"] == par_name]

        # Create dataframe using make_df
        df = make_df(par_name, **par_data)

        results[par_name].append(df)

    return {par_name: pd.concat(dfs) for par_name, dfs in results.items() if dfs}


def integrate_costs_tool(
    scenario: "Scenario", technologies: list[str], ssp: str, local_data: dict
) -> dict[str, pd.DataFrame]:
    """Get inv_cost and fix_cost from tools.costs and merge with local data.

    This function uses the MESSAGE-IX cost projection tool and allows
    local CSV data to override specific values.

    Parameters
    ----------
    scenario
        Scenario instance.
    technologies
        List of technology names to get costs for.
    ssp
        Shared Socioeconomic Pathway.
    local_data
        Local data dictionary that may contain inv_cost and fix_cost.

    Returns
    -------
    dict[str, pd.DataFrame]
        Dictionary with inv_cost and fix_cost DataFrames.
    """
    # TODO: Implement cost tool integration
    # For now, just return the local data if available
    # In future, this will call the cost projection tool and merge results

    results = {}

    # Placeholder: In real implementation, would call:
    # from message_ix_models.tools.costs import create_cost_projections
    # cost_data = create_cost_projections(...)

    # For now, use local data if available
    if "inv_cost" in local_data and not local_data["inv_cost"].empty:
        results["inv_cost"] = local_data["inv_cost"]

    if "fix_cost" in local_data and not local_data["fix_cost"].empty:
        results["fix_cost"] = local_data["fix_cost"]

    return results


def gen_data_hydrogen(scenario: "Scenario", dry_run: bool = False) -> "ParameterData":
    """Generate all MESSAGEix parameter data for hydrogen technologies.

    Main entry point for hydrogen data generation. This orchestrates all
    data reading, processing, and generation steps.

    Parameters
    ----------
    scenario
        Scenario instance to build hydrogen model on.
    dry_run
        If True, do not perform any file writing or scenario modification.

    Returns
    -------
    dict[str, pd.DataFrame]
        Dictionary with MESSAGEix parameters as keys and parametrization as values.
    """
    # Load configuration
    context = read_config()
    # Unwrap nested hydrogen config: context["hydrogen"] = {'hydrogen': {...}}
    hydrogen_outer = context["hydrogen"]
    config = (
        hydrogen_outer["hydrogen"] if "hydrogen" in hydrogen_outer else hydrogen_outer
    )
    ssp = get_ssp_from_context(context)

    # Get scenario information
    s_info = ScenarioInfo(scenario)
    modelyears = s_info.Y
    yv_ya = s_info.yv_ya
    nodes = nodes_ex_world(s_info.N)
    global_region = [i for i in s_info.N if i.endswith("_GLB")][0]

    # Read all data files
    data_hydrogen, data_hydrogen_rel, data_hydrogen_ts = read_data_hydrogen(scenario)

    # Generate parameter dictionaries
    const_dict = gen_data_hydrogen_const(
        data_hydrogen, config, global_region, modelyears, yv_ya, nodes
    )

    ts_dict = gen_data_hydrogen_ts(data_hydrogen_ts, nodes)

    rel_dict = gen_data_hydrogen_rel(data_hydrogen_rel, modelyears)

    hist_dict = gen_historical_data(s_info)

    # Integrate costs from tools.costs
    technologies = [
        t.id if hasattr(t, "id") else t for t in config["technology"]["add"]
    ]
    cost_dict = integrate_costs_tool(scenario, technologies, ssp, const_dict)

    # Merge all data
    results_hydrogen: dict[str, pd.DataFrame] = {}
    merge_data(
        results_hydrogen,
        const_dict,
        ts_dict,
        rel_dict,
        hist_dict,
        cost_dict,
    )

    # Drop duplicates and filter by lifetime
    reduced_pdict = {}
    for k, v in results_hydrogen.items():
        if set(["year_act", "year_vtg"]).issubset(v.columns):
            # Filter to reasonable lifetime (e.g., 60 years max)
            v = v[(v["year_act"] - v["year_vtg"]) <= 60]
        reduced_pdict[k] = v.drop_duplicates().copy(deep=True)

    return reduced_pdict
