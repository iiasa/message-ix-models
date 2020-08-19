from collections import defaultdict
import logging
import message_ix
import ixmp
import numpy as np
import pandas as pd
from message_ix import make_df
from message_ix_models import ScenarioInfo
from message_ix_models.util import broadcast, same_node

from .util import read_config

import re


log = logging.getLogger(__name__)

def read_data():
    """Read and clean data from :file:`n-fertilizer_techno-economic.xlsx`."""
    # Ensure config is loaded, get the context
    context = read_config()

    # Shorter access to sets configuration
    sets = context["material"]["set"]

    # Read the file
    data = pd.read_excel(
        context.get_path("material", "n-fertilizer_techno-economic.xlsx"),
        sheet_name="Sheet1",
    )

    # Prepare contents for the "parameter" and "technology" columns
    # FIXME put these in the file itself to avoid ambiguity/error

    # "Variable" column contains different values selected to match each of
    # these parameters, per technology
    params = [
        "inv_cost",
        "fix_cost",
        "var_cost",
        "technical_lifetime",
        "input_fuel",
        "input_elec",
        "input_water",
        "output_NH3",
        "output_water",
        "output_heat",
        "emissions",
        "capacity_factor",
    ]

    param_values = []
    tech_values = []
    for t in sets["technology"]["add"]:
        param_values.extend(params)
        tech_values.extend([t.id] * len(params))

    # Clean the data
    data = (
        # Insert "technology" and "parameter" columns
        data.assign(technology=tech_values, parameter=param_values)
        # Drop columns that don't contain useful information
        .drop(["Model", "Scenario", "Region"], axis=1)
        # Set the data frame index for selection
        .set_index(["parameter", "technology"])
    )

    # TODO convert units for some parameters, per LoadParams.py

    return data

def read_data_aluminum():
    """Read and clean data from :file:`aluminum_techno_economic.xlsx`."""

    # Ensure config is loaded, get the context
    context = read_config()
    # Shorter access to sets configuration
    sets = context["material"]["aluminum"]

    # Read the file
    data_aluminum = pd.read_excel(
        context.get_path("material", "aluminum_techno_economic.xlsx"),
        sheet_name="aluminum",
    )

    # Clean the data

    data_aluminum= data_aluminum.drop(['Region', 'Source', 'Description'], axis = 1)

    # Unit conversion

    # At the moment this is done in the excel file, can be also done here
    # To make sure we use the same units

    return data_aluminum

def read_data_generic():
    """Read and clean data from :file:`generic_furnace_boiler_techno_economic.xlsx`."""

    # Ensure config is loaded, get the context
    context = read_config()
    # Shorter access to sets configuration
    sets = context["material"]["generic_set"]

    # Read the file
    data_generic = pd.read_excel(
        context.get_path("material", "generic_furnace_boiler_techno_economic.xlsx"),
        sheet_name="generic")

    # Clean the data
    # Drop columns that don't contain useful information

    data_generic= data_generic.drop(['Region', 'Source', 'Description'], axis = 1)

    # Unit conversion

    # At the moment this is done in the excel file, can be also done here
    # To make sure we use the same units

    return data_generic


# TODO: Adding the active years to the tables
# TODO: If there are differnet values for the years.
def gen_data_aluminum(scenario, dry_run=False):
    """Generate data for materials representation of aluminum."""
    # Load configuration

    config = read_config()["material"]["aluminum"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    data_aluminum = read_data_aluminum()

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # Iterate over technologies

    for t in config["technology"]["add"]:

        # Obtain the active and vintage years
        av = data_aluminum.loc[(data_aluminum["technology"] == t),'availability']\
        .values[0]

        # For the technologies with lifetime

        if "technical_lifetime" in data_aluminum.loc[(data_aluminum["technology"] \
        == t)]["parameter"].values:
            lifetime = data_aluminum.loc[(data_aluminum["technology"] == t) & \
            (data_aluminum["parameter"]== "technical_lifetime"),'value'].values[0]
            years_df = scenario.vintage_and_active_years()
            years_df = years_df.loc[years_df["year_vtg"]>= av]
            years_df_final = pd.DataFrame(columns=["year_vtg","year_act"])

        # For each vintage adjsut the active years according to technical lifetime
        for vtg in years_df["year_vtg"].unique():
            years_df_temp = years_df.loc[years_df["year_vtg"]== vtg]
            years_df_temp = years_df_temp.loc[years_df["year_act"]< vtg + lifetime]
            years_df_final = pd.concat([years_df_temp, years_df_final], ignore_index=True)

        vintage_years, act_years = years_df_final['year_vtg'], years_df_final['year_act']

        params = data_aluminum.loc[(data_aluminum["technology"] == t),\
        "parameter"].values.tolist()

        # Iterate over parameters
        for par in params:
            split = par.split("|")
            param_name = split[0]

            # Obtain the scalar value for the parameter

            val = data_aluminum.loc[((data_aluminum["technology"] == t) \
            & (data_aluminum["parameter"] == par)),'value'].values[0]

            common = dict(
            year_vtg= vintage_years,
            year_act = act_years,
            mode="standard",
            time="year",
            time_origin="year",
            time_dest="year",)

            # For the parameters which inlcudes index names
            if len(split)> 1:

                if (param_name == "input")|(param_name == "output"):

                    # Assign commodity and level names
                    com = split[1]
                    lev = split[2]

                    df = (make_df(param_name, technology=t, commodity=com, \
                    level=lev, value=val, unit='t', **common)\
                    .pipe(broadcast, node_loc=nodes).pipe(same_node))

                    results[param_name].append(df)

                elif param_name == "emission_factor":
                    # Assign the emisson type
                    emi = split[1]

                    df = (make_df(param_name, technology=t,value=val,\
                    emission=emi, unit='t', **common).pipe(broadcast, \
                    node_loc=nodes))
                    results[param_name].append(df)

            # Rest of the parameters except input,output and emission_factor

            else:
                df = (make_df(param_name, technology=t, value=val,unit='t', \
                **common).pipe(broadcast, node_loc=nodes))
                results[param_name].append(df)

    # Concatenate to one data frame per parameter
    results_aluminum = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    # Temporary: return nothing, since the data frames are incomplete
    return results_aluminum

# TODO: Different input values over the years: Add a function that modifies
#the values over the years

def gen_data_generic(scenario, dry_run=False):
    # Load configuration

    # Load configuration
    config = read_config()["material"]["generic_set"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    data_generic = read_data_generic()

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # Iterate over technologies
    for t in config["technology"]["add"]:

        # Retreive the technology availability and vintage/active years
        # Limit the years according to the available year

        av = data_generic.loc[(data_generic["technology"] == t),'availability']\
        .values[0]
        lifetime = data_generic.loc[(data_generic["technology"] == t) & \
        (data_generic["parameter"]== "technical_lifetime"),'value'].values[0]

        # Can we use s_info.Y here ?
        years_df = scenario.vintage_and_active_years()
        years_df = years_df.loc[years_df["year_vtg"]>= av]
        years_df_final = pd.DataFrame(columns=["year_vtg","year_act"])

        # For each vintage adjsut the active years according to technical lifetime
        for vtg in years_df["year_vtg"].unique():
            years_df_temp = years_df.loc[years_df["year_vtg"]== vtg]
            years_df_temp = years_df_temp.loc[years_df["year_act"]< vtg + lifetime]
            years_df_final = pd.concat([years_df_temp, years_df_final], ignore_index=True)

        vintage_years, act_years = years_df_final['year_vtg'], years_df_final['year_act']

        params = data_generic.loc[(data_generic["technology"] == t),"parameter"]\
        .values.tolist()

        # Keep the available modes for a technology
         mode_list = []

        # Iterate over parameters (e.g. input|coal|final|low_temp)
        for par in params:
            split = par.split("|")
            param_name = par.split("|")[0]

            val = data_generic.loc[((data_generic["technology"] == t) & \
            (data_generic["parameter"] == par)),'value'].values[0]

            # Common parameters for all input and output tables

            common = dict(
            year_vtg= vintage_years,
            year_act= act_years,
            time="year",
            time_origin="year",
            time_dest="year",)

            if len(split)> 1:

                if (param_name == "input")|(param_name == "output"):

                    com = split[1]
                    lev = split[2]
                    mod = split[3]

                    # Store the available modes for a technology

                    mode_list.append(mod)

                    df = (make_df(param_name, technology=t, commodity=com, \
                    level=lev,mode=mod, value=val, unit='t', **common).\
                    pipe(broadcast, node_loc=nodes).pipe(same_node))

                    results[param_name].append(df)

                elif param_name == "emission_factor":
                    emi = split[1]
                    mod = data_generic.loc[((data_generic["technology"] == t) \
                    & (data_generic["parameter"] == par)),'value'].values[0]

                        # Create emission factor for existing different modes

                    for m in np.unique(np.array(mode_list)):
                        df = (make_df(param_name, technology=t,value=val,\
                        emission=emi,mode= m, unit='t', **common)\
                        .pipe(broadcast, node_loc=nodes))

                        results[param_name].append(df)

            # Rest of the parameters apart from input, output and emission_factor
            else:

                df = (make_df(param_name, technology=t, value=val,unit='t', \
                **common).pipe(broadcast, node_loc=nodes))

                results[param_name].append(df)

    results_generic = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    return results_generic

def gen_data(scenario, dry_run=False):
    """Generate data for materials representation of nitrogen fertilizers.

    .. note:: This code is only partially translated from
       :file:`SetupNitrogenBase.py`.
    """
    # Load configuration
    config = read_config()["material"]["set"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    data = read_data()

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # NH3 production processes
    common = dict(
        year_vtg=s_info.Y,
        commodity="NH3",
        level="material_interim",
        # TODO fill in remaining dimensions
        mode="all",
        time="year",
        time_dest="year",
        time_origin="year",
    )

    # Iterate over new technologies, using the configuration
    for t in config["technology"]["add"]:
        # Output of NH3: same efficiency for all technologies
        # TODO the output commodity and level are different for
        #      t=NH3_to_N_fertil; use 'if' statements to fill in.
        df = (
            make_df("output", technology=t, value=1, unit='t', **common)
            .pipe(broadcast, node_loc=s_info.N)
            .pipe(same_node)
        )
        results["output"].append(df)

        # Heat output

        # Retrieve the scalar value from the data
        row = data.loc[("output_heat", t.id), :]
        log.info(f"Use {repr(row.Variable)} for heat output of {t}")

        # Store a modified data frame
        results["output"].append(
            df.assign(commodity="d_heat", level="secondary", value=row[2010])
        )

    # TODO add other variables from SetupNitrogenBase.py

    # Concatenate to one data frame per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    # Temporary: return nothing, since the data frames are incomplete
    return dict()
    # return result


def read_data():
    """Read and clean data from :file:`n-fertilizer_techno-economic.xlsx`."""
    # Ensure config is loaded, get the context
    context = read_config()

    # Shorter access to sets configuration
    sets = context["material"]["set"]

    # Read the file
    data = pd.read_excel(
        context.get_path("material", "n-fertilizer_techno-economic.xlsx"),
        sheet_name="Sheet1",
    )

    # Prepare contents for the "parameter" and "technology" columns
    # FIXME put these in the file itself to avoid ambiguity/error

    # "Variable" column contains different values selected to match each of
    # these parameters, per technology
    params = [
        "inv_cost",
        "fix_cost",
        "var_cost",
        "technical_lifetime",
        "input_fuel",
        "input_elec",
        "input_water",
        "output_NH3",
        "output_water",
        "output_heat",
        "emissions",
        "capacity_factor",
    ]

    param_values = []
    tech_values = []
    for t in sets["technology"]["add"]:
        param_values.extend(params)
        tech_values.extend([t.id] * len(params))

    # Clean the data
    data = (
        # Insert "technology" and "parameter" columns
        data.assign(technology=tech_values, parameter=param_values)
        # Drop columns that don't contain useful information
        .drop(["Model", "Scenario", "Region"], axis=1)
        # Set the data frame index for selection
        .set_index(["parameter", "technology"])
    )

    # TODO convert units for some parameters, per LoadParams.py

    return data


def get_data(scenario, context, **options):
    """Data for the bare RES."""
    if context.res_with_dummies:
        dt = get_dummy_data(scenario)
        print(dt)
        return dt
    else:
        return dict()


def get_dummy_data(scenario, **options):
    """Dummy data for the bare RES.

    Currently this contains:
    - A dummy 1-to-1 technology taking input from (dummy, primary) and output
      to (dummy, useful).
    - A dummy source technology for (dummy, primary).
    - Demand for (dummy, useful).

    This ensures that the model variable ACT has some non-zero entries.
    """
    info = ScenarioInfo(scenario)

    common = dict(
        node=info.N[0],
        node_loc=info.N[0],
        node_origin=info.N[0],
        node_dest=info.N[0],
        technology="dummy",
        year=info.Y,
        year_vtg=info.Y,
        year_act=info.Y,
        mode="all",
        # No subannual detail
        time="year",
        time_origin="year",
        time_dest="year",
    )

    data = make_io(
        src=("dummy", "primary", "GWa"),
        dest=("dummy", "useful", "GWa"),
        efficiency=1.,
        on='input',
        # Other data
        **common
    )

    # Source for dummy
    data["output"] = data["output"].append(
        data["output"].assign(technology="dummy source", level="primary")
    )

    data.update(
        make_matched_dfs(
            data["output"],
            capacity_factor=1.,
            technical_lifetime=10,
            var_cost=1,
        )
    )

    common.update(dict(
        commodity="dummy",
        level="useful",
        value=1.,
        unit="GWa",
    ))
    data["demand"] = make_df("demand", **common)

    return data
