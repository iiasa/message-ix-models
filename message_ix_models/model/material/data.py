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


# Read in technology-specific parameters from input xlsx
def process_china_data_tec():

    import numpy as np

    # Ensure config is loaded, get the context
    context = read_config()

    # Read the file
    data_steel_china = pd.read_excel(
        context.get_path("material", "China_steel_renamed.xlsx"),
        sheet_name="technologies",
    )

    # Clean the data

    data_steel_china = data_steel_china \
        [['Technology', 'Parameter', 'Level',  \
        'Commodity', 'Species', 'Units', 'Value']] \
        .replace(np.nan, '', regex=True)

    tuple_series = data_steel_china[['Parameter', 'Commodity', 'Level']] \
        .apply(tuple, axis=1)
    tuple_ef = data_steel_china[['Parameter', 'Species']] \
        .apply(tuple, axis=1)


    data_steel_china['parameter'] = tuple_series.str.join('|') \
        .str.replace('\|\|', '')
    data_steel_china.loc[data_steel_china['Parameter'] == "emission_factor", \
        'parameter'] = tuple_ef.str.join('|').str.replace('\|\|', '')

    data_steel_china = data_steel_china.drop(['Parameter', 'Level', 'Commodity'] \
        , axis = 1)
    data_steel_china = data_steel_china.drop( \
        data_steel_china[data_steel_china.Value==''].index)

    data_steel_china.columns = data_steel_china.columns.str.lower()

    # Unit conversion

    # At the moment this is done in the excel file, can be also done here
    # To make sure we use the same units

    return data_steel_china

# Read in relation-specific parameters from input xlsx
def process_china_data_rel():

    import numpy as np

    # Ensure config is loaded, get the context
    context = read_config()

    # Read the file
    data_steel_china = pd.read_excel(
        context.get_path("material", "China_steel_renamed.xlsx"),
        sheet_name="relations",
    )

    return data_steel_china

# Read in time-dependent parameters
# Now only used to add fuel cost for bare model
def read_var_cost():

    import numpy as np

    # Ensure config is loaded, get the context
    context = read_config()

    # Read the file
    df = pd.read_excel(
        context.get_path("material", "China_steel_renamed.xlsx"),
        sheet_name="var_cost",
    )

    df = pd.melt(df, id_vars=['technology', 'mode', 'units'], \
        value_vars=[2010, 2020, 2030, 2040, 2050, 2060, 2070, 2080, 2090, 2100], \
        var_name='year')

    return df


# Question: Do we need read data dunction seperately for all materials ?
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
    # sets = context["material"]["generic"]

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

    return results

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

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    allyears = s_info.set['year'] #s_info.Y is only for modeling years
    modelyears = s_info.Y #s_info.Y is only for modeling years
    nodes = s_info.N
    yv_ya = s_info.yv_ya
    fmy = s_info.y0

    # 'World' is included by default when creating a message_ix.Scenario().
    # Need to remove it for the China bare model
    nodes.remove('World')

    for t in config["technology"]["add"]:

        # years = s_info.Y
        params = data_generic.loc[(data_generic["technology"] == t),"parameter"]\
        .values.tolist()

        # Availability year of the technology
        av = data_generic.loc[(data_generic["technology"] == t),'availability'].\
        values[0]
        modelyears = [year for year in modelyears if year >= av]
        yva = yv_ya.loc[yv_ya.year_vtg >= av, ]

        # Iterate over parameters (e.g. input|coal|final|low_temp)
        for par in params:
            split = par.split("|")
            param_name = par.split("|")[0]

            val = data_generic.loc[((data_generic["technology"] == t) & \
            (data_generic["parameter"] == par)),'value'].values[0]

            # Common parameters for all input and output tables

            common = dict(
            year_vtg= yva.year_vtg,
            year_act= yva.year_act,
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

                    # TODO: Now tentatively fixed to one mode. Have values for the other mode too
                    df = (make_df(param_name, technology=t,value=val,\
                    emission=emi, mode="low_temp", unit='t', **common).pipe(broadcast, \
                    node_loc=nodes))

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

    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    return results


def gen_data_steel(scenario, dry_run=False):
    """Generate data for materials representation of steel industry.

    """
    # Load configuration
    config = read_config()["material"]["steel"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    data_steel = process_china_data_tec()
    # Special treatment for time-dependent Parameters
    data_steel_vc = read_var_cost()
    tec_vc = set(data_steel_vc.technology)

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    allyears = s_info.set['year'] #s_info.Y is only for modeling years
    modelyears = s_info.Y #s_info.Y is only for modeling years
    nodes = s_info.N
    yv_ya = s_info.yv_ya
    fmy = s_info.y0

    nodes.remove('World') # For the bare model

    # for t in s_info.set['technology']:
    for t in config['technology']['add']:

        params = data_steel.loc[(data_steel["technology"] == t),\
            "parameter"].values.tolist()

        # Special treatment for time-varying params
        if t in tec_vc:
            common = dict(
                time="year",
                time_origin="year",
                time_dest="year",)

            param_name = "var_cost"
            val = data_steel_vc.loc[(data_steel_vc["technology"] == t), 'value']
            units = data_steel_vc.loc[(data_steel_vc["technology"] == t), \
            'units'].values[0]
            mod = data_steel_vc.loc[(data_steel_vc["technology"] == t), 'mode']
            yr = data_steel_vc.loc[(data_steel_vc["technology"] == t), 'year']

            df = (make_df(param_name, technology=t, value=val,\
            unit='t', year_vtg=yr, year_act=yr, mode=mod, **common).pipe(broadcast, \
            node_loc=nodes))

            print(param_name, df)
            results[param_name].append(df)

        # Iterate over parameters
        for par in params:

            # Obtain the parameter names, commodity,level,emission
            split = par.split("|")
            param_name = split[0]

            # Obtain the scalar value for the parameter
            val = data_steel.loc[((data_steel["technology"] == t) \
            & (data_steel["parameter"] == par)),'value'].values[0]

            common = dict(
                year_vtg= yv_ya.year_vtg,
                year_act= yv_ya.year_act,
                mode="M1",
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

                elif param_name == "emission_factor":

                    # Assign the emisson type
                    emi = split[1]

                    df = (make_df(param_name, technology=t,value=val,\
                    emission=emi, unit='t', **common).pipe(broadcast, \
                    node_loc=nodes))

                results[param_name].append(df)

            # Parameters with only parameter name
            else:
                # Historical years are earlier than firstmodelyear
                y_hist = [y for y in allyears if y < fmy]
                # print(y_hist, fmy, years)
                if re.search("historical_", param_name):
                    common_hist = dict(
                        year_vtg= y_hist,
                        year_act= y_hist,
                        mode="M1",
                        time="year",)

                    df = (make_df(param_name, technology=t, value=val, unit='t', \
                    **common_hist).pipe(broadcast, node_loc=nodes))
                    # print(common_hist, param_name, t, nodes, val, y_hist)
                else:
                    df = (make_df(param_name, technology=t, value=val, unit='t', \
                    **common).pipe(broadcast, node_loc=nodes))

                results[param_name].append(df)

    # TODO: relation is used to set external demand.
    # We can also have two redundant commodity outputs from manufacturing stage
    # and set external demand on one of them.
    for r in config['relation']['add']:

        # Read the file
        rel_steel = process_china_data_rel()

        params = rel_steel.loc[(rel_steel["relation"] == r),\
            "parameter"].values.tolist()

        common_rel = dict(
            year_rel = modelyears,
            year_act = modelyears,
            mode = 'M1',
            relation = r,)

        for par_name in params:
            if par_name == "relation_activity":

                val = rel_steel.loc[((rel_steel["relation"] == r) \
                    & (rel_steel["parameter"] == par_name)),'value'].values[0]
                tec = rel_steel.loc[((rel_steel["relation"] == r) \
                    & (rel_steel["parameter"] == par_name)),'technology'].values[0]

                df = (make_df(par_name, technology=tec, value=val, unit='t',\
                **common_rel).pipe(broadcast, node_rel=nodes, node_loc=nodes))

                results[par_name].append(df)

            elif par_name == "relation_lower":

                demand = gen_mock_demand()

                df = (make_df(par_name, value=demand, unit='t',\
                **common_rel).pipe(broadcast, node_rel=nodes))

                results[par_name].append(df)

    # Concatenate to one data frame per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    return results


# Generate a fake steel demand
def gen_mock_demand():
    # True steel use 2010 (China) = 537 Mt/year
    # https://www.worldsteel.org/en/dam/jcr:0474d208-9108-4927-ace8-4ac5445c5df8/World+Steel+in+Figures+2017.pdf
    gdp_growth = [0.121448215899944, \
        0.0733079014579874, 0.0348154093342843, \
        0.021827616787921,0.0134425983942219, 0.0108320197485592, \
        0.00884341208063,0.00829374133206562, 0.00649794573935969]
    demand = [(x+1) * 537 for x in gdp_growth]

    return demand


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
