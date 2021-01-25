from .data_util import read_sector_data

import numpy as np
from collections import defaultdict
import logging

import pandas as pd

from .util import read_config
from .data_util import read_rel
from message_data.tools import (
    ScenarioInfo,
    broadcast,
    make_df,
    make_io,
    make_matched_dfs,
    same_node,
    copy_column,
    add_par_data
)


def read_timeseries_buildings(filename):

    import numpy as np

    # Ensure config is loaded, get the context
    context = read_config()

    # Read the file
    bld_input_raw = pd.read_csv(
        context.get_path("material", filename))

    bld_input_mat = bld_input_raw[bld_input_raw['Variable'].\
                                  # str.contains("Floor Space|Aluminum|Cement|Steel|Final Energy")]
                                  str.contains("Floor Space|Aluminum|Cement|Steel")] # Final Energy - Later. Need to figure out carving out
    bld_input_mat['Region'] = 'R11_' + bld_input_mat['Region']

    bld_input_pivot = \
        bld_input_mat.melt(id_vars=['Region','Variable'], var_name='Year', \
                value_vars=list(map(str, range(2015, 2101, 5)))).\
            set_index(['Region','Year','Variable'])\
            .squeeze()\
            .unstack()\
            .reset_index()

    # Divide by floor area to get energy/material intensities
    bld_intensity_ene_mat = bld_input_pivot.iloc[:,2:].div(bld_input_pivot['Energy Service|Residential|Floor Space'], axis=0)
    bld_intensity_ene_mat.columns = [s + "|Intensity" for s in bld_intensity_ene_mat.columns]
    bld_intensity_ene_mat = pd.concat([bld_input_pivot[['Region', 'Year']], \
                                       bld_intensity_ene_mat.reindex(bld_input_pivot.index)], axis=1).\
        drop(columns = ['Energy Service|Residential|Floor Space|Intensity'])

    bld_intensity_ene_mat['Energy Service|Residential|Floor Space'] = bld_input_pivot['Energy Service|Residential|Floor Space']

    # bld_intensity_ene = bld_intensity_ene_mat.iloc[:, 1:7]
    # bld_intensity_mat = bld_intensity_ene_mat.iloc[:, 8:]

    # Calculate material intensities
    # bld_input_pivot['Material Demand|Residential|Buildings|Cement|Intensity'] =\
    #     bld_input_pivot['Material Demand|Residential|Buildings|Cement']/\
    #         bld_input_pivot['Energy Service|Residential|Floor Space']
    # bld_input_pivot['Material Demand|Residential|Buildings|Steel|Intensity'] =\
    #     bld_input_pivot['Material Demand|Residential|Buildings|Steel']/\
    #         bld_input_pivot['Energy Service|Residential|Floor Space']
    # bld_input_pivot['Material Demand|Residential|Buildings|Aluminum|Intensity'] =\
    #     bld_input_pivot['Material Demand|Residential|Buildings|Aluminum']/\
    #         bld_input_pivot['Energy Service|Residential|Floor Space']
    # bld_input_pivot['Scrap Release|Residential|Buildings|Cement|Intensity'] =\
    #     bld_input_pivot['Scrap Release|Residential|Buildings|Cement']/\
    #         bld_input_pivot['Energy Service|Residential|Floor Space']
    # bld_input_pivot['Scrap Release|Residential|Buildings|Steel|Intensity'] =\
    #     bld_input_pivot['Scrap Release|Residential|Buildings|Steel']/\
    #         bld_input_pivot['Energy Service|Residential|Floor Space']
    # bld_input_pivot['Scrap Release|Residential|Buildings|Aluminum|Intensity'] =\
    #     bld_input_pivot['Scrap Release|Residential|Buildings|Aluminum']/\
    #         bld_input_pivot['Energy Service|Residential|Floor Space']

    # Material intensities are in kg/m2
    bld_data_long = bld_intensity_ene_mat.melt(id_vars=['Region','Year'], var_name='Variable')\
        .rename(columns={"Region": "node", "Year": "year"})
    # Both for energy and material
    bld_intensity_long = bld_data_long[bld_data_long['Variable'].\
                                  str.contains("Intensity")]\
        .reset_index(drop=True)
    bld_area_long = bld_data_long[bld_data_long['Variable']==\
                                  'Energy Service|Residential|Floor Space']\
        .reset_index(drop=True)

    tmp = bld_intensity_long.Variable.str.split("|", expand=True)

    bld_intensity_long['commodity'] = tmp[3].str.lower() # Material type
    bld_intensity_long['type'] = tmp[0] # 'Material Demand' or 'Scrap Release'
    bld_intensity_long['unit'] = "kg/m2"

    bld_intensity_long = bld_intensity_long.drop(columns='Variable')
    bld_area_long = bld_area_long.drop(columns='Variable')

    bld_intensity_long = bld_intensity_long\
        .drop(bld_intensity_long[np.isnan(bld_intensity_long.value)].index)

    return bld_intensity_long, bld_area_long


def gen_data_buildings(scenario, dry_run=False):
    """Generate data for materials representation of steel industry.

    """
    # Load configuration
    context = read_config()
    config = context["material"]["buildings"]

    lev = config['level']['add'][0]
    comm = config['commodity']['add'][0]
    tec = config['technology']['add'][0] # "buildings"

    print(lev, comm, tec, type(tec))

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Buildings raw data (from Alessio)
    data_buildings, data_buildings_demand = read_timeseries_buildings('LED_LED_report_IAMC.csv')

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    # allyears = s_info.set['year'] #s_info.Y is only for modeling years
    modelyears = s_info.Y #s_info.Y is only for modeling years
    nodes = s_info.N
    yv_ya = s_info.yv_ya
    # fmy = s_info.y0
    nodes.remove('World')

    # Read field values from the buildings input data
    regions = list(set(data_buildings.node))
    comms = list(set(data_buildings.commodity))
    types = list(set(data_buildings.type)) #
    # ['R11_AFR', 'R11_CPA', 'R11_EEU', 'R11_FSU', 'R11_LAM', \
    #     'R11_MEA', 'R11_NAM', 'R11_PAO', 'R11_PAS', 'R11_SAS', 'R11_WEU']

    common = dict(
        time="year",
        time_origin="year",
        time_dest="year",
        mode="M1")
    
    # Filter only the years in the base scenario
    data_buildings['year'] = data_buildings['year'].astype(int)
    data_buildings_demand['year'] = data_buildings_demand['year'].astype(int)
    data_buildings = data_buildings[data_buildings['year'].isin(modelyears)]
    data_buildings_demand = data_buildings_demand[data_buildings_demand['year'].isin(modelyears)]
            
    for rg in regions:
        for comm in comms:
            # for typ in types:

            val_mat = data_buildings.loc[(data_buildings["type"] == types[0]) \
                & (data_buildings["commodity"] == comm)\
                & (data_buildings["node"] == rg), ]
            val_scr = data_buildings.loc[(data_buildings["type"] == types[1]) \
                & (data_buildings["commodity"] == comm)\
                & (data_buildings["node"] == rg), ]
            

            # Material input to buildings
            df = make_df('input', technology=tec, commodity=comm, \
                level="product", year_vtg = val_mat.year, \
                value=val_mat.value, unit='t', \
                node_loc = rg, **common)\
                .pipe(same_node)\
                .assign(year_act=copy_column('year_vtg'))
            results['input'].append(df)

            # Scrap output back to industry
            df = make_df('output', technology=tec, commodity=comm, \
                level='old_scrap', year_vtg = val_scr.year, \
                value=val_scr.value, unit='t', \
                node_loc = rg, **common)\
                .pipe(same_node)\
                .assign(year_act=copy_column('year_vtg'))
            results['output'].append(df)

        # Service output to buildings demand
        df = make_df('output', technology=tec, commodity='floor_area', \
            level=lev, year_vtg = val_mat.year, \
            value=1, unit='t', \
            node_loc=rg, **common)\
            .pipe(same_node)\
            .assign(year_act=copy_column('year_vtg'))
        results['output'].append(df)

    # Create external demand param
    parname = 'demand'
    demand = data_buildings_demand
    df = make_df(parname, level='demand', commodity='steel', value=demand.value, unit='t', \
        year=demand.year, time='year', node=demand.node)
    results[parname].append(df)

    # Concatenate to one data frame per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    return results









    # for t in s_info.set['technology']:
    for t in config['technology']['add']:

        params = data_steel.loc[(data_steel["technology"] == t),\
            "parameter"].values.tolist()

        # Special treatment for time-varying params
        if t in tec_ts:
            common = dict(
                time="year",
                time_origin="year",
                time_dest="year",)

            param_name = data_steel_ts.loc[(data_steel_ts["technology"] == t), 'parameter']

            for p in set(param_name):
                val = data_steel_ts.loc[(data_steel_ts["technology"] == t) \
                    & (data_steel_ts["parameter"] == p), 'value']
                units = data_steel_ts.loc[(data_steel_ts["technology"] == t) \
                    & (data_steel_ts["parameter"] == p), 'units'].values[0]
                mod = data_steel_ts.loc[(data_steel_ts["technology"] == t) \
                    & (data_steel_ts["parameter"] == p), 'mode']
                yr = data_steel_ts.loc[(data_steel_ts["technology"] == t) \
                    & (data_steel_ts["parameter"] == p), 'year']

                if p=="var_cost":
                    df = (make_df(p, technology=t, value=val,\
                    unit='t', year_vtg=yr, year_act=yr, mode=mod, **common).pipe(broadcast, \
                    node_loc=nodes))
                else:
                    rg = data_steel_ts.loc[(data_steel_ts["technology"] == t) \
                        & (data_steel_ts["parameter"] == p), 'region']
                    df = make_df(p, technology=t, value=val,\
                    unit='t', year_vtg=yr, year_act=yr, mode=mod, node_loc=rg, **common)

                results[p].append(df)

        # Iterate over parameters
        for par in params:

            # Obtain the parameter names, commodity,level,emission
            split = par.split("|")
            print(split)
            param_name = split[0]
            # Obtain the scalar value for the parameter
            val = data_steel.loc[((data_steel["technology"] == t) \
                & (data_steel["parameter"] == par)),'value']#.values
            regions = data_steel.loc[((data_steel["technology"] == t) \
                & (data_steel["parameter"] == par)),'region']#.values

            common = dict(
                year_vtg= yv_ya.year_vtg,
                year_act= yv_ya.year_act,
                # mode="M1",
                time="year",
                time_origin="year",
                time_dest="year",)

            for rg in regions:

                # For the parameters which inlcudes index names
                if len(split)> 1:

                    print('1.param_name:', param_name, t)
                    if (param_name == "input")|(param_name == "output"):

                        # Assign commodity and level names
                        com = split[1]
                        lev = split[2]
                        mod = split[3]
                        print(rg, par, lev)

                        if (param_name == "input") and (lev == "import"):
                            df = make_df(param_name, technology=t, commodity=com, \
                            level=lev, \
                            value=val[regions[regions==rg].index[0]], mode=mod, unit='t', \
                            node_loc=rg, node_origin="R11_GLB", **common)
                        elif (param_name == "output") and (lev == "export"):
                            df = make_df(param_name, technology=t, commodity=com, \
                            level=lev, \
                            value=val[regions[regions==rg].index[0]], mode=mod, unit='t', \
                            node_loc=rg, node_dest="R11_GLB", **common)
                        else:
                            df = (make_df(param_name, technology=t, commodity=com, \
                            level=lev, \
                            value=val[regions[regions==rg].index[0]], mode=mod, unit='t', \
                            node_loc=rg, **common)\
                            .pipe(same_node))

                        # Copy parameters to all regions, when node_loc is not GLB
                        if (len(regions) == 1) and (rg != "R11_GLB"):
                            print("copying to all R11", rg, lev)
                            df['node_loc'] = None
                            df = df.pipe(broadcast, node_loc=nodes)#.pipe(same_node)
                            # Use same_node only for non-trade technologies
                            if (lev != "import") and (lev != "export"):
                                df = df.pipe(same_node)

                    elif param_name == "emission_factor":

                        # Assign the emisson type
                        emi = split[1]
                        mod = split[2]

                        df = make_df(param_name, technology=t, \
                        value=val[regions[regions==rg].index[0]],\
                        emission=emi, mode=mod, unit='t', \
                        node_loc=rg, **common)

                    else: # time-independent var_cost
                        mod = split[1]
                        df = make_df(param_name, technology=t, \
                        value=val[regions[regions==rg].index[0]], \
                        mode=mod, unit='t', node_loc=rg, \
                        **common)

                # Parameters with only parameter name
                else:
                    print('2.param_name:', param_name)
                    df = make_df(param_name, technology=t, \
                    value=val[regions[regions==rg].index[0]], unit='t', \
                    node_loc=rg, **common)

                # Copy parameters to all regions
                if len(set(df['node_loc'])) == 1 and list(set(df['node_loc']))[0]!='R11_GLB':
                    print("Copying to all R11")
                    df['node_loc'] = None
                    df = df.pipe(broadcast, node_loc=nodes)

                results[param_name].append(df)

    # Add relations for scrap grades and availability

    for r in config['relation']['add']:

        params = set(data_steel_rel.loc[(data_steel_rel["relation"] == r),\
            "parameter"].values)

        common_rel = dict(
            year_rel = modelyears,
            year_act = modelyears,
            mode = 'M1',
            relation = r,)

        for par_name in params:
            if par_name == "relation_activity":

                val = data_steel_rel.loc[((data_steel_rel["relation"] == r) \
                    & (data_steel_rel["parameter"] == par_name)),'value'].values
                tec = data_steel_rel.loc[((data_steel_rel["relation"] == r) \
                    & (data_steel_rel["parameter"] == par_name)),'technology'].values

                print(par_name, "val", val, "tec", tec)

                df = (make_df(par_name, technology=tec, \
                            value=val, unit='-', mode = 'M1', relation = r)\
                    .pipe(broadcast, node_rel=nodes, \
                            node_loc=nodes, year_rel = modelyears))\
                    .assign(year_act=copy_column('year_rel'))

                results[par_name].append(df)

            elif par_name == "relation_upper":

                val = data_steel_rel.loc[((data_steel_rel["relation"] == r) \
                    & (data_steel_rel["parameter"] == par_name)),'value'].values[0]

                df = (make_df(par_name, value=val, unit='-',\
                **common_rel).pipe(broadcast, node_rel=nodes))

                results[par_name].append(df)

    # Create external demand param
    parname = 'demand'
    demand = gen_mock_demand_steel(scenario)
    df = make_df(parname, level='demand', commodity='steel', value=demand.value, unit='t', \
        year=demand.year, time='year', node=demand.node)#.pipe(broadcast, node=nodes)
    results[parname].append(df)

    # Concatenate to one data frame per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    return results
