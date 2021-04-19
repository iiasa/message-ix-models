"""Prepare data for water use for cooling & energy technologies."""

import pandas as pd
from message_data.model.water import read_config
from message_data.tools import broadcast,make_df, same_node, make_matched_dfs
from message_ix import make_df
from message_data.model.water.build import get_water_reference_scenario

#: Name of the input file.
# The input file mentions water withdrawals and emission heating fractions for
# cooling technologies alongwith parent technologies:
FILE = 'tech_water_performance_ssp_msg.csv'
# Investment costs and regional shares of historical activities of cooling technologies
FILE1 = 'cooltech_cost_and_shares_ssp_msg.csv'
# input dataframe for reference values from a global scenario
FILE2 = 'ref_input.csv'
# Historical activity  dataframe for reference values from a global scenario
FILE3 = 'ref_hist_act.csv'
# Historical new  capacity  dataframe for reference values from a global scenario
FILE4 = 'ref_hist_cap.csv'

# water & electricity for cooling technologies
def cool_tech(context):
    """
        Parameters
        ----------
        info : .ScenarioInfo
            Information about target Scenario.

        Returns
        -------
        data : dict of (str -> pandas.DataFrame)
            Keys are MESSAGE parameter names such as 'input', 'fix_cost'. Values
            are data frames ready for :meth:`~.Scenario.add_par`.
        """

    # define an empty dictionary
    results = {}

    context = read_config()

    # Reference to the transport configuration
    config = context["water config"]
    tech_info = context["water set"]["technology"]["add"]
    info = context["water build info"]

    path = context.get_path("water","ppl_cooling_tech", FILE)
    df = pd.read_csv(path)
    cooling_df = df.loc[df['technology_group'] == 'cooling']
    # Separate a column for parent technologies of respective cooling
    # techs
    cooling_df['parent_tech'] = cooling_df['technology_name'] \
        .apply(lambda x: pd.Series(str(x).split('__'))).drop(
        columns=1)
    non_cooling_df =  df.loc[df['technology_group'] != 'cooling']

    #TODO commented for now to reduce the time

    scen = get_water_reference_scenario(context)

    # Extracting input database from scenario for parent technologies
    # Extracting input values from scenario
    ref_input = scen.par('input', {'technology': cooling_df['parent_tech']})
    # Extracting historical activity from scenario
    ref_hist_act = scen.par('historical_activity', {'technology': cooling_df['parent_tech']})
    # Extracting historical capacity from scenario
    ref_hist_cap = scen.par('historical_new_capacity', {'technology': cooling_df['parent_tech']})

    # # May possibly remove these in future
    # path2 = context.get_path("water", FILE2)
    # ref_input = pd.read_csv(path2)
    # path3 = context.get_path("water", FILE3)
    # ref_hist_act = pd.read_csv(path3)
    # path4 = context.get_path("water", FILE4)
    # ref_hist_cap = pd.read_csv(path4)

    ref_hist_act = ref_hist_act[ref_hist_act['node_loc'] != 'R11_GLB']
    ref_hist_act = ref_hist_cap[ref_hist_cap['node_loc'] != 'R11_GLB']

    # cooling fraction = H_cool = Hi - 1 - Hi*(h_fg), where h_fg (flue gasses losses) = 0.1
    ref_input['cooling_fraction'] = ref_input['value'] * 0.9 - 1

    def missing_tech(x):
        """
        This function goes through the input data frame and extract the
        technologies which don't have input values and then assign manual
        values to those technologies along with assigning them an arbitrary
        level i.e dummy supply
        """
        data_dic = {
            'geo_hpl': 1 / 0.850,
            'geo_ppl': 1 / 0.385,
            'nuc_hc': 1 / 0.326,
            'nuc_lc': 1 / 0.326,
            'solar_th_ppl': 1 / 0.385
        }

        if data_dic.get(x['technology']):
            if x['level'] == 'cooling':
                return pd.Series((data_dic.get(x['technology']), "dummy_supply"))
            else:
                return pd.Series((data_dic.get(x['technology']), x['level']))
        else:
            return pd.Series((x['value'], x['level']))

    ref_input[['value', 'level']] = ref_input[['technology', 'value', 'level']].apply(missing_tech, axis=1)

    # Combines the input dataframe of parent technologies alongwith the water withdrawal data f
    input_cool = cooling_df.set_index('parent_tech').\
        combine_first(ref_input.set_index('technology')).reset_index()

    # Drops NA values from the value column
    input_cool = input_cool.dropna(subset=['value'])

    # Convert year values into integers to be compatibel for model
    input_cool.year_vtg = input_cool.year_vtg.astype(int)
    input_cool.year_act = input_cool.year_act.astype(int)
    # Drops extra technologies from the data
    input_cool = input_cool[ (input_cool['level'] != "water_supply")
                             & (input_cool['level'] != "cooling")]

    # TODO Remvoing heating plant values temporarily, need to research if hpl needs cooling?
    input_cool = input_cool[~input_cool['technology_name'].str.contains('hpl', na=False)]

    def cooling_fr(x):
        """
            This function returns the cooling fraction
            The calculation is different for two cateogries;
            1. Technologies that produce heat as an output

                cooling_fraction(h_cool) = input value(hi) - 1

            Simply substract 1 from the heating value since the
            rest of the part is already accounted in the heating
            value

            2. Rest of technologies
                h_cool  =  hi -Hi* h_fg - 1,
                where:
                    h_fg (flue gasses losses) = 0.1 (10% assumed losses)
        """
        if "hpl" in x['index']:
            return x['value'] - 1
        else:
            return x['value'] - (x['value']*0.1) - 1


    input_cool['cooling_fraction'] = input_cool.apply(cooling_fr, axis=1)


    # def foo2(x):
    #     """
    #     This function returns input values for cooling technologies
    #         1. The first condition filters out techs with 'hpl' in their names,and
    #          returns water withdrawal values directly without using cooling fraction,
    #          since these technologies have output as heat and not electricity.
    #         2. The remaining technologies returns the value by dividing the water withdrawal
    #          values by cooling fraction
    #     """
    #     if "hpl" in x['index']:
    #         return x['water_withdrawal_mid_m3_per_output'] \
    #                * 60 * 60 * 24 * 365 * (1e-9)
    #     else:
    #         return (x['water_withdrawal_mid_m3_per_output']
    #                 * 60 * 60 * 24 * 365 * (1e-9)) / x['cooling_fraction']

    # Make a new column 'value_cool' for calculating values against technologies
    #input_cool['value_cool'] = input_cool.apply(foo2, axis=1)
    input_cool['value_cool'] = input_cool['water_withdrawal_mid_m3_per_output']*60 * 60 * 24 * 365 * 1e-9 / \
                               input_cool['cooling_fraction']

    # def foo3(x):
    #     """
    #     This function is similar to foo2, but it returns electricity values per unit of cooling
    #     for techs that require parasitic electricity demand
    #     """
    #     if "hpl" in x['index']:
    #         return x['parasitic_electricity_demand_fraction']
    #
    #     elif x['parasitic_electricity_demand_fraction'] > 0.0:
    #         return x['parasitic_electricity_demand_fraction'] / x['cooling_fraction']

    # Filter out technologies that requires parasitic electricity
    electr = input_cool[input_cool['parasitic_electricity_demand_fraction'] > 0.0 ]

    # Make a new column 'value_cool' for calculating values against technologies
    electr['value_cool'] = electr['parasitic_electricity_demand_fraction'] / electr['cooling_fraction']
    # Filters out technologies requiring saline water supply
    saline_df = input_cool[input_cool['technology_name'].str.endswith("ot_saline", na = False)]

    # input_cool_minus_saline_elec_df
    con1 = input_cool['technology_name'].str.endswith("ot_saline", na = False )
    con2 = input_cool['technology_name'].str.endswith("air", na = False)
    icmse_df = input_cool[(~con1) & (~con2)]

    inp = make_df('input',
                node_loc=electr['node_loc'],
                technology=electr['technology_name'],
                year_vtg=electr['year_vtg'],
                year_act=electr['year_act'],
                mode=electr['mode'],
                node_origin=electr['node_origin'],
                commodity='electr',
                level='secondary',
                time='year',
                time_origin='year',
                value=electr['value_cool'],
                unit='GWa'
                )

    inp = inp.append(make_df('input',
                         node_loc=icmse_df['node_loc'],
                         technology=icmse_df['technology_name'],
                         year_vtg=icmse_df['year_vtg'],
                         year_act=icmse_df['year_act'],
                         mode=icmse_df['mode'],
                         node_origin=icmse_df['node_origin'],
                         commodity='freshwater_supply',
                         level='water_supply',
                         time='year',
                         time_origin='year',
                         value=icmse_df['value_cool'],
                         unit='km3/GWa'
                         )
                 )

    inp = inp.append(make_df('input',
                         node_loc=saline_df['node_loc'],
                         technology=saline_df['technology_name'],
                         year_vtg=saline_df['year_vtg'],
                         year_act=saline_df['year_act'],
                         mode=saline_df['mode'],
                         node_origin=saline_df['node_origin'],
                         commodity='saline_supply_ppl',
                         level='water_supply',
                         time='year',
                         time_origin='year',
                         value=saline_df['value_cool'],
                         unit='km3/GWa'
                         )
                 )

    # Drops NA values from the value column
    inp = inp.dropna(subset=['value'])

    # append the input data to results
    results['input'] = inp

    path1 = context.get_path("water","ppl_cooling_tech", FILE1)
    cost = pd.read_csv(path1)
    # Combine technology name to get full cooling tech names
    cost['technology'] = cost['utype'] + '__' + cost['cooling']
    # Filtering out 2010 data to use for historical values
    input_cool_2010 = input_cool[(input_cool['year_act'] == 2010) &
                                 (input_cool['year_vtg'] == 2010)]
    # Filter out columns that contain 'mix' in column name
    columns = [col for col in cost.columns if 'mix_' in col]
    # Rename column names to R11 to match with the previous df
    rename_columns_dict = {column: column.replace("mix_", "R11_") for column in columns}
    cost.rename(columns=rename_columns_dict, inplace=True)

    search_cols = [col for col in cost.columns if "R11_" in col or "technology" in col]
    hold_df = input_cool_2010[["node_loc", "technology_name", "cooling_fraction"]]
    hold_df = hold_df.drop_duplicates()
    search_cols_cooling_fraction = [col for col in search_cols if col != "technology"]

    def shares(x):
        """
        This function takes the value of shares of cooling technology types
        of regions and multiplies them with corresponding cooling fraction
        """
        for col in search_cols_cooling_fraction:
            cooling_fraction = hold_df[(hold_df["node_loc"] == col) &
                                       (hold_df["technology_name"] ==
                                       x['technology'])]['cooling_fraction']
            x[col] = x[col] * cooling_fraction

        results = []
        for i in x:
            if isinstance(i, str):
                results.append(i)
            else:
                if not len(i):
                    return pd.Series([i for i in range(11)] + ['delme'],
                    index=search_cols)
                else:
                    results.append(float(i))
        return pd.Series(results, index=search_cols)

    # Apply function to the
    hold_cost = cost[search_cols].apply(shares, axis=1)
    hold_cost = hold_cost[hold_cost["technology"] != "delme"]

    def hist_act(x):
        """
        This functions multiplies the values calculated in previous function
        with the historical activity of parent technologies to calculate
        historical activities of cooling technologies
        Thus;
        hist_activity(cooling_tech) = hist_activitiy(parent_technology)
                                                    * share * cooling_fraction
        """
        tech_df = hold_cost[hold_cost["technology"].str.startswith(x.technology)]  # [x.node_loc]
        node_loc = x["node_loc"]  # R11_EEU
        technology = x["technology"]
        cooling_technologies = list(tech_df["technology"])
        new_values = tech_df[node_loc] * x.value

        return [[node_loc, technology, cooling_technology, x.year_act, x.value, new_value, x.unit]
                for new_value, cooling_technology in zip(new_values, cooling_technologies)]

    changed_value_series = ref_hist_act.apply(hist_act, axis=1)
    changed_value_series_flat = [row for series in changed_value_series for row in series]
    columns = ["node_loc", "technology", "cooling_technology", "year_act", "value", "new_value", "unit"]
    # dataframe for historical activities of cooling techs
    act_value_df = pd.DataFrame(changed_value_series_flat, columns=columns)

    def hist_cap(x):
        """
        This functions multiplies the values calculated in previous function
        with the historical activity of parent technologies to calculate
        histroical capacities of parent technologies Thus;
        hist_new_capacity(cooling_tech) = historical_new_capacity(parent_technology)
                                                    * share * cooling_fraction
        """
        tech_df = hold_cost[hold_cost["technology"].str.startswith(x.technology)]  # [x.node_loc]
        node_loc = x["node_loc"]  # R11_EEU
        technology = x["technology"]
        cooling_technologies = list(tech_df["technology"])
        new_values = tech_df[node_loc] * x.value

        return [[node_loc, technology, cooling_technology, x.year_vtg, x.value, new_value, x.unit]
                for new_value, cooling_technology in zip(new_values, cooling_technologies)]

    changed_value_series = ref_hist_cap.apply(hist_cap, axis=1)
    changed_value_series_flat = [row for series in changed_value_series for row in series]
    columns = ["node_loc", "technology", "cooling_technology", "year_vtg", "value", "new_value", "unit"]
    cap_value_df = pd.DataFrame(changed_value_series_flat, columns=columns)

    # Make model compatible df for histroical acitvitiy
    h_act = make_df('historical_activity',
                               node_loc=act_value_df['node_loc'],
                               technology=act_value_df['cooling_technology'],
                               year_act=act_value_df['year_act'],
                               mode = 'M1',
                               time = 'year',
                               value=act_value_df['new_value'],
                             # TODO finalize units
                               unit='km3/GWa'
                               )

    results['historical_activity']= h_act
    # Make model compatible df for histroical new capacity
    h_cap = make_df('historical_new_capacity',
                               node_loc=cap_value_df['node_loc'],
                               technology=cap_value_df['cooling_technology'],
                               year_vtg=cap_value_df['year_vtg'],
                               value=cap_value_df['new_value'],
                               unit='km3/GWa'
                               )




    results['historical_new_capacity'] = h_cap


    # Filter out just cl_fresh & air technologies for adding inv_cost in model,
    # The rest fo technologies are assumed to have costs included in parent technologies
    #con3 = cost['technology'].str.endswith("cl_fresh")
    #con4 = cost['technology'].str.endswith("air")
    #con5 = cost.technology.isin(input_cool['technology_name'])
    #inv_cost = cost[(con3) | (con4)]
    inv_cost = cost.copy()
    # Manually removing extra technolgoies not required
    #TODO make it automatic to not include the names manually
    techs_to_remove = ['mw_ppl__ot_fresh','mw_ppl__ot_saline','mw_ppl__cl_fresh',
                       'mw_ppl__air','nuc_fbr__ot_fresh','nuc_fbr__ot_saline',
                       'nuc_fbr__cl_fresh','nuc_fbr__air','nuc_htemp__ot_fresh',
                       'nuc_htemp__ot_saline','nuc_htemp__cl_fresh','nuc_htemp__air']
    inv_cost =inv_cost[~inv_cost['technology'].isin(techs_to_remove)]
    # Converting the cost to USD/GW
    inv_cost['investment_USD_per_GW_mid'] = inv_cost['investment_million_USD_per_MW_mid'] * 1e3

    inv_cost = make_df('inv_cost',
                   technology = inv_cost['technology'],
                   value = inv_cost['investment_USD_per_GW_mid'],
                   unit = 'USD/GWa').pipe(same_node).pipe(broadcast,
                   node_loc= info.N[1:],year_vtg = info.Y)

    results['inv_cost']  = inv_cost

    # Addon conversion
    adon_df = input_cool.copy()
    # Add 'cooling_' before name of parent technologies that are type_addon
    # nomenclature
    adon_df['tech'] = 'cooling__' + adon_df['index'].astype(str)
    # technology : 'parent technology' and type_addon is type of addons such
    # as 'cooling__bio_hpl'
    addon_df = make_df('addon_conversion',
                       node=adon_df['node_loc'],
                       technology=adon_df['index'],
                       year_vtg=adon_df['year_vtg'],
                       year_act=adon_df['year_act'],
                       mode=adon_df['mode'],
                       time='year',
                       type_addon=adon_df['tech'],
                       value=adon_df['cooling_fraction'],
                       unit='GWa'
                      )

    results['addon_conversion'] = addon_df

    # Addon_lo will remain 1 for all cooling techs so it allows 100% activity of
    # parent technologies
    addon_lo = make_matched_dfs(addon_df,
                             addon_lo = 1)
    results['addon_lo']= addon_lo['addon_lo']

    # technical lifetime
    # make_matched_dfs didn't map all technologies
    #tl = make_matched_dfs(inv_cost,
    #                       technical_lifetime = 30)
    year = info.Y
    if 2010 in year:
        pass
    else:
        year.insert(0,2010)

    tl = make_df('technical_lifetime',
                 technology = inp['technology'].drop_duplicates(),
                 value = 30, unit = '-').pipe(broadcast,year_vtg = year,
                                              node_loc= info.N[1:]).pipe(same_node)

    results['technical_lifetime']= tl

    # Add output df  for freshwater & saline supply
    output_df = (
        make_df("output", technology='extract_freshwater_supply',
                value=1, unit='-',
                year_vtg = info.Y,
                year_act = info.Y, level = 'water_supply',
                commodity = 'freshwater_supply',
                mode = 'M1', time = 'year',
                time_dest = 'year', time_origin = 'year'
            )
            .pipe(broadcast, node_loc= info.N[1:])
            .pipe(same_node)
            )

    output_df = output_df.append(
        make_df("output", technology='extract_saline_supply',
                value=1, unit='-',
                year_vtg = info.Y,
                year_act = info.Y, level = 'water_supply',
                commodity = 'saline_supply_ppl',
                mode = 'M1', time = 'year',
                time_dest = 'year', time_origin = 'year'
            )
            .pipe(broadcast, node_loc= info.N[1:])
            .pipe(same_node)
            )

    results["output"]= output_df

    cap_fact = make_matched_dfs(inp,
                             capacity_factor = 1)
    results['capacity_factor'] = cap_fact['capacity_factor']
    #results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}
    return results

# Water use & electricity for non-cooling technologies

def non_cooling_tec(info):
    """
        Parameters
        ----------
        info : .ScenarioInfo
            Information about target Scenario.

        Returns
        -------
        data : dict of (str -> pandas.DataFrame)
            Keys are MESSAGE parameter names such as 'input', 'fix_cost'. Values
            are data frames ready for :meth:`~.Scenario.add_par`.

        This function returns input dataframe to assign water withdrawal for non
        cooling technologies
    """
    results = {}

    context = read_config()

    path = context.get_path("water", FILE)
    df = pd.read_csv(path)
    cooling_df = df.loc[df['technology_group'] == 'cooling']
    # Separate a column for parent technologies of respective cooling
    # techs
    cooling_df['parent_tech'] = cooling_df['technology_name'] \
        .apply(lambda x: pd.Series(str(x).split('__'))).drop(
        columns=1)
    non_cool_df = df[(df['technology_group'] != 'cooling') &
                 (df['water_supply_type'] == 'freshwater_supply')]

    non_cool_df['input_value'] =  non_cool_df['water_withdrawal_mid_m3_per_output'] *60*60*24*365*(1e-9)

    # Input dataframe for non cooling technologies
    # only water withdrawals are being taken
    # Only freshwater supply is assumed for simplicity
    inp_n_cool = make_df('input',
                         technology= non_cool_df['technology_name'],
                         value= non_cool_df['input_value'],
                         unit='km3/GWa',
                         level = 'water_supply',
                         commodity = 'freshwater_supply',
                         time_origin = 'year',
                         mode = 'M1',
                         time = 'year').pipe(broadcast, node_loc=info.N[1:],
                                             year_vtg = info.Y,year_act = info.Y).pipe(same_node)

    # append the input data to results
    results['input'] = inp_n_cool

    # technical lifetime
    year = info.Y
    if 2010 in year:
        pass
    else:
        year.insert(0,2010)

    tl = make_df('technical_lifetime',technology = inp_n_cool['technology'].drop_duplicates(),value = 20, unit = '-').pipe(broadcast,year_vtg = year, node_loc= info.N[1:]).pipe(same_node)

    results['technical_lifetime']= tl

    return results
