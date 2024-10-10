from message_ix_models import ScenarioInfo
import ixmp
import message_ix
import pandas as pd

"""Infrastructure Supply Side Measures"""

def industry_sector_net_zero_targets(scenario):

    # Add iron and steel net zero target

    s_info = ScenarioInfo(scenario)

    scenario.check_out()

    # Remove the technology diffusion constraints

    remove_years = [2035, 2040, 2045, 2050, 2055, 2060, 2070, 2080, 2090, 2100,2110]
    remove_growth_activity_up = scenario.par("growth_activity_up",
    filters={'technology':['dri_gas_steel','dri_h2_steel', 'eaf_steel'],
    'year_act': remove_years})
    remove_initial_activity_up = scenario.par("initial_activity_up",
    filters={'technology':['dri_gas_steel','dri_h2_steel', 'eaf_steel'],
    'year_act': remove_years})

    scenario.remove_par('growth_activity_up', remove_growth_activity_up)
    scenario.remove_par('initial_activity_up', remove_initial_activity_up)

    # To fix: To facilitate gradual phase out, add back the phasing down constraints

    remove_growth_activity_lo = scenario.par("growth_activity_lo",
    filters={'technology':['bof_steel'], 'year_act': remove_years})
    remove_initial_activity_lo = scenario.par("initial_activity_lo",
    filters={'technology':['bof_steel'], 'year_act': remove_years})
    scenario.remove_par('growth_activity_lo', remove_growth_activity_lo)
    scenario.remove_par('initial_activity_lo', remove_initial_activity_lo)

    remove_soft_activity_up = scenario.par("soft_activity_up",
    filters={'technology':['eaf_steel'], 'year_act': remove_years})
    remove_soft_activity_lo = scenario.par("soft_activity_lo",
    filters={'technology':['bof_steel'], 'year_act': remove_years})
    abs_cost_soft_up = scenario.par("abs_cost_activity_soft_up",
    filters={'technology':['eaf_steel', 'bof_steel'], 'year_act': remove_years})
    level_cost_soft_up = scenario.par("level_cost_activity_soft_up",
    filters={'technology':['eaf_steel', 'bof_steel'], 'year_act': remove_years})

    scenario.remove_par('soft_activity_up', remove_soft_activity_up)
    scenario.remove_par('soft_activity_lo', remove_soft_activity_lo)
    scenario.remove_par('abs_cost_activity_soft_up', abs_cost_soft_up)
    scenario.remove_par('level_cost_activity_soft_up', level_cost_soft_up)

    growth_new_capacity_up = scenario.par("growth_new_capacity_up",
    filters={'technology':['dri_gas_ccs_steel', 'bf_ccs_steel'],
    'year_vtg':remove_years})

    scenario.remove_par('growth_new_capacity_up', growth_new_capacity_up)

    initial_new_capacity_up = scenario.par("initial_new_capacity_up",
    filters={'technology':['dri_gas_ccs_steel', 'bf_ccs_steel'],
    'year_vtg': remove_years})

    scenario.remove_par('initial_new_capacity_up', initial_new_capacity_up)

    # Add net-zero relation
    # Note: In updated SSP implementaiton, 'CO2_Emission' does not exist.
    # The negative coefficients should be read from output parameter.

    co2_ind = scenario.par('relation_activity',
    filters = {'relation':'CO2_ind','technology':["DUMMY_coal_supply",
                                                  "DUMMY_gas_supply"]})

    co2_emi = scenario.par('relation_activity',
    filters = {'relation':'CO2_Emission','technology':["dri_gas_ccs_steel",
                                                        "bf_ccs_steel",]})

    rel_new = pd.concat([co2_ind, co2_emi], ignore_index=True)
    rel_new = rel_new[rel_new['year_rel']>=2070]

    rel_new['node_rel'] = 'R12_GLB'
    rel_new['relation'] = 'steel_sector_target'

    scenario.add_set('relation', 'steel_sector_target')

    # Need to add slack values here. Emissions do not go to zero.
    relation_upper_df =  pd.DataFrame({
    "relation": 'steel_sector_target',
    "node_rel": 'R12_GLB',
    "year_rel": [2070, 2080, 2090, 2100],
    "value": [2.7, 2.5, 2.1, 1.8],
    "unit": "???"
    })

    # relation_lower_df =  pd.DataFrame({
    # "relation": 'steel_sector_target',
    # "node_rel": 'R12_GLB',
    # "year_rel": [2070, 2080, 2090, 2100],
    # "value": 0,
    # "unit": "???"
    # })

    scenario.add_par('relation_activity', rel_new)
    scenario.add_par('relation_upper', relation_upper_df)
    # scenario.add_par('relation_lower', relation_lower_df)

    scenario.commit('Steel sector target added.')


def no_substitution(scenario):

    # Clinker substituion not allowed
    s_info = ScenarioInfo(scenario)

    nodes = s_info.N
    yv_ya = s_info.yv_ya
    year_act=yv_ya.year_act
    nodes.remove("World")
    nodes.remove("R12_GLB")

    scenario.check_out()

    for n in nodes:
        bound_activity_clay = pd.DataFrame({
             "node_loc": n,
             "technology": 'DUMMY_clay_supply_cement',
             "year_act": year_act,
             "mode": 'M1',
             "time": 'year',
             "value": 0,
             "unit": 't'})
        scenario.add_par("bound_activity_lo", bound_activity_clay)
        scenario.add_par("bound_activity_up", bound_activity_clay)

        bound_activity_lignin = pd.DataFrame({
             "node_loc": n,
             "technology": 'bitumen_production',
             "year_act": year_act,
             "mode": 'M2',
             "time": 'year',
             "value": 0,
             "unit": 't'})
        scenario.add_par("bound_activity_lo", bound_activity_lignin)
        scenario.add_par("bound_activity_up", bound_activity_lignin)

    scenario.commit('Model changes made.')

def no_ccs(scenario):

    # CCS is not allowed across industry
    # Accelerated carbonation not allowed, 'recycling_cement':['M2']

    s_info = ScenarioInfo(scenario)

    nodes = s_info.N
    yv_ya = s_info.yv_ya
    year_act=yv_ya.year_act
    nodes.remove("World")
    nodes.remove("R12_GLB")

    technologies = {'bf_ccs_steel': ['M2'],
                    'dri_gas_ccs_steel': ['M1'],
                    'clinker_wet_ccs_cement': ['M1'],
                    'clinker_dry_ccs_cement': ['M1'],
                    'meth_bio_ccs': ['fuel', 'feedstock'],
                    'meth_coal_ccs': ['fuel', 'feedstock'],
                    'meth_ng_ccs': ['fuel', 'feedstock'],
                    'gas_NH3_ccs': ['M1'],
                    'coal_NH3_ccs': ['M1'],
                    'biomass_NH3_ccs': ['M1'],
                    'fueloil_NH3_ccs': ['M1'],
                    'recycling_cement':['M2']
                    }

    scenario.check_out()

    for key, value in technologies.items():
        for n in nodes:
            for y in year_act:
                bound_activity = pd.DataFrame({
                     "node_loc": n,
                     "technology": key,
                     "year_act": y,
                     "mode": value,
                     "time": 'year',
                     "value": 0,
                     "unit": 't'})
                scenario.add_par("bound_activity_lo", bound_activity)
                scenario.add_par("bound_activity_up", bound_activity)

    scenario.commit('Model changes made.')

def increased_recycling(scenario):

    s_info = ScenarioInfo(scenario)

    nodes = s_info.N
    yv_ya = s_info.yv_ya
    year_act=yv_ya.year_act
    nodes.remove("World")
    nodes.remove("R12_GLB")

    # IRON & STEEL AND ALUMINUM
    # *************************
    # Increase maximum allowed recycling
    relation_recycling = scenario.par("relation_activity",
                        filters = {"relation": ["maximum_recycling_aluminum",
                                                "max_regional_recycling_steel"],
                                    "technology": ["total_EOL_steel",
                                                    "total_EOL_aluminum"]})
    relation_recycling['value'] = -0.98

    # Lower the recycling costs

    recycling_costs_steel = scenario.par("var_cost",
                      filters = {"technology": ["prep_secondary_steel_1"]})
    recycling_costs_alu = scenario.par("var_cost",
                      filters = {"technology": ["prep_secondary_aluminum_1"]})

    recycling_costs_steel_remove = scenario.par("var_cost",
                      filters = {"technology": ["prep_secondary_steel_2",
                                                "prep_secondary_steel_3"]})
    recycling_costs_alu_remove = scenario.par("var_cost",
                      filters = {"technology": ["prep_secondary_aluminum_2",
                                                "prep_secondary_aluminum_3"]})

    recycling_costs_steel_2 = recycling_costs_steel.copy()
    recycling_costs_steel_2["technology"] = "prep_secondary_steel_2"

    recycling_costs_steel_3 = recycling_costs_steel.copy()
    recycling_costs_steel_3["technology"] = "prep_secondary_steel_3"

    recycling_costs_alu_2 = recycling_costs_alu.copy()
    recycling_costs_alu_2["technology"] = "prep_secondary_aluminum_2"

    recycling_costs_alu_3 = recycling_costs_alu.copy()
    recycling_costs_alu_3["technology"] = "prep_secondary_aluminum_3"

    scenario.check_out()

    scenario.remove_par("var_cost", recycling_costs_steel_remove)
    scenario.remove_par("var_cost", recycling_costs_alu_remove)
    scenario.add_par("relation_activity", relation_recycling)
    scenario.add_par("var_cost", recycling_costs_steel_2)
    scenario.add_par("var_cost", recycling_costs_steel_3)
    scenario.add_par("var_cost", recycling_costs_alu_2)
    scenario.add_par("var_cost", recycling_costs_alu_3)

    # CONCRETE
    # *************************
    # Increase maximum allowed recycling

    relation_recycling_concrete = scenario.par("relation_activity",
                        filters = {"relation": ["max_regional_recycling_cement"],
                                    "technology": ['concrete_production_cement']})
    # 70% (normal share of aggregates that go into concrete production) *
    # 60% (share of primary aggregates that can be replaced by secondary) *
    # (20% Other + 15% plain concrete + 12.5% reinforced concrete + 30% mortar)
    relation_recycling_concrete['value'] = -0.3255
    scenario.add_par("relation_activity", relation_recycling_concrete)

    scenario.commit("Increased recycling limits")

def limit_asphalt_recycling(scenario):

    # Increased recycling modes (M4,M5) are not allowed.
    # M4: Bitumen same, 50% aggregates replaces with RAP
    # M5: Bitumen reduced via rejuvenator agents, 90% RAP

    s_info = ScenarioInfo(scenario)

    nodes = s_info.N
    yv_ya = s_info.yv_ya
    year_act=yv_ya.year_act
    nodes.remove("World")
    nodes.remove("R12_GLB")

    scenario.check_out()

    for n in nodes:
        for y in year_act:
            bound_activity = pd.DataFrame({
                 "node_loc": n,
                 "technology": 'asphalt_mixing',
                 "year_act": y,
                 "mode": ['M4', 'M5'],
                 "time": 'year',
                 "value": 0,
                 "unit": 't'})
            scenario.add_par("bound_activity_lo", bound_activity)
            scenario.add_par("bound_activity_up", bound_activity)

    scenario.commit('Model changes made.')

def keep_fuel_share(scenario):
    # Fuel shares are calculated based on baseline demand, defaut supply scenario
    # "df_ratios_final.xlsx" is used, produced by shares_of_current_fuel.ipynb

    # Add share constraints

    df = pd.read_excel(package_data_path("material", "infrastructure",
    'df_ratios_final.xlsx'))

    df = df.drop(columns=['Unnamed: 0'])
    df = df[df['Region'] != 'World']

    sectors = ['Steel', 'Non-Metallic Minerals', 'Non-Ferrous Metals']

    for s in sectors:
        df_sector = df[df['sector']==s]

        if s == 'Steel':

            s_name = 'Steel'
            scenario.check_out()

            type_tec_tot = 'all_tec_' + s_name

            total_tech = ['bf_steel','bof_steel', 'finishing_steel',
                          'pellet_steel', 'sinter_steel', 'dri_gas_steel',
                          'dri_h2_steel', 'cokeoven_steel', 'bf_ccs_steel',
                          'dri_gas_ccs_steel', 'bf_biomass_steel',
                          'slag_granulator_steel', 'eaf_steel', 'prod_charcoal_steel']

            scenario.add_cat('technology', type_tec_tot, total_tech)

            scenario.commit('Added')

        if s == 'Non-Metallic Minerals':

            s_name = "Cement"

            scenario.check_out()

            type_tec_tot = 'all_tec_' + s_name

            total_tech = ["furnace_foil_cement", "furnace_loil_cement",
                          "furnace_biomass_cement", "furnace_gas_cement",
                          "furnace_coal_cement", "furnace_elec_cement"]

            scenario.add_cat('technology', type_tec_tot, total_tech)

            scenario.commit('Added')

        if s == 'Non-Ferrous Metals':

            s_name = "Aluminum"

            scenario.check_out()

            type_tec_tot = 'all_tec_' + s_name

            total_tech = ["furnace_foil_aluminum", "furnace_loil_aluminum",
                          "furnace_biomass_aluminum", "furnace_gas_aluminum",
                          "furnace_coal_aluminum", "furnace_elec_aluminum",
                          'soderberg_aluminum', 'prebake_aluminum']

            scenario.add_cat('technology', type_tec_tot, total_tech)

            scenario.commit('Added')

        for n in df_sector['Region'].unique():
            for f in df_sector['fuel'].unique():

                scenario.check_out()

                shr_const = 'fuel_share_' + s_name + '_' + n + '_' + f

                scenario.add_set('shares', shr_const)

                type_tec_shr = 'share_tec_' + s_name + '_' + f

                scenario.commit('Added')

                if s == 'Steel':
                    if f == 'Electricity':
                        scenario.check_out()
                        share_tech = ['bf_steel', 'bof_steel', 'eaf_steel',
                                      'finishing_steel', 'pellet_steel',
                                      'sinter_steel', 'dri_gas_steel',
                                      'dri_h2_steel','bf_ccs_steel',
                                      'dri_gas_ccs_steel', 'bf_biomass_steel',
                                      'slag_granulator_steel']
                        scenario.add_cat('technology', type_tec_shr, share_tech)
                        c = 'electr'
                    elif f == 'Gases':
                        scenario.check_out()
                        share_tech = ['eaf_steel', 'dri_gas_steel']
                        scenario.add_cat('technology', type_tec_shr, share_tech)
                        c = 'gas'
                    elif f == 'Biomass':
                        scenario.check_out()
                        share_tech = ['prod_charcoal_steel']
                        scenario.add_cat('technology', type_tec_shr, share_tech)
                        c = 'charcoal'
                    elif f == 'Coal':
                        scenario.check_out()
                        share_tech = ['bf_steel', 'cokeoven_steel', 'sinter_steel']
                        scenario.add_cat('technology', type_tec_shr, share_tech)
                        c = 'coal'
                    else:
                        continue

                    df_share = pd.DataFrame({'shares': shr_const,
                           'node_share': 'R12_'+ n,
                           'node': 'R12_'+ n,
                           'type_tec': type_tec_shr,
                           'mode': ['M1', 'M2', 'M3', 'M4'],
                           'commodity': c,
                           'level': 'final'})
                    scenario.add_set('map_shares_commodity_share', df_share)
                    scenario.add_cat('technology', type_tec_shr, share_tech)

                    for m in ['M1', 'M2', 'M3', 'M4']:
                        df_total = pd.DataFrame({'shares': shr_const,
                                'node_share': 'R12_'+ n,
                                'node': 'R12_'+ n,
                                'type_tec': type_tec_tot,
                                'mode': m,
                                'commodity': ['coal', 'gas', 'electr', 'charcoal'],
                                'level': 'final'})
                        scenario.add_set('map_shares_commodity_total', df_total)

                    # Add lower bound share constraint for the end of the century
                    value_df = df_sector[(df_sector['Region'] == n) & (df_sector['fuel'] == f)
                    & (df_sector['sector'] == 'Steel')]
                    val = value_df['Fuel_Ratio'].values[0]

                    df_up = pd.DataFrame({'shares': shr_const,
                               'node_share': 'R12_'+ n,
                               'year_act': [2025, 2030, 2035, 2040, 2045, 2050,\
                               2055, 2060, 2070,2080,2090,2100,2110],
                               'time': 'year',
                               'value': val,
                               'unit': '%'})

                    scenario.add_par('share_commodity_up', df_up)

                    scenario.commit('Share constraints added')

                if s == 'Non-Metallic Minerals':
                    if f == 'Electricity':
                        scenario.check_out()
                        share_tech = ['furnace_elec_cement']
                        scenario.add_cat('technology', type_tec_shr, share_tech)
                    elif f == 'Gases':
                        scenario.check_out()
                        share_tech = ['furnace_gas_cement']
                        scenario.add_cat('technology', type_tec_shr, share_tech)
                    elif f == 'Biomass':
                        scenario.check_out()
                        share_tech = ['furnace_biomass_cement']
                        scenario.add_cat('technology', type_tec_shr, share_tech)
                    elif f == 'Coal':
                        scenario.check_out()
                        share_tech = ['furnace_coal_cement']
                        scenario.add_cat('technology', type_tec_shr, share_tech)
                    elif f == 'Oil':
                        scenario.check_out()
                        share_tech = ['furnace_foil_cement', 'furnace_loil_cement']
                        scenario.add_cat('technology', type_tec_shr, share_tech)

                    df_share = pd.DataFrame({'shares': shr_const,
                           'node_share': 'R12_'+ n,
                           'node': 'R12_'+ n,
                           'type_tec': type_tec_shr,
                           'mode': 'high_temp',
                           'commodity': 'ht_heat',
                           'level': 'useful_cement'}, index = [0])
                    scenario.add_set('map_shares_commodity_share', df_share)
                    scenario.add_cat('technology', type_tec_shr, share_tech)

                    df_total = pd.DataFrame({'shares': shr_const,
                            'node_share': 'R12_'+ n,
                            'node': 'R12_'+ n,
                            'type_tec': type_tec_tot,
                            'mode': 'high_temp',
                            'commodity': 'ht_heat',
                            'level': 'useful_cement'}, index = [0])
                    scenario.add_set('map_shares_commodity_total', df_total)

                    # Add lower bound share constraint for the end of the century
                    value_df = df_sector[(df_sector['Region'] == n) & \
                    (df_sector['fuel'] == f) & (df_sector['sector'] == 'Non-Metallic Minerals')]
                    val = value_df['Fuel_Ratio'].values[0]

                    df_up = pd.DataFrame({'shares': shr_const,
                               'node_share': 'R12_'+ n,
                               'year_act': [2025, 2030, 2035, 2040, 2045, 2050,\
                               2055, 2060, 2070,2080,2090,2100,2110],
                               'time': 'year',
                               'value': val,
                               'unit': '%'})

                    scenario.add_par('share_commodity_up', df_up)

                    scenario.commit('Share constraints added')


                if s == 'Non-Ferrous Metals':
                    if f == 'Electricity':
                        scenario.check_out()
                        share_tech = ['furnace_elec_aluminum', 'soderberg_aluminum',
                        'prebake_aluminum']
                        scenario.add_cat('technology', type_tec_shr, share_tech)
                        c = ['electr']
                    elif f == 'Gases':
                        scenario.check_out()
                        share_tech = ['furnace_gas_aluminum']
                        scenario.add_cat('technology', type_tec_shr, share_tech)
                        c= ['gas']
                    elif f == 'Biomass':
                        scenario.check_out()
                        share_tech = ['furnace_biomass_aluminum']
                        scenario.add_cat('technology', type_tec_shr, share_tech)
                        c = ['biomass']
                    elif f == 'Coal':
                        scenario.check_out()
                        share_tech = ['furnace_coal_aluminum']
                        scenario.add_cat('technology', type_tec_shr, share_tech)
                        c = ['coal']
                    elif f == 'Oil':
                        scenario.check_out()
                        share_tech = ['furnace_foil_aluminum', 'furnace_loil_aluminum']
                        scenario.add_cat('technology', type_tec_shr, share_tech)
                        c = ['fueloil', 'lightoil']

                    for m in ['high_temp', 'low_temp', 'M1']:
                        df_share = pd.DataFrame({'shares': shr_const,
                               'node_share': 'R12_'+ n,
                               'node': 'R12_'+ n,
                               'type_tec': type_tec_shr,
                               'mode': m,
                               'commodity': c,
                               'level': 'final'})
                        scenario.add_set('map_shares_commodity_share', df_share)
                        scenario.add_cat('technology', type_tec_shr, share_tech)

                        df_total = pd.DataFrame({'shares': shr_const,
                                'node_share': 'R12_'+ n,
                                'node': 'R12_'+ n,
                                'type_tec': type_tec_tot,
                                'mode': m,
                                'commodity': ['electr', 'biomass', 'fueloil',
                                'lightoil', 'coal', 'gas'],
                                'level': 'final'})
                        scenario.add_set('map_shares_commodity_total', df_total)

                    # Add lower bound share constraint for the end of the century
                    value_df = df_sector[(df_sector['Region'] == n) & \
                    (df_sector['fuel'] == f) & (df_sector['sector'] == 'Non-Ferrous Metals')]
                    val = value_df['Fuel_Ratio'].values[0]

                    df_up = pd.DataFrame({'shares': shr_const,
                               'node_share': 'R12_'+ n,
                               'year_act': [2025, 2030, 2035, 2040, 2045, 2050, \
                               2055, 2060, 2070,2080,2090,2100,2110],
                               'time': 'year',
                               'value': val,
                               'unit': '%'})

                    scenario.add_par('share_commodity_up', df_up)

                    scenario.commit('Share constraints added')
