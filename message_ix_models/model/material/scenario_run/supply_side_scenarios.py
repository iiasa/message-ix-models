from message_ix_models import ScenarioInfo
import ixmp
import message_ix
import pandas as pd

"""Infrastructure Supply Side Scenarios"""

def fuel_switch(scenario):

    s_info = ScenarioInfo(scenario)

    nodes = s_info.N
    yv_ya = s_info.yv_ya
    year_act=yv_ya.year_act
    nodes.remove("World")
    nodes.remove("R12_GLB")

    # Clinker substituion not allowed
    # CCS is not allowed across industry
    # Accelerated carbonation not allowed

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
                    'DUMMY_clay_supply_cement': ['M1'],
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

def fuel_switch_and_ccs(scenario):
    s_info = ScenarioInfo(scenario)

    nodes = s_info.N
    yv_ya = s_info.yv_ya
    year_act=yv_ya.year_act
    nodes.remove("World")
    nodes.remove("R12_GLB")

    # Clinker substituion not allowed
    # Both fuel switch and CCS is allowed (for now)

    scenario.check_out()

    for n in nodes:
        bound_activity = pd.DataFrame({
             "node_loc": n,
             "technology": 'DUMMY_clay_supply_cement',
             "year_act": year_act,
             "mode":  'M1',
             "time": 'year',
             "value": 0,
             "unit": 't'})
        scenario.add_par("bound_activity_lo", bound_activity)
        scenario.add_par("bound_activity_up", bound_activity)

    scenario.commit('Model changes made.')

def material_substituion(scenario):

    s_info = ScenarioInfo(scenario)

    nodes = s_info.N
    yv_ya = s_info.yv_ya
    year_act=yv_ya.year_act
    nodes.remove("World")
    nodes.remove("R12_GLB")

    # CCS is not allowed across industry
    # Accelerated carbonation not allowed
    # Material substituion in cement industry

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
