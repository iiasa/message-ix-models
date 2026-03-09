# -*- coding: utf-8 -*-
"""
Add explicit re-exports
"""
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *

def adjust_reexports(base_scenario_name:str,
                     trade_commodity:str,
                     base_level:str):
    
    # Import scenario and models
    mp = ixmp.Platform()
    base_scenario = message_ix.Scenario(mp, model = 'weu_security', scenario = base_scenario_name)
    out_scenario_name = base_scenario_name + '_adjLev'
    out_scenario = base_scenario.clone(model = 'weu_security', scenario = out_scenario_name, keep_solution = False)

    # Output from domestic production should have updated level
    dom_prod = base_scenario.par('output', filters = {'commodity': trade_commodity,
                                                      'level': base_level})
    dom_prod = dom_prod[dom_prod['technology'].str.contains('_imp') == False]
    dom_prod_base = dom_prod.copy()
    dom_prod['level'] = base_level + '_1' # Update level

    # Create fuel balancing to move level from base_level_1 to base_level
    fb_input = base_scenario.par('input', filters = {'technology': 'coal_bal'}) # use coal as basis
    fb_input_base = fb_input.copy()
    fb_input['commodity'] = trade_commodity
    fb_input['level'] = base_level + '_1'
    fb_input['technology'] = trade_commodity + '_bal'

    fb_output = base_scenario.par('output', filters = {'technology': 'coal_bal'})
    fb_output_base = fb_output.copy()
    fb_output['commodity'] = trade_commodity
    fb_output['level'] = base_level
    fb_output['technology'] = trade_commodity + '_bal'

    # Add capacity factor for fuel balancing
    fb_cap = base_scenario.par('capacity_factor', filters = {'technology': 'coal_bal'})
    fb_cap['technology'] = trade_commodity + '_bal'

    # Add historical activity (TODO: Update values from coal_bal)
    fb_hact = base_scenario.par('historical_activity', filters = {'technology': 'coal_bal'})
    fb_hact['technology'] = trade_commodity + '_bal'
    
    # Update fuel export input to use base_level_1
    export_input = base_scenario.par('input', filters = {'commodity': trade_commodity,
                                                        'level': base_level})
    export_input = export_input[export_input['technology'].str.contains('_exp')]
    export_input_base = export_input.copy()
    export_input['level'] = base_level + '_1'

    # Add all back to scenario
    with out_scenario.transact("Add fuel balancing level set"):
        out_scenario.add_set("level", base_level + '_1')
        out_scenario.add_set("technology", trade_commodity + '_bal')
        
    with out_scenario.transact("Update output from domestic production"):
        out_scenario.remove_par('output', dom_prod_base)
        out_scenario.add_par('output', dom_prod)

    with out_scenario.transact("Update fuel balancing"):
        out_scenario.add_par('input', fb_input)
        out_scenario.add_par('output', fb_output)
        out_scenario.add_par('capacity_factor', fb_cap)
        out_scenario.add_par('historical_activity', fb_hact)
        
    with out_scenario.transact("Update export input"):
        out_scenario.remove_par('input', export_input_base)
        out_scenario.add_par('input', export_input)

    out_scenario.solve()
    mp.close_db()

def add_reexports(base_scenario_name:str,
                  covered_trade_technologies:list[str]):
    
    """Add explicit re-exports to the base scenario."""

    # Import scenario and models
    mp = ixmp.Platform()
    base_scenario = message_ix.Scenario(mp, model = 'weu_security', scenario = base_scenario_name)
    out_scenario_name = base_scenario_name + '_reexport'
    out_scenario = base_scenario.clone(model = 'weu_security', scenario = out_scenario_name, keep_solution = False)

    import_technologies = [c + '_imp' for c in covered_trade_technologies]
    import_output = base_scenario.par('output', filters = {'technology': import_technologies})

    reexport_input = import_output.copy()
    reexport_input['technology'] = reexport_input['technology'].str.replace('_imp', '_reexport')
    reexport_input = reexport_input.rename(columns = {'node_dest': 'node_origin',
                                                    'time_dest': 'time_origin'})
    reexport_input = reexport_input.drop_duplicates()

    export_technologies = base_scenario.par('input')['technology'].unique()
    export_technologies = [c for c in export_technologies if (any(sub in c for sub in covered_trade_technologies))&('_exp_' in c)]
    export_input = base_scenario.par('input', filters = {'technology': export_technologies})

    reexport_output = export_input.copy()
    reexport_output['technology'] = reexport_output['technology'].str.replace(r"_exp_.*", '_reexport', regex = True)
    reexport_output = reexport_output.rename(columns = {'node_origin': 'node_dest',
                                                        'time_origin': 'time_dest'})
    reexport_output = reexport_output.drop_duplicates()

    reexport_cf = base_scenario.par('capacity_factor', filters = {'technology': export_technologies})
    reexport_cf['technology'] = reexport_cf['technology'].str.replace(r"_exp_.*", '_reexport', regex = True)
    reexport_cf = reexport_cf.drop_duplicates()

    reexport_tl = base_scenario.par('technical_lifetime', filters = {'technology': export_technologies})
    reexport_tl['technology'] = reexport_tl['technology'].str.replace(r"_exp_.*", '_reexport', regex = True)
    reexport_tl = reexport_tl.drop_duplicates()

    reexport_technologies = set(list(reexport_input['technology'].unique()) +
                                    list(reexport_output['technology'].unique()))

    with out_scenario.transact("add reexport technology sets"):
        out_scenario.add_set("technology", list(reexport_technologies))

    with out_scenario.transact("add reexport input"):
        out_scenario.add_par("input", reexport_input)

    with out_scenario.transact("add reexport output"):
        out_scenario.add_par("output", reexport_output)

    with out_scenario.transact("add reexport capacity factor and technical lifetime"):
        out_scenario.add_par("capacity_factor", reexport_cf)
        out_scenario.add_par("technical_lifetime", reexport_tl)

    out_scenario.solve()
    mp.close_db()


adjust_reexports(base_scenario_name = "SSP2",
                 trade_commodity = 'lightoil',
                 base_level = 'secondary')

#covered_trade_technologies = ['loil_shipped', 'loil_piped']

#add_reexports(base_scenario_name = "SSP2_adjLev", 
#              covered_trade_technologies = covered_trade_technologies)