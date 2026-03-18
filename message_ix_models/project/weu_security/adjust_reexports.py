# -*- coding: utf-8 -*-
"""
Add explicit re-exports
"""
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *

def adjust_reexports(base_scenario,
                     trade_commodity_list:list,
                     base_level:str):
    
    for trade_commodity in trade_commodity_list:

        # Output from domestic production should have updated level
        dom_prod = base_scenario.par('output', filters = {'commodity': trade_commodity,
                                                          'level': base_level})
        dom_prod = dom_prod[dom_prod['technology'].str.contains('_imp') == False]
        dom_prod_base = dom_prod.copy()
        dom_prod['level'] = 'primary' #base_level + '_1' # Update level
        full_mode_list = dom_prod['mode'].unique()
    
        # Create fuel balancing to move level from base_level_1 to base_level
        fb_input = base_scenario.par('input', filters = {'technology': 'coal_bal'}) # use coal as basis
        fb_input['commodity'] = trade_commodity
        fb_input['level'] = 'primary' #base_level + '_1'
        fb_input['technology'] = trade_commodity + '_bal'

        fb_input_out = pd.DataFrame()
        for m in full_mode_list:
            tdf = fb_input.copy()
            tdf['mode'] = m
            fb_input_out = pd.concat([fb_input_out, tdf])
    
        fb_output = base_scenario.par('output', filters = {'technology': 'coal_bal'})
        fb_output['commodity'] = trade_commodity
        fb_output['level'] = base_level
        fb_output['technology'] = trade_commodity + '_bal'
    
        fb_output_out = pd.DataFrame()
        for m in full_mode_list:
            tdf = fb_output.copy()
            tdf['mode'] = m
            fb_output_out = pd.concat([fb_output_out, tdf])
    
        # Add capacity factor for fuel balancing
        fb_cap = base_scenario.par('capacity_factor', filters = {'technology': 'coal_bal'})
        fb_cap['technology'] = trade_commodity + '_bal'
    
        # Update fuel export input to use base_level_1
        export_input = base_scenario.par('input', filters = {'commodity': trade_commodity,
                                                            'level': base_level})
        export_input = export_input[export_input['technology'].str.contains('_exp')]
        export_input_base = export_input.copy()
        export_input['level'] = 'primary' #base_level + '_1'
    
        # Add all back to scenario
        with base_scenario.transact("Add fuel balancing level set"):
            # base_scenario.add_set("level", base_level + '_1')
            base_scenario.add_set("technology", trade_commodity + '_bal')
            
        with base_scenario.transact("Update output from domestic production"):
            base_scenario.remove_par('output', dom_prod_base)
            base_scenario.add_par('output', dom_prod)
    
        with base_scenario.transact("Update fuel balancing"):
            base_scenario.add_par('input', fb_input_out)
            base_scenario.add_par('output', fb_output_out)
            base_scenario.add_par('capacity_factor', fb_cap)
            
        with base_scenario.transact("Update export input"):
            base_scenario.remove_par('input', export_input_base)
            base_scenario.add_par('input', export_input)



