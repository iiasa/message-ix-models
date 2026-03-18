# -*- coding: utf-8 -*-
"""
Add explicit re-exports
"""
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *
from message_ix_models.project.weu_security.adjust_reexports import *

def add_reexports(base_scenario_name:str,
                  covered_trade_technologies:list[str]):
    
    """Add explicit re-exports to the base scenario."""

    # Import scenario and models
    mp = ixmp.Platform()
    base_scenario = message_ix.Scenario(mp, model = 'weu_security', scenario = base_scenario_name)
    out_scenario_name = base_scenario_name
    out_scenario = base_scenario.clone(model = 'weu_security_reexports', scenario = out_scenario_name, keep_solution = False)

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
    
# Run re-export sensitivities
covered_trade_technologies = ['biomass_shipped', 'coal_shipped',
                              'crudeoil_shipped', 'crudeoil_piped',
                              'foil_shipped', 'foil_piped',
                              'LNG_shipped', 'gas_piped',
                              'loil_shipped', 'loil_piped',]

for scen in ['SSP2', 'FSU2040', 'FSU2100', 
             'SSP2_NAMboost', 'FSU2040_NAMboost', 'FSU2100_NAMboost',
             'SSP2_MEACON', 'FSU2040_MEACON', 'FSU2100_MEACON']:
    add_reexports(base_scenario_name = scen, 
                  covered_trade_technologies = covered_trade_technologies)
