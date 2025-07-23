# -*- coding: utf-8 -*-
"""
Created on Fri Jul 18 15:51:14 2025

@author: shepard
"""

# Load config
config_name = "config.yaml"
full_path = package_data_path("bilateralize", config_name)
config_dir = os.path.dirname(full_path)
config = load_config(full_path)
   
# Load the scenario
mp = ixmp.Platform()
 
# Clone scenario
target_model = config.get("scenario", {}).get("target_model", [])
target_scen = config.get("scenario", {}).get("target_scen", [])
scen = message_ix.Scenario(mp, target_model, target_scen)
scen.set_as_default()

  
# Sandbox
df = trade_dict['LNG_shipped']['flow']['historical_activity']
teclist = list(df['technology'].unique())
basedf = scen.par('historical_activity', filters = {'technology': teclist})

with scen.transact('Add historical activity for LNG tanker'):
    scen.remove_par('historical_activity', basedf)
    scen.add_par('historical_activity', df)
      
gdx_location: str = os.path.join("H:", "script", "message_ix", "message_ix", "model", "data")
save_to_gdx(mp = mp,
            scenario = scen,
            output_path = Path(os.path.join(gdx_location, 'MsgData_'+ target_model + '_' + target_scen + '.gdx')))     
      
