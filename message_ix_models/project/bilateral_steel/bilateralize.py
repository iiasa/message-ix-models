# -*- coding: utf-8 -*-
"""
Bilateralize base scenarios for gas security analysis
"""
# Import packages
from message_ix_models.tools.bilateralize.prepare_edit import *
from message_ix_models.tools.bilateralize.bare_to_scenario import *
from message_ix_models.tools.bilateralize.load_and_solve import *
from message_ix_models.project.bilateral_steel.steel_yearbook import *

import os
from ixmp import Platform

# Import scenario and models
config, config_path = load_config(project_name = 'bilateral_steel', config_name = 'config.yaml')
models_scenarios = config['models_scenarios']
data_path = package_data_path("bilateralize")

# Clear bare files
print("Clearing bare files")
for tec in config['covered_trade_technologies']:
    if os.path.exists(os.path.join(data_path, tec, "bare_files")):
       for file in os.listdir(os.path.join(data_path, tec, "bare_files")):
            if os.path.isfile(os.path.join(data_path, tec, "bare_files", file)):
               os.remove(os.path.join(data_path, tec, "bare_files", file))
    if os.path.exists(os.path.join(data_path, tec, "bare_files", "flow_technology")):
        for file in os.listdir(os.path.join(data_path, tec, "bare_files", "flow_technology")):
            if os.path.isfile(os.path.join(data_path, tec, "bare_files", "flow_technology", file)):
                os.remove(os.path.join(data_path, tec, "bare_files", "flow_technology", file))
        
# Generate edit files
prepare_edit_files(project_name = 'bilateral_steel', 
                   config_name = 'config.yaml',
                   P_access = True,)
                   #reimport_BACI = True)

# Generate scenario
trade_dict = bare_to_scenario(project_name = 'bilateral_steel', 
                              config_name = 'config.yaml',
                              p_drive_access = True)

# Historical calibration for shipped steel
p_drive = config["p_drive_location"]
steel_yearbook_path = os.path.join(p_drive, "MESSAGE_trade", "Steel", "Steel-Statistical-Yearbook-2025.pdf")

hist_activity = trade_dict['steel_shipped']['trade']['historical_activity']
hist_activity['Exporter'] = hist_activity['node_loc']
hist_activity['Importer'] = "R12_" + hist_activity['technology'].str.replace('steel_shipped_exp_', '').str.upper()
hist_activity['Year'] = hist_activity['year_act']
hist_activity['Value'] = hist_activity['value']
hist_activity['Unit'] = hist_activity['unit']
hist_activity = hist_activity[['Exporter', 'Importer', 'Year', 'Value', 'Unit']]
hist_activity['Source'] = 'MESSAGEix'

gross_exports = hist_activity.groupby(['Exporter', 'Year', 'Unit'])['Value'].sum().reset_index()
gross_imports = hist_activity.groupby(['Importer', 'Year', 'Unit'])['Value'].sum().reset_index()
gross_imports['Value'] *= -1
net_exports = pd.concat([gross_exports, gross_imports])
net_exports = net_exports.groupby(['Exporter', 'Year', 'Unit'])['Value'].sum().reset_index()
net_exports['Source'] = 'MESSAGEix'

# Compute net trade based on export import tables
#df_steel_trade = get_net_trade(steel_yearbook_path, [49, 50, 51], [52, 53, 54])
#df_steel_trade = gen_r12_data(df_steel_trade)
#df_steel_trade = df_steel_trade.reset_index()

#df_steel_use = pd.DataFrame()
#for y in df_steel_trade.columns[1:]:
#    ydf = df_steel_trade[["R12", y]]
#    ydf = ydf.rename(columns={y: 'Value'})
#    ydf['Year'] = y
#    ydf['Exporter'] = ydf['R12']
#    ydf['Unit'] = 'Thousand tonnes'
#    ydf = ydf[['Exporter', 'Year', 'Value', 'Unit']]
#    df_steel_use = pd.concat([df_steel_use, ydf])

#df_steel_use['Value'] = df_steel_use['Value']/1000
#df_steel_use['Unit'] = 'Mt'
#df_steel_use['Source'] = 'Steel Statistical Yearbook 2025'

#net_exports = net_exports[df_steel_use.columns]
#df_steel_out = pd.concat([net_exports, df_steel_use])
#df_steel_out.to_csv(package_data_path("bilateral_steel", "historical_calibration.csv"), index=False)

print("Setting up scenario")
mp = ixmp.Platform()
mp.add_unit("USD/Mt")

load_and_solve(trade_dict = trade_dict,
               solve = True,
               project_name = 'bilateral_steel', 
               config_name = 'config.yaml', 
               start_model = models_scenarios['SSP2']['model'],
               start_scen = models_scenarios['SSP2']['scenario'],
               target_model = 'steel_trade',
               target_scen = 'only_steel')