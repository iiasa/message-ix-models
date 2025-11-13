import pandas as pd
from message_ix_models.util import package_data_path
from message_ix_models.tools.bilateralize.prepare_edit import load_config
import yaml
import numpy as np

def load_all_configs():
    config, config_path = load_config(project_name = 'led_china', config_name = 'config.yaml')
    with open(package_data_path("led_china", "reporting", "plot_configs.yaml"), "r") as f:
        plot_config = yaml.safe_load(f)
        
    return config, plot_config

def pull_legacy_data(scenario_list: list[str],
              plot_type: str,
              plot_config: dict):
    outdf = pd.DataFrame()
    for model in scenario_list:
        df = pd.read_excel(package_data_path("led_china", "reporting",
                                            "legacy", f"china_security_{model}.xlsx"))
        df_long = pd.melt(df, 
                        id_vars=['Model','Region','Scenario','Unit','Variable'], 
                        var_name='Year', 
                        value_name='Value')
        df_long['Year'] = df_long['Year'].astype(int)

        df_long = df_long[df_long['Variable'].isin(plot_config[plot_type]['iamc_vars'])]

        df_long_var = df_long['Variable'].str.split('|', expand = True)
        df_long_var.columns = ['Key', 'Source']
        df_long = pd.concat([df_long, df_long_var], axis = 1)
        
        df_long['Scenario'] = df_long['Scenario'].str.replace('_', ' ')
        
        outdf = pd.concat([outdf, df_long])

    return outdf

def pull_trade_data(scenario_list: list[str],
                    trade_type: str,
                    plot_config: dict):
    outdf = pd.DataFrame()
    for model in scenario_list:
        df = pd.read_csv(package_data_path("led_china", "reporting",
                                            trade_type, f"china_security_{model}.csv"))
        df = df.drop(columns = ['T'])

        df_long = pd.melt(df, 
                        id_vars=['Model','Region','Scenario','Unit','Variable'], 
                        var_name='Year', 
                        value_name='Value')
        df_long['Year'] = df_long['Year'].astype(int)

        #df_long = df_long[df_long['Variable'].isin(plot_config[trade_type]['iamc_vars'])]

        df_long_var = df_long['Variable'].str.split('|', expand = True)
        if trade_type == 'gross_exports':
            region_col = 'Importer'
        elif trade_type == 'gross_imports':
            region_col = 'Exporter'

        df_long_var.columns = ['Variable', region_col, 'Commodity']
        df_long = pd.concat([df_long, df_long_var], axis = 1)
        
        commodity_dict = {'Biomass': 'Biomass',
                          'Coal': 'Coal',
                          'Crude Oil': 'Crudeoil',
                          'Light Oil': 'Lightoil',
                          'Fuel Oil': 'Fueloil',
                          'LNG': 'Lng',
                          'Ethanol': 'Ethanol',
                          'Liquid H2': 'Lh2',
                          'Pipeline Gas': 'Gas'}

        source_dict = {'Biomass': ['Biomass'],
                        'Coal': ['Coal'],
                        'Oil': ['Crudeoil', 'Lightoil', 'Fueloil'],
                        'Ethanol': ['Ethanol'],
                        'Liquid H2': ['Lh2'],
                        'Gas': ['Gas', 'Lng']}
        df_long['Source'] = ''
        for s in source_dict.keys():
            df_long['Source'] = np.where(df_long['Commodity'].isin(source_dict[s]), s, df_long['Source'])
        for c in commodity_dict.keys():
            df_long['Commodity'] = np.where(df_long['Commodity'] == commodity_dict[c], c, df_long['Commodity'])

        df_long['Scenario'] = df_long['Scenario'].str.replace('_', ' ')
        df_long = df_long.drop_duplicates()

        df_long['Value'] = df_long['Value'] * 8760 * (3.6e-6) # Convert GWa to EJ/yr
        df_long['Unit'] = 'EJ/yr'
        df_long['Region'] = df_long['Region'].str.replace('R12_', '')

        outdf = pd.concat([outdf, df_long])

    return outdf

def calc_net_imports(scenario_list: list[str],
                     plot_config: dict):
    
    importdf = pull_trade_data(scenario_list = scenario_list,
                              trade_type = 'gross_imports',
                              plot_config = plot_config)
    exportdf = pull_trade_data(scenario_list = scenario_list,
                              trade_type = 'gross_exports',
                              plot_config = plot_config)
    importdf = importdf.rename(columns = {'Exporter': 'Partner'})
    exportdf = exportdf.rename(columns = {'Importer': 'Partner'})
    tradedf = importdf.merge(exportdf, on = ['Model', 'Region', 'Scenario',
                                             'Partner', 'Year', 'Source',
                                             'Commodity', 'Unit'], how = 'left')
    tradedf['Net Imports'] = tradedf['Value_x'] - tradedf['Value_y']
    tradedf['Variable'] = "Net Imports|" + tradedf['Partner'] + "|" + tradedf['Source']
    tradedf = tradedf.rename(columns = {'Partner': 'Exporter',
                                        'Value_x': 'Gross Imports',
                                        'Value_y': 'Gross Exports'})

    tradedf = tradedf.drop(columns = ['Variable_x', 'Variable_y']).reset_index(drop = True)

    return tradedf