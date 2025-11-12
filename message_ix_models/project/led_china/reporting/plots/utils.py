import pandas as pd
from message_ix_models.util import package_data_path
from message_ix_models.tools.bilateralize.prepare_edit import load_config
import yaml

def load_all_configs():
    config, config_path = load_config(project_name = 'led_china', config_name = 'config.yaml')
    with open(package_data_path("led_china", "reporting", "plot_configs.yaml"), "r") as f:
        plot_config = yaml.safe_load(f)
        
    return config, plot_config

def pull_data(scenario_list: list[str],
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