import pandas as pd
import numpy as np

# Update scenario names
def update_scenario_names(df:pd.DataFrame) -> pd.DataFrame:
    update_scenario_names = {'SSP2': 'REF', 
                            'FSU2100': 'FSULONG',}
    for sn in update_scenario_names.keys():
        df['scenario'] = np.where(df['scenario'] == sn, update_scenario_names[sn], df['scenario'])

    df['scenario'] = np.where(df['scenario'].str.contains("SSP2_"),
                                            df['scenario'].str.replace("SSP2_", "REF_"),
                                            df['scenario'])
    df['scenario'] = np.where(df['scenario'].str.contains("FSU2100"),
                                        df['scenario'].str.replace("FSU2100", "FSULONG"),
                                        df['scenario'])
    df['scenario'] = np.where(df['scenario'].str.contains("MEACON"),
                                        df['scenario'].str.replace("MEACON_", "MEACON"),
                                        df['scenario'])   
    return df