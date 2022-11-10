"""
This script calculates envrionmental flows using monthly runoff (qtot from
                                                                 ISIMIP)
It calculates environmental flows using Variable Monthly Flow method

"""

from sqlite3 import TimeFromTicks
import numpy as np
import pandas as pd
import os

type_reg = 'global' # else 'global'
country = 'ZMB'
global_reg = 'R11'


if type_reg == 'global':
    wd = os.path.join("p:", "ene.model", "NEST","hydrology" ,"post-processed_qs",f"global_{global_reg}")
    # upload_files = glob.glob(os.path.join(upload_dir, "MESSAGEix*.xlsx"))
elif type_reg == 'country':
    # This is for Zambia
    wd = os.path.join("p:", "ene.model", "NEST", country, "hydrology")
   

# climate forcing
scenarios = ["rcp26", "rcp60"]

scen = "rcp26"
# variable, for detailed symbols, refer to ISIMIP2b documentation
variables = [
    "qtot",  # total runoff
    "dis",   # discharge
    "qg",    # groundwater runoff
    "qr",    # groundwater recharge
]  
var = "qr"
timestep = '5y'



# read quantiles

# Q50 
df_26_q50 = pd.read_csv(wd + f"\{var}_{timestep}_mmean_{scenarios[0]}_q50.csv")
df_26_q70 = pd.read_csv(wd + f"\{var}_{timestep}_mmean_{scenarios[0]}_q70.csv")
df_26_q90 = pd.read_csv(wd + f"\{var}_{timestep}_mmean_{scenarios[0]}_q90.csv")

df_60_q50 = pd.read_csv(wd + f"\{var}_{timestep}_mmean_{scenarios[1]}_q50.csv")
df_60_q70 = pd.read_csv(wd + f"\{var}_{timestep}_mmean_{scenarios[1]}_q70.csv")
df_60_q90 = pd.read_csv(wd + f"\{var}_{timestep}_mmean_{scenarios[1]}_q90.csv")

# Bias Correction 
df_26_q50.drop(["Unnamed: 0"], axis=1, inplace=True)
df_60_q50.drop(["Unnamed: 0"], axis=1, inplace=True)

# Bias adjustment in the data 
val2020_q50 = (df_60_q50.iloc[:,1:5].mean(axis =1) + df_26_q50.iloc[:,:5].mean(axis =1))/2
delta60 = df_60_q50.iloc[:,1:5].mean(axis =1) - val2020_q50
delta26 = df_26_q50.iloc[:,1:5].mean(axis =1) - val2020_q50

# df_sw26_adjusted_q50 = df_26_q50.apply(lambda x:x - delta26)
# df_sw60_adjusted_q50 = df_60_q50.apply(lambda x:x - delta60)

df_26_q50['2020-12-31'] =  val2020_q50
df_26_q50['2025-12-31'] =  df_26_q50['2025-12-31'] - delta26
df_26_q50['2030-12-31'] =  df_26_q50['2030-12-31'] - (0.8 * delta26)
df_26_q50['2035-12-31'] =  df_26_q50['2035-12-31'] - (0.6 * delta26)
df_26_q50['2040-12-31'] =  df_26_q50['2040-12-31'] - (0.4 * delta26)
df_26_q50['2045-12-31'] =  df_26_q50['2045-12-31'] - (0.2 * delta26)

df_60_q50['2020-12-31'] =  val2020_q50
df_60_q50['2025-12-31'] =  df_60_q50['2025-12-31'] - delta60
df_60_q50['2030-12-31'] =  df_60_q50['2030-12-31'] - (0.8 * delta60)
df_60_q50['2035-12-31'] =  df_60_q50['2035-12-31'] - (0.6 * delta60)
df_60_q50['2040-12-31'] =  df_60_q50['2040-12-31'] - (0.4 * delta60)
df_60_q50['2045-12-31'] =  df_60_q50['2045-12-31'] - (0.2 * delta60)

df_60_q50.to_csv(wd + f"\{var}_{timestep}_mmean_{scenarios[0]}_q50_ba.csv")
df_26_q50.to_csv(wd + f"\{var}_{timestep}_mmean_{scenarios[1]}_q50_ba.csv")


# Q70 
# Bias Correction 
df_26_q70.drop(["Unnamed: 0"], axis=1, inplace=True)
df_60_q70.drop(["Unnamed: 0"], axis=1, inplace=True)


# Bias adjustment in the data 
val2020_q70 = (df_60_q70.iloc[:,1:5].mean(axis =1) + df_26_q70.iloc[:,:5].mean(axis =1))/2
delta60 = df_60_q70.iloc[:,1:5].mean(axis =1) - val2020_q70
delta26 = df_26_q70.iloc[:,1:5].mean(axis =1) - val2020_q70

df_26_q70['2020-12-31'] =  val2020_q70
df_26_q70['2025-12-31'] =  df_26_q70['2025-12-31'] - delta26
df_26_q70['2030-12-31'] =  df_26_q70['2030-12-31'] - (0.8 * delta26)
df_26_q70['2035-12-31'] =  df_26_q70['2035-12-31'] - (0.6 * delta26)
df_26_q70['2040-12-31'] =  df_26_q70['2040-12-31'] - (0.4 * delta26)
df_26_q70['2045-12-31'] =  df_26_q70['2045-12-31'] - (0.2 * delta26)

df_60_q70['2020-12-31'] =  val2020_q70
df_60_q70['2025-12-31'] =  df_60_q70['2025-12-31'] - delta60
df_60_q70['2030-12-31'] =  df_60_q70['2030-12-31'] - (0.8 * delta60)
df_60_q70['2035-12-31'] =  df_60_q70['2035-12-31'] - (0.6 * delta60)
df_60_q70['2040-12-31'] =  df_60_q70['2040-12-31'] - (0.4 * delta60)
df_60_q70['2045-12-31'] =  df_60_q70['2045-12-31'] - (0.2 * delta60)

# df_sw26_adjusted_q70 = df_26_q70.apply(lambda x:x - delta26)
# df_sw60_adjusted_q70 = df_60_q70.apply(lambda x:x - delta60)

df_26_q70.to_csv(wd + f"\{var}_{timestep}_mmean_{scenarios[0]}_q70_ba.csv")
df_60_q70.to_csv(wd + f"\{var}_{timestep}_mmean_{scenarios[1]}_q70_ba.csv")


# Q90
# Bias Correction 
df_26_q90.drop(["Unnamed: 0"], axis=1, inplace=True)
df_60_q90.drop(["Unnamed: 0"], axis=1, inplace=True)


# Bias adjustment in the data 
val2020_q90 = (df_60_q90.iloc[:,1:5].mean(axis =1) + df_26_q90.iloc[:,:5].mean(axis =1))/2
delta60 = df_60_q90.iloc[:,1:5].mean(axis =1) - val2020_q90
delta26 = df_26_q90.iloc[:,1:5].mean(axis =1) - val2020_q90

# df_sw26_adjusted_q90 = df_26_q90.apply(lambda x:x - delta26)
# df_sw60_adjusted_q90 = df_60_q90.apply(lambda x:x - delta60)


df_26_q90['2020-12-31'] =  val2020_q90
df_26_q90['2025-12-31'] =  df_26_q90['2025-12-31'] - delta26
df_26_q90['2030-12-31'] =  df_26_q90['2030-12-31'] - (0.8 * delta26)
df_26_q90['2035-12-31'] =  df_26_q90['2035-12-31'] - (0.6 * delta26)
df_26_q90['2040-12-31'] =  df_26_q90['2040-12-31'] - (0.4 * delta26)
df_26_q90['2045-12-31'] =  df_26_q90['2045-12-31'] - (0.2 * delta26)

df_60_q90['2020-12-31'] =  val2020_q90
df_60_q90['2025-12-31'] =  df_60_q90['2025-12-31'] - delta60
df_60_q90['2030-12-31'] =  df_60_q90['2030-12-31'] - (0.8 * delta60)
df_60_q90['2035-12-31'] =  df_60_q90['2035-12-31'] - (0.6 * delta60)
df_60_q90['2040-12-31'] =  df_60_q90['2040-12-31'] - (0.4 * delta60)
df_60_q90['2045-12-31'] =  df_60_q90['2045-12-31'] - (0.2 * delta60)

df_26_q90.to_csv(wd + f"\{var}_{timestep}_mmean_{scenarios[0]}_q90_ba.csv")
df_60_q90.to_csv(wd + f"\{var}_{timestep}_mmean_{scenarios[1]}_q90_ba.csv")


# Reliability Scenarios 
# Low Reliability = q50 
df_60_q50.to_csv(wd + f"\{var}_{timestep}_2p6_low_R11.csv")
df_26_q50.to_csv(wd + f"\{var}_{timestep}_6p0_low_R11.csv")

df_sw_noclimate  = df_60_q50.apply(lambda x:val2020_q50)
df_sw_noclimate.to_csv(wd + f"\{var}_{timestep}_no_climate_low_R11.csv")

# Medium Reliability = q70 
df_med_26_1 = df_26_q50.iloc[:,:3]
df_med_26_2 = df_26_q70.iloc[:,4:]
df_med_26 = pd.concat([df_med_26_1, df_med_26_2], axis = 1)
df_med_26.to_csv(wd + f"\{var}_{timestep}_2p6_med_R11.csv")

df_med_60_1 = df_60_q50.iloc[:,:3]
df_med_60_2 = df_60_q70.iloc[:,4:]
df_med_60 = pd.concat([df_med_60_1, df_med_60_2], axis = 1)
df_med_60.to_csv(wd + f"\{var}_{timestep}_6p0_med_R11.csv")

df_med_noclimate_1  = df_med_60.iloc[:,:3].apply(lambda x:val2020_q50)
df_med_noclimate_2 =  df_med_60.iloc[:,4:].apply(lambda x:val2020_q70)
df_med_noclimate = pd.concat([df_med_noclimate_1, df_med_noclimate_2], axis = 1)
df_med_noclimate.to_csv(wd + f"\{var}_{timestep}_no_climate_med_R11.csv")

df_med_noclimate.iloc[:,:3] = df_sw_noclimate.iloc[:,:3]

# High Reliability - 2020,q50 . 2025:2030, q70 , 2035:2100, q90 
df_high_26_1 = df_26_q50.iloc[:,:3]
df_high_26_2 = df_26_q70.iloc[:,4:6]
df_high_26_3 = df_26_q90.iloc[:,6:]
df_high_26 = pd.concat([df_high_26_1, df_high_26_2,df_high_26_3], axis = 1)
df_high_26.to_csv(wd + f"\{var}_{timestep}_2p6_high_R11.csv")

df_high_60_1 = df_60_q50.iloc[:,:3]
df_high_60_2 = df_60_q70.iloc[:,4:6]
df_high_60_3 = df_60_q90.iloc[:,6:]
df_high_60 = pd.concat([df_high_60_1, df_high_60_2,df_high_60_3], axis = 1)
df_high_60.to_csv(wd + f"\{var}_{timestep}_6p0_high_R11.csv")

df_high_noclimate_1  = df_high_60.iloc[:,:3].apply(lambda x:val2020_q50)
df_high_noclimate_2 =  df_high_60.iloc[:,4:6].apply(lambda x:val2020_q70)
df_high_noclimate_3 =  df_high_60.iloc[:,6:].apply(lambda x:val2020_q90)
df_high_noclimate = pd.concat([df_high_noclimate_1, df_high_noclimate_2], axis = 1)
df_high_noclimate.to_csv(wd + f"\{var}_{timestep}_no_climate_high_R11.csv")