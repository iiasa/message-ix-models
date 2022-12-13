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
isimip = '3b'

if type_reg == 'global':
    wd = os.path.join("p:", "ene.model", "NEST", "hydrological_data_agg")
    # upload_files = glob.glob(os.path.join(upload_dir, "MESSAGEix*.xlsx"))
    wd_out = os.path.join("p:", "ene.model", "NEST","hydrology" ,"post-processed_qs",f"global_{global_reg}")


elif type_reg == 'country':
    # This is for Zambia
    wd = os.path.join("p:", "ene.model", "NEST", country, "hydrology")
    wd_out = os.path.join("p:", "ene.model", "NEST", country, "hydrology","post-processed")
   

# climate model
if isimip == '2b':
    climmodels = ["gfdl-esm2m", "hadgem2-es", "ipsl-cm5a-lr", "miroc5"]
    climmodel = "gfdl-esm2m"
    # climate forcing
    scenarios = ["rcp26", "rcp60"]
    scen = "rcp26"
    wd1 =  os.path.join("p:", "ene.model", "NEST", "hydrological_data_agg")
    wd =  os.path.join("p:", "watxene", "ISIMIP","ISIMIP2b" ,"output","LPJmL")
    wd2 =  os.path.join("p:", "ene.model", "NEST", "hydrology","processed_nc4")
else:
    climmodels = ["gfdl-esm4", "ipsl-cm6a-lr", "mpi-esm1-2-hr", "mri-esm2-0","ukesm1-0-ll"]
    scenarios = ["ssp126", "ssp370","ssp585"]
    scen= "ssp126"
    wd1 =  os.path.join("p:", "ene.model", "NEST", "hydrological_data_agg")
    wd =  os.path.join("p:", "watxene", "ISIMIP","ISIMIP3b" ,"CWatM_results",f"{cl}",f"{data}")
    wd2 =  os.path.join("p:", "ene.model", "NEST", "hydrology","processed_nc4")

# Define the scenario 'extreme' or 'normal'
extreme_scen = 'high'


# climate forcing
scenarios = ["rcp26", "rcp60"]

scen = "rcp60"
# variable, for detailed symbols, refer to ISIMIP2b documentation
variables = [
    "qtot",  # total runoff
    "dis",   # discharge
    "qg",    # groundwater runoff
    "qr",    # groundwater recharge
]  
var = "qr"
# climate model
climmodels = ["gfdl-esm2m", "hadgem2-es", "ipsl-cm5a-lr", "miroc5"]
climmodel = "miroc5"
timestep = 'monthly'


# select interpolation method 
intp = 'linear'
quant = 0.5
env_flow = True

# Read sample file to keep all columns
df_chk = pd.read_csv(wd + f"\{var}_monthly_miroc5_rcp26.csv")

# Reading files and taking multimodel ensemble
# df1 = pd.read_csv(wd + f"\{var}_{timestep}_miroc5_{scen}.csv").iloc[:, 8:]
df2 = pd.read_csv(wd + f"\{var}_{timestep}_gfdl-esm2m_{scen}.csv").iloc[:, 8:]
df3 = pd.read_csv(wd + f"\{var}_{timestep}_hadgem2-es_{scen}.csv").iloc[:, 8:]
df4 = pd.read_csv(wd + f"\{var}_{timestep}_ipsl-cm5a-lr_{scen}.csv").iloc[:, 8:]


data = pd.concat([ df2, df3, df4]).groupby(level=0).mean()
# data["basin"] = "B" + df_chk["BCU_name"]

# data.to_csv(wd + f"\{var}_{timestep}_{scen}_{extreme_scen}_{country}.csv")

if extreme_scen == 'high':
    data = data.rolling(240,
                        min_periods = 1,
                        axis = 1).quantile(
                            quant, interpolation= intp)
        # data = data.rolling(
        #     240, min_periods = 1, axis = 1).mean()
elif extreme_scen == 'med':
    data = data.rolling(240,
                        min_periods = 1,
                        axis = 1).quantile(
                            .5, interpolation= intp)
    # data = data.rolling(
    #     240, min_periods = 1, axis = 1).mean()

new_cols = pd.to_datetime(data.columns, format="sum.X%Y.%m.%d")    
data.columns = new_cols
data = data.iloc[:,48:]  
# data = data.resample('5Y', axis = 1).mean()  

data.to_csv(wd_out + f"\{var}_monthly_mmean_{scen}.csv")

# df_6p0_temp = df_6p0.iloc[:, :216]

# df = df_6p0_temp.copy()

# # Convert monthly values to wet and dry
# for z in range(len(df.columns) // 12):
#     col_end = (z + 1) * 12  # ending col number
#     col_start = 0 if z == 0 else col_end  # start col number

#     data = df.iloc[:, col_start:col_end]  # assigning relevant data
#     maxindex = data.idxmax(axis=1)  # getting max indexes

#     wet_name = "mean." + maxindex[0][4:9] + "_wet"
#     dry_name = "mean." + maxindex[0][4:9] + "_dry"

#     year_data = pd.DataFrame(
#         columns=[wet_name, dry_name]
#     )  # creaing and empty data frame

#     for i in range(len(maxindex)):
#         col_names = data.columns  # getting column names
#         get_col_index = col_names == maxindex[i]  # getting match column index
#         wet_start = (
#             0
#             if np.where(get_col_index)[0] - 3 < 0
#             else np.where(get_col_index)[0][0] - 3
#         )  # condition for negative col
#         wet_add = np.where(get_col_index)[0] - 3  # getting negative columns
#         wet_end = np.where(get_col_index)[0] + 2
#         wet_col = col_names[wet_start : wet_end[0] + 1].tolist()

#         # for adding negative columns
#         if np.where(get_col_index)[0] - 3 >= 0:
#             wet_col
#         else:
#             wet_col.extend(col_names[wet_add[0] :].tolist())

#         # for adding positive columns
#         if len(wet_col) < 6:
#             col_add = 6 - len(wet_col)
#             wet_col.extend(col_names[:col_add])

#         start_row = 0 if i == 0 else end_row
#         end_row = i + 1

#         wet_mean = data.iloc[start_row:end_row, :][wet_col].mean(axis=1)
#         dry_mean = (
#             data.iloc[start_row:end_row, :]
#             .loc[:, ~data.iloc[start_row:end_row, :].columns.isin(wet_col)]
#             .mean(axis=1)
#         )

#         row_array = np.array([wet_mean[i], dry_mean[i]])  # making an array

#         year_data.loc[len(year_data)] = row_array  # appending an array

    
#     #
#     # Final data frame after choosing multi model mean
#     df_mmmean[scen] = pd.concat(
#         [df1, df2, df3]).groupby(
#             level=0).mean()
            
#     dffinal[scen] = temporal_agg(df_mmmean[scen],extreme_scen)
   
#     # data_1_mean = temporal_agg(df2,extreme_scen)
#     # data_2_mean = temporal_agg(df3,extreme_scen)
#     # data_3_mean = temporal_agg(df4,extreme_scen)

# print("Done")

# df_2p6_temp1 = final_data.copy()
# df_2p6_temp1.to_csv(wd + r"\qtot_wetdry_multimodelmean_rcp2p6.csv")

# # df_2p6_temp1 = pd.read_csv(wd + r"\qtot_wetdry_multimodelmean_rcp2p6.csv")

# final_data = df_2p6_temp1.copy()

# df_6p0_temp1 = final_data.copy()
# df_6p0_temp1.to_csv(wd + r"\qtot_wetdry_multimodelmean_rcp6p0.csv")

# df_6p0_temp1 = pd.read_csv(wd + r"\qtot_wetdry_multimodelmean_rcp6p0.csv")

    # mean_values = np.stack((data_0_mean.mean(axis = 1).values,
    #                         data_1_mean.mean(axis = 1).values,
    #                         data_2_mean.mean(axis = 1).values))
    #                         # data_3_mean.mean(axis = 1).values))

    # final_array = []
    # for i, min_col in enumerate(mean_values.argmin(axis = 0)):
    #     if min_col == 0:
    #         final_array.append(data_0_mean.iloc[i].values)
    #     elif min_col == 1:
    #         final_array.append(data_1_mean.iloc[i].values)
    #     elif min_col == 2:
    #         final_array.append(data_2_mean.iloc[i].values)
        # else:
        #     final_array.append(data_3_mean.iloc[i].values)
if GCM == 'dry':
    # Final data frame after choosing driest GCM
    dfs_dry[scen] = pd.DataFrame(final_array, columns = data_0_mean.columns)

# Calculating environmental flows using wet and dry seasonal calculations
for i in range(len(final_data.columns) // 2):
    col_start = 0 if i == 0 else col_end
    col_end = (i + 1) * 2
    data = final_data.iloc[:, col_start:col_end]

    name = data.columns[0][6:10]

    wet_mul = data.iloc[:, 0]  # * 0.6
    dry_mul = data.iloc[:, 1]  # * 0.3

    mul_data = (wet_mul + dry_mul) / 2

    if i == 0:
        PCT_data = pd.DataFrame(mul_data, columns=[name])
    else:
        tmp_data = pd.DataFrame(mul_data, columns=[name])
        PCT_data = pd.concat([PCT_data, tmp_data], axis=1)

print("Done")

#                     mul_data = (wet_mul + dry_mul) / 2
#                     value.append(mul_data.values)
#                 # Developed basins
#                 else:
#                     temp2 = data[data["BCU_name"] == j]
#                     wet_mul = temp2.iloc[:, 0] * wetvalind
#                     dry_mul = temp2.iloc[:, 1] * dryvalind

#                     mul_data = (wet_mul + dry_mul) / 2
#                     value.append(mul_data.values)

#         if i == 0:
#             PCT_data = pd.DataFrame(data=value, columns=[name])
#         else:
#             tmp_data = pd.DataFrame(data=value, columns=[name])
#             PCT_data = pd.concat([PCT_data, tmp_data], axis=1)

#     return PCT_data

dfs_dry = {}
df_mmmean = {}
dfs_wd = {}
dfs_ann = {}
df_env = {}
eflow = {}
eflow1 = {}
eflow2 = {}
dffinal = {}
var = 'qtot'
env_flow = True

for scen in scenarios:
    # Reading data files from four GCMs
    df = pd.read_csv(
        wd + f"\{var}_monthly_{climmodels[0]}_{scen}.csv")

    df1 = pd.read_csv(
        wd + f"\{var}_monthly_{climmodels[1]}_{scen}.csv")


    df2 = pd.read_csv(
        wd + f"\{var}_monthly_{climmodels[2]}_{scen}.csv")

    # df3 = pd.read_csv(
    #     wd + f"\{var}_monthly_{climmodels[3]}_{scen}.csv")


        
    # Final data frame after choosing multi model mean
    df_mmmean[scen] = pd.concat(
        [df1, df2, df3]).groupby(
            level=0).mean()


    df_env[scen] = df_mmmean[scen].iloc[:,5:]

    eflow[scen] = []

    # df = data.iloc[:,5:]
    for z in range(len(df_env[scen].columns) // 12):

        col_start = 0 if z == 0 else col_end  # start col number
        col_end = (z + 1) * 12  # ending col number
        temp = df_env[scen].iloc[:, col_start:col_end]  # assigning relevant data

        col_names = temp.columns  # getting column names
        MAF = temp.mean(axis = 1) # take mean acorss 

        for j in range(len(temp.columns)):
            temp.iloc[:,j] = np.where(temp.iloc[:,j] > 0.8 * MAF,
                                        temp.iloc[:,j]*0.2, 
                                        np.where(
                                            (temp.iloc[:,j] > 0.4 * MAF) & 
                                                                    (temp.iloc[:,j] <= 0.8 * MAF), 
                                                                        temp.iloc[:,j]*0.45, temp.iloc[:,j]*0.6))
        
        if z == 0:
            eflow[scen] = temp
        else:
            eflow[scen] = pd.concat((eflow[scen], temp), axis = 1)

    # Convert to 5 year annual values 
    new_cols = pd.to_datetime(eflow[scen].columns, format='sum.X%Y.%m.%d')
    eflow[scen].columns = new_cols
    eflow[scen] = eflow[scen].iloc[:,48:] 
    eflow[scen] = eflow[scen].resample('5Y',axis = 1).mean() 
    eflow[scen].to_csv(wd_out + f"\e-flow_{scen}.csv") 

    # df = eflow[scen]

    # for i in range(1, round(len(df.transpose())/60+1)):
    #     start = 0 if i == 1 else end
    #     end = i*60

    #     temp = df.transpose().iloc[start:end]

    #     name = str(temp.index[-1].year)
    #     temp = temp.groupby(temp.index.month).mean()
    #     col_name = [name + "," + str(i) for i in range(1, 13)]

    #     if i == 1:
    #         eflow2[scen] = temp.transpose()
    #         eflow2[scen] = eflow2[scen].set_axis(col_name, axis=1)
    #     else:
    #         eflow1[scen] = temp.transpose()
    #         eflow1[scen] = eflow1[scen].set_axis(col_name, axis=1)
    #         eflow2[scen] = pd.concat((eflow2[scen], eflow1[scen]), axis = 1)        
            
    # new_cols = pd.to_datetime(eflow2[scen].columns, format='%Y,%m')
    # eflow2[scen].columns = new_cols
    # eflow2[scen] = eflow2[scen].resample('5Y',axis = 1).mean()  

        
    # eflow2[scen].to_csv(wd + f"\e-flow_{var}_{scen}.csv") 

        
    # df1 = pd.read_csv(wd + f"\e-flow_{variables[3]}_{scen}.csv")
    # df2 = pd.read_csv(wd + f"\e-flow_{variables[0]}_{scen}.csv")
    
    # df2.subtract(df1,axis = 1).drop(columns = 'Unnamed: 0').to_csv(wd + f"\e-flow__{scen}.csv")
    
    
    print("Environmental Flow Values processed")


## Adjusting bias for the data in qs and return 