"""
This script calculates envrionmental flows using monthly runoff (qtot from
                                                                 ISIMIP)
It calculates environmental flows using Variable Monthly Flow method

"""


import numpy as np
import pandas as pd

wd = r"P:\ene.model\NEST\hydrology\post-processed"

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
# climate model
climmodels = ["gfdl-esm2m", "hadgem2-es", "ipsl-cm5a-lr", "miroc5"]
climmodel = "miroc5"


# select interpolation method 
intp = 'linear'
quant = 0.3
env_flow = True

df_2p6_temp = df_2p6.iloc[:, :216]

# rcp 6p0
df5 = pd.read_csv(wd + r"\qtot_monthly_miroc5_rcp60.csv").iloc[:, 8:]
df6 = pd.read_csv(wd + r"\qtot_monthly_gfdl-esm2m_rcp60.csv").iloc[:, 8:]
df7 = pd.read_csv(wd + r"\qtot_monthly_hadgem2-es_rcp60.csv").iloc[:, 8:]
df8 = pd.read_csv(wd + r"\qtot_monthly_ipsl-cm5a-lr_rcp60.csv").iloc[:, 8:]

df_6p0 = pd.concat([df5, df6, df7, df8]).groupby(level=0).mean()
df_6p0["basin"] = "B" + df_chk["BCU_name"]
df_6p0.to_csv(wd + r"\qtot_monthly_multimodelmean_rcp6p0.csv")

    if extreme_scen == 'extreme':
        data = data.rolling(240,
                            min_periods = 1,
                            axis = 1).quantile(
                                quant, interpolation= intp)
            # data = data.rolling(
            #     240, min_periods = 1, axis = 1).mean()
    elif extreme_scen == 'normal':
        data = data.rolling(240,
                            min_periods = 1,
                            axis = 1).quantile(
                                .5, interpolation= intp)
        # data = data.rolling(
        #     240, min_periods = 1, axis = 1).mean()

df_6p0_temp = df_6p0.iloc[:, :216]

df = df_6p0_temp.copy()

# Convert monthly values to wet and dry
for z in range(len(df.columns) // 12):
    col_end = (z + 1) * 12  # ending col number
    col_start = 0 if z == 0 else col_end  # start col number

    data = df.iloc[:, col_start:col_end]  # assigning relevant data
    maxindex = data.idxmax(axis=1)  # getting max indexes

    wet_name = "mean." + maxindex[0][4:9] + "_wet"
    dry_name = "mean." + maxindex[0][4:9] + "_dry"

    year_data = pd.DataFrame(
        columns=[wet_name, dry_name]
    )  # creaing and empty data frame

    for i in range(len(maxindex)):
        col_names = data.columns  # getting column names
        get_col_index = col_names == maxindex[i]  # getting match column index
        wet_start = (
            0
            if np.where(get_col_index)[0] - 3 < 0
            else np.where(get_col_index)[0][0] - 3
        )  # condition for negative col
        wet_add = np.where(get_col_index)[0] - 3  # getting negative columns
        wet_end = np.where(get_col_index)[0] + 2
        wet_col = col_names[wet_start : wet_end[0] + 1].tolist()

        # for adding negative columns
        if np.where(get_col_index)[0] - 3 >= 0:
            wet_col
        else:
            wet_col.extend(col_names[wet_add[0] :].tolist())

        # for adding positive columns
        if len(wet_col) < 6:
            col_add = 6 - len(wet_col)
            wet_col.extend(col_names[:col_add])

        start_row = 0 if i == 0 else end_row
        end_row = i + 1

        wet_mean = data.iloc[start_row:end_row, :][wet_col].mean(axis=1)
        dry_mean = (
            data.iloc[start_row:end_row, :]
            .loc[:, ~data.iloc[start_row:end_row, :].columns.isin(wet_col)]
            .mean(axis=1)
        )

        row_array = np.array([wet_mean[i], dry_mean[i]])  # making an array

        year_data.loc[len(year_data)] = row_array  # appending an array

    
    #
    # Final data frame after choosing multi model mean
    df_mmmean[scen] = pd.concat(
        [df1, df2, df3]).groupby(
            level=0).mean()
            
    dffinal[scen] = temporal_agg(df_mmmean[scen],extreme_scen)
   
    # data_1_mean = temporal_agg(df2,extreme_scen)
    # data_2_mean = temporal_agg(df3,extreme_scen)
    # data_3_mean = temporal_agg(df4,extreme_scen)

print("Done")

df_2p6_temp1 = final_data.copy()
df_2p6_temp1.to_csv(wd + r"\qtot_wetdry_multimodelmean_rcp2p6.csv")

# df_2p6_temp1 = pd.read_csv(wd + r"\qtot_wetdry_multimodelmean_rcp2p6.csv")

final_data = df_2p6_temp1.copy()

df_6p0_temp1 = final_data.copy()
df_6p0_temp1.to_csv(wd + r"\qtot_wetdry_multimodelmean_rcp6p0.csv")

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
