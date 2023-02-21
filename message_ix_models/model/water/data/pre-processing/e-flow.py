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
    "dis",  # discharge
    "qg",  # groundwater runoff
    "qr",  # groundwater recharge
]
var = "qr"
# climate model
climmodels = ["gfdl-esm2m", "hadgem2-es", "ipsl-cm5a-lr", "miroc5"]
climmodel = "miroc5"

GCM = "mmean"  # 'dry'choose driest GCM else 'mmean' for multi model mean
# else mmmean
extreme_scen = "extreme"  # 'extreme' q90 else 'normal' q50

# select interpolation method
intp = "linear"

env_flow = True


dfs_dry = {}
df_mmmean = {}
dfs_wd = {}
dfs_ann = {}
df_env = {}
eflow = {}
eflow1 = {}
eflow2 = {}
dffinal = {}
#%%
def temporal_agg(data, extreme_scen):

    # data = data.set_axis(pd.date_range(
    #     '2006-1-1', '2099-12-31', freq = 'M'), axis=1)

    if extreme_scen == "extreme":
        data = data.rolling(240, min_periods=1, axis=1).quantile(
            0.1, interpolation=intp
        )
        # data = data.rolling(
        #     240, min_periods = 1, axis = 1).mean()
    elif extreme_scen == "normal":
        data = data.rolling(240, min_periods=1, axis=1).quantile(
            0.5, interpolation=intp
        )
        # data = data.rolling(
        #     240, min_periods = 1, axis = 1).mean()

    for i in range(1, round(len(data.transpose()) / 60 + 1)):
        start = 0 if i == 1 else end
        end = i * 60

        temp = data.transpose().iloc[start:end]

        name = str(temp.index[-1].year)
        temp = temp.groupby(temp.index.month).mean()
        col_name = [name + "," + str(i) for i in range(1, 13)]

        if i == 1:
            final_data = temp.transpose()
            final_data = final_data.set_axis(col_name, axis=1)
        else:
            temp_data = temp.transpose()
            temp_data = temp_data.set_axis(col_name, axis=1)
            final_data = pd.concat((final_data, temp_data), axis=1)

    return final_data


#%%
for scen in scenarios:
    # Reading data files from four GCMs
    df = (
        pd.read_csv(wd + f"\{var}_monthly_{climmodels[0]}_{scen}.csv")
        .iloc[:, 8:]
        .set_axis(pd.date_range("2006-1-1", "2099-12-31", freq="M"), axis=1)
    )

    df1 = (
        pd.read_csv(wd + f"\{var}_monthly_{climmodels[0]}_{scen}.csv")
        .iloc[:, 8:]
        .set_axis(pd.date_range("2006-1-1", "2099-12-31", freq="M"), axis=1)
    )

    df2 = (
        pd.read_csv(wd + f"\{var}_monthly_{climmodels[1]}_{scen}.csv")
        .iloc[:, 8:]
        .set_axis(pd.date_range("2006-1-1", "2099-12-31", freq="M"), axis=1)
    )

    df3 = (
        pd.read_csv(wd + f"\{var}_monthly_{climmodels[2]}_{scen}.csv")
        .iloc[:, 8:]
        .set_axis(pd.date_range("2006-1-1", "2099-12-31", freq="M"), axis=1)
    )

    # df4 = pd.read_csv(
    #     wd + f"\{var}_monthly_{climmodels[3]}_{scen}.csv")

    #
    # Final data frame after choosing multi model mean
    df_mmmean[scen] = pd.concat([df1, df2, df3]).groupby(level=0).mean()

    dffinal[scen] = temporal_agg(df_mmmean[scen], extreme_scen)
    df1test = temporal_agg(df1, extreme_scen)
    # data_1_mean = temporal_agg(df2,extreme_scen)
    # data_2_mean = temporal_agg(df3,extreme_scen)
    # data_3_mean = temporal_agg(df4,extreme_scen)

    # new_cols = pd.to_datetime(data_0_mean.columns, format='%Y,%m')
    # data_0_mean.columns = new_cols
    # data_1_mean.columns = new_cols
    # data_2_mean.columns = new_cols
    # data_3_mean.columns = new_cols

    # Choose driest value from each cell from four dataframes
    # final_array = np.amin(np.vstack((np.array(data_0_mean).flatten(),
    #                                  np.array(data_1_mean).flatten(),
    #                                  np.array(data_2_mean).flatten(),
    #                                  np.array(data_3_mean).flatten())),
    #                       axis=0).reshape(210,228)

    # df_drymodel = pd.DataFrame(final_array, columns = data_0_mean.columns)

    # Choose direst GCM per basin

    mean_values = np.stack(
        (
            data_0_mean.mean(axis=1).values,
            data_1_mean.mean(axis=1).values,
            data_2_mean.mean(axis=1).values,
        )
    )
    # data_3_mean.mean(axis = 1).values))

    final_array = []
    for i, min_col in enumerate(mean_values.argmin(axis=0)):
        if min_col == 0:
            final_array.append(data_0_mean.iloc[i].values)
        elif min_col == 1:
            final_array.append(data_1_mean.iloc[i].values)
        elif min_col == 2:
            final_array.append(data_2_mean.iloc[i].values)
        # else:
        #     final_array.append(data_3_mean.iloc[i].values)
    if GCM == "dry":
        # Final data frame after choosing driest GCM
        dfs_dry[scen] = pd.DataFrame(final_array, columns=data_0_mean.columns)

        dfs_dry[scen].to_csv(
            wd + f"\{var}_5ymonthly_{GCM}_{extreme_scen}_{scen}_{intp}.csv"
        )
        print("5 year Monthly values and quantiles processed")
        # Store for next processing
        df = dfs_dry[scen]
        # Convert to 5 year annual values
        new_cols = pd.to_datetime(dfs_dry[scen].columns, format="%Y,%m")
        dfs_dry[scen].columns = new_cols
        dfs_ann[scen] = dfs_dry[scen].resample("5Y", axis=1).mean()

        dfs_ann[scen].to_csv(wd + f"\{var}_5y_{GCM}_{extreme_scen}_{scen}_{intp}.csv")

        print("5 year annual values processed")

    elif GCM == "mmean":

        # Final data frame after choosing multi model mean
        # df_mmmean[scen] = pd.concat(
        #     [data_0_mean, data_1_mean, data_2_mean]).groupby(
        #         level=0).mean()
        dffinal[scen].to_csv(
            wd + f"\{var}_5ymonthly_{GCM}_{extreme_scen}_{scen}_{intp}.csv"
        )
        print("5 year Monthly values and quantiles processed")
        # Store for next processing
        df = dffinal[scen]
        # Convert to 5 year annual values
        new_cols = pd.to_datetime(dffinal[scen].columns, format="%Y,%m")
        dffinal[scen].columns = new_cols

        dfs_ann[scen] = dffinal[scen].resample("5Y", axis=1).mean()

        dfs_ann[scen].to_csv(wd + f"\{var}_5y_{GCM}_{extreme_scen}_{scen}_{intp}.csv")

        print("5 year annual values processed")

    # # Convert monthly values to wet and dry
    # for z in range(len(df.columns) // 12):

    #     col_start = 0 if z == 0 else col_end  # start col number
    #     col_end = (z + 1) * 12  # ending col number
    #     data = df.iloc[:, col_start:col_end]  # assigning relevant data
    #     maxindex = data.idxmax(axis=1)  # getting max indexes

    #     wet_name = "mean." + maxindex[0][:4] + "_wet"
    #     dry_name = "mean." + maxindex[0][:4] + "_dry"

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

    #     if z == 0:
    #         dfs_wd[scen] = year_data.copy()
    #     else:
    #         dfs_wd[scen] = pd.concat([dfs_wd[scen], year_data], axis=1)

    # dfs_wd[scen].to_csv(wd + f"\{var}_wetdry_{GCM}_unprocessed_{scen}.csv")

    # print("Wet Dry values processed")

    # # Converting to 5 year annual
    # for i in range(len(dfs_wd[scen].columns) // 2):
    #     col_start = 0 if i == 0 else col_end
    #     col_end = (i + 1) * 2
    #     data = dfs_wd[scen].iloc[:, col_start:col_end]

    #     name = data.columns[0][5:9]

    #     wet_mul = data.iloc[:, 0]
    #     dry_mul = data.iloc[:, 1]

    #     mul_data = (wet_mul + dry_mul) / 2

    #     if i == 0:
    #         dfs_ann[scen] = pd.DataFrame(mul_data, columns=[name])
    #     else:
    #         tmp_data = pd.DataFrame(mul_data, columns=[name])
    #         dfs_ann[scen] = pd.concat([dfs_ann[scen], tmp_data], axis=1)


#%%
var = "qtot"

if env_flow:
    for scen in scenarios:
        # Reading data files from four GCMs
        df = pd.read_csv(wd + f"\{var}_monthly_{climmodels[0]}_{scen}.csv")

        df1 = pd.read_csv(wd + f"\{var}_monthly_{climmodels[0]}_{scen}.csv")

        df2 = pd.read_csv(wd + f"\{var}_monthly_{climmodels[1]}_{scen}.csv")

        df3 = pd.read_csv(wd + f"\{var}_monthly_{climmodels[2]}_{scen}.csv")

        # Final data frame after choosing multi model mean
        df_mmmean[scen] = pd.concat([df1, df2, df3]).groupby(level=0).mean()

        df_env[scen] = df_mmmean[scen].iloc[:, 4:]
        eflow[scen] = []

        # df = data.iloc[:,5:]
        for z in range(len(df_env[scen].columns) // 12):

            col_start = 0 if z == 0 else col_end  # start col number
            col_end = (z + 1) * 12  # ending col number
            temp = df_env[scen].iloc[:, col_start:col_end]  # assigning relevant data

            col_names = temp.columns  # getting column names
            MAF = temp.mean(axis=1)  # take mean acorss

            for j in range(len(temp.columns)):
                temp.iloc[:, j] = np.where(
                    temp.iloc[:, j] > 0.8 * MAF,
                    temp.iloc[:, j] * 0.2,
                    np.where(
                        (temp.iloc[:, j] > 0.4 * MAF) & (temp.iloc[:, j] <= 0.8 * MAF),
                        temp.iloc[:, j] * 0.45,
                        temp.iloc[:, j] * 0.6,
                    ),
                )

            if z == 0:
                eflow[scen] = temp
            else:
                eflow[scen] = pd.concat((eflow[scen], temp), axis=1)

        # Convert to 5 year annual values
        new_cols = pd.to_datetime(eflow[scen].columns, format="sum.X%Y.%m.%d")
        eflow[scen].columns = new_cols

        df = eflow[scen]

        for i in range(1, round(len(df.transpose()) / 60 + 1)):
            start = 0 if i == 1 else end
            end = i * 60

            temp = df.transpose().iloc[start:end]

            name = str(temp.index[-1].year)
            temp = temp.groupby(temp.index.month).mean()
            col_name = [name + "," + str(i) for i in range(1, 13)]

            if i == 1:
                eflow2[scen] = temp.transpose()
                eflow2[scen] = eflow2[scen].set_axis(col_name, axis=1)
            else:
                eflow1[scen] = temp.transpose()
                eflow1[scen] = eflow1[scen].set_axis(col_name, axis=1)
                eflow2[scen] = pd.concat((eflow2[scen], eflow1[scen]), axis=1)

        new_cols = pd.to_datetime(eflow2[scen].columns, format="%Y,%m")
        eflow2[scen].columns = new_cols
        eflow2[scen] = eflow2[scen].resample("5Y", axis=1).mean()

        eflow2[scen].to_csv(wd + f"\e-flow_{var}_{scen}.csv")

    df1 = pd.read_csv(wd + f"\e-flow_{variables[3]}_{scen}.csv")
    df2 = pd.read_csv(wd + f"\e-flow_{variables[0]}_{scen}.csv")

    df2.subtract(df1, axis=1).drop(columns="Unnamed: 0").to_csv(
        wd + f"\e-flow__{scen}.csv"
    )

    print("Environmental Flow Values processed")
#%%
## Unporcessed monthly data dry and extreme for comparison


for scen in scenarios:
    # Reading data files from four GCMs
    df = pd.read_csv(wd + f"\{var}_monthly_{climmodels[0]}_{scen}.csv")

    df1 = pd.read_csv(wd + f"\{var}_monthly_{climmodels[0]}_{scen}.csv")

    df2 = pd.read_csv(wd + f"\{var}_monthly_{climmodels[1]}_{scen}.csv")

    df3 = pd.read_csv(wd + f"\{var}_monthly_{climmodels[2]}_{scen}.csv")

    # def temporal_agg(data,extreme_scen):

    #     data = data.iloc[:,8:].set_axis(pd.date_range(
    #         '2006-1-1', '2099-12-31', freq = 'M'), axis=1)

    #     for i in range(1, round(len(data.transpose())/60+1)):
    #         start = 0 if i == 1 else end
    #         end = i*60

    #         temp = data.transpose().iloc[start:end]

    #         name = str(temp.index[-1].year)
    #         temp = temp.groupby(temp.index.month).mean()
    #         col_name = [name + "," + str(i) for i in range(1, 13)]

    #         if i == 1:
    #             final_data = temp.transpose()
    #             final_data = final_data.set_axis(col_name, axis=1)
    #         else:
    #             temp_data = temp.transpose()
    #             temp_data = temp_data.set_axis(col_name, axis=1)
    #             final_data = pd.concat((final_data, temp_data), axis = 1)

    #     return final_data

    # data_0_mean = temporal_agg(df1,extreme_scen)
    # data_1_mean = temporal_agg(df2,extreme_scen)
    # data_2_mean = temporal_agg(df3,extreme_scen)

    data_0_mean = df1
    data_1_mean = df2
    data_2_mean = df3

    # Choose direst GCM per basin

    mean_values = np.stack(
        (
            data_0_mean.mean(axis=1).values,
            data_1_mean.mean(axis=1).values,
            data_2_mean.mean(axis=1).values,
        )
    )
    # data_3_mean.mean(axis = 1).values))

    final_array = []
    for i, min_col in enumerate(mean_values.argmin(axis=0)):
        if min_col == 0:
            final_array.append(data_0_mean.iloc[i].values)
        elif min_col == 1:
            final_array.append(data_1_mean.iloc[i].values)
        elif min_col == 2:
            final_array.append(data_2_mean.iloc[i].values)
        # else:
        #     final_array.append(data_3_mean.iloc[i].values)
    if GCM == "dry":
        # Final data frame after choosing driest GCM
        dfs_dry[scen] = pd.DataFrame(final_array, columns=data_0_mean.columns)

        dfs_dry[scen].to_csv(wd + f"\{var}_1ymonthly_{GCM}_unprocessed_{scen}.csv")
        # Store for next processing
        df = dfs_dry[scen]

    elif GCM == "mmean":

        # Final data frame after choosing multi model mean
        df_mmmean[scen] = (
            pd.concat([data_0_mean, data_1_mean, data_2_mean]).groupby(level=0).mean()
        )
        df_mmmean[scen].to_csv(wd + f"\{var}_1ymonthly_{GCM}_unprocessed_{scen}.csv")
        # Store for next processing

        df = df_mmmean[scen]
        d
        df1.iloc[:, 8:]
        # Convert to 5 year annual values
        new_cols = pd.to_datetime(df_mmmean[scen].columns, format="%Y,%m")
        df_mmmean[scen].columns = new_cols

        dfs_ann[scen] = df_mmmean[scen].resample("5Y", axis=1).mean()

        dfs_ann[scen].to_csv(wd + f"\{var}_5y_{GCM}_{extreme_scen}_{scen}_{intp}.csv")

        df = df1.iloc[:, 8:]
        df = df3.iloc[:, 8:]
        new_cols = pd.to_datetime(df.columns, format="sum.X%Y.%m.%d")
        df.columns = new_cols
        df = df.resample("5Y", axis=1).mean()

        df.to_csv(wd + f"\{var}_5y_{climmodels[2]}_{scen}.csv")

    print("5 year Monthly values and quantiles processed")

    # # Convert monthly values to wet and dry
    # for z in range(len(df.columns) // 12):

    #     col_start = 0 if z == 0 else col_end  # start col number
    #     col_end = (z + 1) * 12  # ending col number
    #     data = df.iloc[:, col_start:col_end]  # assigning relevant data
    #     maxindex = data.idxmax(axis=1)  # getting max indexes

    #     wet_name = "mean." + maxindex[0][:4] + "_wet"
    #     dry_name = "mean." + maxindex[0][:4] + "_dry"

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

    #     if z == 0:
    #         dfs_wd[scen] = year_data.copy()
    #     else:
    #         dfs_wd[scen] = pd.concat([dfs_wd[scen], year_data], axis=1)

    # dfs_wd[scen].to_csv(wd + f"\{var}_wetdry_{GCM}_unprocessed_{scen}.csv")

    # print("Wet Dry values processed")

    # # Calculating environmental flows using wet and dry seasonal calculations
    # for i in range(len(dfs_wd[scen].columns) // 2):
    #     col_start = 0 if i == 0 else col_end
    #     col_end = (i + 1) * 2
    #     data = dfs_wd[scen].iloc[:, col_start:col_end]

    #     name = data.columns[0][5:9]

    #     wet_mul = data.iloc[:, 0]
    #     dry_mul = data.iloc[:, 1]

    #     mul_data = (wet_mul + dry_mul) / 2

    #     if i == 0:
    #         dfs_ann[scen] = pd.DataFrame(mul_data, columns=[name])
    #     else:
    #         tmp_data = pd.DataFrame(mul_data, columns=[name])
    #         dfs_ann[scen] = pd.concat([dfs_ann[scen], tmp_data], axis=1)

    # dfs_ann[scen].to_csv(wd + f"\{var}_5yann_{GCM}_unprocessed_{scen}.csv")

    # print("5 year annual values processed")


#%%
# def env_flow(df, context, wetvaldev, dryvaldev, wetvalind, dryvalind):

#     # reading basin mapping to countries
#     FILE = f"basins_country_{context.regions}.csv"
#     PATH = private_data_path("water", "delineation", FILE)

#     basin = pd.read_csv(PATH)
#     basin["BCU_name"] = "B" + basin["BCU_name"]

#     bcu_names = df.basin

#     for i in range(len(df.columns) // 2):

#         col_start = 0 if i == 0 else col_end
#         col_end = (i + 1) * 2

#         data = df.iloc[:, col_start:col_end]
#         data["BCU_name"] = bcu_names

#         name = data.columns[0][5:9]

#         value = []
#         for j in df.basin.unique():
#             temp = basin[basin["BCU_name"] == j]
#             sizes = temp.pivot_table(index=["STATUS"], aggfunc="size")
#             # if size of countries in basins is > 1
#             if len(sizes) > 1:
#                 # Developing Basins
#                 if sizes["DEV"] > sizes["IND"] or sizes["DEV"] == sizes["IND"]:
#                     temp2 = data[data["BCU_name"] == j]
#                     wet_mul = temp2.iloc[:, 0] * wetvaldev
#                     dry_mul = temp2.iloc[:, 1] * dryvaldev

#                     mul_data = (wet_mul + dry_mul) / 2
#                     value.append(mul_data.values)
#                 # Developed basins
#                 else:
#                     temp2 = data[data["BCU_name"] == j]
#                     wet_mul = temp2.iloc[:, 0] * wetvalind
#                     dry_mul = temp2.iloc[:, 1] * dryvalind

#                     mul_data = (wet_mul + dry_mul) / 2
#                     value.append(mul_data.values)
#             # no. of basin in country = 1
#             else:
#                 # Developing Basins
#                 if sizes.index[0] == "DEV":
#                     temp2 = data[data["BCU_name"] == j]
#                     wet_mul = temp2.iloc[:, 0] * wetvaldev
#                     dry_mul = temp2.iloc[:, 1] * dryvaldev

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
