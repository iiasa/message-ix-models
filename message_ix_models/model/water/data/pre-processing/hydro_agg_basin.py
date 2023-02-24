"""
This script aggregates the global gridded data to any scale. The following
script specifically aggregates global gridded hydrological data onto the basin
 mapping used in the nexus module.
"""

import os

import numpy as np
import pandas as pd

# variable, for detailed symbols, refer to ISIMIP2b documentation
variables = ["qtot", "dis", "qr"]  # total runoff  # discharge  # groundwater run

var = "qr"


GCMs = ["mmean", "dry"]
GCM = "mmean"  # 'dry'choose driest GCM else 'mmean' for multi model mean
# else mmmean
iso3 = "ZMB"
isimip = "3b"

# climate model
if isimip == "2b":
    climmodels = ["gfdl-esm2m", "hadgem2-es", "ipsl-cm5a-lr", "miroc5"]
    climmodel = "gfdl-esm2m"
    # climate forcing
    scenarios = ["rcp26", "rcp60"]
    scen = "rcp26"
    # This is an internal IIASA directory path
    wd1 = os.path.join("p:", "ene.model", "NEST", "hydrological_data_agg") + os.sep
    wd = os.path.join("p:", "watxene", "ISIMIP", "ISIMIP2b", "output", "LPJmL") + os.sep
    wd2 = os.path.join("p:", "ene.model", "NEST", "hydrology", "processed_nc4") + os.sep
else:
    climmodels = [
        "gfdl-esm4",
        "ipsl-cm6a-lr",
        "mpi-esm1-2-hr",
        "mri-esm2-0",
        "ukesm1-0-ll",
    ]
    cl = "gfdl-esm4"
    scenarios = ["ssp126", "ssp370", "ssp585"]
    scen = "ssp126"
    wd1 = os.path.join("p:", "ene.model", "NEST", "hydrological_data_agg") + os.sep
    wd11 = (
        os.path.join("p:", "ene.model", "NEST", iso3, "hydrology", "isimip3B_CWatM")
        + os.sep
    )


qtot_7p0_gfdl = pd.read_csv(wd11 + f"{var}_monthly_gfdl-esm4_ssp370_future.csv").drop(
    [
        "Unnamed: 0",
        "NAME",
        "BASIN",
        "BASIN_ID",
        "area",
        "area_km2",
        "X",
        "REGION",
        "BCU_name",
    ],
    axis=1,
)

new_cols = pd.date_range("2015-01-01", periods=1032, freq="M")
qtot_7p0_gfdl.columns = new_cols


qtot_2p6_gfdl = pd.read_csv(wd11 + f"{var}_monthly_gfdl-esm4_ssp126_future.csv").drop(
    [
        "Unnamed: 0",
        "NAME",
        "BASIN",
        "BASIN_ID",
        "area",
        "area_km2",
        "X",
        "REGION",
        "BCU_name",
    ],
    axis=1,
)

new_cols = pd.date_range("2015-01-01", periods=1032, freq="M")
qtot_2p6_gfdl.columns = new_cols


qtot_2p6_gfdl_avg = qtot_2p6_gfdl[pd.date_range("2015-01-01", periods=192, freq="M")]
qtot_7p0_gfdl_avg = qtot_7p0_gfdl[pd.date_range("2015-01-01", periods=192, freq="M")]

val_2020 = (
    qtot_7p0_gfdl_avg.groupby(qtot_7p0_gfdl_avg.columns.month, axis=1).mean()
    + qtot_2p6_gfdl_avg.groupby(qtot_2p6_gfdl_avg.columns.month, axis=1).mean()
) / 2
val_2020_annual = val_2020.mean(axis=1)

delta60 = (
    qtot_7p0_gfdl_avg.groupby(qtot_7p0_gfdl_avg.columns.month, axis=1).mean() - val_2020
)
delta26 = (
    qtot_2p6_gfdl_avg.groupby(qtot_2p6_gfdl_avg.columns.month, axis=1).mean() - val_2020
)


def bias_correction(df):
    """ias corrects the data such that 2020 value is same
    for both scenario and then apply
    Input
    ----------
    df : raw input monthly data
    Returns
    -------
    df : bias corrected monthly data with also replacing 5 year timestep average
    df_monthly: monthly bias corrected data
    df_5y_m: bias corrected 5 y monthly
    df_5y: bias corrected 5 y average
    """
    # Starting value of delat is 1
    # it will reduce to zero with each 5 year timestep
    # till 2045 with a difference of 0.2.
    # This means the bias correction will fade away till 2045
    delta_multiply = 1

    for year in np.arange(2020, 2105, 5):
        # for 2020 delta all scenario data frame will be same
        if year == 2020:
            df[pd.date_range("2020-01-01", periods=12, freq="M")] = val_2020

        else:
            # for delta years after 2020
            if delta_multiply > 0.1:
                for i in np.arange(4, -1, -1):
                    if i == 4:
                        delta60.columns = pd.date_range(
                            str(year - i) + "-01-01", periods=12, freq="M"
                        )
                        final_temp = df[
                            pd.date_range(
                                str(year - i) + "-01-01", periods=12, freq="M"
                            )
                        ] - (delta_multiply * delta60)

                    else:
                        delta60.columns = pd.date_range(
                            str(year - i) + "-01-01", periods=12, freq="M"
                        )
                        temp = df[
                            pd.date_range(
                                str(year - i) + "-01-01", periods=12, freq="M"
                            )
                        ] - (delta_multiply * delta60)

                        final_temp = pd.concat((final_temp, temp), axis=1)

                df_monthly = final_temp
                df[
                    pd.date_range(str(year) + "-01-01", periods=12, freq="M")
                ] = final_temp.groupby(final_temp.columns.month, axis=1).mean()
                # 5 year monthly data
                df_5y_m = df[df.columns[df.columns.year.isin(years)]]
                # 5 year annual
                # 50th quantile - q50
                df_q50 = (
                    df_monthly.rolling(240, min_periods=1, axis=1)
                    .quantile(0.5, interpolation="linear")
                    .resample("5Y", axis=1)
                    .mean()
                )
                df_q50["2020-12-31"] = val_2020_annual
                # 70th quantile - q70
                df_q70 = (
                    df_monthly.rolling(240, min_periods=1, axis=1)
                    .quantile(0.3, interpolation="linear")
                    .resample("5Y", axis=1)
                    .mean()
                )
                df_q70["2020-12-31"] = val_2020_annual
                # 90th quantile - q90
                df_q90 = (
                    df_monthly.rolling(240, min_periods=1, axis=1)
                    .quantile(0.1, interpolation="linear")
                    .resample("5Y", axis=1)
                    .mean()
                )

                df_q90["2020-12-31"] = val_2020_annual

                delta_multiply -= 0.2

            else:
                # after detlta years
                temp_daterange = pd.date_range(
                    str(year - 4) + "-01-01", periods=60, freq="M"
                )
                # Monthly Bias corrected data
                df_monthly = df

                df[pd.date_range(str(year) + "-01-01", periods=12, freq="M")] = (
                    df[temp_daterange]
                    .groupby(df[temp_daterange].columns.month, axis=1)
                    .mean()
                )
                # 5 year monthly
                df_5y_m = df[df.columns[df.columns.year.isin(years)]]
                # 5 year annual
                # 50th quantile - q50
                df_q50 = (
                    df_monthly.rolling(240, min_periods=1, axis=1)
                    .quantile(0.5, interpolation="linear")
                    .resample("5Y", axis=1)
                    .mean()
                )
                df_q50["2020-12-31"] = val_2020_annual
                # 70th quantile - q70
                df_q70 = (
                    df_monthly.rolling(240, min_periods=1, axis=1)
                    .quantile(0.3, interpolation="linear")
                    .resample("5Y", axis=1)
                    .mean()
                )
                df_q70["2020-12-31"] = val_2020_annual
                # 90th quantile - q90
                df_q90 = (
                    df_monthly.rolling(240, min_periods=1, axis=1)
                    .quantile(0.1, interpolation="linear")
                    .resample("5Y", axis=1)
                    .mean()
                )

                df_q90["2020-12-31"] = val_2020_annual

    return df, df_monthly, df_5y_m, df_q50, df_q70, df_q90


years = np.arange(2015, 2105, 5)

df_26, df_monthly_26, df_5y_m_26, df_q50_26, df_q70_26, df_q90_26 = bias_correction(
    qtot_2p6_gfdl
)

df_70, df_monthly_70, df_5y_m_70, df_q50_70, df_q70_70, df_q90_70 = bias_correction(
    qtot_7p0_gfdl
)


df_monthly_26.to_csv(wd11 + f"/{var}_monthly_2p6_{iso3}.csv")
df_5y_m_26.to_csv(wd11 + f"/{var}_5y_m_2p6_low_{iso3}.csv")
df_q50_26.to_csv(wd11 + f"/{var}_5y_2p6_low_{iso3}.csv")
df_q70_26.to_csv(wd11 + f"/{var}_5y_2p6_med_{iso3}.csv")
df_q90_26.to_csv(wd11 + f"/{var}_5y_2p6_high_{iso3}.csv")

df_monthly_70.to_csv(wd11 + f"/{var}_monthly_7p0_{iso3}.csv")
df_5y_m_70.to_csv(wd11 + f"/{var}_5y_m_7p0_low_{iso3}.csv")
df_q50_70.to_csv(wd11 + f"/{var}_5y_7p0_low_{iso3}.csv")
df_q70_70.to_csv(wd11 + f"/{var}_5y_7p0_med_{iso3}.csv")
df_q90_70.to_csv(wd11 + f"/{var}_5y_7p0_high_{iso3}.csv")

# No climate scenarios
df_q50_70.apply(lambda x: val_2020_annual)
for y in years:
    df_5y_m_70[pd.date_range(f"{y}-01-01", periods=12, freq="M")] = val_2020
df_5y_m_70.to_csv(wd11 + f"/{var}_5y_m_no_climate_low_{iso3}.csv")
df_q50_70.apply(lambda x: val_2020_annual).to_csv(
    wd11 + f"/{var}_5y_no_climate_low_{iso3}.csv"
)
df_q70_70.apply(lambda x: val_2020_annual).to_csv(
    wd11 + f"/{var}_5y_no_climate_med_{iso3}.csv"
)
df_q90_70.apply(lambda x: val_2020_annual).to_csv(
    wd11 + f"/{var}_5y_no_climate_high_{iso3}.csv"
)
# Environmental Flow
df_env = df_monthly_70
col_end = None
# df = data.iloc[:,5:]
for z in range(len(df_env.columns) // 12):
    col_start = 0 if z == 0 else col_end  # start col number
    col_end = (z + 1) * 12  # ending col number
    temp = df_env.iloc[:, col_start:col_end]  # assigning relevant data

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
        eflow = temp
    else:
        eflow = pd.concat((eflow, temp), axis=1)

    eflow = eflow.abs()

# Convert to 5 year annual values
eflow_5y = eflow.resample("5Y", axis=1).mean()
eflow_5y.to_csv(wd11 + f"e-flow_7p0_{iso3}.csv")

for year in np.arange(2020, 2105, 5):
    for i in np.arange(4, -1, -1):
        if i == 4:
            final_temp = eflow[
                pd.date_range(str(year - i) + "-01-01", periods=12, freq="M")
            ]

        else:
            final_temp = eflow[
                pd.date_range(str(year - i) + "-01-01", periods=12, freq="M")
            ]

    df_monthly = final_temp
    eflow[
        pd.date_range(str(year) + "-01-01", periods=12, freq="M")
    ] = final_temp.groupby(final_temp.columns.month, axis=1).mean()

    eflow_5y_m = eflow[eflow.columns[eflow.columns.year.isin(years)]]
    eflow_5y_m.to_csv(wd11 + f"e-flow_5y_m_7p0_{iso3}.csv")

val_2020_eflow = eflow_5y_m[pd.date_range("2020-01-01", periods=12, freq="M")].values()
val_2020_eflowy = eflow_5y["2020-12-31"]
eflow_5y.apply(lambda x: val_2020_eflowy).to_csv(wd11 + f"e-flow_no_climate_{iso3}.csv")

for y in years:
    eflow_5y_m[pd.date_range(f"{y}-01-01", periods=12, freq="M")] = val_2020_eflow

eflow_5y_m.to_csv(wd11 + f"e-flow_5y_m_no_climate_{iso3}.csv")
