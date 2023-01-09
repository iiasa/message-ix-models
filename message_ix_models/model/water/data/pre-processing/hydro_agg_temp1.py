"""
This script aggregates the global gridded data to any scale. The following
script specifically aggregates global gridded hydrological data onto the basin
 mapping used in the nexus module.
"""
import sys
import os

print(sys.executable)
#  Import packages
from datetime import datetime as dt
import xarray as xr
import numpy as np
import pandas as pd

# import salem
import glob
import dask

start = dt.now()
from dask.diagnostics import ProgressBar

# from salem import open_wrf_dataset, get_demo_file


# variable, for detailed symbols, refer to ISIMIP2b documentation
variables = [
    "qtot",  # total runoff
    "dis",  # discharge
    "qg",  # groundwater runoff
    "qr",
]  # groudnwater recharge
var = "qr"

isimip = "3b"

data = "future"  # else future

#%%
# if multimodelensemble:
#     # iterate through scenarios (3)
#     for model in climmodels:
#         hydro_data = wd + f"\hydrology\{model}*{scen}*{var}*.nc4"
#         files = glob.glob(hydro_data)
#         ds = xr.Dataset()
#         # Iterate through 5 GCMs for each scenario
#         for file in files:
#             model = file[57:-57]
#             da = xr.open_dataarray(file, decode_times=False)  # open
#             # da['time'] = np.arange(2005,2100, dtype=int) #fix time
#             ds[model] = da  # add to main dataset
#         # collapse the 4 GCMs to one dataarray and take mean
#         ds["multimodelmean"] = ds.to_array(dim="memean").mean(axis=0)
#         # Assign attributes
#         ds.attrs["scenario"] = scen
#         ds.to_netcdf(wd + f"{var}_{scen}_non_agg_combined.nc")

# else:
# wd = "P:\watxene\ISIMIP\ISIMIP3b\CWatM_results\gfdl-esm4\future"

for cl in climmodels:
    # climate model
    if isimip == "2b":
        climmodels = ["gfdl-esm2m", "hadgem2-es", "ipsl-cm5a-lr", "miroc5"]
        climmodel = "gfdl-esm2m"
        # climate forcing
        scenarios = ["rcp26", "rcp60"]
        scen = "rcp26"
        wd1 = os.path.join("p:", "ene.model", "NEST", "hydrological_data_agg")
        wd = os.path.join("p:", "watxene", "ISIMIP", "ISIMIP2b", "output", "LPJmL")
        wd2 = os.path.join("p:", "ene.model", "NEST", "hydrology", "processed_nc4")
    else:
        climmodels = [
            "gfdl-esm4",
            "ipsl-cm6a-lr",
            "mpi-esm1-2-hr",
            "mri-esm2-0",
            "ukesm1-0-ll",
        ]
        scenarios = ["ssp126", "ssp370", "ssp585"]
        scen = "ssp126"
        wd1 = os.path.join("p:", "ene.model", "NEST", "hydrological_data_agg")
        wd = os.path.join(
            "p:", "watxene", "ISIMIP", "ISIMIP3b", "CWatM_results", f"{cl}", f"{data}"
        )
        wd2 = os.path.join("p:", "ene.model", "NEST", "hydrology", "processed_nc4")

    # Define if monthly aggregation is required
    monthlyscale = True
    # define qauantile for statistical aggregation
    quant = 0.1
    # define if multi climate models mean
    multimodelensemble = True
    # deinfe lat and long chunk for reducing computational load
    latchunk = 120
    lonchunk = 640
    # Define if use all touched raster
    all_touched = True

    if var == "dis":
        # define a spatial method to aggregate
        spatialmethod = "meansd"
    else:
        # define a spatial method to aggregate
        spatialmethod = "sum"

    """
    The hydrological data can be accessed in watxene p drive. For accessing
    particular drive, seek permission from Edward Byers (byers@iiasa.ac.at)
    The files should be copied on to local drive
    """

    # Open raster area file
    # The file landareamaskmap0.nc can be found under
    # P:\ene.model\NEST\delineation\data\delineated_basins_new

    # t = np.arange(datetime(2006,1,1), datetime(2099,1,1), timedelta(month =1)).astype(datetime)
    # t = np.arange(np.datetime64('2006-01-01'), np.datetime64('2099-01-08'))
    area = xr.open_dataarray(wd1 + "\landareamaskmap0.nc")

    # TO AVOID ERROR WHEN OPENING AND SLICING INPUT DATA - CHECK!
    dask.config.set({"array.slicing.split-large-chunks": False})

    if data == "historical":
        hydro_data = wd + f"\*{cl}*{var}*monthly*.nc"
    elif data == "future":
        hydro_data = wd + f"\*{cl}*{scen}*{var}*monthly*.nc"

    files = glob.glob(hydro_data)
    if var != "qr":
        # Open hydrological data as a combined dataset
        da = xr.open_mfdataset(files)
    else:
        # Open hydrological data as a combined dataset
        da = xr.open_mfdataset(files)
        # da["time"] = pd.date_range(start="1/1/2015", end="31/12/2100", freq="M")

        # da.to_netcdf(wd+f'\{var}_memean_{scen}.nc')

    if monthlyscale:
        # da = da.sel(time=slice('2010-01-01', '2025-12-31'))
        # years = pd.DatetimeIndex(da["time"].values)
        years = np.arange(2010, 2105, 5)
    else:
        years = np.arange(2010, 2105, 5)

    # ds = xr.open_dataset(wd+f'{var}_memean_{scen}.nc')
    # Resample daily data to monthly (by summing daily values)

    # if var != "qr":
    #     da = da.resample(time="M").mean()
    #     # Calculate roolling average of 3 months to make the data consistent
    #     da = da.rolling(time=3, min_periods=1).mean()

    # da = da.fillna(0)

    #%%
    if monthlyscale:
        # dpm = {
        #     "noleap": [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
        #     "365_day": [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
        #     "standard": [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
        #     "gregorian": [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
        #     "proleptic_gregorian": [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
        #     "all_leap": [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
        #     "366_day": [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
        #     "360_day": [0, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30],
        # }

        # def leap_year(year, calendar="standard"):
        #     """Determine if year is a leap year"""
        #     leap = False
        #     if (
        #         calendar in ["standard", "gregorian", "proleptic_gregorian", "julian"]
        #     ) and (year % 4 == 0):
        #         leap = True
        #         if (
        #             (calendar == "proleptic_gregorian")
        #             and (year % 100 == 0)
        #             and (year % 400 != 0)
        #         ):
        #             leap = False
        #         elif (
        #             (calendar in ["standard", "gregorian"])
        #             and (year % 100 == 0)
        #             and (year % 400 != 0)
        #             and (year < 1583)
        #         ):
        #             leap = False
        #     return leap

        # def get_dpm(time, calendar="standard"):
        #     """
        #     return a array of days per month corresponding to the months provided
        #     in `months`
        #     """
        #     month_length = np.zeros(len(time), dtype=np.int)
        #     cal_days = dpm[calendar]
        #     for i, (month, year) in enumerate(zip(time.month, time.year)):
        #         month_length[i] = cal_days[month]
        #         if leap_year(year, calendar=calendar):
        #             month_length[i] += 1
        #     return month_length

        # month_length = xr.DataArray(
        #     get_dpm(da.time.to_index(), calendar="standard"),
        #     coords=[da.time],
        #     name="month_length",
        # )

        # da = (da * month_length).resample(time="QS").sum() / month_length.resample(
        #     time="QS"
        # ).sum()

        if var == "dis":
            # # Converts the discharge  into km3/year
            da = da * 0.031556952
            da["dis"] = da.dis.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(da.dis.time)}
            )
            # slice is required to reduce the dataset.
            # da = da.sel(time=slice("2010-01-01", "2050-12-31"))

            da.dis.attrs["unit"] = "km3/year"
            # chunking reduces computational burden
            da["dis"] = da.dis.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(da.dis.time)}
            )
            # saving the file here and reloading reduces disk space
            da.to_netcdf(wd2 + f"{var}_monthly__{cl}_{scen}.nc")
        elif var == "qtot":
            # da["qtot"] = da.qtot.chunk(
            #     {"lat": latchunk, "lon": lonchunk, "time": len(da.qtot.time)}
            # )
            # Filter 5 year interval monthly time step
            # da = da.sel(time=da.time.dt.year.isin(years))
            # 1kg/m2/sec = 86400 mm/day
            # 86400 mm/day X  Area (mm2) = 86400 mm3/day
            # 86400 mm3/day = 86400 X  1000000 3.65 e-16 km3/year
            da = da * 86400 * area * 3.65e-16 * 1000000

            # Resample daily data to monthly (by summing daily values)
            # da = da.resample(time="M").mean()

            # da.rolling(time=12, center=True).construct(
            #     'tmp').quantile(.1, dim='tmp', skipna=False).dropna(
            #         'time')

            # da = da.rolling(time=20, min_periods=1).mean()
            # da["qtot"] = da.qtot.chunk(
            #     {"lat": latchunk, "lon": lonchunk, "time": len(da.qtot.time)}
            # )

            da.to_netcdf(wd2 + f"\{var}_monthly_{cl}_{scen}_{data}.nc")
        elif var == "qr":
            # Filter 5 year interval monthly time step
            # da = da.sel(time=da.time.dt.year.isin(years))
            # 1kg/m2/sec = 86400 mm/day
            # 86400 mm/day X  Area (mm2) = 86400 mm3/day
            # 86400 mm3/day = 86400 X  1000000 3.65 e-16 km3/year
            da = da * 86400 * area * 3.65e-16 * 1000000
            da.qr.attrs["unit"] = "km3/year"
            # da["qr"] = da.qr.chunk(
            #     {"lat": latchunk, "lon": lonchunk, "time": len(da.qr.time)}
            # )

            da.to_netcdf(wd2 + f"\{var}_monthly_{cl}_{scen}_{data}.nc")

    else:
        if var == "dis":
            da["dis"] = da.dis.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(da.dis.time)}
            )
            # Resample daily data to monthly (by summing daily values)
            da = da.resample(time="Y").mean()

            da["dis"] = da.dis.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(da.dis.time)}
            )
            # Take 20 year rolling average to make the time scale cosnistent
            da = da.rolling(time=20).mean()

            # Converts the discharge  into km3/year
            da = da * 0.031556952
            da.dis.attrs["unit"] = "km3/year"
            # Long term Mean annual  discharge
            davg = da.resample(time="30Y", loffset="4Y").mean()
            davg["dis"] = davg.dis.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(davg.dis.time)}
            )
            davg.to_netcdf(
                wd2 + f"\output\{var}_90Y_avg_5y__{climmodel}_{scen}_temp_agg.nc"
            )

            # Now resample to an average value for each 5-year block, and
            # offset by 2 years so that the value is centred to start 2101
            da = da.resample(time="5Y", loffset="4Y").mean()

            da.to_netcdf(wd2 + f"\output\{var}_5y__{climmodel}_{scen}_temp_agg.nc")
            # read in saved data again to further process
            da = xr.open_dataset(
                wd2 + f"\output\{var}_5y__{climmodel}_{scen}_temp_agg.nc"
            )

        elif var == "qtot":
            da["qtot"] = da.qtot.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(da.qtot.time)}
            )
            # Resample daily data to monthly (by summing daily values)
            da = da.resample(time="Y").mean()
            da["qtot"] = da.qtot.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(da.qtot.time)}
            )
            da = da.rolling(time=20, min_periods=1).mean()
            da["qtot"] = da.qtot.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(da.qtot.time)}
            )
            # Converts to total runoff per gs into km3/year
            da = da * 86400 * area * 3.65e-16 * 1000000
            da.qtot.attrs["unit"] = "km3/year"
            da["qtot"] = da.qtot.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(da.qtot.time)}
            )

            # Calculate roolling average of 20 years to make the data consistent
            # da = da.rolling(time = 20).mean()
            # Now resample to an average value for each 5-year block
            da = da.resample(time="5Y", loffset="4Y").mean()
            da.to_netcdf(wd2 + f"\output\{var}_monthly__{climmodel}_{scen}_temp_agg.nc")
            # read in saved data again to further process
            # da = xr.\
            #    open_dataset( wd2+f'\output\{var}_5y__{climmodel}_{scen}_temp_agg.nc')
            # da['qtot'] = da.qtot.chunk({'lat': latchunk, 'lon': lonchunk,\
            #                                    'time': len(da.qtot.time)})

        elif var == "qr":
            da["qr"] = da.qr.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(da.qr.time)}
            )
            # Resample daily data to monthly (by summing daily values)
            da = da.resample(time="Y").mean()
            da["qr"] = da.qr.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(da.qr.time)}
            )
            da = da.rolling(time=20, min_periods=1).mean()
            da["qr"] = da.qr.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(da.qr.time)}
            )
            # TODO check unit
            # 1kg/m2/sec = 86400 mm/day
            # 86400 mm/day X  Area (mm2) = 86400 mm3/day
            # 86400 mm3/day = 86400 X  1000000 3.65 e-16 km3/year
            da = da * 86400 * area * 3.65e-16 * 1000000
            da.qr.attrs["unit"] = "km3/year"
            da["qr"] = da.qr.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(da.qr.time)}
            )

            # Calculate roolling average of 20 years to make the data consistent
            # da = da.rolling(time = 3).mean()
            # Now resample to an average value for each 5-year block
            da = da.resample(time="5Y", loffset="4Y").mean()

            da.to_netcdf(wd2 + f"\output\{var}_5y__{climmodel}_{scen}.nc")
            # read in saved data again to further process
            # da = xr.\
        #     open_dataset( wd+f'\output\{var}_5y__{climmodel}_{scen}_temp_agg.nc')
#%%
# if all_touched:
#     # open raster fle in wdhsapes directory, if not there copy from p drive
#     basinraster = xr.open_dataarray(
#         wd + "\delineation\\basins_by_region_simpl_R11_at.nc"
#     )
# else:
#     # open raster fle in wdhsapes directory, if not there copy from p drive
#     basinraster = xr.open_dataarray(
#         wd + "\delineation\\basins_by_region_simpl_R11_nt.nc"
#     )
# # helps to load the dataset into memory
# with ProgressBar():
#     da.compute()
# #%% also
# # read shapefile of basins
# shp_fn = wd + "\delineation\\basins_by_region_simpl_R11.shp"
# shapes = salem.read_shapefile(shp_fn, cached=True)

# df = shapes[["BASIN_ID", "BCU_name"]]
# df["Model"] = f"LPJML"
# df["Scenario"] = f"{climmodel}_{scen}"
# df["Variable"] = f"{var}"
# df["Region"] = df["BCU_name"]

# if var == "dis":
#     df["Unit"] = "km3/year"
# else:
#     df["Unit"] = "km3/year"


# for row in df.itertuples():
#     for year in years:
#         basinid = row.Index  # BASIN_ID
#         print(row.Index)

#         if monthlyscale:
#             da_t = da.sel(time=da.time.isin(years))
#         else:
#             da_t = da.sel(time=da.time.dt.year.isin(years))
#         # for year in years:
#         yr = f"{year}"
#         if var == "dis":
#             val = da_t.dis.where(basinraster == basinid)
#         elif var == "qtot":
#             val = da_t.qtot.where(basinraster == basinid)
#         elif var == "qr":
#             val = da_t.qr.where(basinraster == basinid)

#         std = val.squeeze().std().values

#         # Try dim dimension and provide exact dimension
#         # Handle non-nans
#         if spatialmethod == "sum":
#             val = val.sum()
#         elif spatialmethod == "max":
#             val = val.max()
#         elif spatialmethod == "mean":
#             val = val.mean()
#         elif spatialmethod == "quantile":
#             val = val.quantile(quant)
#         elif spatialmethod == "meansd":
#             val = std + val.mean()

#         df.loc[row.Index, year] = val.values
#         print(dt.now() - start)

# #%%
# # val.to_netcdf(wd+ f'\output\{var}_5y__{climmodel}_{scen}_aggregated.nc')

# df.to_csv(
#     wd + "\output" + f"\LPJML_{var}_{climmodel}_{scen}_{spatialmethod}_5y_rasternew.csv"
# )


# if all_touched:

#     df.to_csv(
#         wd + "\output" + f"\LPJML_{var}_{climmodel}_{scen}_{spatialmethod}_5y_at.csv"
#     )
# else:
#     df.to_csv(
#         wd + "\output" + f"\LPJML_{var}_{climmodel}_{scen}_{spatialmethod}_5y_nt.csv"
#     )


# #%%
# air = ds.air.isel(time=0)

# df, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(8, 6))

# # The first plot (in kelvins) chooses "viridis" and uses the data's min/max
# air.plot(ax=ax1, cbar_kwargs={"label": "K"})
# ax1.set_title("Kelvins: default")
# ax2.set_xlabel("")

# # The second plot (in celsius) now chooses "BuRd" and centers min/max around 0
# airc = air - 273.15
# airc.plot(ax=ax2, cbar_kwargs={"label": "°C"})
# ax2.set_title("Celsius: default")
# ax2.set_xlabel("")
# ax2.set_ylabel("")

# # The center doesn't have to be 0
# air.plot(ax=ax3, center=273.15, cbar_kwargs={"label": "K"})
# ax3.set_title("Kelvins: center=273.15")

# # Or it can be ignored
# airc.plot(ax=ax4, center=False, cbar_kwargs={"label": "°C"})
# ax4.set_title("Celsius: center=False")
# ax4.set_ylabel("")

# # Make it nice
# plt.tight_layout()

# da = da.qtot.isel(time=da.time.isin(year)).salem.quick_map()
# da.salem.quick_map()
