"""
This script aggregates the global gridded data to any
scale and also adjust unit conversions. The following
script specifically aggregates global gridded hydrological
data onto the basin
 mapping used in the nexus module.
"""
import glob

#  Import packages
import os
from datetime import datetime as dt

import dask
import numpy as np
import xarray as xr

start = dt.now()


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
climmodels = ["gfdl-esm2m", "hadgem2-es", "ipsl-cm5a-lr", "miroc5"]

for cl in climmodels:
    # climate model
    if isimip == "2b":
        climmodels = ["gfdl-esm2m", "hadgem2-es", "ipsl-cm5a-lr", "miroc5"]
        climmodel = "gfdl-esm2m"
        # climate forcing
        scenarios = ["rcp26", "rcp60"]
        scen = "rcp26"
        wd1 = os.path.join("p:", "ene.model", "NEST", "hydrological_data_agg") + os.sep
        wd = (
            os.path.join("p:", "watxene", "ISIMIP", "ISIMIP2b", "output", "LPJmL")
            + os.sep
        )
        wd2 = (
            os.path.join("p:", "ene.model", "NEST", "hydrology", "processed_nc4")
            + os.sep
        )
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
        wd = (
            os.path.join(
                "p:",
                "watxene",
                "ISIMIP",
                "ISIMIP3b",
                "CWatM_results",
                f"{cl}",
                f"{data}",
            )
            + os.sep
        )
        wd2 = (
            os.path.join("p:", "ene.model", "NEST", "hydrology", "processed_nc4")
            + os.sep
        )

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
    area = xr.open_dataarray(wd1 + "landareamaskmap0.nc")

    # TO AVOID ERROR WHEN OPENING AND SLICING INPUT DATA - CHECK!
    dask.config.set({"array.slicing.split-large-chunks": False})

    if data == "historical":
        hydro_data = wd + f"*{cl}*{var}*monthly*.nc"
    elif data == "future":
        hydro_data = wd + f"*{cl}*{scen}*{var}*monthly*.nc"

    files = glob.glob(hydro_data)
    if var != "qr":
        # Open hydrological data as a combined dataset
        da = xr.open_mfdataset(files)
    else:
        # Open hydrological data as a combined dataset
        da = xr.open_mfdataset(files)

        # da.to_netcdf(wd+f'\{var}_memean_{scen}.nc')

    if monthlyscale:
        years = np.arange(2010, 2105, 5)
    else:
        years = np.arange(2010, 2105, 5)

    # ds = xr.open_dataset(wd+f'{var}_memean_{scen}.nc')
    # Resample daily data to monthly by summing daily values

    # if var != "qr"\:
    #     da = da.resample(time="M").mean()
    #     # Calculate roolling average of 3 months to make the data consistent
    #     da = da.rolling(time=3, min_periods=1).mean()

    # da = da.fillna(0)

    # %%
    if monthlyscale:
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

            da.to_netcdf(wd2 + f"{var}_monthly_{cl}_{scen}_{data}.nc")
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

            da.to_netcdf(wd2 + f"{var}_monthly_{cl}_{scen}_{data}.nc")

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
            davg.to_netcdf(wd2 + f"{var}_90Y_avg_5y__{climmodel}_{scen}_temp_agg.nc")

            # Now resample to an average value for each 5-year block, and
            # offset by 2 years so that the value is centred to start 2101
            da = da.resample(time="5Y", loffset="4Y").mean()

            da.to_netcdf(wd2 + f"{var}_5y__{climmodel}_{scen}_temp_agg.nc")
            # read in saved data again to further process
            da = xr.open_dataset(wd2 + f"{var}_5y__{climmodel}_{scen}_temp_agg.nc")

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
            da.to_netcdf(wd2 + f"{var}_monthly__{climmodel}_{scen}_temp_agg.nc")
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

            da.to_netcdf(wd2 + f"{var}_5y__{climmodel}_{scen}.nc")
