"""
This script aggregates the global gridded data to any
scale and also adjust unit conversions. The following
script specifically aggregates global gridded hydrological
data onto the basin mapping used in the nexus module.

The hydrological data can be accessed in watxene p drive. For accessing
particular drive, seek permission from Edward Byers (byers@iiasa.ac.at)
The files should be copied on to local drive
"""

import glob

#  Import packages
import os
from datetime import datetime as dt
from itertools import product

import dask
import xarray as xr

start = dt.now()

#: Specify user
username = "meas"  # options: "meas", "mengm_unicc"

#: ISIMIP version
isimipvers = "3b"

#: Monthly scale
monthlyscale = False

#: Type of data
#: Options: "historical", "future"
datatype = ["future", "historical"]

#: Water variables
#: Options: "qtot", "dis", "qr", "qg"
#: For detailed symbols, refer to ISIMIP2b documentation
#: NOTE: "qg" (groundwater runoff) seems to be an option but
#: no code has been written for aggregating this variable
variables = [
    "qtot",  # total runoff
    "dis",  # discharge
    "qr",  # groundwater recharge
]


def set_scenarios(isimip):
    if isimip == "2b":
        climmodels = [
            "gfdl-esm2m",
            "hadgem2-es",
            "ipsl-cm5a-lr",
            "miroc5",
        ]

        scenarios = ["rcp26", "rcp60"]
    else:
        climmodels = [
            "gfdl-esm4",
            "ipsl-cm6a-lr",
            "mpi-esm1-2-hr",
            "mri-esm2-0",
            "ukesm1-0-ll",
        ]

        scenarios = ["ssp126", "ssp370"]
    return climmodels, scenarios


def set_paths(user, isimip, cl, data):
    # Specify path depending on user
    if user == "meas":
        pdrive_path = "/Volumes/mengm.pdrv"
        enemodel = "ene.model"
    elif user == "mengm_unicc":
        pdrive_path = "/pdrive/projects/"
        enemodel = "ene.model3"
    else:
        pdrive_path = "/pdrive/projects/"
        enemodel = "ene.model"

    # Set path depending on isimip scenario
    if isimip == "2b":
        path_hydrology = (
            os.path.join(pdrive_path, enemodel, "NEST", "hydrology") + os.sep
        )
        path_water = (
            os.path.join(
                pdrive_path, "watxene", "ISIMIP", "ISIMIP2b", "output", "LPJmL"
            )
            + os.sep
        )
        path_isimip = (
            os.path.join(pdrive_path, enemodel, "NEST", "hydrology", "isimip2b")
            + os.sep
        )
    else:
        path_hydrology = os.path.join(pdrive_path, enemodel, "NEST", "hydrology")
        path_water = (
            os.path.join(
                pdrive_path,
                "watxene",
                "ISIMIP",
                "ISIMIP3b",
                "CWatM_results",
                cl,
                data,
            )
            + os.sep
        )
        path_isimip = (
            os.path.join(
                pdrive_path,
                enemodel,
                "NEST",
                "hydrology",
                "processed_nc4-isimip3b",
            )
            + os.sep
        )
    return path_hydrology, path_water, path_isimip


def process_raster(user, isimip, cl, scen, var, data):
    print(
        "Processing ISIMIP "
        + isimip
        + " - "
        + cl
        + " - "
        + scen
        + " - "
        + var
        + " - "
        + data
    )

    path_hydrology, path_water, path_isimip = set_paths(user, isimip, cl, data)

    # define lat and long chunk for reducing computational load
    latchunk = 120
    lonchunk = 640

    # NOTE: the previous version of the script had a spatialmethod parameter
    # but this parameter is not used anywhere
    # spatialmethod == "meansd" if var == "dis" else "sum"

    # Open raster area file
    # The file landareamaskmap0.nc can be found under
    # P:\ene.model\NEST\delineation\data\delineated_basins_new
    area = xr.open_dataarray(os.path.join(path_hydrology, "landareamaskmap0.nc"))

    # TO AVOID ERROR WHEN OPENING AND SLICING INPUT DATA - CHECK!
    dask.config.set({"array.slicing.split-large-chunks": False})

    if data == "historical":
        hydro_data = path_water + f"*{cl}*{var}*monthly*.nc"
    elif data == "future":
        hydro_data = path_water + f"*{cl}*{scen}*{var}*monthly*.nc"

    files = glob.glob(hydro_data)
    da = xr.open_mfdataset(files)

    if monthlyscale:
        if var == "dis":
            # # Converts the discharge  into km3/year
            da = da * 0.031556952
            da["dis"] = da.dis.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(da.dis.time)}
            )

            da.dis.attrs["unit"] = "km3/year"

            # chunking reduces computational burden
            da["dis"] = da.dis.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(da.dis.time)}
            )

            # saving the file here and reloading reduces disk space
            da.to_netcdf(path_isimip + f"{var}_monthly__{cl}_{scen}.nc")
        elif var == "qtot":
            # Filter 5 year interval monthly time step
            # da = da.sel(time=da.time.dt.year.isin(years))
            # 1kg/m2/sec = 86400 mm/day
            # 86400 mm/day X  Area (mm2) = 86400 mm3/day
            # 86400 mm3/day = 86400 X  1000000 3.65 e-16 km3/year
            da = da * 86400 * area * 3.65e-16 * 1000000
            da.to_netcdf(path_isimip + f"{var}_monthly_{cl}_{scen}_{data}.nc")
        elif var == "qr":
            # Filter 5 year interval monthly time step
            # da = da.sel(time=da.time.dt.year.isin(years))
            # 1kg/m2/sec = 86400 mm/day
            # 86400 mm/day X  Area (mm2) = 86400 mm3/day
            # 86400 mm3/day = 86400 X  1000000 3.65 e-16 km3/year
            da = da * 86400 * area * 3.65e-16 * 1000000
            da.qr.attrs["unit"] = "km3/year"
            da.to_netcdf(path_isimip + f"{var}_monthly_{cl}_{scen}_{data}.nc")

    else:
        if var == "dis":
            da["dis"] = da.dis.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(da.dis.time)}
            )
            # Resample daily data to monthly (by summing daily values)
            da = da.resample(time="YE").mean()
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
            davg.to_netcdf(path_isimip + f"{var}_90Y_avg_5y__{cl}_{scen}_temp_agg.nc")

            # Now resample to an average value for each 5-year block, and
            # offset by 2 years so that the value is centred to start 2101
            da = da.resample(time="5Y", loffset="4Y").mean()

            da.to_netcdf(path_isimip + f"{var}_5y__{cl}_{scen}_temp_agg.nc")
            # read in saved data again to further process
            da = xr.open_dataset(path_isimip + f"{var}_5y__{cl}_{scen}_temp_agg.nc")

        elif var == "qtot":
            da["qtot"] = da.qtot.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(da.qtot.time)}
            )
            # Resample daily data to monthly (by summing daily values)
            da = da.resample(time="YE").mean()
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

            # Now resample to an average value for each 5-year block
            da = da.resample(time="5Y", loffset="4Y").mean()
            da.to_netcdf(path_isimip + f"{var}_monthly__{cl}_{scen}_temp_agg.nc")

        elif var == "qr":
            da["qr"] = da.qr.chunk(
                {"lat": latchunk, "lon": lonchunk, "time": len(da.qr.time)}
            )
            # Resample daily data to monthly (by summing daily values)
            da = da.resample(time="YE").mean()
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

            # Now resample to an average value for each 5-year block
            da = da.resample(time="5Y", loffset="4Y").mean()

            da.to_netcdf(path_isimip + f"{var}_5y__{cl}_{scen}.nc")

    print(
        "...Completed ISMIP "
        + isimip
        + " - "
        + cl
        + " - "
        + scen
        + " - "
        + var
        + " - "
        + data
    )


if __name__ == "__main__":
    # Select climate models and scenarios to run based on ISIMIP version
    clim_models, scenarios = set_scenarios(isimipvers)

    # Run the script for all combinations of user, ISIMIP version,
    # climate model, scenario, variable, and data type
    for user, isimip, cl, scen, var, data in product(
        [username], [isimipvers], clim_models, scenarios, variables, datatype
    ):
        process_raster(user, isimip, cl, scen, var, data)
