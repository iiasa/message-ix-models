
"""
This script aggregates the global gridded data to any scale. The following script specifically aggregates global
gridded hydrological data onto the basin mapping used in the nexus module.
"""

#  Import packages
from datetime import datetime as dt
import xarray as xr
import numpy as np
import pandas as pd
import salem
import glob
start = dt.now()
from dask.diagnostics import ProgressBar


monthlyscale = False

# The hydrological data can be accessed in watxene p drive. For accessing particular drive, seek
# permission from Edward Byers
#TODO check if the hardcoded data paths can be changed
data_hydrology = 'P:\watxene\ISIMIP\ISIMIP2b\output\PCR-GLOBWB\gfdl-esm2m\future'
wd = 'P:\ene.model\NEST\delineation\data\delineated_basins_new'
# The shapefile and raster file should be copied under the following path from p drive
wdshapes = private_data_path("water", "delineation")

# rcp scenario
scen = 'rcp26'
# variable, for detailed symbols, refer to ISIMIP2b documentation
var = 'qtot'
# climate model
climscen = 'hadgem2-es'

#TODO check if the hardcoded data paths can be changed
ddstr = f'C:\\Users\\awais\\Documents\\GitHub\\awais\\basin_process\\hydrological_data\\*{climscen}*{scen}*{var}*.nc4'
files = glob.glob(ddstr)

# Open hydrological data as a combined dataset
qtot_y = xr.open_mfdataset(files)
# years array to be considered for processing
years = np.arange(2010, 2105, 5)


# The file landareamaskmap0.nc can be found under P:\ene.model\NEST\delineation\data\delineated_basins_new
# Open raster area file
area = xr.open_dataarray(wd + 'landareamaskmap0.nc')

# Resample daily data to monthly (by summing daily values)
qtot_y = qtot_y.resample(time='M').sum()


if monthlyscale:
    dpm = {'noleap': [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
           '365_day': [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
           'standard': [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
           'gregorian': [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
           'proleptic_gregorian': [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
           'all_leap': [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
           '366_day': [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
           '360_day': [0, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30]}


    def leap_year(year, calendar='standard'):
        """Determine if year is a leap year"""
        leap = False
        if ((calendar in ['standard', 'gregorian',
                          'proleptic_gregorian', 'julian']) and
                (year % 4 == 0)):
            leap = True
            if ((calendar == 'proleptic_gregorian') and
                    (year % 100 == 0) and
                    (year % 400 != 0)):
                leap = False
            elif ((calendar in ['standard', 'gregorian']) and
                  (year % 100 == 0) and (year % 400 != 0) and
                  (year < 1583)):
                leap = False
        return leap


    def get_dpm(time, calendar='standard'):
        """
        return a array of days per month corresponding to the months provided in `months`
        """
        month_length = np.zeros(len(time), dtype=np.int)
        cal_days = dpm[calendar]
        for i, (month, year) in enumerate(zip(time.month, time.year)):
            month_length[i] = cal_days[month]
            if leap_year(year, calendar=calendar):
                month_length[i] += 1
        return month_length


    month_length = xr.DataArray(
        get_dpm(qtot_y.time.to_index(), calendar='standard'),
        coords=[qtot_y.time],
        name='month_length'
    )
    qtot_y = ((qtot_y * month_length).resample(time='QS').sum() /
              month_length.resample(time='QS').sum())

    if var == 'dis':
        qtot_y = qtot_y = qtot_y * 0.031556952  # # Converts the discharge  into km3/year
        qtot_y['dis'] = qtot_y.dis.chunk({'lat': 60, 'lon': 360, 'time': len(qtot_y.dis.time)})
        # slice is required to reduce the dataset.
        qtot_y = qtot_y.sel(time=slice('2010-01-01', '2050-12-31'))

        qtot_y.dis.attrs['unit'] = 'km3/year'
        # chunking reduces computational burden
        qtot_y['dis'] = qtot_y.dis.chunk({'lat': 60, 'lon': 360, 'time': len(qtot_y.dis.time)})
        # saving the file here and reloading reduces disk space
        qtot_y.to_netcdf('dis_3m_rcp60_2010_2050.nc4')
    elif var == 'qtot':
        qtot_y = qtot_y * area * 24 * 3600 * 365 * 1e-18  # Converts total runoff per gs into km3/year
        qtot_y['qtot'] = qtot_y.qtot.chunk({'lat': 60, 'lon': 360, 'time': len(qtot_y.qtot.time)})

        qtot_y.to_netcdf('dis_3monthly.nc4')

else:


    # Now resample to an average value for each 5-year block, and offset by 2 years so that the value is centred to start 2101
    # qtot_y = qtot_y.resample(time='20Y', loffset='2Y').quantile(0.1)
    # qtot_y = qtot_y.resample(time='20Y').quantile(0.1)
    # qtot_y = qtot_y.sel(time=slice('2010-01-01', '2020-12-31'))

    if var == 'dis':
        qtot_y['dis'] = qtot_y.dis.chunk({'lat': 60, 'lon': 360, 'time': len(qtot_y.dis.time)})
        # Resample daily data to monthly (by summing daily values)
        qtot_y = qtot_y.resample(time='Y').mean()
        qtot_y['dis'] = qtot_y.dis.chunk({'lat': 60, 'lon': 360, 'time': len(qtot_y.dis.time)})

        qtot_y = qtot_y * 0.031556952  # Converts the discharge  into km3/year
        qtot_y.dis.attrs['unit'] = 'km3/year'

        qtot_y.to_netcdf('dis_5yr_sp.nc4')

    elif var == 'qtot':
        qtot_y['qtot'] = qtot_y.qtot.chunk({'lat': 60, 'lon': 360, 'time': len(qtot_y.qtot.time)})
        # Resample daily data to monthly (by summing daily values)
        qtot_y = qtot_y.resample(time='Y').mean()
        qtot_y['qtot'] = qtot_y.qtot.chunk({'lat': 60, 'lon': 360, 'time': len(qtot_y.qtot.time)})
        qtot_y = qtot_y * area * 24 * 3600 * 365 * 1e-18  # Converts to total runoff per gs into km3/year
        qtot_y.qtot.attrs['unit'] = 'km3/year'
        qtot_y['qtot'] = qtot_y.qtot.chunk({'lat': 60, 'lon': 360, 'time': len(qtot_y.qtot.time)})
        # qtot_y = qtot_y.groupby('time.year')
        year_sel = np.arange(2010, 2105, 5)

        qtot_y_t = qtot_y.sel(time=qtot_y.time.dt.year.isin(year_sel))

        qtot_y_t.to_netcdf('qtot_5yr_rcp26_sp.nc4')

# Note this will force compute of the data, if this explodes your memory you may need to use chunksize.
# read in saved data again to further process
ddstr = f'C:\\Users\\awais\\Documents\\GitHub\\awais\\basin_process\\{var}*5yr*{scen}*.nc4'

files = glob.glob(ddstr)

qtot_y = xr.open_mfdataset(files)

# open raster fle in wdhsapes directory, if not there copy from p drive
basinraster = xr.open_dataarray(wdshapes + 'basins_by_region_simpl_R11.nc')
# helps to load the dataset into memory
with ProgressBar():
    qtot_y.compute()
# %%
year_sel = np.arange(2010, 2105, 5)
qtot_y_t = qtot_y.sel(time=qtot_y.time.dt.year.isin(year_sel))

# read shapefile of basins
shp_fn = wdshapes + 'basins_by_region_simpl_R11.shp'
shapes = salem.read_shapefile(shp_fn, cached=True)

writer = pd.ExcelWriter(wd + var + scen + '.xlsx')

# define a spatial method to aggregate
spatialmethod = 'meansd'

df = shapes[['BASIN_ID', 'BCU_name']]
df['BASIN_ID'] = df['BASIN_ID'].astype(int)

if monthlyscale:
    # qtot_y = qtot_y.sel(time=slice('2010-01-01', '2025-12-31'))
    years = pd.DatetimeIndex(qtot_y_t['time'].values)
    # years = np.arange(2010,2105,5)
else:
    years = np.arange(2010, 2105, 5)
# years = pd.date_range(*(pd.to_datetime(['2009-12-31', '2099-10-01']) - MonthBegin(n=1)), freq='3M')


for year in years:

    for row in df.itertuples():
        basinid = row.BASIN_ID
        print(row.Index)

        yr = f'{year}'
        # if monthlyscale:
        #    qtot_y_t = qtot_y.sel(time=qtot_y.time.isin(year))
        # qtot_y_t = qtot_y.sel(time=year)
        # else:
        #    qtot_y_t = qtot_y.sel(time=qtot_y.time.dt.year.isin(year))
        if var == 'dis':
            val = qtot_y_t.dis.where(basinraster == basinid)
        elif var == 'qtot':
            val = qtot_y_t.qtot.where(basinraster == basinid)

        std = val.squeeze().std().values

        # Try dim dimension and provide exact dimension
        # Handle non-nans

        if spatialmethod == 'sum':
            val = val.sum()
        elif spatialmethod == 'max':
            val = val.max()
        elif spatialmethod == 'mean':
            val = val.mean()
        elif spatialmethod == 'quantile':
            val = val.quantile(0.75)
        elif spatialmethod == 'meansd':
            val = (std + val.mean())

        df.loc[row.Index, year] = val.values
        print(dt.now() - start)

df['unit'] = 'km3/year'

# %%
df.to_csv('{}_{}_3m_{}.csv'.format(var, scen, spatialmethod))
