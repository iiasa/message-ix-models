
"""
This script aggregates the global gridded data to any scale. The following
script specifically aggregates global gridded hydrological data onto the basin
 mapping used in the nexus module.
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


# climate forcing 
scenarios = ['rcp26',
             'rcp60']

scen = 'rcp26'
# variable, for detailed symbols, refer to ISIMIP2b documentation
variables = ['qtot', #total runoff 
             'dis', # discharge
             'qg', #groundwater runoff
             'qr'] # groudnwater recharge
var = 'dis'
# climate model
climmodels = ['gfdl-esm2m',
              'hadgem2-es',
              'ipsl-cm5a-lr',
              'microc5']
climmodel = 'gfdl-esm2m'
# Define if monthly aggregation is required
monthlyscale = False
# define qauantile for statistical aggregation
quant= 0.75
# define if multi climate models mean 
multimodelensemble = False
# deinfe lat and long chunk for reducing computational load
latchunk = 60
lonchunk = 360 
# define a spatial method to aggregate
spatialmethod = 'meansd'

"""
The hydrological data can be accessed in watxene p drive. For accessing
particular drive, seek permission from Edward Byers (byers@iiasa.ac.at)
The files should be copied on to local drive 
"""

wd = r"C:\Users\awais\Documents\GitHub\agg_data"

# Open raster area file
# The file landareamaskmap0.nc can be found under
# P:\ene.model\NEST\delineation\data\delineated_basins_new

area = xr.open_dataarray(wd + '\landareamaskmap0.nc')
#%%
if multimodelensemble: 
    # iterate through scenarios (3)
    for model in climmodels:
        hydro_data = wd + f'\hydrology\{model}*{scen}*{var}*.nc4'
        files = glob.glob(hydro_data)
        ds = xr.Dataset()
        # Iterate through 5 GCMs for each scenario
        for file in files:
            model = file[57: -57]
            da = xr.open_dataarray(file,decode_times=False) #open
            #da['time'] = np.arange(2005,2100, dtype=int) #fix time
            ds[model] = da # add to main dataset
        #collapse the 4 GCMs to one dataarray and take mean
        ds['multimodelmean']= ds.to_array(dim='memean').mean(axis=0)
        # Assign attributes
        ds.attrs['scenario'] = scen
        ds.to_netcdf(wd+f'{var}_{scen}_non_agg_combined.nc')

else:

    hydro_data = wd + f'\hydrology\*{climmodel}*{scen}*{var}*.nc4'
    files = glob.glob(hydro_data)
    # Open hydrological data as a combined dataset
    da = xr.open_mfdataset(files)
    
    # da.to_netcdf(wd+f'\{var}_memean_{scen}.nc')  
    

if monthlyscale:
    # qtot_y = qtot_y.sel(time=slice('2010-01-01', '2025-12-31'))
    years = pd.DatetimeIndex(ds['time'].values)
    # years = np.arange(2010,2105,5)
else:
    years = np.arange(2010, 2105, 5)

ds = xr.open_dataset(wd+f'{var}_memean_{scen}.nc')
# Resample daily data to monthly (by summing daily values)
qtot_y = da
qtot_y = qtot_y.resample(time='M').sum()

#%%
if monthlyscale:
    dpm = {'noleap': [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
           '365_day': [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
           'standard': [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
           'gregorian': [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
           'proleptic_gregorian': [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31,\
                                   30, 31],
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
        return a array of days per month corresponding to the months provided 
        in `months`
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
        # # Converts the discharge  into km3/year
        qtot_y = qtot_y = qtot_y * 0.031556952  
        qtot_y['dis'] = qtot_y.dis.chunk({'lat': latchunk, 'lon': lonchunk,\
                                          'time': len(qtot_y.dis.time)})
        # slice is required to reduce the dataset.
        qtot_y = qtot_y.sel(time=slice('2010-01-01', '2050-12-31'))

        qtot_y.dis.attrs['unit'] = 'km3/year'
        # chunking reduces computational burden
        qtot_y['dis'] = qtot_y.dis.chunk({'lat': latchunk, 'lon': lonchunk,\
                                          'time': len(qtot_y.dis.time)})
        # saving the file here and reloading reduces disk space
        qtot_y.to_netcdf(wd+ f'{var}_m3monthly__{climmodel}_{scen}.nc')
    elif var == 'qtot':
         # Converts total runoff per gs into km3/year
        qtot_y = qtot_y * area * 24 * 3600 * 365 * 1e-9 
        qtot_y['qtot'] = qtot_y.qtot.chunk({'lat': latchunk, 'lon': lonchunk,\
                                            'time': len(qtot_y.qtot.time)})

        qtot_y.to_netcdf(wd+f'{var}_3monthly_{climmodel}_{scen}.nc')

else:
    if var == 'dis':
        qtot_y['dis'] = qtot_y.dis.chunk({'lat': latchunk, 'lon': lonchunk, \
                                          'time': len(qtot_y.dis.time)})
        # Resample daily data to monthly (by summing daily values)
        qtot_y = qtot_y.resample(time='Y').mean()
        qtot_y['dis'] = qtot_y.dis.chunk({'lat': latchunk, 'lon': lonchunk,\
                                          'time': len(qtot_y.dis.time)})
        # Converts the discharge  into km3/year
        qtot_y = qtot_y * 0.031556952  
        qtot_y.dis.attrs['unit'] = 'km3/year'
        
        # Now resample to an average value for each 5-year block, and
        # offset by 2 years so that the value is centred to start 2101
        qtot_y = qtot_y.resample(time='5Y', loffset='4Y').mean()
        
        qtot_y.to_netcdf(wd+ f'\output\{var}_5y__{climmodel}_{scen}.nc')
        # read in saved data again to further process
        qtot_y = xr.\
            open_dataset( wd+f'\output\{var}_5y__{climmodel}_{scen}_temp_agg.nc')
        

    elif var == 'qtot':
        qtot_y['qtot'] = qtot_y.qtot.chunk({'lat':  latchunk, 'lon': lonchunk,\
                                            'time': len(qtot_y.qtot.time)})
        # Resample daily data to monthly (by summing daily values)
        qtot_y = qtot_y.resample(time='Y').mean()
        qtot_y['qtot'] = qtot_y.qtot.chunk({'lat': latchunk, 'lon': lonchunk,\
                                            'time': len(qtot_y.qtot.time)})
        # Converts to total runoff per gs into km3/year
        qtot_y = qtot_y * area *  0.03 #* 1e-9 
        qtot_y.qtot.attrs['unit'] = 'km3/year'
        qtot_y['qtot'] = qtot_y.qtot.chunk({'lat': latchunk, 'lon': lonchunk,\
                                            'time': len(qtot_y.qtot.time)})
        # qtot_y = qtot_y.groupby('time.year')
        #year_sel = np.arange(2010, 2105, 5)
        

        #qtot_y_t = qtot_y.sel(time=qtot_y.time.dt.year.isin(year_sel))

        qtot_y.to_netcdf(wd+ f'\output\{var}_5y__{climmodel}_{scen}_temp_agg.nc')
        # read in saved data again to further process
        qtot_y = xr.\
            open_dataset( wd+f'\output\{var}_5y__{climmodel}_{scen}.nc')
        qtot_y['qtot'] = qtot_y.qtot.chunk({'lat': latchunk, 'lon': lonchunk,\
                                            'time': len(qtot_y.qtot.time)})
     
    elif var == 'qg':
        qtot_y['qg'] = qtot_y.qtot.chunk({'lat':  latchunk, 'lon': lonchunk, \
                                          'time': len(qtot_y.qtot.time)})
        # Resample daily data to monthly (by summing daily values)
        qtot_y = qtot_y.resample(time='Y').mean()
        qtot_y['qg'] = qtot_y.qtot.chunk({'lat': latchunk, 'lon': lonchunk,\
                                         'time': len(qtot_y.qtot.time)})
        #TODO check unit
        qtot_y = qtot_y * area * 24 * 3600 * 365 * 1e-18  
        qtot_y.qtot.attrs['unit'] = 'km3/year'
        qtot_y['qg'] = qtot_y.qtot.chunk({'lat': latchunk, 'lon': lonchunk,\
                                          'time': len(qtot_y.qtot.time)})
        # Now resample to an average value for each 5-year block
        qtot_y = qtot_y.resample(time='5Y', loffset='4Y').mean()
        qtot_y.to_netcdf(wd+ f'\output\{var}_5y__{climmodel}_{scen}_temp_agg.nc')
#%%

# read in saved data again to further process
qtot_y = xr.open_dataset(wd+ f'\output\{var}_5y__{climmodel}_{scen}.nc')
qtot_y[f'{var}'] = qtot_y.dis.chunk({'lat': latchunk, 'lon': lonchunk,\
                                      'time': len(qtot_y.dis.time)})
# open raster fle in wdhsapes directory, if not there copy from p drive
basinraster = xr.open_dataarray(wd+ \
                                '\delineation\\basins_by_region_simpl_R11.nc')
# helps to load the dataset into memory
with ProgressBar():
    qtot_y.compute()
#%%
# read shapefile of basins
shp_fn = wd+ '\delineation\\basins_by_region_simpl_R11.shp'
shapes = salem.read_shapefile(shp_fn, cached=True)

df = shapes[['BASIN_ID', 'BCU_name']]
df['Model'] = f'LPJML_{var}_{climmodel}_{scen}'

df['Region'] = df['BASIN_ID'].astype(int)

if var!= 'qg':
    df['unit'] = 'km3/year'
else:
    df['unit'] = 'km3/year'



for row in df.itertuples():
    for year in years:
        basinid = row.BASIN_ID
        print(row.Index)
        #for year in years:
            
        if monthlyscale:
            qtot_y_t = qtot_y.sel(time=qtot_y.time.isin(year))
        # qtot_y_t = qtot_y.sel(time=year)
        else:
            qtot_y_t = qtot_y.sel(time=qtot_y.time.dt.year.isin(year))
        #for year in years:
        yr = f'{year}'
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
            val = val.quantile(quant)
        elif spatialmethod == 'meansd':
            val = (std + val.mean())
        
            df.loc[row.Index, year] = val.values
            print(dt.now() - start)

#%%
#val.to_netcdf(wd+ f'\output\{var}_5y__{climmodel}_{scen}_aggregated.nc')

df.to_csv(wd + '\output' + f'\LPJML_{var}_{climmodel}_{scen}_5y.csv')
