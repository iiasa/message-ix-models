"""Converts a shapefile to a raster"""


# Import packages
from datetime import datetime as dt

import numpy as np
import salem
import xarray as xr

from message_ix_models.util import private_data_path

# NB The code runs only when the data is in local folder because of salem package dependency
# wdshapes =
wdshapes = r"C:\Users\awais\Documents\GitHub\agg_data\delineation"


manual_edit = False
rasterize = True
all_touched = True
nx = 720
ny = 360
# Define grid (must match template below)
grid = salem.Grid(proj=salem.wgs84, x0y0=(-180, -90), nxny=(nx, ny), dxdy=(0.5, 0.5))
# temporary netcdf that gives the dimensions of output file,should match the salem.Grid defined below
# should be copied from P:\ene.model\NEST\delineation\data\delineated_basins
tempgrid = wdshapes + "\\template360x720_05.nc"


# Define custom fucntions
def create_raster(
    shp_fn, namestr, grid, tempgrid, melt=False, all_touched=False, epsg=None
):
    # Read in shapes, then look into table to see the columns names, e.g. shapes.columns
    shapes = salem.read_shapefile(shp_fn, cached=True)
    if epsg:
        shapes = shapes.to_crs(epsg=epsg)
    print(shapes.columns)
    print("Start " + shp_fn)
    print(dt.now())
    # e.g. states, countries, basins
    import os

    tempshape = (
        wdshapes + "\\temp.shp"
    )  # this is the filename of a temporary shapefile written in and out. No need to change

    out_fn = shp_fn[:-3]  # output filename

    # Open grid that is same size, e.g. from template (or create empty xarray with desired dimensions)
    template = xr.open_dataarray(tempgrid)
    nd = xr.full_like(template, np.nan, dtype=float)
    nd.attrs = {}
    td = nd.copy(deep=True)

    idxs = {}

    if melt:
        mask_default = grid.region_of_interest(shape=tempshape, all_touched=all_touched)
        mask_default = mask_default.astype(float)
        # mask_default[mask_default==1] = g
        mask_default[mask_default == 0] = np.nan
        td.values = np.flipud(mask_default)
        nd = nd.combine_first(td)
        # idxs[str(g)] = str(shapes[namestr].iloc[0])
    else:
        # Run loop through geometries
        for g, geo in enumerate(shapes.geometry):
            print(g)
            region = shapes.iloc[[g]]
            region.set_crs(epsg=4326)
            region.to_file(tempshape)
            mask_default = grid.region_of_interest(
                shape=tempshape, all_touched=all_touched
            )
            mask_default = mask_default.astype(float)
            mask_default[mask_default == 1] = g + 1
            mask_default[mask_default == 0] = np.nan
            td.values = np.flipud(mask_default)
            nd = nd.combine_first(td)
            idxs[str(g + 1)] = str(region[namestr].iloc[0])
    nd.attrs = idxs
    # nd.attrs['index'] = idxs
    # nd.attrs['meta'] = {'file': shp_fn,
    #                     'date': str(dt.now()),
    #                     }
    # File saves out using same name as the input shapefile.
    nd.name = namestr
    fname = shp_fn[:-4]
    if all_touched:
        fname = fname
    nd.to_netcdf(fname + "0.05" ".nc")

    # Downsized
    xmin = shapes.min_x[0]
    xmax = shapes.max_x[0]
    ymin = shapes.min_y[0]
    ymax = shapes.max_y[0]

    das = nd.sel(lat=slice(ymax, ymin), lon=slice(xmin, xmax))
    das.to_netcdf(fname + "_crop.nc")
    das.close()
    # plt.figure()
    # plt.savefig(fname+'.png', dpi=400, bbox_inches='tight')

    os.remove(tempshape)
    return nd


# NB The shapefiles are located in P:\ene.model\NEST\delineation\data\delineated_basins_new and should be copied in
# wdhsapes folder. Below is an example for R11
shp_fn = wdshapes + "\\basins_by_region_simpl_R11.shp"

create_raster(
    shp_fn,
    namestr="BCU_name",
    grid=grid,
    tempgrid=tempgrid,
    melt=False,
    all_touched=all_touched,
)
