require(reshape)
require(rgdal)
require(raster)
require(rgeos)
require(tidyr)
require(dplyr)
require(maptools)
require(ncdf4)
library(sp)
library(countrycode)
library(ggmap)
library(sf)
library(exactextractr)

nc = nc_open(
    paste0(
        "C:/Users/awais/Documents/GitHub/agg_data/output/dis_5y__hadgem2-es_rcp60_temp_agg.nc"
    ),
    verbose=FALSE,
)
nc.brick = brick(
    "C:/Users/awais/Documents/GitHub/agg_data/output/qtot_5y__gfdl-esm2m_rcp60_temp_agg.nc"
)


basin < -read.csv(
    "C:/Users/awais/Documents/GitHub/agg_data/output/basins_by_region_simpl_R11.csv"
)

# load shapefile
basin_by_region.spdf = read_sf(
    "P:/ene.model/NEST/delineation/data/delineated_basins_new",
    paste0("basins_by_region_simpl_R11"),
)


agg < -exact_extract(nc.brick, basin_by_region.spdf, "sum")


output < -cbind(basin, agg)

write.csv(
    output,
    "C:/Users/awais/Documents/GitHub/agg_data/output/qtot_5y__gfdl-esm2m_rcp60_temp_agg.nc",
)

basin < -read.csv(
    "C:/Users/awais/Documents/GitHub/agg_data/output/basins_by_region_simpl_R11.csv"
)
