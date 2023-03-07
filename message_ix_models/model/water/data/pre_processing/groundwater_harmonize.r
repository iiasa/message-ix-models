
#source('P:/ene.model/NEST/groundwater/groundwater_harmonize.r')

# Clear memory 
rm(list = ls()) 

# close all plot windows
graphics.off() 

# libraries
library(raster)
library(rgdal)
require(rgeos)
library(ncdf4)
library(tidyverse)

#### REGION ####
# define what region you work on
# R11, R12 or a country
reg = 'ZMB'

# Define global raster at 1/8th degree
base_raster = raster()
res(base_raster) = 1/8
base_raster = crop(base_raster,extent(-180,180,-60,85))
base_raster[] = 0

# Continental Tiles
tile_names = c('Africa','Australia','Eurasia','N_America','S_America')
for (i in 1:length(tile_names))
	{
	
	# Import data from netcdf and convert to raster aligned with base raster
  # unit meters m
	nc_filename = paste(tile_names[i],'_model_wtd_v2.nc',sep='')
	folder = c('P:/ene.model/NEST/groundwater/table_depth/')
	raster_shape = raster(paste(folder,nc_filename,sep=''), varname='WTD') # doesn't seem to work for this type of ncdf file but provides shape
	nc = nc_open(paste(folder,nc_filename,sep=''))
	wtd = t(ncvar_get(nc, 'WTD')) # transpose and flip
	wtd = wtd[nrow(wtd):1,]
	wtd.raster = raster(wtd)
	extent(wtd.raster) = extent(raster_shape)
	proj4string(wtd.raster) = proj4string(raster_shape)
	wtd.raster = projectRaster( from = wtd.raster , to = base_raster , method = "bilinear" )
	
	# Add to global raster
	if(i == 1)
		{
		global_water_table_depth_0125 = wtd.raster
		}else
		{
		global_water_table_depth_0125 = do.call(merge, list(global_water_table_depth_0125, wtd.raster))
		}
	}

# Write to geotiff
temp = writeRaster(global_water_table_depth_0125, paste(folder,'global_water_table_depth_0125',sep=''), format = 'GTiff', overwrite = TRUE)

#### TAKE MESSAGE REGION-BASIN structure ####
basin_by_region.spdf = readOGR('P:/ene.model/NEST/delineation/data/delineated_basins_new',
                               paste0('basins_by_region_simpl_',reg), verbose=FALSE)
row.names(basin_by_region.spdf@data) = 1:length(basin_by_region.spdf$BASIN)
# basin_by_region.spdf2 = gBuffer(basin_by_region.spdf, byid=TRUE, width=-0.2)

proj4string(global_water_table_depth_0125) = basin_by_region.spdf@proj4string
identicalCRS(global_water_table_depth_0125, basin_by_region.spdf)

table_depth.df  = raster::extract(global_water_table_depth_0125, basin_by_region.spdf,fun=mean,df=TRUE,na.rm=TRUE)
names(table_depth.df) = c('id','table_depth_m')
table_depth.spdf = cbind(basin_by_region.spdf, table_depth.df)
library(broom)
table_depth.tidy <- broom::tidy(table_depth.spdf)
table_depth.tidy2 <- dplyr::left_join(table_depth.tidy %>% mutate(id = as.integer(id)),
                                table_depth.spdf@data, by='id')
# table_depth.tidy2 = table_depth.tidy2[table_depth.tidy2$BASIN_ID %in% c(64), ]
# plot mismatch regions...
ggplot() + 
  geom_polygon( data=table_depth.tidy2, aes(x=long, y=lat, group = group, fill = table_depth_m),
                color="black" )

#some regions are not plotted correctly but the data is there
table_depth.out = table_depth.spdf@data %>% 
  mutate(table_depth_m = if_else(is.na(table_depth_m), 
                                 mean(table_depth_m,na.rm = T) , table_depth_m),
         # Energy use in GW / MCM/day
         GW_per_MCM_per_day = round( 0.85 * 9.81 / 86400 * table_depth_m , digits = 5 ),
         # in GW/km3/year 
         GW_per_km3_per_year = GW_per_MCM_per_day *1000/365)
# to be changed to the unit we need

write.csv( table_depth.out, paste0('gw_energy_intensity_depth_',reg,'.csv'), row.names = FALSE)	


#### GW abstraction, historical capaicty ####
# 1- get historical and projections for abstraction (only use hist for now)
# 2- from Wada et al, also import different water demands
# 3- gw_fraction = tot_demand / gw abstraction
# 4- use gw_fraction on our actual demands in the model -> historical capacity
setwd('P:/ene.model/NEST/groundwater/')
# Groundwater abstraction fro Wada et al

nc = nc_open('Wada_groundwater_abstraction/waterdemand_30min_groundwaterabstraction_million_m3_month.nc', verbose=FALSE)
gwabstract.stack = stack( 'Wada_groundwater_abstraction/waterdemand_30min_groundwaterabstraction_million_m3_month.nc' )
extent(gwabstract.stack) = extent( min( ncvar_get(nc, "longitude") ), max( ncvar_get(nc, "longitude") ), min( ncvar_get(nc, "latitude") ), max( ncvar_get(nc, "latitude") ) )
proj4string( gwabstract.stack ) = proj4string( basin_by_region.spdf )
#gwabstract.stack = crop( gwabstract.stack, extent(basin_by_region.spdf) )
names(gwabstract.stack) = c( sapply( 1:(nlayers(gwabstract.stack)/12), function(yy){ return( paste( ( as.numeric( unlist( strsplit( as.character( as.Date("1901-01-01") + min( ncvar_get(nc, "time") ) ), '-' ) )[1] ) + yy - 1 ), seq(1,12,by=1), sep='.') ) } ) ) 
gwabstract.stack = gwabstract.stack[[ c( which(grepl( 'X2010',names(gwabstract.stack) )) ) ]] # keep 2010

# Irrigation
nc = nc_open('Wada_groundwater_abstraction/pcrglobwb_WFDEI_historical_PIrrWW_monthly_1960_2010.nc4', verbose=FALSE)
irrigation.stack = stack( 'Wada_groundwater_abstraction/pcrglobwb_WFDEI_historical_PIrrWW_monthly_1960_2010.nc4' )
extent(irrigation.stack) = extent( min( ncvar_get(nc, "longitude") ), max( ncvar_get(nc, "longitude") ), min( ncvar_get(nc, "latitude") ), max( ncvar_get(nc, "latitude") ) )
proj4string( irrigation.stack ) = proj4string( basin_by_region.spdf )
# irrigation.stack = crop( irrigation.stack, extent(buff.sp) )
names(irrigation.stack) = c( sapply( 1:(nlayers(irrigation.stack)/12), function(yy){ return( paste( ( as.numeric( unlist( strsplit( as.character( as.Date("1901-01-01") + min( ncvar_get(nc, "time") ) ), '-' ) )[1] ) + yy - 1 ), seq(1,12,by=1), sep='.') ) } ) ) 
irrigation.stack = irrigation.stack[[ c( which(grepl( 'X2010',names(irrigation.stack) )) ) ]] # keep 2010

year_irr = sum(irrigation.stack)
tot_km3 = sum(year_irr@data@values,na.rm = T)*0.001
# Industrial
nc = nc_open('Wada_groundwater_abstraction/pcrglobwb_WFDEI_historical_PIndWW_monthly_1960_2010.nc4', verbose=FALSE)
industrial.stack = stack( 'Wada_groundwater_abstraction/pcrglobwb_WFDEI_historical_PIndWW_monthly_1960_2010.nc4' )
extent(industrial.stack) = extent( min( ncvar_get(nc, "longitude") ), max( ncvar_get(nc, "longitude") ), min( ncvar_get(nc, "latitude") ), max( ncvar_get(nc, "latitude") ) )
proj4string( industrial.stack ) = proj4string( basin_by_region.spdf )
# industrial.stack = crop( industrial.stack, extent(buff.sp) )
names(industrial.stack) = c( sapply( 1:(nlayers(industrial.stack)/12), function(yy){ return( paste( ( as.numeric( unlist( strsplit( as.character( as.Date("1901-01-01") + min( ncvar_get(nc, "time") ) ), '-' ) )[1] ) + yy - 1 ), seq(1,12,by=1), sep='.') ) } ) ) 
industrial.stack = industrial.stack[[ c( which(grepl( 'X2010',names(industrial.stack) )) ) ]] # keep 2010

# Domestic
nc = nc_open('Wada_groundwater_abstraction/pcrglobwb_WFDEI_historical_PDomWW_monthly_1960_2010.nc4', verbose=FALSE)
domestic.stack = stack( 'Wada_groundwater_abstraction/pcrglobwb_WFDEI_historical_PDomWW_monthly_1960_2010.nc4' )
extent(domestic.stack) = extent( min( ncvar_get(nc, "longitude") ), max( ncvar_get(nc, "longitude") ), min( ncvar_get(nc, "latitude") ), max( ncvar_get(nc, "latitude") ) )
proj4string( domestic.stack ) = proj4string( basin_by_region.spdf )
# domestic.stack = crop( domestic.stack, extent(buff.sp) )
names(domestic.stack) = c( sapply( 1:(nlayers(domestic.stack)/12), function(yy){ return( paste( ( as.numeric( unlist( strsplit( as.character( as.Date("1901-01-01") + min( ncvar_get(nc, "time") ) ), '-' ) )[1] ) + yy - 1 ), seq(1,12,by=1), sep='.') ) } ) ) 
domestic.stack = domestic.stack[[ c( which(grepl( 'X2010',names(domestic.stack) )) ) ]] # keep 2010

# Existing groundwater capacity from initial extraction levels
total.stack = domestic.stack + industrial.stack + irrigation.stack
total.stack  =  sum(stack(total.stack))
total_gwabstract.stack = sum(stack(gwabstract.stack))
gwfraction.stack = ( total_gwabstract.stack / total.stack )
# areas with 0 demand nad maybe little vals of gw abstraction
# otherwise results in 100% where actually gw is minimal, no demand
gwfraction.stack[gwfraction.stack == Inf] = 0
gwfraction.stack[gwfraction.stack[]>1]=1
gwfraction.stack[is.na(gwfraction.stack)] = 0


frac = c( unlist( lapply( 1:length(basin_by_region.spdf), function(x){ 
  
  gw = sum( data.frame( raster::extract( total_gwabstract.stack, as(basin_by_region.spdf[x,],'SpatialPolygons'), na.rm=TRUE, cellnumbers = TRUE )[[1]])['value'] )
  
  ww = sum( data.frame( raster::extract( total.stack, as(basin_by_region.spdf[x,],'SpatialPolygons'), na.rm=TRUE, cellnumbers = TRUE )[[1]])['value'] )
  
  return( round( min( 1, max( 0, gw/ww, na.rm=TRUE ) ), digits = 3 ) ) 
  
} ) ) )

# Data frame output by PID
groundwater_fraction.df = data.frame( gw_fraction = frac ) %>% 
  mutate(id = row_number(),
         # many values close to 0 are bo due to lack of data
         gw_fraction = if_else(gw_fraction <= 0.02,mean(gw_fraction),gw_fraction))
groundwater_fraction.spdf = cbind(basin_by_region.spdf,groundwater_fraction.df) 

writeOGR(
  groundwater_fraction.spdf, 
  'P:/ene.model/NEST/groundwater', 
  paste0('gw_fraction_',reg),
  driver="ESRI Shapefile", 
  overwrite_layer=TRUE
)

gw_fraction.tidy <- broom::tidy(groundwater_fraction.spdf)
gw_fraction.tidy2 <- dplyr::left_join(gw_fraction.tidy %>% mutate(id = as.integer(id)),
                                      groundwater_fraction.spdf@data, by='id')
gw_fraction.tidy2 = gw_fraction.tidy2[gw_fraction.tidy2$BASIN_ID %in% c(64), ]
# this plot is completely wrong!! use ArcGIS instead
ggplot() + 
  geom_polygon( data=gw_fraction.tidy2, aes(x=long, y=lat, group = group, fill = gw_fraction),
                color="black" )

# Now use the groundwater fraction to set the historical capacities based on the historical demand level
# load all demands
## CAREFUL! irr_dem needs to be manually defined
# for R11 it is from GLOBIOM, in MCM
irr_dem = read.csv(paste0('P:/ene.model/NEST/groundwater/hist_irrigation_withdrawals_',reg,'.csv'),
                   check.names = F) %>% 
  gather(key = 'year',value = 'irr_dem',-node) %>% rename(REGION = node) %>% 
  mutate(year = as.numeric(year))
# urban + manufacturing and rural demand depend on the region, in MCM!!
urban_man_dem = read.csv(paste0('P:/ene.model/NEST/water_demands/harmonized/',
                                reg,'/ssp2_regional_urban_withdrawal_baseline.csv'),
                         check.names = F) %>% rename(year = '') %>% 
  gather(key = 'BCU_name',value = 'urb_dem',-year) %>% 
  mutate(urb_dem = urb_dem/1000) # km3

rural_dem = read.csv(paste0('P:/ene.model/NEST/water_demands/harmonized/',
                            reg,'/ssp2_regional_rural_withdrawal_baseline.csv'),
                     check.names = F) %>% rename(year = '') %>% 
  gather(key = 'BCU_name',value = 'rur_dem',-year) %>% 
  mutate(rur_dem = rur_dem/1000) # km3

#specific region name for R11 or R12
if (reg %in% c(" R11"," R12")){
  reg_add = paste0(reg,"_")
} else{
  reg_add = ""
}
tot_demand = urban_man_dem %>% left_join(rural_dem) %>% 
  left_join(basin_by_region.spdf@data %>% select(REGION,BCU_name) %>% 
              mutate(REGION = paste0(reg_add,REGION))) %>% 
  filter(year == 2010) %>% 
  left_join(irr_dem) %>% 
  group_by(REGION) %>% 
  mutate(tot_urb_reg_wtr = sum(urb_dem),
         region_water_share = urb_dem  / tot_urb_reg_wtr) %>% ungroup() %>% 
  #use this share to allocate irrigation withdrawals to basins
  mutate(irr_dem_bas = irr_dem * region_water_share,
         tot_dem = irr_dem_bas + urb_dem + rur_dem)

hist_cap_gw_sw = tot_demand %>% select(BCU_name,tot_dem) %>% 
  left_join(groundwater_fraction.spdf@data %>% select(BCU_name,gw_fraction)) %>% 
  mutate(hist_cap_gw_km3_year = round( tot_dem * gw_fraction,3 ),
         hist_cap_sw_km3_year = round( tot_dem * (1- gw_fraction) ,3) ) %>% 
  select(-tot_dem,-gw_fraction)

### Append historical capacity csv to include freshwater extration capacities

# historical capacity csv
write.csv( 	hist_cap_gw_sw, 
            paste0("historical_new_cap_gw_sw_km3_year_",reg,".csv"), 
            row.names = FALSE )

