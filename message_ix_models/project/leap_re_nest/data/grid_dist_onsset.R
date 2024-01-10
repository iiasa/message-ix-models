# dsitance to grid and cost based on Onset data
rm(list = ls())
require(rgeos)
require(overpass)
require(rgdal)
require(raster)
library(tidyverse)
library(sf)
library(geosphere)
library(tictoc)
library("xlsx")

country = "Zambia"
iso3 = "ZMB"
country_lc = tolower(country)

basin.spdf = readOGR( paste0( getwd(),'/',country, '_NEST_delineation'), 
                      paste0("basins_by_region_simpl_",iso3), verbose = FALSE )
basin.spdf$BCU = as.numeric(gsub("\\|.*","",basin.spdf$BCU_name))
basin.sf = st_read( paste0(country, '_NEST_delineation/',"basins_by_region_simpl_",iso3,".csv"))

# scen_name = "baseline"
scenarios = c("baseline",
              "improved_access","ambitious_development")
# there are no differences among scenarios

dem_all_scen = data.frame()
for (scen_name in scenarios){
  print(scen_name)

  # load onsset file
  ons_data = read_csv(paste0( getwd(),'/OnSSET/onsset_mled_scenario_results_files_full/',
                              scen_name,'_zm-2-0_0_0_0_0_0.csv'))
  
  #MLED data to get urban/rural definition
  data = st_read(paste0('M-LED/',country_lc,"_onsset_clusters_with_mled_loads_",
                          scen_name,"_tot_lat_d.gpkg"))
  
  urb_rur = data %>% select(id,isurban, BCU, geom)
  urb_rur.sf = st_cast(urb_rur, "POLYGON", do_split = FALSE)
  urb_rur.sp = as(urb_rur.sf, 'Spatial')
  
  # grid data
  ons_grid = ons_data %>% select(Pop,CurrentMVLineDist,X_deg,Y_deg) %>% 
    rename(lon = X_deg, lat = Y_deg,
           dist = CurrentMVLineDist,
           pop = Pop)
  
  # ons_grid.sf <- st_as_sf(x = ons_grid,                         
  #                coords = c("lon", "lat"),
  #                crs = proj4string(basin.spdf))
  # sf_use_s2(FALSE)
  # b = st_intersection(ons_grid.sf, basin.sf)
  
  #use spDF
  coords.df = ons_grid %>% select(lon,lat)
  grid.spdf <- SpatialPointsDataFrame(coords = coords.df, data = ons_grid,
                                 proj4string = CRS("+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0"))
  
  proj4string(basin.spdf) <- CRS("+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0")
  
  # grid over polygon with urban,rural
  sp_ov_urb = over(grid.spdf,urb_rur.sp)
  
  # grid over basin, maybe not needed
  sp_ov = over(grid.spdf,basin.spdf) %>% select(BCU)
  
  # cost values come from the file OnSSET/grid_cost_calculation.xlsx
  cost_grid.s = ons_grid %>% bind_cols(sp_ov,
                                       sp_ov_urb %>% select(isurban)) %>% 
    mutate(urb_rur = if_else((isurban == 0 | is.na(isurban) ), "_rur", "_urb") ) %>% 
    group_by(BCU,urb_rur) %>% 
    summarise(mean_dist = mean(dist),
              w.mean_dist = weighted.mean(dist,pop)) %>% 
    ungroup() %>% 
    # add empty categories for future years
    complete(BCU, urb_rur, fill = list(urb_rur = "_urb")) %>% 
    complete(BCU, urb_rur, fill = list(urb_rur = "_rur"))
  # convert na into 0
  cost_grid.s[is.na(cost_grid.s)] = 0
  # calculate costs
  cost_grid.s = cost_grid.s %>% 
    group_by(BCU) %>% 
    mutate(mean_dist = if_else(mean_dist == 0, max(mean_dist),mean_dist),
           w.mean_dist = if_else(w.mean_dist == 0, max(w.mean_dist),w.mean_dist)) %>% 
    ungroup() %>% 
    # HV lines cost less per km and kW
    mutate(cost_usd_kW_km = if_else(w.mean_dist < 35, 212, 25),
           # assume also 5km mv line in long distances
           fix_cost_usd_kW = if_else(w.mean_dist < 35, 1770, 2831)) %>% 
    mutate(tot_cost_usd2010_kW = (cost_usd_kW_km * w.mean_dist + fix_cost_usd_kW) / 1.2 ) %>% 
    na.omit()
  # 1.2 is the value conversion from 2020 to 2010 dollars used in the model
  
  cost_flat = cost_grid.s %>% select(BCU,urb_rur, tot_cost_usd2010_kW) %>% 
    spread(urb_rur, tot_cost_usd2010_kW)
  
  write_csv(cost_grid.s,paste0( getwd(),paste0("/OnSSET/grid_cost_bcu_",scen_name,".csv")) )
} 
