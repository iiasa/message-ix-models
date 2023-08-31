# population,  GDP and water access policies

require(rgeos)
require(overpass)
library(ncdf4) # package for netcdf manipulation
require(rgdal)
require(raster)
library(tidyverse)
library(sf)
library(geosphere)
library(tictoc)

country = "Zambia"
country_lc = tolower(country)

basin.spdf = readOGR( paste0( getwd(),'/',country, '_NEST_delineation'), 
                      paste0(country,'_NEST_delineation'), verbose = FALSE )
basin.sf = st_read( paste0(country, '_NEST_delineation/',country,'_NEST_delineation.shp'))
proj4string(basin.spdf)

pop.br = brick(paste0('SSP/population_ssp2soc_0p5deg_annual_2006-2100.nc'))
gdp.br = brick(paste0('SSP/gdp_ssp2soc_10km_2010-2100.nc'))
# nc_data <- nc_open(paste0('SSP/population_ssp2soc_0p5deg_annual_2006-2100.nc'))
# print(nc_data)
# 
# pop = ncvar_get(nc_data,'population')
# dim(pop)
# 
# lat <- ncvar_get(nc_data, "lat", verbose = F)
# lon <- ncvar_get(nc_data, "lon", verbose = F)
# 
# t <- ncvar_get(nc_data, "time")
# fillvalue <- ncatt_get(nc_data, "population", "_FillValue")
# pop[pop == fillvalue$value] <- NA
# 
# nc_close(nc_data) 
# 
# pop_brick <- brick(pop, xmn=min(lat), xmx=max(lat), 
#                  ymn=min(lon), ymx=max(lon), 
#                  crs=CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs+ towgs84=0,0,0"))
# pop_brick <- flip(t(pop_brick), direction='y') #wrong direction
# plot(pop_brick)

# need to extract/over 
pop.crs <- CRS(projection(pop.br))
basin.spdfr <- spTransform(basin.spdf, pop.crs)

# extract raster with sf or spdf
a = as.data.frame(raster::extract(pop.br , basin.spdfr, sum) )
names(a) = seq(2006, 2100)
pop.df = as.data.frame(basin.spdfr) %>% 
  bind_cols(a) %>% select(BCU, `2020`,`2030`,`2040`,`2050`,`2060`) %>% 
  gather(key = year, value = pop,-BCU) %>% # unit: people
  mutate(pop = pop /1000,
         unit = "thousand people")

sum((pop.df %>% filter(year == 2020))$pop)

# same for gdp
gdp.crs <- CRS(projection(gdp.br))
basin.spdfg <- spTransform(basin.spdf, gdp.crs)

# extract raster with sf or spdf
a = as.data.frame(raster::extract(gdp.br , basin.spdfg, sum) )
names(a) = seq(2010, 2100, 10)
gdp.df = as.data.frame(basin.spdfg) %>% 
  bind_cols(a) %>% select(BCU, `2020`,`2030`,`2040`,`2050`,`2060`) %>% 
  gather(key = year, value = gdp,-BCU) %>% # unit: people
  mutate(gdp = gdp /1000,
         unit_gdp = "thousand USD") %>% left_join(
           pop.df
         ) %>% 
  mutate(gdp_pc = gdp/pop)

sum((gdp.df %>% filter(year == 2020))$gdp)


# policy
rur_urb = c("rural","urban")
access_type = c("connection","treatment")
path = "P:/ene.model/NEST/water_demands/harmonized/ZMB"
for (ru in rur_urb){
  for (ac in access_type){
    
    temp = read_csv(paste0(path,"/ssp2_regional_",ru,"_",ac,"_rate_baseline.csv")) %>% 
      gather(key = BCU, value = baseline,2:25)
    
    names(temp) = c("year",'BCU',"baseline")
    temp2 = temp %>% 
      mutate(gap99 = 0.99-baseline,
             gap99 = if_else(gap99 < 0.01,0,gap99)) %>% 
      # improved scenario: 50% gap in 2030,close from 2040 on
      mutate(improved = if_else((year == 2030 & baseline != 0), baseline + 0.5 * gap99,
                                if_else((year > 2030 & baseline != 0), 0.99, 
                                        baseline))) %>% 
      # ambitious access + sustainable development: close gap in 2030
      mutate(ambitious = if_else((year >= 2030 & baseline != 0), 0.99, baseline))
    
    summary_temp = temp2 %>% gather(key = scenario, value = value, baseline, improved, ambitious) %>% 
      filter(value !=0 ) %>% 
      group_by(year,scenario) %>% 
      summarise(value = mean(value)) %>% ungroup()
    
    
    pl = ggplot(summary_temp %>% filter(year <= 2060))+
      geom_line(aes(year,value,color = scenario),
                size = 1)+
      ggtitle(paste0(country," ", ru," ", ac," rate for different scenarios"))+
      ylab("share")+
      theme_bw()
    
    png(paste0("out_figures/",country,"_", ru,"_", ac,"_rate.png"),
        height = 10,width = 14,units = "cm",res = 300)
    # pdf(file = paste0('Plots/pl_sdg',n,".pdf"), useDingbats=FALSE,
    #     height = 4,width = 5.5)
    print(pl)
    dev.off()
    
    #make output csv
    mod_out = temp2 %>% dplyr::select(year,BCU,improved) %>% 
      spread(BCU,improved)
    
    write.csv(mod_out,paste0(path,"/ssp2_regional_",ru,"_",ac,"_rate_improved.csv"),
              row.names = FALSE)
    imp_out = temp2 %>% dplyr::select(year,BCU,ambitious) %>% 
      spread(BCU,ambitious)
    
    write.csv(imp_out,paste0(path,"/ssp2_regional_",ru,"_",ac,"_rate_ambitious.csv"),
              row.names = FALSE)
  }
}

# recycling rate = 0.8 treatment
for (sc in c("baseline","improved","ambitious")){
  treat_rate = read_csv(paste0(path,"/ssp2_regional_urban_treatment_rate_",sc,".csv"))
  cols = names(treat_rate[-1])
  recycle_rate = treat_rate %>% mutate(across(-1, ~ . * 0.8)) %>% 
    dplyr::select(-all_of(cols))
  names(recycle_rate) = names(treat_rate)
  
  write.csv(recycle_rate,paste0(path,"/ssp2_regional_urban_recycling_rate_",sc,".csv"),
            row.names = FALSE)
  
}


#what about the region with 0 access for after 2030? maybe no population?
