# load water crop input and shape them for NEST use for Zambia

require(rgeos)
require(overpass)
library(ncdf4) # package for netcdf manipulation
require(rgdal)
require(raster)
library(tidyverse)
library(sf)
library(geosphere)
library(tictoc)
library(exactextractr)

country = "Zambia"
country_lc = tolower(country)

basin.spdf = readOGR( paste0( getwd(),'/',country, '_NEST_delineation'), 
                      paste0(country,'_NEST_delineation'), verbose = FALSE )
basin.sf = st_read( paste0(country, '_NEST_delineation/',country,'_NEST_delineation.shp'))
proj4string(basin.spdf)

# crop file
crops = c(# "Barley",
          "Cassava",
          "Cocoa",
          "Cotton",
          "Groundnut",
          "Maize",
          "Millet_pearl",
          "Millet_small",
          # "Oil Palm",
          "Potatoes",
          # "Rapeseed",
          "Rice",
          "Sorghum",
          "Soybean",
          # "Sugarbeet",
          "Sugarcane",
          "Sunflower",
          "Wheat"
          # "Yams"
)
years = c(2020,2030,2040,2050)
months = c("January", "February", "March", "April", "May", "June", "July", 
           "August", "September", "October", "November", "December")
map_time = data.frame(time = months,
                      timen = seq(1,12,1))
scens = c("baseline", "improved", "ambitious")
# mapping 3 scenarios with 3 names
scens_map = data.frame(scen_num = c("scen1", "scen2", "scen3"),
                       scen_nam = c("baseline", "improved", "ambitious"))
# cr = "Maize"
# yr = 2040
# sc = "baseline"
ww.df0 = data.frame()
tic()
for (sc in scens){
  print(sc)
  sc_num = scens_map[scens_map$scen_nam == sc,]$scen_num
  for (cr in crops){
    print(paste0("- ",cr))
    for (yr in years){
      print(paste0("-- ",yr))
      for (ms in months){
        print(paste0("--- ",ms))
        # WW are the baseline values, same for all scenarios
        # z dimension is the months of 2020
        rr = raster(paste0(getwd(),'/crops/Risultati_WaterCROP/',sc,'/',cr,'/',yr,"_370_GFDL",
                           '/waterwith_m3_',sc_num,'_',cr,"_",yr,"_370_GFDL_",ms,".tif") )
        # checking
        # extent(rr)
        # crs(rr)
        # proj4string(rr)
        # plot(rr)
        
        raster.crs <- CRS(projection(rr))
        basin.spdfr <- spTransform(basin.spdf, raster.crs)
        
        # extract raster with sf or spdf
        a = exact_extract(rr, basin.spdfr, function(values, coverage_fraction)
          sum(values * coverage_fraction, na.rm=TRUE))
        ww.temp = as.data.frame(basin.spdf@data) %>% dplyr::select(BCU) %>% 
          bind_cols(data.frame(value = a)) %>% 
          mutate(year = yr,
                 time = ms,
                 scenario = sc,
                 crop = cr)  
        ww.df0 = bind_rows(ww.df0,ww.temp)
      }
    }
  }
}
toc()
ww.df = ww.df0 %>% mutate(value = value *1e-9,
                         unit = "km3") %>% 
  merge(map_time) %>% select(-time) %>% rename(time = timen) %>% 
  mutate(crop = tolower(crop))
# save as csv
write.csv(ww.df,paste0(getwd(),"/crops/ww_km3_all_crops_NEST_BCU.csv"),row.names = FALSE)

#summary to check if numbers are realistic
ww_sum = ww.df %>% group_by(year,scenario) %>% 
  summarise(value = sum(value)) %>% ungroup()
# 1.7 km3/yr from https://www.iwmi.cgiar.org/Publications/Other/Reports/PDF/country_report_zambia.pdf
# https://awm-solutions.iwmi.org/wp-content/uploads/sites/12/documents/Country_Docs/Zambia/Situation%20Analysis%20Brief%20Zambia.pdf

ww_ctry_m = ww.df %>% group_by(year,scenario,time,crop) %>% 
  summarise(value = sum(value)) %>% ungroup()

ggplot(ww_ctry_m %>% filter(scenario == "baseline"))+
  geom_line(aes(x= time,
                 y = value, color = crop))+
  facet_wrap(~year)+theme_bw()
# gather some crops, leave the main 5-7 ones
ww_ctry = ww.df %>% group_by(year,scenario,crop) %>% 
  summarise(value = sum(value)) %>% ungroup()

ggplot(ww_ctry %>% filter(scenario == "baseline"))+
  geom_point(aes(x= crop,
                y = value, color = crop))+
  facet_wrap(~year)+theme_bw()

# keep wheat, sugarcane, cotton, rice, maize, others
main_crops = c("wheat", "cotton", "sugarcane", "rice", "maize")
ww_top6.df = ww.df %>% 
  mutate(crop = if_else(crop %in% main_crops, crop, "others")) %>% 
  group_by_all() %>% summarise(value = sum(value)) %>% ungroup()

write.csv(ww_top6.df,paste0(getwd(),"/crops/ww_km3_main_crops_NEST_BCU.csv"),row.names = FALSE)

# plot main crops
pl = ggplot(ww_top6.df %>% filter(scenario == "baseline",
                             year %in% c(2020,2050))%>% 
         group_by(year,scenario,time,crop) %>% 
         summarise(value = sum(value)) %>% ungroup())+
  geom_line(aes(x= time,
                 y = value*1000, color = crop),
            size = 1)+
  scale_color_brewer(palette = "Dark2", direction = -1)+
  ggtitle("Water withdrawals by main crop, Zambia, baseline")+
  ylab("MCM")+xlab("months")+
  scale_x_continuous(breaks = c(2,4,6,8,10,12))+
  facet_wrap(~year)+theme_classic()+
  theme()

png(paste0("out_figures/water_crop_crop_ww_baseline_",country,".png"),
    height = 9,width = 15,units = "cm",res = 300)
# pdf(file = paste0('Plots/pl_sdg',n,".pdf"), useDingbats=FALSE,
#     height = 4,width = 5.5)
print(pl)
dev.off()

# baseline has no water gaps, thefor the other scenarios wg is actually the 
# water increase needed to meet the expected diet, by increasing internal 
# production
scens_wg = c("improved","ambitious")
# water gap
wg.df = data.frame()
tic()
for (sc in scens_wg){
  print(sc)
  sc_num = scens_map[scens_map$scen_nam == sc,]$scen_num
  for (cr in crops){
    print(paste0("- ",cr))
    # there are no 2020 values
    for (yr in years[-1]){
      print(paste0("-- ",yr))
      for (ms in months){
        print(paste0("--- ",ms))
        # temp only for 2020
        # z dimension is the months of 2020
        rr = raster(paste0(getwd(),'/crops/Risultati_WaterCROP/',sc,'/',cr,'/',yr,"_370_GFDL",
                           '/watergap_m3_',sc_num,'_',cr,"_",yr,"_370_GFDL_",ms,".tif") )
        # checking
        # extent(rr)
        # crs(rr)
        # proj4string(rr)
        # plot(rr)
        
        raster.crs <- CRS(projection(rr))
        basin.spdfr <- spTransform(basin.spdf, raster.crs)
        
        # extract raster with sf or spdf
        a = exact_extract(rr, basin.spdfr, function(values, coverage_fraction)
          sum(values * coverage_fraction, na.rm=TRUE))
        wg.temp = as.data.frame(basin.spdf@data) %>% dplyr::select(BCU) %>% 
          bind_cols(data.frame(value = a)) %>% 
          mutate(year = yr,
                 time = ms,
                 scenario = sc,
                 crop = cr)  
        wg.df = bind_rows(wg.df,wg.temp)
      }
    }
  }
}
toc()
wg.df = wg.df %>% mutate(value = value *1e-9,
                         unit = "km3") %>% 
  merge(map_time) %>% select(-time) %>% rename(time = timen) %>% 
  mutate(crop = tolower(crop))

#summary to check if numbers are realistic
wg_sum = wg.df %>% group_by(year,scenario) %>% 
  summarise(value = sum(value)) %>% ungroup()

wg_top6.df = wg.df %>% 
  mutate(crop = if_else(crop %in% main_crops, crop, "others")) %>% 
  group_by_all() %>% summarise(value = sum(value)) %>% ungroup()

#summing ww and wg
ww_wg_top6 = ww_top6.df %>% bind_rows(wg_top6.df) %>% 
  group_by(BCU,year,scenario,crop,unit,time) %>% 
  summarise(value = sum(value)) %>% ungroup() %>% 
  select(names(wg_top6.df))

write.csv(wg_top6.df,paste0(getwd(),"/crops/wg_km3_main_crops_NEST_BCU.csv"),row.names = FALSE)
write.csv(ww_wg_top6,paste0(getwd(),"/crops/ww_scen_km3_main_crops_NEST_BCU.csv"),row.names = FALSE)

# plot main crops
pl = ggplot(ww_wg_top6 %>% filter(year %in% c(2020,2030,2050))%>% 
              group_by(year,scenario,time,crop) %>% 
              summarise(value = sum(value)) %>% ungroup())+
              geom_line(aes(x= time,
                            y = value*1000, color = crop),
                        size = 0.5)+
              scale_color_brewer(palette = "Dark2", direction = -1)+
              ggtitle("Water withdrawals by main crop, Zambia, baseline")+
              ylab("MCM")+xlab("months")+
              scale_x_continuous(breaks = c(2,4,6,8,10,12))+
              # order the scenarios in facet_wrap by main_scen
              facet_wrap(factor(scenario, levels =
                                  c("baseline", "improved", "ambitious"))~year, 
                         ncol = 3)+theme_classic()+
              # add main grid and horizontal lines
              theme(panel.grid.major.y = element_line(colour = scales::alpha("grey40", alpha = 0.3), size = 0.1),
                    panel.background = element_rect(color = "black"))

png(paste0("out_figures/water_crop_crop_ww_baseline_",country,".png"),
    height = 14,width = 16,units = "cm",res = 300)
# pdf(file = paste0('Plots/pl_sdg',n,".pdf"), useDingbats=FALSE,
#     height = 4,width = 5.5)
print(pl)
dev.off()

#### yield ####
yy.df = data.frame()
ym.df = data.frame()
tic()
for (sc in scens){
  print(sc)
  sc_num = scens_map[scens_map$scen_nam == sc,]$scen_num
  for (cr in crops){
    print(paste0("- ",cr))
    for (yr in years){
      print(paste0("-- ",yr))
      # temp only for 2020
      # z dimension is the months of 2020
      # check if fle exists paste0(getwd(),'/crops/Risultati_WaterCROP/',sc,'/',cr,'/',yr,"_370_GFDL",
      #                    '/yield_avg_ton_ha_',sc_num,'_',cr,"_",yr,"_370_GFDL.tif")
      if(file.exists(paste0(getwd(),'/crops/Risultati_WaterCROP/',sc,'/',cr,'/',yr,"_370_GFDL",
                             '/yield_avg_ton_ha_',sc_num,'_',cr,"_",yr,"_370_GFDL.tif"))){
        rr = raster(paste0(getwd(),'/crops/Risultati_WaterCROP/',sc,'/',cr,'/',yr,"_370_GFDL",
                          '/yield_avg_ton_ha_',sc_num,'_',cr,"_",yr,"_370_GFDL.tif") )
        
        raster.crs <- CRS(projection(rr))
        basin.spdfr <- spTransform(basin.spdf, raster.crs)
        rr[rr == 0] <- NA
        # do AVERAGE per BCU
        a = exact_extract(rr, basin.spdfr, function(values, coverage_fraction)
          mean(values * coverage_fraction, na.rm=TRUE))
        yy.temp = as.data.frame(basin.spdf@data) %>% dplyr::select(BCU) %>%
          bind_cols(data.frame(value = a)) %>%
          mutate(year = yr,
                 scenario = sc,
                 crop = cr)
        
        yy.df = bind_rows(yy.df,yy.temp)
        
      }
      if(file.exists(paste0(getwd(),'/crops/Risultati_WaterCROP/',sc,'/',cr,'/',yr,"_370_GFDL",
                         '/yield_avg_closure_ton_ha_',sc_num,'_',cr,"_",yr,"_370_GFDL.tif"))){
        rrm = raster(paste0(getwd(),'/crops/Risultati_WaterCROP/',sc,'/',cr,'/',yr,"_370_GFDL",
                         '/yield_avg_closure_ton_ha_',sc_num,'_',cr,"_",yr,"_370_GFDL.tif") )
      
        raster.crs <- CRS(projection(rrm))
        basin.spdfr <- spTransform(basin.spdf, raster.crs)
        rrm[rrm == 0] <- NA
        # extract raster with sf or spdf and to AVERAGE per BCU
        b = exact_extract(rrm, basin.spdfr, function(values, coverage_fraction)
          mean(values * coverage_fraction, na.rm=TRUE))
        ym.temp = as.data.frame(basin.spdf@data) %>% dplyr::select(BCU) %>% 
          bind_cols(data.frame(value = b)) %>% 
          mutate(year = yr,
                 scenario = sc,
                 crop = cr)  
        # 
        ym.df = bind_rows(ym.df,ym.temp)
      }
    }
  }
}
toc()
all_yield = yy.df %>% mutate(variable = "yield") %>% bind_rows(
  ym.df %>% mutate(variable = "yield")) %>% 
  mutate(unit  = "ton/ha/yr") %>% 
  mutate(crop = tolower(crop))

# keep wheat, barley, sugarcane, cotton, rice, maize, others
all_yy_top6.df = all_yield %>% 
  mutate(crop = if_else(crop %in% main_crops, crop, "others")) %>% 
  group_by_all() %>% summarise(value = sum(value)) %>% ungroup()

#summary
all_yy_sum = all_yy_top6.df %>% group_by(year,scenario,variable, unit) %>% 
  summarise(value = sum(value)) %>% ungroup()

write.csv(all_yy_top6.df,paste0(getwd(),"/crops/all_yields_main_crops_NEST_BCU.csv"),row.names = FALSE)

#### spammap 2017 ####
# taking harvested area only of irrigated regions, unit: [ha]
files_h = list.files(path = paste0(getwd(),
                                 "/crops/spam2017v2r1_ssa_harv_area.geotiff/."),
                   pattern = "_I.tif", all.files = FALSE,
                   full.names = FALSE, recursive = FALSE,
                   ignore.case = FALSE, include.dirs = FALSE, no.. = FALSE)
# crop yields for all crops, unit: [kg/ha]
files_y = list.files(path = paste0(getwd(),
                                   "/crops/spam2017v2r1_ssa_yield.geotiff/."),
                     pattern = "_I.tif", all.files = FALSE,
                     full.names = FALSE, recursive = FALSE,
                     ignore.case = FALSE, include.dirs = FALSE, no.. = FALSE)

spam_h.df = data.frame()
tic()
for (fl in files_h){
    print(paste0("- ",fl))
    # temp only for 2020
    # z dimension is the months of 2020
    rr = raster(paste0(getwd(),'/crops/spam2017v2r1_ssa_harv_area.geotiff/',fl) )
    
    # checking
    # extent(rr)
    # crs(rr)
    # proj4string(rr)
    # plot(rr)
    
    raster.crs <- CRS(projection(rr))
    basin.spdfr <- spTransform(basin.spdf, raster.crs)
    
    # extract raster with sf or spdf
    a = exact_extract(rr, basin.spdfr, function(values, coverage_fraction)
      sum(values * coverage_fraction, na.rm=TRUE))
    smap_h.temp = as.data.frame(basin.spdf@data) %>% dplyr::select(BCU) %>% 
      bind_cols(data.frame(value = a)) %>% 
      mutate(file = fl,
             variable = "harvested_crop")  
    spam_h.df = bind_rows(spam_h.df,smap_h.temp)
}
toc()
spam_h.df = spam_h.df %>% mutate(unit = "ha") %>% 
  mutate(crop_s = gsub("_I.tif","", gsub("spam2017V2r1_SSA_H_","", file))) %>% 
  group_by(BCU, crop_s, variable,unit ) %>% summarise(value = sum(value)) %>% ungroup()

# total irrigated area in 2010, ha
tot_irr_area2020 = sum(spam_h.df$value)
# tot irrigationw ater withdrawals 2010 (WaterCROP) km3
tot_irr_ww = (ww_sum %>% filter(year == 2020))$value 
# avg water ww per ha
water_ratio = tot_irr_ww / tot_irr_area2020
#literature example, 1 l/s/ha
1/31710

# spam_h_top6
map_crops_spam = data.frame(crop = main_crops,
                            crop_s = c("WHEA","COTT","SUGC","RICE","MAIZ"))

map_h_top6 = spam_h.df %>% left_join(map_crops_spam) %>% 
  mutate(crop = if_else(is.na(crop), "others",crop)) %>% 
  group_by(BCU, crop, variable, unit, value) %>% summarise(value = sum(value)) %>% 
  ungroup()
# save for possible use
write.csv(map_h_top6,paste0(getwd(),"/crops/SPAM_irr_crop_area_2017_NEST_BCU.csv"),row.names = FALSE)

# yield of all crops. unit kg/ha/yr 
# TO Be COMPARED wih WATERCROP VALUES
spam_y.df = data.frame()
tic()
for (fl in files_y){
  print(paste0("- ",fl))
  # temp only for 2020
  # z dimension is the months of 2020
  rr = raster(paste0(getwd(),'/crops/spam2017v2r1_ssa_yield.geotiff/',fl) )
  
  # checking
  # extent(rr)
  # crs(rr)
  # proj4string(rr)
  # plot(rr)
  
  raster.crs <- CRS(projection(rr))
  basin.spdfr <- spTransform(basin.spdf, raster.crs)
  rr[rr == 0] <- NA
  # extract raster with sf or spdf and do AVERAGE per BCU
  a = exact_extract(rr, basin.spdfr, function(values, coverage_fraction)
    mean(values * coverage_fraction, na.rm=TRUE))
  smap_y.temp = as.data.frame(basin.spdf@data) %>% dplyr::select(BCU) %>% 
    bind_cols(data.frame(value = a)) %>% 
    mutate(file = fl,
           variable = "yield")  
  spam_y.df = bind_rows(spam_y.df,smap_y.temp)
}
toc()
spam_y.df = spam_y.df %>% mutate(unit = "kg/ha") %>% 
  mutate(crop_s = gsub("_I.tif","", gsub("spam2017V2r1_SSA_Y_","", file)),
         value = if_else(is.na(value), 0, value)) %>% 
  group_by(BCU, crop_s, variable,unit ) %>% summarise(value = sum(value)) %>% ungroup()


# compare sugarcane
sc_comp = spam_y.df %>% filter(crop_s == "SUGC") %>% 
  mutate(crop = "sugarcane",
         value = value /1000) %>% select(BCU,crop, value) %>%
  rename(value_spam = value ) %>% 
  left_join(all_yy_top6.df %>% 
              filter(crop == "sugarcane",
              variable == 'yield',
              year == 2020,
              scenario == "baseline")  %>% 
              select(BCU, value, unit, crop) %>% 
              rename(value_WaterCrop = value ))


# map plotting
library(rworldmap)
regions <- st_as_sf(rworldmap::countriesLow)
# basin.sf

wat_ww.df = ww_wg_top6 %>%  group_by(BCU,year,scenario,unit) %>% 
  summarise(value = sum(value)) %>% ungroup()

comp.sf = basin.sf %>% select(BCU,geometry) %>% left_join(wat_ww.df)

a1 = ggplot()+
  theme_classic()+
  geom_sf(data=regions, fill="#f5efdf")+
  geom_sf(data=comp.sf %>% filter(year %in% c(2020,2030,2050),
                                  value >= 0), 
          aes(fill=cut(value*1000, breaks=c(0,1,10,30,70,150,400),include.lowest=TRUE)) )+
  facet_wrap(factor(scenario, levels =
                      c("baseline", "improved", "ambitious"))~year, 
             ncol = 3)+
  scale_fill_brewer(name="MCM/yr.", palette = "YlGnBu")+
  coord_sf(xlim=c(21, 35), ylim=c(-18, -7))+
  ggtitle("")+
  theme(legend.position = "right", 
        legend.direction = "vertical", 
        aspect.ratio = 3/4, 
        axis.text.x=element_blank(),
        axis.ticks.x=element_blank(), 
        axis.text.y=element_blank(), 
        axis.ticks.y=element_blank())

ggsave(paste0("out_figures/water_crop_irr_map_actual_max_",country,".png"), a1, scale=1, height = 7, width = 7)

