
rm(list=ls())
graphics.off()
library(raster)
library(sp)
library(ncdf4)
library(rgdal)
library(rgeos)
library(maptools)
library(geosphere)
library(countrycode)
library(ggmap)
library(tidyverse)
library(yaml)
library("readxl")
#library(reticulate)

# Increase memory size
memory.limit(size = 1e6)

#this path need tobe automatize, maybe loading with retisulate a python ojects that contains the rith path.
#otherwise environment path an be used
msg_data = Sys.getenv("MESSAGE_DATA_PATH")
# message-ix-models path
msg_ix_model = Sys.getenv("MESSAGE-IX-MODELS")
data_subf = path.expand(paste0(msg_data,'\\data\\water\\ppl_cooling_tech'))

all_units = read_excel('P:/ene.model/NEST/ppl_cooling_tech/PLATTS_3.7.xlsx')

all_units.df = all_units %>% select(UNIT,STATUS,ISO,msgregion, msg_combo2,cool_group_msg,MW_x,WW,WC,lat,long)
# here we need to adapt for a different regional conigurations of the GLOBAL model

for (reg in c('R11','R12')){
# for global R11 model
  cooling_plants = all_units.df %>% filter(STATUS == 'OPR',
                                                !cool_group_msg %in% c('CHP','NCN'),
                                           !is.na(cool_group_msg),
                                           !is.na(msgregion)) %>% 
    rename(utype = msg_combo2, cooling = cool_group_msg) %>% ungroup()
  
  # get mapping from yaml file in message-ix-models
  file = paste0(msg_ix_model,'/message_ix_models/data/node/',reg,'.yaml')
  from_yaml = read_yaml(file, fileEncoding = "UTF-8" )
  names_loop = names(from_yaml)[names(from_yaml) != 'World']
  reg_map.df = data.frame()
  for (rr in names_loop){
    reg_map.df = rbind(reg_map.df,data.frame(msgregion = rr, ISO = from_yaml[[rr]]$child))
  }
  
  # add R## to the msgregions, to go the in any column
  cooling_plants = cooling_plants %>% select(-msgregion) %>% 
    left_join(reg_map.df)
  
  # add the data to the cooltech_cost_and_shares_ssp_msg.csv file
  cooltech_cost_shares = read.csv(paste0(data_subf,'/cooltech_cost_and_shares_ssp_msg.csv'),stringsAsFactors=FALSE)
  
  #### shares by message REGION #### needed to establish initial mapping
  shars_cooling_MSG_global = cooling_plants %>% group_by(utype,cooling,msgregion) %>% #change to COUNTRY
    summarise(MW_x = sum(MW_x)) %>% ungroup() %>% 
    group_by(utype,msgregion) %>% #change to COUNTRY
    mutate(cap_reg_unit = sum(MW_x)) %>% ungroup() %>% 
    mutate(shares = MW_x / cap_reg_unit) %>% 
    select(utype,cooling,msgregion,shares) %>% 
    spread(msgregion, shares)
  
  shars_cooling_MSG_global[is.na(shars_cooling_MSG_global)] = 0
  
  # mapping in order to add missing technologies
  platts_types = shars_cooling_MSG_global %>% select(utype) %>% rename(utype_pl = utype) %>% 
    mutate(match = gsub('_.*','',utype_pl)) %>% group_by(match) %>% 
    summarise(utype_pl = first(utype_pl), match = first(match))
  map_all_types = cooltech_cost_shares %>% select(utype,cooling) %>% 
    mutate(match = gsub('_.*','',utype)) %>% left_join(platts_types) %>% select(-match) %>% 
    filter(!is.na(utype_pl)) %>% distinct()
  
  # This will be the file
  write.csv(shars_cooling_MSG_global,paste0(data_subf,'/cool_techs_region_share_',reg,'.csv'),row.names = FALSE)
  
  # back to shares, change names
  new_names = paste0('mix_', names(shars_cooling_MSG_global)[!names(shars_cooling_MSG_global) %in% c('utype','cooling')] )
  names(shars_cooling_MSG_global)[!names(shars_cooling_MSG_global) %in% c('utype','cooling')] = new_names
  shars_cooling_MSG_global = shars_cooling_MSG_global %>% mutate(utype_pl = utype)
  
  # missing shares
  all_shares = map_all_types %>% 
    filter(!utype %in% unique(shars_cooling_MSG_global$utype) ) %>% 
    left_join(shars_cooling_MSG_global %>% select(-utype), by = c('utype_pl','cooling') ) %>% 
    select(-utype_pl) %>% bind_rows(shars_cooling_MSG_global)
  
  cooltech_cost_shares = cooltech_cost_shares %>%
    select(utype,	cooling,investment_million_USD_per_MW_low,	investment_million_USD_per_MW_mid,	investment_million_USD_per_MW_high) %>% 
    left_join(all_shares)
  
  cooltech_cost_shares[is.na(cooltech_cost_shares)] = 0
  # write new file
  write.csv(cooltech_cost_shares,paste0(data_subf,'/cooltech_cost_and_shares_ssp_msg_',reg,'.csv'),row.names = FALSE)
}

#### SHARES by COUNTRY ####
#shares by message REGION
shars_cooling_country = cooling_plants %>% group_by(utype,cooling,ISO) %>% #change to COUNTRY
  summarise(MW_x = sum(MW_x)) %>% ungroup() %>% 
  group_by(utype,ISO) %>% #change to COUNTRY
  mutate(cap_reg_unit = sum(MW_x)) %>% ungroup() %>% 
  mutate(shares = MW_x / cap_reg_unit) %>% 
  select(utype,cooling,ISO,shares) %>% 
  spread(ISO, shares)

shars_cooling_country[is.na(shars_cooling_country)] = 0

# This will be the file
write.csv(shars_cooling_country,paste0(data_subf,'/cool_techs_country_share.csv'),row.names = FALSE)

# back to shares, change names
new_names = paste0('mix_', names(shars_cooling_country)[!names(shars_cooling_country) %in% c('utype','cooling')] )
names(shars_cooling_country)[!names(shars_cooling_country) %in% c('utype','cooling')] = new_names
shars_cooling_country = shars_cooling_country %>% mutate(utype_pl = utype)

# missing shares
all_shares_c = map_all_types %>% 
  filter(!utype %in% unique(shars_cooling_country$utype) ) %>% 
  left_join(shars_cooling_country %>% select(-utype), by = c('utype_pl','cooling') ) %>% 
  select(-utype_pl) %>% bind_rows(shars_cooling_country)

cooltech_cost_shares_c = cooltech_cost_shares %>%
  select(utype,	cooling,investment_million_USD_per_MW_low,	investment_million_USD_per_MW_mid,	investment_million_USD_per_MW_high) %>% 
  left_join(all_shares_c)

cooltech_cost_shares_c[is.na(cooltech_cost_shares_c)] = 0
# write new file
write.csv(cooltech_cost_shares_c,paste0(data_subf,'/cooltech_cost_and_shares_country.csv'),row.names = FALSE)


# OLD DATABASE, with the new one, most of this processing is done already by the script
# https://github.com/OFR-IIASA/message_data/blob/RES_add_5_year_timesteps2/data/model/platts_historical_capacity/merge_WEPP_Raptis_CARMA.py

# # loading
# temp = readOGR(path.expand(paste0(data_subf,'\\delineation')),'MSG11_reg_simpl',verbose=FALSE)
# 
# # the input shape file should come externally, in some model setup
# # usually it has two columns, the first should be called 'PID', the second 'REGION'
# # only the second column matter in terms of name, and should have the region names used in the model
# 
# region_units.spdf = spTransform(temp, CRS("+proj=longlat"))
# rm(temp)

# # Read in Raptis et al data
# ppl.df = data.frame(read.csv(paste0(data_subf,'/ppl_cooling_tech/POWER_PLANTS_2016_Raptis.csv'),stringsAsFactors=FALSE))
# raptis_types_map_cooling.df = data.frame(read.csv(paste0(data_subf,'/ppl_cooling_tech/raptis_types_map_cooling_type.csv'),stringsAsFactors=FALSE))
# raptis_types_map_unit.df = data.frame(read.csv(paste0(data_subf,'/ppl_cooling_tech/raptis_types_map_unit_type.csv'),stringsAsFactors=FALSE))
# 
# # MESSAGE technologies
# msg_types.df = data.frame(read.csv(paste0(data_subf,'/ppl_cooling_tech/tech_names_ssp_msg.csv'),stringsAsFactors=FALSE))
# msg_ppl = unique(c(raptis_types_map_unit.df$msgssp_type,raptis_types_map_unit.df$alt1,raptis_types_map_unit.df$alt2,raptis_types_map_unit.df$alt3))
# msg_ppl = msg_ppl[-1*(which(msg_ppl==''))]
# msg_cool = unique(raptis_types_map_cooling.df$msgssp_type)
# 
# fuel_group = lapply(1:length(msg_ppl),function(x){ 
# 	ind1 = which( as.character(raptis_types_map_unit.df$msgssp_type) == as.character(msg_ppl[x]) | as.character(raptis_types_map_unit.df$alt1) == as.character(msg_ppl[x]) | as.character(raptis_types_map_unit.df$alt2) == as.character(msg_ppl[x]) | as.character(raptis_types_map_unit.df$alt3) == as.character(msg_ppl[x]) )
# 	return(unique(unlist(strsplit(  as.character(raptis_types_map_unit.df$raptis_type[ind1]),'[.]' ))[seq(1,length(unlist(strsplit(  as.character(raptis_types_map_unit.df$raptis_type[ind1]),'[.]' ))),b=2)]))   
# 	})
# 
# utype = lapply(1:length(msg_ppl),function(x){ 
# 	ind1 = which( as.character(raptis_types_map_unit.df$msgssp_type) == as.character(msg_ppl[x]) | as.character(raptis_types_map_unit.df$alt1) == as.character(msg_ppl[x]) | as.character(raptis_types_map_unit.df$alt2) == as.character(msg_ppl[x]) | as.character(raptis_types_map_unit.df$alt3) == as.character(msg_ppl[x]) )
# 	return(unique(unlist(strsplit(  as.character(raptis_types_map_unit.df$raptis_type[ind1]),'[.]' ))[seq(2,length(unlist(strsplit(  as.character(raptis_types_map_unit.df$raptis_type[ind1]),'[.]' ))),b=2)]))   
# 	}	)
# 
# cool = lapply(1:length(msg_cool),function(x){ as.character(raptis_types_map_cooling.df$raptis_type[which( as.character(raptis_types_map_cooling.df$msgssp_type) == as.character(msg_cool[x]) )])})
# 	
# # Pull out data for this specific technology for each region
# cool_techs = unlist(lapply(1:length(msg_cool), function(x){as.matrix(unlist(lapply(1:length(msg_ppl),function(y){paste(msg_ppl[y],msg_cool[x],sep='.')})))}))
# xs = rep(seq(1,length(msg_ppl),by=1),length(msg_cool))
# ys = unlist(lapply(1:length(msg_cool),function(dd){ rep(dd,length(msg_ppl)) })) 
# 
# # Cooling techs by pid
# cool_techs_by_pid = data.frame(do.call(rbind, lapply(1:length(cool_techs), function(z){
# 	xx = xs[z]
# 	yy = ys[z]
# 	temp = ppl.df[which( as.character(ppl.df$Fuel_group) %in% as.character(fuel_group[[xx]]) &
# 	                       as.character(ppl.df$UTYPE) %in% as.character(utype[[xx]]) & 
# 	                       as.character(ppl.df$COOL_complete) %in% as.character(cool[[yy]]) ),]
# 	if(nrow(temp)>0)
# 		{
# 		temp$x = temp$long
# 		temp$y = temp$lat
# 		coordinates(temp) = ~ x + y
# 		proj4string(temp) = proj4string(region_units.spdf)
# 		temp$pid = unlist(over(temp,region_units.spdf[,which(names(region_units.spdf) == 'PID')]))
# 		temp = temp[-1*(which(is.na(temp$pid) | is.na(temp$MW))),]
# 		mw_by_pid = unlist(lapply(1:length(region_units.spdf@data$PID), function(bb){ max(0, sum( temp$MW[ which( as.character(temp$pid) == as.character(region_units.spdf@data$PID[bb]) ) ], na.rm=TRUE ), na.rm=TRUE) } ) )
# 		return(mw_by_pid)
# 		}else
# 		{
# 		mw_by_pid  = rep(0,length(region_units.spdf@data$REGION))
# 		return(mw_by_pid)
# 		}	
# 	})))
# row.names(cool_techs_by_pid) = cool_techs
# 	
# type_general = unlist(strsplit( cool_techs, '[.]' ))[seq(1,length(unlist(strsplit( cool_techs, '[.]' ))),by=2)]
# cool_general = unlist(strsplit( cool_techs, '[.]' ))[seq(2,length(unlist(strsplit( cool_techs, '[.]' ))),by=2)]
# cool_techs_shares_by_pid = data.frame( do.call( cbind, lapply(1:length(region_units.spdf@data$REGION), function(r){ as.matrix( unlist( lapply( 1:length(cool_techs), function(z){
# 	share =  cool_techs_by_pid[z,r] / sum( c( cool_techs_by_pid[ which( as.character(type_general) == as.character(type_general[z]) ), r] ), na.rm = TRUE)
# 	if( is.na(share) | length(share) == 0 ){share =  round( sum( c( cool_techs_by_pid[ which( as.character(cool_general) == as.character(cool_general[z]) ), r] ), na.rm = TRUE) / sum( c( cool_techs_by_pid[ , r] ), na.rm = TRUE), digits = 3)}
# 	return(round(share, digits = 3)) } ) ) ) } ) ) )	
# row.names(cool_techs_shares_by_pid) = cool_techs		
# names(cool_techs_shares_by_pid) = 	as.character(region_units.spdf@data$REGION)	
# cool_techs_shares_by_pid$cooling_utype = as.character(cool_techs)
# cool_techs_shares_by_pid$cooling = as.character(cool_general)
# cool_techs_shares_by_pid$utype = as.character(type_general)
# cool_techs_shares_by_pid = cool_techs_shares_by_pid[,c('utype','cooling',as.character(region_units.spdf@data$REGION))]
# cool_techs_shares_by_pid = cool_techs_shares_by_pid[ order( cool_techs_shares_by_pid$utype ), ]
# 
# # This will be the file
# write.csv(cool_techs_shares_by_pid,paste0(data_subf,'/cool_techs_region_share.csv'),row.names = FALSE)
# 
# #add 'mix_' to the columns names for regions
# cool_techs_shares_by_pid2 = cool_techs_shares_by_pid
# new_names = paste0('mix_', names(cool_techs_shares_by_pid2)[!names(cool_techs_shares_by_pid2) %in% c('utype','cooling')] )
# names(cool_techs_shares_by_pid2)[!names(cool_techs_shares_by_pid2) %in% c('utype','cooling')] = new_names
# 
# # add the data to the cooltech_cost_and_shares_ssp_msg.csv file
# cooltech_cost_shares = read.csv(paste0(data_subf,'/cooltech_cost_and_shares_ssp_msg.csv'),stringsAsFactors=FALSE)
# cooltech_cost_shares = cooltech_cost_shares %>% 
#   select(utype,	cooling,investment_million_USD_per_MW_low,	investment_million_USD_per_MW_mid,	investment_million_USD_per_MW_high) %>% 
#   left_join(cool_techs_shares_by_pid2)
# 
# # write new file
# write.csv(cool_techs_shares_by_pid,paste0(data_subf,'/cooltech_cost_and_shares_ssp_msg.csv'),row.names = FALSE)
