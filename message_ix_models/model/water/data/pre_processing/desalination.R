# Desalination
rm(list = ls())
require(reshape)
require(rgdal)
require(raster)
require(rgeos)
require(countrycode)
require(RCurl)
require(xlsx)

require(maptools)
require(ncdf4)

library(broom)
library(readstata13)
library(sandwich)
library(lmtest)
library(zoo)
library(readxl)
library(corrplot)
library(plm)
library(car)
# library(gghighlight)

library(tidyverse)
# library(caret)

# here we put together the data processing scripts that
# 1. determine historical capacity (from oder add_Water_infractructure.R from Simon)
# 2. set future base desalination trends
# 3. set upperbound for desalination expansion/speed of growth

model_years = c(1990,1995,2000,2005,2010,2015)
#define region

#### not needed####
# Load in the cleaned desal database from Hanasaki et al 2016 and add MESSAGE regions
reg.spdf = readOGR('P:/ene.model/data/desalination','REGION_dissolved',verbose=FALSE) # old
global_desal.spdf = spTransform( readOGR('P:/ene.model/NEST/desalination/Hanasaki_et_al_2015','global_desalination_plants'), crs(reg.spdf) )
global_desal.spdf@data$region = over(global_desal.spdf,reg.spdf[,which(names(reg.spdf) == 'REGION')])$REGION
global_desal.spdf = global_desal.spdf[ -1*which( is.na(global_desal.spdf@data$region) ), ]
global_desal.spdf@data$msg_vintage = sapply( global_desal.spdf@data$online, function(x){ as.numeric( model_years[ which.min( ( as.numeric( model_years ) - x )^2 ) ] ) } )
global_desal.spdf@data$technology_2 = sapply( global_desal.spdf@data$technology, function(x){ if( grepl('MSF',x)|grepl('MED',x) ){ return('distillation') }else{ return('membrane') }} )

# From : Chart 1.1 in 'Executive Summary Desalination Technology Markets Global Demand Drivers, Technology Issues, Competitive Landscape, and Market Forecasts'
# Global desalination capacity in 2010 was approx. 24 km3 / year
global_desal.spdf@data$m3_per_day = global_desal.spdf@data$m3_per_day  * ( 24  / ( sum( global_desal.spdf@data$m3_per_day ) * 365 / 1e9 ) )

region = unique(global_desal.spdf$region)
# Match to message vintaging and regions, km3/years
historical_desal_capacity.list = lapply( c('membrane','distillation'), function(tt){
  temp = data.frame( do.call(cbind, lapply( region, function(reg){ sapply( unique(global_desal.spdf@data$msg_vintage)[
    order(unique(global_desal.spdf@data$msg_vintage))], function(y){
      (365/1e9) * max( 0,  sum( global_desal.spdf@data$m3_per_day[
        which( global_desal.spdf@data$msg_vintage == y & global_desal.spdf@data$region == reg &
                 global_desal.spdf@data$technology_2 == tt ) ] , na.rm=TRUE ), na.rm=TRUE ) } ) } ) ) )
  names(temp) = region
  row.names(temp) = unique(global_desal.spdf@data$msg_vintage)[order(unique(global_desal.spdf@data$msg_vintage))]
  return(temp)
} )
names(historical_desal_capacity.list) = c('membrane','distillation')

#### from here ####

#summarise for regression
global_desal.csv = read.csv('P:/ene.model/NEST/desalination/Hanasaki_et_al_2015/desal_data_Hanasaki_et_al_2015.csv')

desal_data.df = global_desal.csv
# clean up names
names(desal_data.df) = c("country","location","m3_per_day","technology","water_type","online","user","price","lat","lon")
desal_data.df$country = as.character(desal_data.df$country)
desal_data.df$location = as.character(desal_data.df$location)
desal_data.df$technology = as.character(desal_data.df$technology)
desal_data.df$water_type = as.character(desal_data.df$water_type)
desal_data.df$user = as.character(desal_data.df$user)
desal_data.df$price = as.character(desal_data.df$price)
desal_data.df$m3_per_day = gsub(",", "", desal_data.df$m3_per_day)
desal_data.df$m3_per_day = as.numeric(desal_data.df$m3_per_day)
desal_data.df$online = as.numeric(desal_data.df$online)
desal_data.df$lat = as.numeric(desal_data.df$lat)
desal_data.df$lon = as.numeric(desal_data.df$lon)
temp1 = unique(desal_data.df$user)
temp2 = c('municipal','industry','power','tourism','irrigation','military')
for (i in 1:length(temp1))
{
  desal_data.df$user[desal_data.df$user == temp1[i]] = temp2[i]
}
temp1 = unique(desal_data.df$water_type)
temp2 = c('seawater','brine')
for (i in 1:length(temp1))
{
  desal_data.df$water_type[desal_data.df$water_type == temp1[i]] = temp2[i]
}
head(desal_data.df)

map_iso3 = data.frame(country = unique(desal_data.df$country),
                      iso3 = countrycode(unique(desal_data.df$country),
                                         origin = 'country.name',
                                         destination = 'iso3c' ))
desal_data.df = desal_data.df %>% left_join(map_iso3)

#### DESALDATA direclty form database and see if there is more data than from Hanasaki et al 2016 ####
desaldata.csv = read.csv('P:/ene.model/NEST/desalination/DESALCAPACITYDATA/DesalData-2016-06-15.csv')

desaldata.df = desaldata.csv %>%
  select(Country,Location,Capacity..m3.d.,Technology,Feedwater,Online.date,Customer.type,EPC.price,Plant.status,Latitude,Longitude)

# clean up names
names(desaldata.df) = c("country","location","m3_per_day","technology","water_type","online","user","price","status","lat","lon")
desaldata.df$country = as.character(desaldata.df$country)
desaldata.df$location = as.character(desaldata.df$location)
desaldata.df$technology = as.character(desaldata.df$technology)
desaldata.df$water_type = as.character(desaldata.df$water_type)
desaldata.df$user = as.character(desaldata.df$user)
desaldata.df$price = as.character(desaldata.df$price)
desaldata.df$m3_per_day = gsub(",", "", desaldata.df$m3_per_day)
desaldata.df$m3_per_day = as.numeric(desaldata.df$m3_per_day)
desaldata.df$online = as.numeric(desaldata.df$online)
desaldata.df$status = as.character(desaldata.df$status)
desaldata.df$lat = as.numeric(desaldata.df$lat)
desaldata.df$lon = as.numeric(desaldata.df$lon)
temp1 = unique(desaldata.df$user)
temp2 = c('municipal','industry','power','tourism','irrigation','military')
for (i in 1:length(temp1))
{
  desaldata.df$user[desaldata.df$user == temp1[i]] = temp2[i]
}
temp1 = unique(desaldata.df$water_type)
temp2 = c('seawater','brine')
for (i in 1:length(temp1))
{
  desaldata.df$water_type[desaldata.df$water_type == temp1[i]] = temp2[i]
}
# status online, or ofline
desaldata.df = desaldata.df %>% mutate(status = if_else(grepl('Offline|Cancelled',status), 'offline','online') )
head(desaldata.df)
write.csv(desaldata.df,'P:/ene.model/NEST/desalination/DESALCAPACITYDATA2016_clean.csv')
# online plants for historical capacity
desaldata_online.df = desaldata.df %>% filter(status == 'online')
write.csv(desaldata_online.df,'P:/ene.model/NEST/desalination/DESALCAPACITYDATA2016_current_online.csv')

desaldata.df = desaldata.df %>% select(-status)
map_iso3 = data.frame(country = unique(desaldata.df$country),
                      iso3 = countrycode(unique(desaldata.df$country),
                                         origin = 'country.name',
                                         destination = 'iso3c' ))
desaldata.df = desaldata.df %>% left_join(map_iso3)

# m3_day yearly new capacity
desal_new_cap.df = desal_data.df %>%
  rename(year = online) %>%
  group_by(iso3,year) %>%
  summarise(desal_cap = sum(m3_per_day)) %>% ungroup() %>%
  group_by(iso3) %>%
  mutate(cum_desal = cumsum(desal_cap)) %>% ungroup()

#from desalCAPACITYDATA database
desal_new_cap2.df = desaldata.df %>%
  rename(year = online) %>% filter(!is.na(year),!is.na(m3_per_day)) %>%
  group_by(iso3,year) %>%
  summarise(desal_cap = sum(m3_per_day)) %>% ungroup() %>%
  group_by(iso3) %>%
  mutate(cum_desal = cumsum(desal_cap)) %>% ungroup()

desal_new_cap_glb.df = desal_new_cap2.df %>% group_by(year) %>% 
  summarise(desal_cap = sum(desal_cap),
            cum_desal = sum(cum_desal)) %>% ungroup() %>% 
  mutate(cum_desal2 = cumsum(desal_cap)) %>% 
  gather(key = "cum_type",value = "value",cum_desal,cum_desal2)

ggplot(desal_new_cap_glb.df)+
  geom_line(aes(x = year,y = value, color = cum_type))

#comparison
desal_comp = desal_new_cap.df %>%
  left_join(desal_new_cap2.df  %>%
              rename(desal2 = desal_cap,
                     cumsum2 = cum_desal)) %>%
  select(iso3,year,desal_cap,desal2,cum_desal,cumsum2) %>%
  group_by(iso3) %>%
  mutate(max_cumacp1 = max(cum_desal),
         max_cumcap2 = max(cumsum2)) %>% ungroup() %>%
  filter( (cum_desal == max_cumacp1),
          cum_desal > 1000000)

# it seems that the NA in the DESALCAPACITYDATA are included in other years in Hanasaki et al 2016
# moreover DESALCAPACITYDATA has longer historical than Hanasaki et al 2016
# Hanasaki et al 2016 summarizes some histircal data in later starting years
desal_new_cap3.df = desal_new_cap2.df %>% ungroup()

write.csv(desal_new_cap3.df,'P:/ene.model/NEST/desalination/DESALCAPACITYDATA2016_aggregated.csv')
# highest
quantiles = desal_new_cap3.df %>% group_by(iso3) %>%
  filter(cum_desal == max(cum_desal))
quantiles = quantiles %>%
  mutate(quant = if_else(cum_desal <= quantile(quantiles$cum_desal, 0.33), 'q3_3',
                         if_else(quantile(quantiles$cum_desal, 0.33) < cum_desal & cum_desal <= quantile(quantiles$cum_desal, 0.66), 'q2_3',
                                 'q1_3'))) %>%
  select(iso3,quant) %>% drop_na()

# plot to see if lines can be approssimates with linear models
ggplot(desal_new_cap3.df %>% left_join(quantiles) %>% filter(quant == 'q1_3'))+
  geom_line(aes(x = year,y = cum_desal))+
  facet_wrap(~iso3)+theme_bw()
ggplot(desal_new_cap3.df %>% left_join(quantiles) %>% filter(quant == 'q2_3'))+
  geom_line(aes(x = year,y = cum_desal))+
  facet_wrap(~iso3)+theme_bw()
ggplot(desal_new_cap3.df %>% left_join(quantiles) %>% filter(quant == 'q3_3'))+
  geom_line(aes(x = year,y = cum_desal))+
  facet_wrap(~iso3)+theme_bw()
# veryfy normal distribution assumption
hist(desal_new_cap3.df$desal_cap)
hist(desal_new_cap3.df$cum_desal)
# log
hist(log(desal_new_cap3.df$desal_cap) )
#it does not look very normal, it is right-skewed
hist(log(desal_new_cap3.df$cum_desal) )
# using cumulative capacity it has a normal shape

# load other variables for regression model
# GDP WB 2021 data , it has long history wrt to what Marina used
# change to NAVIGATE updates
gdp = read.csv('P:/ene.model/NEST/GDP/WB_GDP_2021/GDP_country_cleaned.csv') %>%
  gather(key = 'year',value = 'gdp',5:65) %>% select(iso3,year,gdp) %>%
  mutate(year = as.numeric(gsub('X','',year)))

gdp_historical<-read.csv("P:/ene.model/NEST/governance/governance_2021/input/navigate_ssp.csv") %>%
  filter(scenario=="WDI_2021", variable=="gdppc") %>%
  rename(iso3=region) %>%
  select(-c(scenario, variable)) %>%
  pivot_longer(!iso3, names_to = "Year", values_to = "gdp") %>%
  mutate(year = as.numeric(gsub('X','',Year))) %>%
  filter(year<2020) %>%
  select(iso3,year,gdp)

gdp_new_hist_proj = read.csv('P:/ene.model/NEST/governance/governance_2021/input/gdp_full_WB_NAVIGATE.csv')
gdp_new = gdp_new_hist_proj %>% select(iso3c,year,gdppc) %>%
  rename(iso3 = iso3c, gdp = gdppc) %>% drop_na()

hist(gdp$gdp)
hist(gdp_historical$gdp)
hist(log(gdp$gdp) )
hist(log(gdp_historical$gdp) )
hist(gdp_new$gdp)
hist(log(gdp_new$gdp))
# log has a better distribution

# Worldwide Governance Indicators - updates Kaufmann, Kraay & Mastruzzi (2021)
# The values of indicators span a range from -2.5 to 2.5, here they were
# transformed to fall in the 0 to 1 range

# Function to standardize the values from 0 to 1
range01 <- function(x){(x - min(x, na.rm = T))/(max(x, na.rm = T) - min(x, na.rm = T))}

wgi <- read.dta13('P:/ene.model/NEST/governance/wgidataset.dta') %>%
  select(code, countryname, year, contains('e'), -ges, -gen, -ger, -gel, -geu) %>%
  rename(voic.ac = vae,
         pol.stab = pve,
         gov.eff = gee,
         reg.qual = rqe,
         ru.law = rle,
         corr.cont = cce,
         countrycode = code) %>%
  gather(var, value, -countrycode, -countryname, -year) %>%
  group_by(var) %>%
  mutate(value = range01(value)) %>%
  ungroup() %>%
  mutate(year = as.integer(year)) %>%
  spread(var, value) %>%
  mutate(governance = rowMeans(select(., voic.ac,
                                      pol.stab, gov.eff, reg.qual, ru.law, corr.cont))) %>%  # # Governance variable as the arithmetic average of the six components of WGI) %>%
  select(-countryname)

gov = wgi %>% select(countrycode,year,governance)
names(gov) = c('iso3','year','gov')
names(wgi) = c('iso3','year',"corr.cont","gov.eff","pol.stab","reg.qual","ru.law","voic.ac",'gov')

# other from Elina
gov_eff<- read.csv("P:/ene.model/NEST/governance/governance_2021/input/governance_2021.csv") %>%
  mutate(gov_norm=range01(gee)) %>%
  select(countryname, year, gov_norm) %>%
  arrange(countryname, year) %>%
  mutate(iso3=countrycode(countryname, "country.name", "iso3c")) %>%
  select(iso3,year,gov_norm)

gov_eff<- read.csv("P:/ene.model/NEST/governance/governance_2021/input/qog_2021.csv") %>%
  mutate(gov_norm=range01(icrg_qog)) %>%
  select(ccodealp, year, gov_norm) %>%
  arrange(ccodealp, year) %>%
  mutate(iso3=ccodealp) %>%
  select(iso3,year,gov_norm) %>% drop_na()

hist(gov$gov)
hist(gov_eff$gov_norm)
# without log, it has a normal distribution

# WSI from Ed
wsi_hist = read.xlsx('P:/ene.model/data/Water/water_stress_cc/ISO_water_stress/wsi_tables_hist_new.xlsx',
                     sheetIndex = 1)

wsi.df = wsi_hist %>% gather(key = year, value = wsi , 3:length(wsi_hist)) %>%
  filter(!grepl('_bin',year)) %>%
  mutate(year = as.numeric(gsub('X','',year)),
         wsi = round(as.numeric(wsi),digits = 3)) %>%
  rename(iso3 = ISO) %>%
  select(iso3,year,wsi)
#very high values

hist(wsi.df$wsi)
hist(log(wsi.df$wsi))
#use log
library(WDI)
wsi_wb <- WDI(country = "all",
                     indicator = c('water_stress'="ER.H2O.FWST.ZS"),
                     start = 1960,end = 2020,extra = TRUE) %>%
  as_tibble() %>%
  filter(region!="Aggregates")
wsi_wb = wsi_wb %>% drop_na()
# probably notusing it, weird values

# presence of coast # of gridcels
# it would be nice to use historical trends of population living close to the coast
coast = read.csv('P:/ene.model/NEST/desalination/coast.csv')
names(coast) = c('iso3','coast')

hist(coast$coast)
# skewed, take log
hist(log(coast$coast))

# use all log variables, except from gov
# what about NA, how to handle?
master = desal_new_cap3.df %>% select(-cum_desal) %>%
  full_join(gdp_historical) %>%  # add gdp
  mutate(desal_cap = if_else(is.na(desal_cap), 0 ,desal_cap)) %>%
  # calculate cum capacity with in na after merging
  #re-aggregate desalination capacity
  arrange(year, .by_group = T) %>% ungroup() %>% group_by(iso3) %>%
  mutate(cum_desal = cumsum(desal_cap)) %>% ungroup() %>%
  # remove gdp na
  filter(!is.na(gdp)) %>%
  #retail only countries with desaliantion
  filter(iso3 %in% unique(desal_new_cap3.df$iso3)) %>%
  left_join(gov_eff) %>%
  left_join(wgi ) %>%
  left_join(coast) %>% filter(!is.na(coast)) %>%
  left_join(wsi.df) %>%
  ungroup() %>% group_by(iso3) %>%
  mutate(wsi = as.numeric(if_else(is.na(wsi), stats::quantile(wsi,0.75,na.rm = T), wsi)),
         gov_norm = as.numeric(if_else(year < 1984, stats::quantile(gov_norm,0.25,na.rm = T), gov_norm)),
         gov = as.numeric(if_else(year < 2004, stats::quantile(gov,0.25,na.rm = T), gov)),
         # corr.cont = as.numeric(if_else(year < 1996, stats::quantile(corr.cont,0.25,na.rm = T), corr.cont)),
         # gov.eff = as.numeric(if_else(year < 1996, stats::quantile(gov.eff,0.25,na.rm = T), gov.eff)),
         # pol.stab = as.numeric(if_else(year < 1996, stats::quantile(pol.stab,0.25,na.rm = T), pol.stab)),
         # ru.law = as.numeric(if_else(year < 1996, stats::quantile(ru.law,0.25,na.rm = T), ru.law)),
         # voic.ac = as.numeric(if_else(year < 1996, stats::quantile(voic.ac,0.25,na.rm = T), voic.ac))
         ) %>%
  ungroup() %>%
  mutate(log_desal = log(cum_desal),
         log_gdp = log(gdp),
         log_coast = log(coast),
         log_wsi = log(wsi)) %>%
  # filter out -inf, all val before first desal plant
  filter(!is.infinite(log_desal),!is.infinite(log_wsi)) %>%
  # select(iso3,year,log_desal,log_gdp,gov.eff,pol.stab,reg.qual,ru.law,voic.ac,gov_norm,log_wsi,log_coast)
  select(iso3,year,log_desal,log_gdp,gov_norm,log_wsi,log_coast)

master = master %>% rename(gov = gov_norm)
#### ANALYSIS ###
# Explore correlations between the variables in the dataset
cor.exp <- master %>%
  select(log_desal, log_gdp, gov,log_wsi, log_coast) %>%
  # select(log_desal, log_gdp, gov.eff, pol.stab, reg.qual, ru.law, voic.ac, gov_norm,log_wsi, log_coast) %>%
  drop_na() %>%
  cor()

corrplot(cor.exp, method = 'number',type = 'upper')

# Split the data into training and test set
data(mtcars)

## 75% of the sample size
smp_size <- floor(0.75 * nrow(master))

## set the seed to make your partition reproducible
set.seed(123)
train_ind <- sample(seq_len(nrow(master)), size = smp_size)

train.data <- master[train_ind, ]
test.data <- master[-train_ind, ]

#### simple model #### checking multicollinearity
# lm1 <- lm(log_desal ~ log_gdp + pol.stab + gov.eff + log_wsi + log_coast, data = train.data)
lm1 <- lm(log_desal ~ log_gdp + gov + log_wsi + log_coast, data = master)
summary(lm1)
# multicolinearity
# The smallest possible value of VIF is one (absence of multicollinearity).
# As a rule of thumb, a VIF value that exceeds 5 or 10 indicates a problematic amount of collinearity (James et al. 2014).
vif(lm1)
# remove gov.eff,ru.law,reg.qual  highest value

predictions = lm1 %>% predict(test.data)
RMSE(predictions, test.data$log_desal)
R2(predictions, test.data$log_desal)
# plot prediction
pred1 = test.data %>% bind_cols(prediction = predictions)

ggplot(pred1)+
  geom_point(aes(x = log_desal,y = prediction))+
  geom_abline()+
  scale_x_continuous(limits = c(0,20))+
  scale_y_continuous(limits = c(0,20))+
  xlab('log desal cap')+ylab('predictions')

#### fixed effect test ####
# 1) way
# lm2 <- lm(log_desal ~ log_gdp + pol.stab + gov.eff + log_wsi + log_coast + factor(iso3) -1, data = master)
# lm2 <- lm(log_desal ~ log_gdp + gov + log_wsi + log_coast + factor(iso3) -1, data = master)
# summary(lm2)
# 
# predictions2 = lm2 %>% predict()
# # remove countries that do not have enough data
# # BHS, HKG, MLT, PSE
# test.data2 = test.data %>% filter(!iso3 %in% c('BHS','HKG','MLT','PSE' ))
# predictions2 = lm2 %>% predict(test.data2)
# pred2 = test.data2 %>% bind_cols(prediction = predictions2)
# 
# ggplot(pred2)+
#   geom_point(aes(x = log_desal,y = prediction, color = iso3))+
#   geom_abline()+
#   scale_x_continuous(limits = c(0,20))+
#   scale_y_continuous(limits = c(0,20))+
#   xlab('log desal cap')+ylab('predictions')

# 2) way, with plm
lm3 <- plm(log_desal ~ log_gdp + gov + log_wsi + log_coast,
           data = master,
           index = c("iso3"),
           model = "within",
           effect = 'individual'
           )
summary(lm3)
summary(fixef(lm3))

pred3 = master %>%
  group_by(iso3) %>%
  mutate(y_mean = mean(log_desal)) %>% ungroup() %>%
  mutate(pred = predict(lm3)  + y_mean) # this predict only work on the train.data, canno apply to other data

ggplot(pred3)+
  geom_point(aes(x = log_desal, y = pred, color = iso3))+
  geom_abline()+
  scale_x_continuous(limits = c(0,20))+
  scale_y_continuous(limits = c(0,20))+
  xlab('log desal cap')+ylab('predictions')+theme_bw()

# Ftest. The null is that no time-fixed effects are needed
# maybe not needed, it was for testing time effect
pFtest(lm3, lm1)
plmtest(lm3, c("time"), type=("bp"))

# store models
# manually select model names
model_names = c("lm1",'lm2',"lm3")

# create a list based on models names provided
list_models = lapply(model_names, get)
names(list_models) = model_names
summary(list_models[['lm2']])

library(rlist)
list.save(list_models, 'P:/ene.model/NEST/desalination/regression_models_lm_plm.rdata')
# test load
tes = list.load(file="P:/ene.model/NEST/desalination/regression_models_lm_plm.RData")

# print summary using robust standard errors
coeftest(lm3, vcov. = vcovHC, type = "HC1")

#### TEST and COMPARE ####

get_estimate = function(es,var){
  c = es %>%
    filter(term == var) %>%
    select(estimate)
  return(c)
}

adjust_fe = function(fe){

  years <- c(year = seq(1995, 2100, 1))

  cntry.fe <-fe %>%
    merge(years, all.y = T) %>%
    dplyr::rename(year = y) %>% drop_na()
  # mutate(fe=abs(fe))

  # Country fixed effects: they are expected to converge to the 75th percentile of the present-day distribution,
  # in years in the future changing by the SSPs. We follow the approach of Crespo Cuaresma (2017),
  # and let the fixed effects converge for SSP1: 2130, SSP2:2250, SSP3: No convergence, SSP4: 2250, SSP5: 2180
  #adjust for the year 2020 as the base

  #The convergence rates for SSP2 and SSP4 are the same

  conv.rates <- data.frame(years = c(110, 230, 3000, 230, 160),
                           scenario = c('SSP1', 'SSP2', 'SSP3', 'SSP4', 'SSP5'))
  target <- quantile(cntry.fe$fe, 0.50)
  if (target > 0){
    max.fe <- max(cntry.fe$fe)
    } else {
    max.fe <- min(cntry.fe$fe)
    }

  #Calculated decreasing rates for each SSP

  ssps<-conv.rates %>%
    mutate(r=-log(target/max.fe)/years) %>%
    select(-years)

  ssps<- merge(ssps, years, all.y = T) %>%
    dplyr::rename(year=y)

  fe_adjusted<-merge(cntry.fe, ssps, by=c("year"))

  fe_adjusted<-fe_adjusted %>%
    filter(year>2019) %>%
    group_by(iso3, scenario) %>%
    mutate(fe_adj=fe) %>%
    mutate(fe_adj=ifelse(abs(fe)>abs(target), fe*(1+r)^(2021-year), fe_adj)) %>%
    # mutate(fe_adj=-fe_adj) %>%
    # rename(Year=year)
  return(fe_adjusted)
}

#lm2
lm2_es <- tidy(lm2)

c1l2 <- get_estimate(lm2_es,'log_gdp')$estimate
# c2l2  <- get_estimate(lm2_es,'pol.stab')$estimate
c3l2 <- get_estimate(lm2_es,'gov')$estimate
c4l2 <- get_estimate(lm2_es,'log_wsi')$estimate
c5l2 <- get_estimate(lm2_es,'log_coast')$estimate

cntry.fe2 <- lm2_es %>%
  filter(grepl('factor',term)) %>%
  dplyr::rename(iso3 = term,fe = estimate) %>%
  mutate(iso3 = gsub('[factor(iso3)]','',iso3)) %>%
  select(iso3,fe)

fe2 = adjust_fe(cntry.fe2)

#lm3
lm3_es <- tidy(lm3)

c1l3 <- get_estimate(lm3_es,'log_gdp')$estimate
# c2l3  <- get_estimate(lm3_es,'pol.stab')$estimate
c3l3 <- get_estimate(lm3_es,'gov')$estimate
c4l3 <- get_estimate(lm3_es,'log_wsi')$estimate
# c5l3 <- get_estimate(lm3_es,'log_coast')$estimate

#Calculation country fixed effects

cntry.fe3 <- fixef(lm3) %>%
  as.data.frame() %>%
  rownames_to_column("iso3") %>%
  dplyr::rename(fe = ".")

fe3 = adjust_fe(cntry.fe3)

# plot
fe2 %>%
  filter(iso3 == "CHN") %>%
  filter(scenario!="SSP4") %>%
  ggplot(aes(x = year, y = fe_adj, color = scenario)) +
  geom_line(size = 1) +
  labs(y = "Fixed effect", x = "China")

# tests
test.res = master %>%
  left_join(cntry.fe2 %>% dplyr::rename(fe2 = fe)) %>%
  left_join(cntry.fe3 %>% dplyr::rename(fe3 = fe))

projected<-test.res %>%
  mutate(log_desal2 = log_gdp*(c1l2) + gov*(c3l2) + log_wsi*(c4l2) + log_coast*(c5l2) + fe2,
         log_desal3 = log_gdp*(c1l3) + gov*(c3l3) + log_wsi*(c4l3) + fe3)

# plot
ggplot(projected %>% filter( iso3 %in% c('USA','ARE','SAU','ESP','KWT','QAT')) )+
  geom_line(aes(x = year,y = exp(log_desal) ), color = 'black')+
  geom_line(aes(x = year,y = exp(log_desal2) ), color = 'blue')+
  geom_line(aes(x = year,y = exp(log_desal3) ), color = 'red')+
  facet_wrap(~iso3)+theme_bw()

# check master, which variables are driving drop in projections for ARE, SAU, KWT
ggplot( )+
  geom_line(data = projected %>% filter( iso3 %in% c('ARE','SAU','KWT','QAT')),aes(x = year,y = log_desal2 ), color = 'grey',size = 2)+
  geom_line(data = master %>% filter( iso3 %in% c('ARE','SAU','KWT','QAT')),aes(x = year,y = log_desal ), color = 'black')+
  geom_line(data = master %>% filter( iso3 %in% c('ARE','SAU','KWT','QAT')),aes(x = year,y = log_gdp ), color = 'blue')+
  geom_line(data = master %>% filter( iso3 %in% c('ARE','SAU','KWT','QAT')),aes(x = year,y = log_wsi ), color = 'red')+
  geom_line(data = master %>% filter( iso3 %in% c('ARE','SAU','KWT','QAT')),aes(x = year,y = gov ), color = 'green')+
  facet_wrap(~iso3)+theme_bw()

ggplot(projected %>% filter( iso3 %in%  quantiles[quantiles$quant == 'q1_3',]$iso3 ,
                             !iso3 %in% c('USA','ARE','SAU','ESP','KWT','QAT') ) )+
  geom_line(aes(x = year,y = exp(log_desal) ), color = 'black')+
  geom_line(aes(x = year,y = exp(log_desal2) ), color = 'blue')+
  geom_line(aes(x = year,y = exp(log_desal3) ), color = 'red')+
  facet_wrap(~iso3)+theme_bw()

ggplot(projected %>% filter( iso3 %in%  quantiles[quantiles$quant == 'q2_3',]$iso3 ,
                             !iso3 %in% c('USA','ARE','SAU','ESP','KWT','QAT') ) )+
  geom_line(aes(x = year,y = exp(log_desal) ), color = 'black')+
  geom_line(aes(x = year,y = exp(log_desal2) ), color = 'blue')+
  geom_line(aes(x = year,y = exp(log_desal3) ), color = 'red')+
  facet_wrap(~iso3)+theme_bw()

ggplot(projected %>% filter( iso3 %in%  quantiles[quantiles$quant == 'q3_3',]$iso3 ) )+
  geom_line(aes(x = year,y = exp(log_desal) ), color = 'black')+
  geom_line(aes(x = year,y = exp(log_desal2) ), color = 'blue')+
  geom_line(aes(x = year,y = exp(log_desal3) ), color = 'red')+
  facet_wrap(~iso3)+theme_bw()

# lm2 and lm3 equal now
###########################
##### PROJECTIONS #########
###########################

#### TAKE MESSAGE REGION-BASIN structure ####
reg = 'R11' # or R12 or a ISO3
scen = 'SSP2'
if (!reg %in% master$iso3){
  print(paste0("ATTENTION ",reg," is not in the dataset, continue only if its is a non-country (e.g. R11)"))
}
basin_by_region.spdf = readOGR('P:/ene.model/NEST/delineation/data/delineated_basins_new',
                               paste0('basins_by_region_simpl_',reg), verbose=FALSE)
row.names(basin_by_region.spdf@data) = 1:length(basin_by_region.spdf$BASIN)

# Gadm country deineation
countries.spdf = readOGR('P:/ene.general/Water/global_basin_modeling/basin_delineation/data/country_delineation/gadm/output_data', 'gadm_country_boundaries', verbose=FALSE)
countries.spdf@data$ID = as.numeric(countrycode(countries.spdf@data$ISO, "iso3c", "iso3n"))
countries.spdf = countries.spdf[,c('ID','ISO','NAME','REGION','STATUS','CONTINENT')]

#read in raster from ED. ATTENTION 2100 and 2110 missing
## Set path data folder in message_ix working copy
msg_data = paste0('P:/ene.model/NEST')
data_path = path.expand(msg_data)
RCPs = c('no_climate','2p6','6p0')

nc = nc_open( paste0(data_path,'/water_scarcity/wsi_memean_ssp2_rcp6p0.nc'), verbose=FALSE)
watstress.brick = brick( paste0(data_path,'/water_scarcity/wsi_memean_ssp2_rcp6p0.nc') )

ras1 = watstress.brick$X2010s
plot(ras1)
start.spdf = rasterToPoints(ras1, spatial = TRUE)

writeOGR(
  start.spdf,
  'P:/ene.model/NEST/desalination',
  paste0('WSI_check'),
  driver="ESRI Shapefile",
  overwrite_layer=TRUE
)
start.spdf$X2010s = NULL

basin_info = over(start.spdf,basin_by_region.spdf)
country_info = over(start.spdf,countries.spdf)

for (rcp in RCPs) {
  print(paste0("RCP: ",rcp))
  if (rcp == '2p6'){
    nc = nc_open( paste0(data_path,'/water_scarcity/wsi_memean_ssp1_rcp4p5.nc'), verbose=FALSE)
    watstress.brick = brick( paste0(data_path,'/water_scarcity/wsi_memean_ssp1_rcp4p5.nc') )

  } else{
    nc = nc_open( paste0(data_path,'/water_scarcity/wsi_memean_ssp2_rcp6p0.nc'), verbose=FALSE)
    watstress.brick = brick( paste0(data_path,'/water_scarcity/wsi_memean_ssp2_rcp6p0.nc') )
  }

  ras1 = watstress.brick$X2010s
  plot(ras1)
  start.spdf = rasterToPoints(ras1, spatial = TRUE)
  start.spdf$X2010s = NULL
  initial_years <- seq(2010, 2090, 5)

  for (i in seq(1:nlayers(watstress.brick)) ){
    r <- raster(watstress.brick, layer=i)
    proj4string(r) = proj4string(watstress.brick)
    yr = as.numeric(gsub('X','',gsub('s','',names(r)) ))
    if (yr %in% initial_years){
      column_name = paste0('WSI.', yr )
      start.spdf@data[,column_name] = raster::extract(r,start.spdf)
      # there are 876 Na values, we set them = 0
      start.spdf@data[column_name][is.na(start.spdf@data[column_name])] = 0
    } else {}

  }

  data.spdf = start.spdf
  data.spdf@data = data.spdf@data %>%
    bind_cols(basin_info %>% select(BCU_name),
              country_info %>% select(ISO) %>% dplyr::rename(iso3 = ISO))

  map_years = rbind(data.frame(my = initial_years) %>% mutate(sy = my -2),
                    data.frame(my = initial_years) %>% mutate(sy = my -1),
                    data.frame(my = initial_years) %>% mutate(sy = my),
                    data.frame(my = initial_years) %>% mutate(sy = my +1),
                    data.frame(my = initial_years) %>% mutate(sy = my +2)) %>%
    arrange(my)

  # - makje a df with coordinates and years as columns
  data.df = cbind(data.spdf@coords, data.spdf@data) %>% select(BCU_name,iso3,everything()) %>%
    gather(key = year, value = wsi, 5:19) %>%
    mutate(year = as.numeric( gsub('WSI.','',year))) %>% drop_na()
  # - get national GDP, national governance indicator. point wsi and national coast values (as in regression)
  # - mask out points not in coast area
  # - project and make basin average

  # GDP
  gdp_proj<-read.csv("P:/ene.model/NEST/governance/governance_2021/input/navigate_ssp.csv") %>%
    filter(scenario=="SSP2", variable=="gdppc") %>%
    rename(iso3=region) %>%
    select(-c(scenario, variable)) %>%
    pivot_longer(!iso3, names_to = "Year", values_to = "gdp") %>%
    mutate(year = as.numeric(gsub('X','',Year))) %>%
    filter(year>=2020) %>%
    select(iso3,year,gdp)

  # make 5y averages
  gdp_proj = gdp_proj %>% filter(year <= 2092) %>% drop_na() %>%
    rename(sy = year) %>%
    left_join(map_years) %>% group_by(iso3,my) %>%
    summarise(gdp = mean(gdp)) %>% ungroup() %>%
    rename(year = my) %>%
    select(iso3, year, gdp)

  # Governance projections
  gov_proj<- read.csv("P:/ene.model/NEST/governance/governance_obs_project.csv") %>%
    mutate(gov=range01(governance)) %>%
    arrange(countrycode, year) %>%
    dplyr::rename(iso3 = countrycode) %>%
    filter(year >= 2018,year < 2092,
           scenario == scen) %>%
    select(iso3, year, gov)

  # make 5y averages
  gov_proj = gov_proj %>% filter(year <= 2092) %>% drop_na() %>%
    rename(sy = year) %>%
    left_join(map_years) %>% group_by(iso3,my) %>%
    summarise(gov = mean(gov)) %>% ungroup() %>%
    rename(year = my) %>%
    select(iso3, year, gov)

  # Governance from Elina
  read.csv("P:/ene.model/NEST/governance/governance_2021/output/projections_country_ssp2.csv")

  #values in lat historical period, LOG
  hist_cum_cap = master %>% select(iso3,year,log_desal) %>% group_by(iso3) %>%
    mutate(hist_cap = max(log_desal)) %>% ungroup() %>%
    select(iso3,hist_cap) %>% distinct()

  future_proj = data.df %>% left_join(gdp_proj) %>% left_join(gov_proj) %>%
    left_join(hist_cum_cap) %>% drop_na()

  if (rcp == 'no_climate'){
    future_proj = future_proj %>% group_by(iso3,x,y) %>%
    mutate(wsi = first(wsi)) %>% ungroup()
    }

  future_proj = future_proj %>% mutate(log_gdp = log(gdp),
                                       log_wsi = log(wsi)) %>%
    left_join(fe3 %>% ungroup() %>%
                select(iso3,year,fe_adj)) %>%
    mutate(log_desal3 = log_gdp*(c1l3) + gov*(c3l3) + log_wsi*(c4l3) + fe_adj) %>%
    filter(!is.infinite(log_wsi)) %>% drop_na(fe_adj)

  #scale based on last historical year
  future_plot = future_proj %>% group_by(iso3,year) %>%
    mutate(desal_cap = exp(log_desal3) ) %>% 
    summarise(desal_cap = mean(desal_cap)) %>%
    # adjust to hit_Cap
    group_by(iso3) %>% mutate(min_cap = min(desal_cap)) %>%
    left_join(hist_cum_cap) %>%
    # move to absolute values
    mutate(hist_cap = exp(hist_cap) ) %>% 
    # diff_hist is in ABSOLUTE value
    mutate(diff_hist =  hist_cap - min_cap,
           desal_cap = desal_cap + diff_hist) %>%
    ungroup()

  #apply diff_hist to all countries in the point database
  diff_hist = future_plot %>% select(iso3,diff_hist) %>% distinct()
  ## check if initial projected values is higher/equal to historical data

  future_proj_out= future_proj %>% left_join(diff_hist) %>%
    mutate(desal_cap = exp(log_desal3),
           desal_cap = desal_cap + diff_hist)

  ggplot( )+
    # geom_line(data = projected %>% filter( iso3 %in% c('ARE','USA','ESP','TKM','SAU','KWT','QAT')),aes(x = year,y = log_desal3 ), color = 'grey',size = 2)+
    geom_line(data = master %>% filter( iso3 %in% c('ARE','USA','ESP','TKM','SAU','KWT','QAT')),aes(x = year,y = exp(log_desal) ), color = 'black')+
    # geom_line(data = gdp_proj %>% filter( iso3 %in% c('ARE','USA','ESP','TKM','SAU','KWT','QAT')),aes(x = year,y = log(gdp) ), color = 'green')+
    geom_line(data = future_plot %>% filter( iso3 %in% c('ARE','USA','ESP','TKM','SAU','KWT','QAT')),aes(x = year,y = desal_cap ), color = 'red')+
    facet_wrap(~iso3)+theme_bw()+ggtitle('SECOND case, abs val')

  ggplot ()+
    # geom_line(data = projected %>% filter( iso3 %in%  quantiles[quantiles$quant == 'q1_3',]$iso3 ,
    #                                        !iso3 %in% c('USA','ARE','SAU','ESP','KWT','QAT','TKM') ),
    #           aes(x = year,y = exp(log_desal3) ), color = 'grey', size = 2)+
    geom_line(data = master %>% filter( iso3 %in%  quantiles[quantiles$quant == 'q1_3',]$iso3 ,
                                           !iso3 %in% c('USA','ARE','SAU','ESP','KWT','QAT','TKM') ),
              aes(x = year,y = exp(log_desal) ), color = 'black')+
    geom_line(data = future_plot %>% filter( iso3 %in%  quantiles[quantiles$quant == 'q1_3',]$iso3 ,
                                           !iso3 %in% c('USA','ARE','SAU','ESP','KWT','QAT','TKM') ),
              aes(x = year,y = desal_cap ), color = 'red')+
    facet_wrap(~iso3)+theme_bw()

  # TKM weird GDP trend
  # basin average without trimming out location far from coasts
  fut_proj_basin = future_proj_out %>% group_by(BCU_name, year) %>%
    summarise(cap_m3_day = mean(desal_cap)) %>% ungroup() %>%
    mutate(cap_km3_year = cap_m3_day *1e-9 * 365)

  assign(paste0('basin_desal_',rcp),fut_proj_basin)
}

# compare
comp = basin_desal_2p6 %>% mutate(rcp = '2p6') %>% bind_rows(
  basin_desal_6p0 %>% mutate(rcp = '6p0'),
  basin_desal_no_climate %>% mutate(rcp = 'no_climate')
)
# global capacity
comp_glob = comp %>% group_by(rcp,year) %>%
  summarise(cap_km3_year = sum(cap_km3_year)) %>% ungroup()

ggplot(comp_glob)+
  geom_line(aes(x = year,y = cap_km3_year, color = rcp),size = 1)+
  theme_bw()+
  ggtitle("Desalination potential projections, global, SSP2")+
  ylab("km3/year")+
  theme(axis.title.x = element_blank(),
        legend.position = 'bottom')

# Main basins
ggplot(comp %>% filter(BCU_name %in% c("120|MEA","66|SAS","33|NAM","4|WEU","158|AFR","7|AFR")) )+
  geom_line(aes(x = year,y = cap_km3_year, color = rcp),
            size = 1)+
  facet_wrap(~BCU_name)+theme_bw()+
  ggtitle("Desalination potential projections, basins, SSP2")+
  ylab("km3/year")+
  theme(axis.title.x = element_blank(),
        legend.position = 'bottom')

# set negative values to 0
comp = comp %>% 
  mutate(cap_km3_year = if_else(cap_km3_year < 0, 0, cap_km3_year))

# output
write.csv(comp %>% select(BCU_name,rcp,year,cap_km3_year),
          paste0('P:/ene.model/NEST/desalination/projected_desalination_potential_km3_year_',reg,'.csv'),
          row.names = F)


#### Historical capacity by basin ####
# to set existing capacity in historical time periods
hist_cap.df = read.csv('P:/ene.model/NEST/desalination/DESALCAPACITYDATA2016_current_online.csv') %>%
  mutate(tec_type = if_else(technology %in% c( "MSF (Multi-stage Flash)","MED (Multi-effect Distillation)",
                                               "MD (Membrane Distillation)","VC (Vapour Compression)","Other / Unknown"),
                            'distillation','membrane')) %>%
  rename(year = online) %>%
  group_by(lat,lon,year,tec_type) %>%
  summarise(cap_km3_year = sum(m3_per_day) * 1e-9*365) %>% ungroup() %>%
  drop_na()

map_hist_years = rbind(data.frame(my = seq(1995,2015,5)) %>% mutate(sy = my -2),
                  data.frame(my = seq(1995,2015,5)) %>% mutate(sy = my -1),
                  data.frame(my = seq(1995,2015,5)) %>% mutate(sy = my),
                  data.frame(my = seq(1995,2015,5)) %>% mutate(sy = my +1),
                  data.frame(my = seq(1995,2015,5)) %>% mutate(sy = my +2)) %>%
  arrange(my)

hist_cap.df = hist_cap.df %>% left_join(map_hist_years %>% rename(year = sy)) %>%
  mutate(my = if_else(is.na(my) & year < 1995, 1995,
                      if_else(is.na(my) & year > 2015,2015,my))) %>%
  filter(!year > 2020, !year < 1990) %>%
  group_by(lat,lon,my,tec_type) %>%
  summarise(cap_km3_year = sum(cap_km3_year) ) %>% ungroup() %>%
  rename(year = my)

hist_cap.spdf = hist_cap.df
coordinates(hist_cap.spdf) = ~ lon + lat
proj4string( hist_cap.spdf) = crs(basin_by_region.spdf)
basin_info_desal = over(hist_cap.spdf,basin_by_region.spdf)

hist_cap.out = hist_cap.df %>% bind_cols(basin_info_desal) %>%
  group_by(BCU_name,year,tec_type) %>%
  summarise(cap_km3_year = sum(cap_km3_year)) %>% ungroup() %>%
  #there are soem NA
  drop_na()

#technical lifetime of desal tech = 30 y

# compare historical with first year of projections
hist_comp = hist_cap.out %>% ungroup() %>% group_by(BCU_name, year) %>% 
  summarise(hist_cap = sum(cap_km3_year)) %>% 
  summarise(year = year,
            hist_cap = hist_cap,
            hist_cap_cum = cumsum(hist_cap)) %>% ungroup() %>% 
  left_join(comp %>% select(BCU_name,rcp,year,cap_km3_year) %>% 
  filter(year == 2020) %>% select(-year) ) %>% 
  # any cases of hist 2015 higher than 2020
  mutate(diff2020 = cap_km3_year - hist_cap_cum)

# for cases of mismatch, cum 2015 and projections
high2020 = hist_comp %>% filter(year == 2015, diff2020 < 0) %>% 
  mutate(Xhist = hist_cap_cum/ cap_km3_year,
         fianl_hist_cum = hist_cap_cum / (Xhist * 1.05)) %>% 
  filter(rcp == 'no_climate') %>% 
  select(BCU_name, Xhist)

# adjust hist cap
hist_cap.out = hist_cap.out %>% left_join(high2020) %>% 
  mutate(cap_km3_year = if_else(!is.na(Xhist),cap_km3_year / (Xhist * 1.05),cap_km3_year )) %>% 
  select(-Xhist)

write.csv(hist_cap.out,
          paste0('P:/ene.model/NEST/desalination/historical_capacity_desalination_km3_year_',reg,'.csv'),
          row.names = F)

# for countries with no desalination"))
hist_cap.out2 = data.frame(BCU_name = basin_by_region.spdf$BCU_name) %>% 
  crossing(year = c(2010,2015)) %>% 
  crossing(tec_type = c("membrane","distillation")) %>% 
  mutate(cap_km3_year = 0)

write.csv(hist_cap.out,
          paste0('P:/ene.model/NEST/desalination/historical_capacity_desalination_km3_year_',reg,'.csv'),
          row.names = F)

# projection
comp2 = data.frame(BCU_name = basin_by_region.spdf$BCU_name) %>% 
  crossing(rcp = RCPs) %>% 
  crossing(year = seq(2020, 2090, 5)) %>% 
  mutate(cap_km3_year = 0.001)

write.csv(comp2 %>% select(BCU_name,rcp,year,cap_km3_year),
          paste0('P:/ene.model/NEST/desalination/projected_desalination_potential_km3_year_',reg,'.csv'),
          row.names = F)
