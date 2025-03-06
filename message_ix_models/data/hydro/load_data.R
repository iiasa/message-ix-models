# process energy potentials from david Gernaat
# for wind, solar, hydro (no biomass)
options( java.parameters = "-Xmx16g" )
library(tidyverse)

# getwd()
# setwd(getwd())
setwd("P:/ene.model/NEST/energy_potentials_Gernaat/MESSAGE")

# start with solar and wind
sources = c('Solar','Wind')
climate_models = c('GFDL-ESM2M','HadGEM2-ES','IPSL-CM5A-LR','MIROC5')
RCPs = c('2p6','6p0')
time_periods = c('20310101-20701231','20710101-20991231')
wind_tecs = c('onshore','offshore')
solar_rec = c('CSP','PV','PVres')

#for testing
ss='Solar'
cm = 'GFDL-ESM2M'
rcp = '2p6'
tp = '20310101-20701231'
tt = 'CSP'

cost_curve.df = data.frame()
max_pot.df = data.frame()
load_fact.df = data.frame()
for (ss in sources){
  for (cm in climate_models) {
    if (ss=='Wind'){tecs = wind_tecs}
    if (ss=='Solar'){tecs = solar_rec}
    for (tt in tecs) {
      print(paste0('source: ',ss,', model: ',cm,', tec: ', tt) )
      # load historical values
      path_hist = path.expand(paste0(getwd(),'/',ss,'/',ss,'_',cm,'_histor_19710101-20001231') )
      # unit: $/kWh
      cost_curve.hist = read.table(paste0(path_hist,'/CostCurve_',tt,'.dat'),header=TRUE,sep = ';') %>% 
        select(-X)
      names(cost_curve.hist) = gsub('.*\\.','',names(cost_curve.hist))
      #fraction on maximum potential
      names(cost_curve.hist)[1] = 'fract_pot'
      cost_curve.hist = cost_curve.hist %>% 
        gather(key = 'country',value = 'value',2:196 ) %>% 
        mutate(RCP = 'hist',climate_mod = cm, source = ss, tec = tt,  period = 'historical') %>% 
        select(RCP,climate_mod,source,tec,period,fract_pot,country,value)
      
      cost_curve.df = cost_curve.df %>% bind_rows(cost_curve.hist)
      # unit: kWh/y
      max_pot.hist = read.table(paste0(path_hist,'/MaxProd_',tt,'.dat'),header=TRUE,sep = ';') %>% 
        select(-X)
      names(max_pot.hist) = gsub('.*\\.','',names(max_pot.hist))
      
      max_pot.hist = max_pot.hist %>% 
        gather(key = 'country',value = 'value',2:195 ) %>% 
        mutate(RCP = 'hist',climate_mod = cm, source = ss, tec = tt,  period = 'historical') %>% 
        select(RCP,climate_mod,source,tec,period,country,value)
      
      max_pot.df = max_pot.df %>% bind_rows(max_pot.hist)
      print('hstorical')
      # load RCP values for two different time periods
      for (rcp in RCPs) {
        for (tp in time_periods) {
          print(paste0('RCP: ',rcp,', timeperiod: ',tp) )
          path = path.expand(paste0(getwd(),'/',ss,'/',ss,'_',cm,'_rcp',rcp,'_',tp) )
          # unit: $/kWh
          cost_curve.temp = read.table(paste0(path,'/CostCurve_',tt,'.dat'),header=TRUE,sep = ';') %>% 
            select(-X)
          names(cost_curve.temp) = gsub('.*\\.','',names(cost_curve.temp))
          #fraction on maximum potential
          names(cost_curve.temp)[1] = 'fract_pot'
          cost_curve.temp = cost_curve.temp %>% 
            gather(key = 'country',value = 'value',2:196 ) %>% 
            mutate(RCP = rcp,climate_mod = cm, source = ss, tec = tt,  period = tp) %>% 
            select(RCP,climate_mod,source,tec,period,fract_pot,country,value)
          
          cost_curve.df = cost_curve.df %>% bind_rows(cost_curve.temp)
          
          max_pot.temp = read.table(paste0(path,'/MaxProd_',tt,'.dat'),header=TRUE,sep = ';') %>% 
            select(-X)
          names(max_pot.temp) = gsub('.*\\.','',names(max_pot.temp))
          
          max_pot.temp = max_pot.temp %>% 
            gather(key = 'country',value = 'value',2:195 ) %>% 
            mutate(RCP = rcp,climate_mod = cm, source = ss, tec = tt,  period = tp) %>% 
            select(RCP,climate_mod,source,tec,period,country,value)
          
          max_pot.df = max_pot.df %>% bind_rows(max_pot.temp)
          
        }
        
      }
      
    }
    
  }

}
cost_curve.df = cost_curve.df %>% 
  mutate(tec = paste0(source,' - ',tec),
         period = if_else(period == '20310101-20701231','2030-2070',
                          if_else(period == 'historical',period,'2071-2100')) )

max_pot.df = max_pot.df %>% 
  mutate(tec = paste0(source,' - ',tec),
         period = if_else(period == '20310101-20701231','2030-2070',
                          if_else(period == 'historical',period,'2071-2100')) )
  
### HYDROPOWER ####
climate_models = tolower(c('GFDL-ESM2M','HadGEM2-ES','IPSL-CM5A-LR','MIROC5') )
RCPs = c('26','60')
time_periods = c('2031_2070','2071_2099')

#for testing
cm = "gfdl-esm2m"
rcp = '26'
tp = '2031_2070'

cost_curve_h.df = data.frame()
max_pot_h.df = data.frame()
load_fact_h.df = data.frame()

for (cm in climate_models) {
  print(paste0('climate model: ',cm))
  path = path.expand(paste0(getwd(),'/Hydro/raw_input') )
  
  #load only historical
  cost_curve.hist = read.table( file = 
                                  paste0(path,'/CostCurve/CostCurveHYD_lpjml_',cm,
                                         '_ewembi_histor_historsoc_co2_qtot_global_daily_1971_2000_merge_monmean',
                                         '.dat'),
                                header=TRUE, fill =T,sep = ';') %>% 
    select(-X)
  names(cost_curve.hist) = gsub('.*\\.','',names(cost_curve.hist))
  #fraction on maximum potential
  names(cost_curve.hist)[1] = 'fract_pot'
  cost_curve.hist = cost_curve.hist %>% 
    gather(key = 'country',value = 'value',2:196 ) %>% 
    mutate(RCP = 'hist',climate_mod = cm, source = 'Hydro', tec = 'Hydro',  period = 'historical') %>% 
    select(RCP,climate_mod,source,tec,period,fract_pot,country,value)
  
  cost_curve_h.df = cost_curve_h.df %>% bind_rows(cost_curve.hist)
  
  # Maximum potential
  # unit: kWh/y
  max_pot.hist = read.table(
    paste0(path,'/MaxProd/MaxProdHYD_lpjml_',cm,
           '_ewembi_histor_historsoc_co2_qtot_global_daily_1971_2000_merge_monmean',
           '.dat'),
    header=TRUE,fill=T,sep = ';') %>% 
    select(-X)
  names(max_pot.hist) = gsub('.*\\.','',names(max_pot.hist))
  
  max_pot.hist = max_pot.hist %>% 
    gather(key = 'country',value = 'value',2:195 ) %>% 
    mutate(RCP = 'hist',climate_mod = cm, source = 'Hydro', tec = 'Hydro',  period = 'historical') %>% 
    select(RCP,climate_mod,source,tec,period,country,value)
  
  max_pot_h.df = max_pot_h.df %>% bind_rows(max_pot.hist)
  
  #Load factor, monthly, needs to be aggregated
  path_h = path.expand(paste0(path,'/LoadFactor/lpjml_',cm,
           '_ewembi_histor_historsoc_co2_qtot_global_daily_1971_2000') )
  load_fact_all.hist = data.frame()
  for (ii in seq(1,12,1)){
    load_fact_m.hist = read.table(
      paste0(path_h,'/LoadFactorHYDm',ii,'_lpjml_',cm,
             '_ewembi_histor_historsoc_co2_qtot_global_daily_1971_2000',
             '.dat'),
      header=TRUE,fill=T,sep = ';') %>% select(-X) %>% 
      mutate(month = ii)
    names(load_fact_m.hist) = gsub('.*\\.','',names(load_fact_m.hist))
    names(load_fact_m.hist)[1] = 'fract_pot'
    load_fact_all.hist = load_fact_all.hist %>% bind_rows(load_fact_m.hist)
  }
  load_fact_h.hist = load_fact_all.hist %>% select(month,fract_pot, everything()) %>% 
    gather(key = 'country',value = 'value',3:197 ) %>% ungroup() %>% 
    group_by(fract_pot,country) %>% 
    summarise(value = mean(value)) %>% ungroup() %>% 
    mutate(RCP = 'hist',climate_mod = cm, source = 'Hydro', tec = 'Hydro',  period = 'historical') %>% 
    select(RCP,climate_mod,source,tec,period,fract_pot,country,value)
  
  load_fact_h.df = load_fact_h.df %>% bind_rows(load_fact_h.hist)
  
  print('hisorical')
  
  # 3 projections
  for (rcp in RCPs) {
    for (tp in time_periods) {
      print(paste0('RCP: ',rcp,', timeperiod: ', tp))
        # unit: $/kWh
        cost_curve.temp = read.table( file = 
          paste0(path,'/CostCurve/CostCurveHYD_lpjml_',cm,'_ewembi_rcp',rcp,'_rcp',rcp,
                 'soc_co2_qtot_global_daily_',tp,'_merge_monmean','.dat'),
          header=TRUE, fill =T,sep = ';') %>% 
          select(-X)
        names(cost_curve.temp) = gsub('.*\\.','',names(cost_curve.temp))
        #fraction on maximum potential
        names(cost_curve.temp)[1] = 'fract_pot'
        cost_curve.temp = cost_curve.temp %>% 
          gather(key = 'country',value = 'value',2:196 ) %>% 
          mutate(RCP = rcp,climate_mod = cm, source = 'Hydro', tec = 'Hydro',  period = tp) %>% 
          select(RCP,climate_mod,source,tec,period,fract_pot,country,value)
        
        cost_curve_h.df = cost_curve_h.df %>% bind_rows(cost_curve.temp)
        
        # Maximum potential
        # unit: kWh/y
        max_pot.temp = read.table(
          paste0(path,'/MaxProd/MaxProdHYD_lpjml_',cm,'_ewembi_rcp',rcp,'_rcp',rcp,
                 'soc_co2_qtot_global_daily_',tp,'_merge_monmean','.dat'),
          header=TRUE,fill=T,sep = ';') %>% 
          select(-X)
        names(max_pot.temp) = gsub('.*\\.','',names(max_pot.temp))
        
        max_pot.temp = max_pot.temp %>% 
          gather(key = 'country',value = 'value',2:195 ) %>% 
          mutate(RCP = rcp,climate_mod = cm, source = 'Hydro', tec = 'Hydro',  period = tp) %>% 
          select(RCP,climate_mod,source,tec,period,country,value)
        
        max_pot_h.df = max_pot_h.df %>% bind_rows(max_pot.temp)
        
        # load factor
        #Load factor, monthly, needs to be aggregated
        path_h = path.expand(paste0(path,'/LoadFactor/lpjml_',cm,
                                    '_ewembi_rcp',rcp,'_rcp',rcp,'soc_co2_qtot_global_daily_',tp) )
        load_fact_all.temp = data.frame()
        for (ii in seq(1,12,1)){
          load_fact_m.temp = read.table(
            paste0(path_h,'/LoadFactorHYDm',ii,'_lpjml_',cm,
                   '_ewembi_rcp',rcp,'_rcp',rcp,'soc_co2_qtot_global_daily_',tp,
                   '.dat'),
            header=TRUE,fill=T,sep = ';') %>% select(-X) %>% 
            mutate(month = ii)
          names(load_fact_m.temp) = gsub('.*\\.','',names(load_fact_m.temp))
          names(load_fact_m.temp)[1] = 'fract_pot'
          load_fact_all.temp = load_fact_all.temp %>% bind_rows(load_fact_m.temp)
        }
        load_fact_h.temp = load_fact_all.temp %>% select(month,fract_pot, everything()) %>% 
          gather(key = 'country',value = 'value',3:197 ) %>% ungroup() %>% 
          group_by(fract_pot,country) %>% 
          summarise(value = mean(value)) %>% ungroup() %>% 
          mutate(RCP = rcp,climate_mod = cm, source = 'Hydro', tec = 'Hydro',  period = tp) %>% 
          select(RCP,climate_mod,source,tec,period,fract_pot,country,value)
        
        load_fact_h.df = load_fact_h.df %>% bind_rows(load_fact_h.temp)
        
    }
    
  }
  
}
print('Data loaded')

cost_curve_h.df = cost_curve_h.df %>% 
  mutate(period = if_else(period == '2031_2070','2030-2070',
                          if_else(period == 'historical',period,'2071-2100')),
         climate_mod = toupper(climate_mod),
         RCP = if_else(RCP == '26','2p6',if_else(RCP == '60','6p0',RCP)  ),
         climate_mod =if_else(climate_mod == "HADGEM2-ES","HadGEM2-ES" ,climate_mod)) 

max_pot_h.df = max_pot_h.df %>% 
  mutate(period = if_else(period == '2031_2070','2030-2070',
                          if_else(period == 'historical',period,'2071-2100')),
         climate_mod = toupper(climate_mod),
         RCP = if_else(RCP == '26','2p6',if_else(RCP == '60','6p0',RCP)  ),
         climate_mod =if_else(climate_mod == "HADGEM2-ES","HadGEM2-ES" ,climate_mod))

load_fact_h.df = load_fact_h.df %>% 
  mutate(period = if_else(period == '2031_2070','2030-2070',
                          if_else(period == 'historical',period,'2071-2100')),
         climate_mod = toupper(climate_mod),
         RCP = if_else(RCP == '26','2p6',if_else(RCP == '60','6p0',RCP)  ),
         climate_mod =if_else(climate_mod == "HADGEM2-ES","HadGEM2-ES" ,climate_mod)) 

cost_curve.df = cost_curve.df %>% bind_rows(cost_curve_h.df)
max_pot.df = max_pot.df %>% bind_rows(max_pot_h.df)
load_fact.df = load_fact.df %>% bind_rows(load_fact_h.df)

# plot to check outliers
ggplot(cost_curve.df)+
  geom_boxplot(aes(climate_mod, value))

# remove outliers
outl_cc = cost_curve.df %>% filter(value >= 10)
to_repl = cost_curve.df %>% filter(fract_pot == 0.98,
                                   RCP %in% outl_cc$RCP,
                                   climate_mod %in% outl_cc$climate_mod,
                                   period %in% outl_cc$period,
                                   country %in% outl_cc$country) %>% 
  rename(new_val = value) %>% select(-fract_pot)

cost_curve.df = cost_curve.df %>% left_join(to_repl) %>% 
  mutate(value = if_else(value >= 10, new_val, value)) %>% 
  select(-new_val)

#### save in .csv format ####
write.csv(cost_curve.df,file = 'cost_curves_all_techs_scenarios_$_kWh.csv', row.names = F)
write.csv(max_pot.df,file = 'maximum_potential_all_techs_scenarios_kWh_y.csv',row.names = F)
write.csv(load_fact.df,file = 'load_factor_all_techs_scenarios_kWh_y.csv',row.names = F)

#final variables to cross
RCPs = unique(cost_curve.df$RCP)
climate_mods = unique(cost_curve.df$climate_mod)
periods = unique(cost_curve.df$period)

# TEMP take old data from David and use them for all our scenarios
library("readxl")
library('countrycode')
# load factor
load_fact_old.df <- read_excel("Hydro/HYDRO_cost_country_Gernaat et al._2018.xlsx",
                        sheet = 'LOAD_FACTOR') %>% 
  gather(key = 'country',value = 'value',2:196) %>% 
  rename(fract_pot = x)

map_iso3 = data.frame(country = unique(load_fact_old.df$country),
                      iso3 = countrycode(unique(load_fact_old.df$country), 
                                         origin = 'country.name', 
                                         destination = 'iso3c' ))
map_iso3$iso3[map_iso3$country == 'Micronesia']  = 'FSM'
load_fact_old.df = load_fact_old.df %>% 
  left_join(map_iso3) %>% 
  mutate(country = iso3) %>% 
  select(fract_pot, country, value) %>% 
  rename(load_fact = value)
# capital cost
cap_cost_old.df <- read_excel("Hydro/HYDRO_cost_country_Gernaat et al._2018.xlsx",
                        sheet = 'CAP_COST') %>% 
  gather(key = 'country',value = 'value',2:196) %>% 
  rename(fract_pot = x) %>% 
  left_join(map_iso3) %>% 
  mutate(country = iso3) %>% 
  select(fract_pot, country, value)  %>% rename(cap_cost = value)

# capital cost
cost_curve_old.df <- read_excel("Hydro/HYDRO_cost_country_Gernaat et al._2018.xlsx",
                              sheet = 'LCOE') %>% 
  gather(key = 'country',value = 'value',2:196) %>% 
  rename(fract_pot = x) %>% 
  left_join(map_iso3) %>% 
  mutate(country = iso3) %>% 
  select(fract_pot, country, value) %>% rename(lcoe = value)

# LCOE ~ cap_cost/ load_factor * Z , we find Z and use it to estimate the cap_cost for other scenarios
# Z = LCOE * load_fact / cap_cost
# cap_cost = LCOE * load_factor / Z

Z_calc = cost_curve_old.df %>% left_join(cap_cost_old.df) %>% 
  left_join(load_fact_old.df) %>% 
  mutate(Z_val = lcoe * load_fact / cap_cost) %>% 
  mutate(Z_val = if_else(is.na(Z_val), 0 , Z_val)) %>% 
  select(fract_pot,country,Z_val)

# compile new cap_cost for all scenarios after ensemble

############
# add ensemble mean
cost_curve.df = cost_curve.df %>% 
  bind_rows(cost_curve.df %>% 
              group_by(RCP,source,tec,period,fract_pot,country) %>% 
              summarise(value = mean(value), climate_mod = 'ensemble') %>% ungroup())
max_pot.df = max_pot.df %>% 
  bind_rows(max_pot.df %>% 
              group_by(RCP,source,tec,period,country) %>% 
              summarise(value = mean(value), climate_mod = 'ensemble') %>% ungroup())
load_fact.df = load_fact.df %>% 
  bind_rows(load_fact.df %>% 
              group_by(RCP,source,tec,period,fract_pot,country) %>% 
              summarise(value = mean(value), climate_mod = 'ensemble') %>% ungroup())

cap_cost.df = cost_curve.df %>% rename(lcoe = value) %>% 
  left_join(load_fact.df %>% rename(load_fact = value)) %>% 
  left_join(Z_calc) %>% 
  mutate(value = lcoe * load_fact / Z_val) %>% 
  mutate(value = if_else(is.na(value), 0 , value)) %>% 
  select(-lcoe,-load_fact,-Z_val)

ggplot(cap_cost.df)+
  geom_boxplot(aes(climate_mod, value))

# compare cap_cost of ensemble with old ones
# seems ok, need to clean up some outliers in some previous data (MICRO5)

metadata = data.frame(var = c('capital cost','load factor','LCOE','Max potential' ),
                      source = c('old','old','new','new'),
                      unit = c('2010 USD/kW','%','2010 USD/kWh','GJ') )

write.csv(cap_cost.df,file = 'Hydro/input_MESSAGE_aggregation/CAP_COST_hydro_$_kW.csv', row.names = F)
write.csv(load_fact.df,file = 'Hydro/input_MESSAGE_aggregation/LOAD_FACTOR_hydro.csv',row.names = F)
write.csv(cost_curve.df,file = 'Hydro/input_MESSAGE_aggregation/LCOE_hydro_$_kWh.csv', row.names = F)
write.csv(max_pot.df,file = 'Hydro/input_MESSAGE_aggregation/MAX_POTENTIAL_hydro_kWh_y.csv',row.names = F)

# Data manipulation and plotting
max_pot.calc = max_pot.df %>% 
  spread(key = RCP,value = value) %>% 
  mutate(cc_increase = `6p0`-`2p6`,
         pc_inc = cc_increase/`6p0`) %>% 
  filter(!is.na(pc_inc),pc_inc != -Inf,
         pc_inc > -2)

max_pot.summ = max_pot.calc %>% 
  group_by(tec, period) %>% 
  summarize(ensemble = mean(pc_inc)) %>% ungroup()

for (tt in unique(max_pot.calc$tec)) {
  pl1 = ggplot()+
    geom_density(data = max_pot.calc %>% filter(tec == tt),
                 aes(x = pc_inc*100,fill = climate_mod),alpha = 0.3)+
    geom_vline(data = max_pot.summ %>% filter(tec == tt),
               (aes(xintercept = ensemble*100, color = period)),size = 0.5)+
    ggtitle(paste0(tt,' max potential') )+
    theme_bw()+
    theme(axis.text = element_text(size = 8))+xlab('variation from rcp 2p6 to 6p0 [%]')
  pdf(file = paste0('Plots/max_potential_CC_impact_',tt,".pdf"), useDingbats=FALSE,
      height = 5,width = 5)
  print(pl1)
  dev.off()
}

pdf(file = paste0("Plots/max_potential_CC_impact_all_tech.pdf"), useDingbats=FALSE,
    height = 8,width = 8)
ggplot(max_pot.calc %>% mutate(climate_mod = as.factor(climate_mod)),
       aes(x = climate_mod,y = pc_inc,fill = climate_mod))+
  geom_violin(trim = TRUE)+
  #geom_boxplot()+
  facet_wrap(~tec)+theme_bw()+
  theme(axis.text = element_text(size = 10))+
  xlab('Climate model')+ylab('variation from rcp 2p6 to 6p0 [%]')+
  ggtitle('Maximum potential' )
dev.off() 

#### similar on cost increases in 2050 ####
cost_curve.calc = cost_curve.df %>% 
  spread(key = RCP,value = value) %>% 
  mutate(cc_increase = `6p0`-`2p6`,
         pc_inc = cc_increase/`6p0`) %>% 
  group_by(climate_mod,tec, period,country) %>% 
  summarise(pc_inc = mean(pc_inc)) %>% ungroup() %>% 
  filter(!is.na(pc_inc),pc_inc != -Inf,
         pc_inc < 1)

# aggregate climate models
cost_curve.summ = cost_curve.calc %>% 
  group_by(tec, period) %>% 
  summarize(ensemble = mean(pc_inc)) %>% ungroup()

pdf(file = paste0("Plots/avg_cost_variations_CC_impact_all_tech.pdf"), useDingbats=FALSE,
    height = 8,width = 8)
ggplot(cost_curve.calc %>% mutate(climate_mod = as.factor(climate_mod)),
       aes(x = climate_mod,y = pc_inc,fill = climate_mod))+
  geom_violin(trim = TRUE)+
  #geom_boxplot()+
  facet_wrap(period~tec)+theme_bw()+
  theme(axis.text = element_text(size = 8))+
  xlab('Climate model')+ylab('variation from rcp 2p6 to 6p0 [%]')+
  ggtitle('Average cost increase' )
dev.off() 

for (tt in unique(cost_curve.calc$tec)) {
  pl2 = ggplot()+
    geom_density(data = cost_curve.calc %>% filter(tec == tt),
                 aes(x = pc_inc*100,fill = climate_mod),alpha = 0.3, trim = T, na.rm = T)+
    geom_vline(data = cost_curve.summ %>% filter(tec == tt),
               (aes(xintercept = ensemble*100, color = period)),size = 0.5)+
    ggtitle(paste0(tt,' Average cost increase') )+
    theme_bw()+
    theme(axis.text = element_text(size = 10))+xlab('variation from rcp 2p6 to 6p0 [%]')
  pdf(file = paste0('Plots/avg_cost_variations_CC_impact_',tt,".pdf"), useDingbats=FALSE,
      height = 4,width = 5)
  print(pl2)
  dev.off()
}
print('Plots generated in /Plot folder')
