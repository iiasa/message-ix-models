library(tidyverse)
library(readxl)
library(sitools)

# Data file names and path
# datapath = '../../../../data/material/'
datapath = 'H:/MyDocuments/MESSAGE/message_data/data/material/'

file_cement = "CEMENT.BvR2010.xlsx"
file_steel = "STEEL_database_2012.xlsx"
file_gdp = "iamc_db ENGAGE baseline GDP PPP.xlsx"
# file_pop = "iamc_db ENGAGE baseline population.xlsx"
# 
gdp.ppp = read_excel(paste0(datapath, file_gdp), sheet="data_R12") %>% filter(Scenario == "baseline") %>%
  select(region=Region, `2020`:`2100`) %>%
  pivot_longer(cols=`2020`:`2100`, names_to="year", values_to="gdp.ppp") %>% # billion US$2010/yr OR local currency
  filter(region != "World") %>%
  mutate(year = as.integer(year),
         region = paste0('R12_', region))
  
# popul = read_excel(paste0(datapath, file_pop), sheet="data_R12") %>%
#   select(Region, `2020`:`2100`) %>%
#   pivot_longer(cols=`2020`:`2100`, names_to="year", values_to="pop.mil")
# df_input = gdp.ppp %>% left_join(popul, by=c("Region", "year")) %>%
#   filter(Region != "World") %>%
#   mutate(gdp.pcap = gdp.ppp*giga/pop.mil/mega,   # USD/cap
#          year = as.integer(year),
#          Region = paste0('R12_', Region)) 
  
derive_steel_demand <- function(df_pop, df_demand) {
  # df_in will have columns:
  # region
  # year
  # gdp.pcap
  # population
  
  df_raw_steel_consumption = read_excel(paste0(datapath, file_steel), 
                                        sheet="Consumption regions", n_max=27) %>%  # kt
    select(-2) %>% 
    pivot_longer(cols="1970":"2012", 
                 values_to='consumption', names_to='year') 
  df_population = read_excel(paste0(datapath, file_cement), 
                             sheet="Timer_POP", skip=3, n_max=27) %>%  # million
    pivot_longer(cols="1970":"2100", values_to='pop', names_to='year')
  df_gdp = read_excel(paste0(datapath, file_cement), 
                      sheet="Timer_GDPCAP", skip=3, n_max=27) %>%  # million
    pivot_longer(cols="1970":"2100", values_to='gdp.pcap', names_to='year')
  
  #### Organize data ####
  names(df_raw_steel_consumption)[1] =
    names(df_population)[1] = names(df_gdp)[1] = "reg_no"
  names(df_raw_steel_consumption)[2] = 
    names(df_population)[2] = names(df_gdp)[2] = "region"
  df_steel_consumption = df_raw_steel_consumption %>% 
    left_join(df_population  %>% select(-region)) %>%
    left_join(df_gdp  %>% select(-region)) %>%
    mutate(cons.pcap = consumption/pop, del.t = as.numeric(year)-2010) %>% #kg/cap
    drop_na() %>%
    filter(cons.pcap > 0)
  
  nlnit.s = nls(cons.pcap ~ a * exp(b/gdp.pcap) * (1-m)^del.t, data=df_steel_consumption, start=list(a=600, b=-10000, m=0))
  
  df_in = df_pop %>% left_join(df_demand %>% select(-year)) %>% # df_demand is only for 2020
    inner_join(gdp.ppp) %>% mutate(del.t = year - 2010, gdp.pcap = gdp.ppp*giga/pop.mil/mega)
  demand = df_in %>% 
    mutate(demand.pcap0 = predict(nlnit.s, .)) %>% #kg/cap
    group_by(region) %>%
    mutate(demand.pcap.base = first(demand.tot.base*giga/pop.mil/mega)) %>%
    mutate(gap.base = first(demand.pcap.base - demand.pcap0)) %>%
    mutate(demand.pcap = demand.pcap0 + gap.base * (1-exp(-9*exp(-0.1*(year - 2010))))) %>% # Bas' equation
    mutate(demand.tot = demand.pcap * pop.mil * mega / giga) # Mt
  
  return(demand %>% select(region, year, demand.tot)) # Mt
} 



derive_cement_demand <- function(df_pop, df_demand) {
  # df_in will have columns:
  # region
  # year
  # gdp.pcap
  # population (in mil.)
  
  df_raw_cement_consumption = read_excel(paste0(datapath, file_cement), 
                                         sheet="Regions", skip=122, n_max=27) %>%  # kt
    pivot_longer(cols="1970":"2010", values_to='consumption', names_to='year')
  df_population = read_excel(paste0(datapath, file_cement), 
                             sheet="Timer_POP", skip=3, n_max=27) %>%  # million
    pivot_longer(cols="1970":"2100", values_to='pop', names_to='year')
  df_gdp = read_excel(paste0(datapath, file_cement), 
                      sheet="Timer_GDPCAP", skip=3, n_max=27) %>%  # million
    pivot_longer(cols="1970":"2100", values_to='gdp.pcap', names_to='year')
  
  #### Organize data ####
  names(df_raw_cement_consumption)[1] = 
    names(df_population)[1] = names(df_gdp)[1] = "reg_no"
  names(df_raw_cement_consumption)[2] = 
    names(df_population)[2] = names(df_gdp)[2] = "region"
  df_cement_consumption = df_raw_cement_consumption %>% 
    left_join(df_population  %>% select(-region)) %>%
    left_join(df_gdp  %>% select(-region)) %>%
    mutate(cons.pcap = consumption/pop/1e6, del.t= as.numeric(year) - 2010) %>% #kg/cap
    drop_na() %>%
    filter(cons.pcap > 0)
  
  nlni.c = nls(cons.pcap ~ a * exp(b/gdp.pcap), data=df_cement_consumption, start=list(a=500, b=-3000))
  
  df_in = df_pop %>% left_join(df_demand %>% select(-year)) %>% # df_demand is only for 2020
    inner_join(gdp.ppp) %>% mutate(gdp.pcap = gdp.ppp*giga/pop.mil/mega)
  demand = df_in %>% 
    mutate(demand.pcap0 = predict(nlni.c, .)) %>% #kg/cap
    group_by(region) %>%
    mutate(demand.pcap.base = first(demand.tot.base*giga/pop.mil/mega)) %>%
    mutate(gap.base = first(demand.pcap.base - demand.pcap0)) %>%
    mutate(demand.pcap = demand.pcap0 + gap.base * (1-exp(-10*exp(-0.1*(year - 2010))))) %>% # Bas' equation
    mutate(demand.tot = demand.pcap * pop.mil * mega / giga) # Mt
  
  return(demand %>% select(region, year, demand.tot)) # Mt
} 



#### test
# year = seq(2020, 2100, 10)
# df = data.frame(region = "Korea", gdp.pcap = seq(3000, 50000, length.out = length(year)), year, 
#                 population = seq(300, 500, length.out = length(year))) 
# a = derive_cement_demand(df) 
# b = derive_steel_demand(df)
