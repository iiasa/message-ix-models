library(tidyverse)
library(readxl)

# Data file names and path
datapath = '../../../../data/material/'

file_cement = "CEMENT.BvR2010.xlsx"
file_steel = "STEEL_database_2012.xlsx"
file_al = "demand_aluminum.xlsx"

#### Import raw data (from Bas) - Cement & Steel ####
# Apparent consumption
# GDP per dap
# Population
df_raw_steel_consumption = read_excel(paste0(datapath, file_steel),
                                      sheet="Consumption regions", n_max=27) %>%  # kt
  select(-2) %>%
  pivot_longer(cols="1970":"2012",
               values_to='consumption', names_to='year')
df_raw_cement_consumption = read_excel(paste0(datapath, file_cement),
                                       sheet="Regions", skip=122, n_max=27) %>%  # kt
  pivot_longer(cols="1970":"2010", values_to='consumption', names_to='year')
df_population = read_excel(paste0(datapath, file_cement),
                           sheet="Timer_POP", skip=3, n_max=27) %>%  # million
  pivot_longer(cols="1970":"2100", values_to='pop', names_to='year')
df_gdp = read_excel(paste0(datapath, file_cement),
                    sheet="Timer_GDPCAP", skip=3, n_max=27) %>%  # million
  pivot_longer(cols="1970":"2100", values_to='gdp.pcap', names_to='year')

df_raw_aluminum_consumption = read_excel(paste0(datapath, file_al),
                             sheet="final_table", n_max = 378) # kt

#### Organize data ####
names(df_raw_steel_consumption)[1] = names(df_raw_cement_consumption)[1] =
  names(df_population)[1] = names(df_gdp)[1] = "reg_no"
names(df_raw_steel_consumption)[2] = names(df_raw_cement_consumption)[2] =
  names(df_population)[2] = names(df_gdp)[2] = "region"
df_steel_consumption = df_raw_steel_consumption %>%
  left_join(df_population  %>% select(-region)) %>%
  left_join(df_gdp  %>% select(-region)) %>%
  mutate(cons.pcap = consumption/pop, del.t = as.numeric(year)-2010) %>% #kg/cap
  drop_na() %>%
  filter(cons.pcap > 0)
df_cement_consumption = df_raw_cement_consumption %>%
  left_join(df_population  %>% select(-region)) %>%
  left_join(df_gdp  %>% select(-region)) %>%
  mutate(cons.pcap = consumption/pop/1e6, del.t= as.numeric(year) - 2010) %>% #kg/cap
  drop_na() %>%
  filter(cons.pcap > 0)

df_aluminum_consumption = df_raw_aluminum_consumption %>%
  mutate(cons.pcap = consumption/pop, del.t= as.numeric(year) - 2010) %>% #kg/cap
  drop_na() %>%
  filter(cons.pcap > 0)

#### Fit models ####

# Note: IMAGE adopts NLI for cement, NLIT for steel.

# . Linear ====
lni.c = lm(log(cons.pcap) ~ I(1/gdp.pcap), data=df_cement_consumption)
lni.s = lm(log(cons.pcap) ~ I(1/gdp.pcap), data=df_steel_consumption)
lni.a = lm(log(cons.pcap) ~ I(1/gdp.pcap), data=df_aluminum_consumption)
summary(lni.c)
summary(lni.s)
summary(lni.a)

lnit.c = lm(log(cons.pcap) ~ I(1/gdp.pcap)+del.t, data=df_cement_consumption)
lnit.s = lm(log(cons.pcap) ~ I(1/gdp.pcap)+del.t, data=df_steel_consumption)
lnit.a = lm(log(cons.pcap) ~ I(1/gdp.pcap)+del.t, data=df_aluminum_consumption)
summary(lnit.c)
summary(lnit.s)
summary(lnit.a) # better in linear

# . Non-linear ====
nlni.c = nls(cons.pcap ~ a * exp(b/gdp.pcap), data=df_cement_consumption, start=list(a=500, b=-3000))
nlni.s = nls(cons.pcap ~ a * exp(b/gdp.pcap), data=df_steel_consumption, start=list(a=600, b=-10000))
nlni.a = nls(cons.pcap ~ a * exp(b/gdp.pcap), data=df_aluminum_consumption, start=list(a=600, b=-10000))

summary(nlni.c)
summary(nlni.s)
summary(nlni.a)

nlnit.c = nls(cons.pcap ~ a * exp(b/gdp.pcap) * (1-m)^del.t, data=df_cement_consumption, start=list(a=500, b=-3000, m=0))
nlnit.s = nls(cons.pcap ~ a * exp(b/gdp.pcap) * (1-m)^del.t, data=df_steel_consumption, start=list(a=600, b=-10000, m=0))
nlnit.a = nls(cons.pcap ~ a * exp(b/gdp.pcap) * (1-m)^del.t, data=df_aluminum_consumption, start=list(a=600, b=-10000, m=0))

summary(nlnit.c)
summary(nlnit.s)
summary(nlnit.a)


#### Prediction ####

year = seq(2020, 2100, 10)
df = data.frame(gdp.pcap = seq(3000, 70000, length.out = length(year)), year) %>% mutate(del.t = year - 2010)
df2 = df %>% mutate(gdp.pcap = 2*gdp.pcap)
predict(nlnit.s, df)
predict(nlni.s, df)
exp(predict(lnit.s, df))
exp(predict(lni.s, df))
predict(nlnit.s, df2)

predict(nlni.c, df)
predict(nlni.c, df2)

predict(nlni.a, df)
predict(nlnit.a, df)
exp(predict(lni.a, df))
exp(predict(lnit.a, df))
