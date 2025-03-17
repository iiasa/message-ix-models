library(readxl)
library(tidyverse)

p = file.path("p:", "ene.model", "NEST", "energy_potentials_Gernaat", "MESSAGE")

cap_cost0.long <- read_csv(file.path(p, "CAP_COST_hydro_$_kW.csv")) %>% #pivot_wider(names_from = "country") %>%
  rename(x = fract_pot, cost = value, ISO = country) %>%
  unite("code", climate_mod, RCP, period, sep='_') %>%
  arrange(code, ISO, x) %>% select(-source, -tec) %>%
  group_by(code, ISO) %>% slice(-1)
      
max_pot0.long <- read_csv(file.path(p, "MAX_POTENTIAL_hydro_kWh_y.csv")) %>% #pivot_wider(names_from = "country") %>% 
  rename(ISO = country, pot = value) %>%
  filter(source=="Hydro") %>%
  unite("code", climate_mod, RCP, period, sep='_') %>% select(-source, -tec) 
max_pot0 <- max_pot0.long %>% pivot_wider(names_from = "ISO") 

load_fact0.long <- read_csv(file.path(p, "LOAD_FACTOR_hydro.csv")) %>% #pivot_wider(names_from = "country") %>%
  rename(x = fract_pot, lfact = value, ISO = country) %>%
  unite("code", climate_mod, RCP, period, sep='_') %>%
  arrange(code, ISO, x) %>% select(-source, -tec) %>%
  group_by(code, ISO) %>% slice(-1)

# cap_cost0 <- read_csv(file.path(p, "CAP_COST_hydro_$_kW.csv")) %>% pivot_wider(names_from = "country") %>%
#   rename(x = fract_pot) %>%
#   unite("code", climate_mod, RCP, period, sep='_') %>%
#   arrange(code, x) %>% group_by(code)
# 
# load_fact0 <- read_csv(file.path(p, "LOAD_FACTOR_hydro.csv")) %>% pivot_wider(names_from = "country") %>%
#   rename(x = fract_pot) %>%
#   unite("code", climate_mod, RCP, period, sep='_') %>%
#   arrange(code, x) %>% group_by(code)

cap_cost0 <- cap_cost0.long %>% 
  pivot_wider(names_from = "ISO", values_from= "cost") %>% group_by(code)
load_fact0 <- load_fact0.long %>% 
  pivot_wider(names_from = "ISO", values_from= "lfact") %>% group_by(code)

