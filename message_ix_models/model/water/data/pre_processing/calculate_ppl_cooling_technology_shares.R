rm(list = ls())
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
# library(reticulate)

# Increase memory size
memory.limit(size = 1e6)

# this path need tobe automatize, maybe loading with retisulate a python ojects that contains the rith path.
# otherwise environment path an be used
msg_data <- Sys.getenv("MESSAGE_DATA_PATH")
# message-ix-models path
msg_ix_model <- Sys.getenv("MESSAGE-IX-MODELS")
data_subf <- path.expand(paste0(msg_data, "\\data\\water\\ppl_cooling_tech"))

all_units <- read_excel("P:/ene.model/NEST/ppl_cooling_tech/PLATTS_3.7.xlsx")

all_units.df <- all_units %>% select(UNIT, STATUS, ISO, msgregion, msg_combo2, cool_group_msg, MW_x, WW, WC, lat, long)
# here we need to adapt for a different regional conigurations of the GLOBAL model

for (reg in c("R11", "R12")) {
  # for global R11 model
  cooling_plants <- all_units.df %>%
    filter(
      STATUS == "OPR",
      !cool_group_msg %in% c("CHP", "NCN"),
      !is.na(cool_group_msg),
      !is.na(msgregion)
    ) %>%
    rename(utype = msg_combo2, cooling = cool_group_msg) %>%
    ungroup()

  # get mapping from yaml file in message-ix-models
  file <- paste0(msg_ix_model, "/message_ix_models/data/node/", reg, ".yaml")
  from_yaml <- read_yaml(file, fileEncoding = "UTF-8")
  names_loop <- names(from_yaml)[names(from_yaml) != "World"]
  reg_map.df <- data.frame()
  for (rr in names_loop) {
    reg_map.df <- rbind(reg_map.df, data.frame(msgregion = rr, ISO = from_yaml[[rr]]$child))
  }

  # add R## to the msgregions, to go the in any column
  cooling_plants <- cooling_plants %>%
    select(-msgregion) %>%
    left_join(reg_map.df)

  # add the data to the cooltech_cost_and_shares_ssp_msg.csv file
  cooltech_cost_shares <- read.csv(paste0(data_subf, "/cooltech_cost_and_shares_ssp_msg.csv"), stringsAsFactors = FALSE)

  #### shares by message REGION #### needed to establish initial mapping
  shars_cooling_MSG_global <- cooling_plants %>%
    group_by(utype, cooling, msgregion) %>% # change to COUNTRY
    summarise(MW_x = sum(MW_x)) %>%
    ungroup() %>%
    group_by(utype, msgregion) %>% # change to COUNTRY
    mutate(cap_reg_unit = sum(MW_x)) %>%
    ungroup() %>%
    mutate(shares = MW_x / cap_reg_unit) %>%
    select(utype, cooling, msgregion, shares) %>%
    spread(msgregion, shares)

  shars_cooling_MSG_global[is.na(shars_cooling_MSG_global)] <- 0

  # mapping in order to add missing technologies
  platts_types <- shars_cooling_MSG_global %>%
    select(utype) %>%
    rename(utype_pl = utype) %>%
    mutate(match = gsub("_.*", "", utype_pl)) %>%
    group_by(match) %>%
    summarise(utype_pl = first(utype_pl), match = first(match))

  map_all_types <- cooltech_cost_shares %>%
    select(utype, cooling) %>%
    mutate(match = gsub("_.*", "", utype)) %>%
    left_join(platts_types) %>%
    select(-match) %>%
    filter(!is.na(utype_pl)) %>%
    distinct()
  # add csp to the mapping
  cool_tecs <- unique(map_all_types$cooling)
  # start an empty dataframe with the same columns as map_all_types
  csp_map <- data.frame()

  for (csp_tec in c("csp_sm1_res", "csp_sm3_res")) {
    csp_map <- csp_map %>%
      bind_rows(data.frame(
        utype = csp_tec,
        utype_pl = "solar_th_ppl"
      ) %>%
        crossing(cooling = cool_tecs))
    for (res in c(1:7)) {
      csp_map <- csp_map %>%
        bind_rows(data.frame(
          utype = paste0(csp_tec, res),
          utype_pl = "solar_th_ppl"
        ) %>%
          crossing(cooling = cool_tecs))
    }
  }
  csp_map <- csp_map %>% select(utype, cooling, utype_pl)
  map_all_types <- bind_rows(map_all_types, csp_map)

  # change names
  new_names <- paste0("mix_", names(shars_cooling_MSG_global)[!names(shars_cooling_MSG_global) %in% c("utype", "cooling")])
  names(shars_cooling_MSG_global)[!names(shars_cooling_MSG_global) %in% c("utype", "cooling")] <- new_names
  shars_cooling_MSG_global <- shars_cooling_MSG_global %>% mutate(utype_pl = utype)

  # missing shares
  all_shares <- map_all_types %>%
    filter(!utype %in% unique(shars_cooling_MSG_global$utype)) %>%
    left_join(shars_cooling_MSG_global %>% select(-utype), by = c("utype_pl", "cooling")) %>%
    select(-utype_pl) %>%
    bind_rows(shars_cooling_MSG_global)

  cooltech_cost_shares <- cooltech_cost_shares %>%
    select(utype, cooling, investment_million_USD_per_MW_low, investment_million_USD_per_MW_mid, investment_million_USD_per_MW_high) %>%
    left_join(all_shares)

  cooltech_cost_shares[is.na(cooltech_cost_shares)] <- 0
  
  # for each utype, make sure no shares are 0
  cooltech_cost_shares <- cooltech_cost_shares %>%
    gather(msgregion, shares, -c(utype, cooling, 
                                 investment_million_USD_per_MW_low,
                                 investment_million_USD_per_MW_mid,
                                 investment_million_USD_per_MW_high,
                                 utype_pl)) %>%
    group_by(utype, msgregion) %>%
    mutate(max_shares = max(shares)) %>%
    ungroup() %>%
    group_by(utype) %>%
    mutate(main_tec_gbl = cooling[which.max(shares)]) %>%
    ungroup() %>%
    # change values
    mutate(shares = if_else(max_shares == 0 & cooling == main_tec_gbl, 1, shares)) %>%
    select(-c(max_shares, main_tec_gbl)) %>%
    spread(msgregion, shares) %>% 
    select(-utype_pl,everything(), utype_pl)
  
  # write new file
  write.csv(cooltech_cost_shares, paste0(data_subf, "/cooltech_cost_and_shares_ssp_msg_", reg, ".csv"), row.names = FALSE)
}

#### SHARES by COUNTRY ####
# shares by message REGION
shars_cooling_country <- cooling_plants %>%
  group_by(utype, cooling, ISO) %>% # change to COUNTRY
  summarise(MW_x = sum(MW_x)) %>%
  ungroup() %>%
  group_by(utype, ISO) %>% # change to COUNTRY
  mutate(cap_reg_unit = sum(MW_x)) %>%
  ungroup() %>%
  mutate(shares = MW_x / cap_reg_unit) %>%
  select(utype, cooling, ISO, shares) %>%
  spread(ISO, shares)

shars_cooling_country[is.na(shars_cooling_country)] <- 0

# change names
new_names <- paste0("mix_", names(shars_cooling_country)[!names(shars_cooling_country) %in% c("utype", "cooling")])
names(shars_cooling_country)[!names(shars_cooling_country) %in% c("utype", "cooling")] <- new_names
shars_cooling_country <- shars_cooling_country %>% mutate(utype_pl = utype)

# missing shares
all_shares_c <- map_all_types %>%
  filter(!utype %in% unique(shars_cooling_country$utype)) %>%
  left_join(shars_cooling_country %>% select(-utype), by = c("utype_pl", "cooling")) %>%
  select(-utype_pl) %>%
  bind_rows(shars_cooling_country)

cooltech_cost_shares_c <- cooltech_cost_shares %>%
  select(utype, cooling, investment_million_USD_per_MW_low, investment_million_USD_per_MW_mid, investment_million_USD_per_MW_high) %>%
  left_join(all_shares_c)

cooltech_cost_shares_c$utype_pl[is.na(cooltech_cost_shares_c$utype_pl)] <- "0"
cooltech_cost_shares_c[is.na(cooltech_cost_shares_c)] <- 0

cooltech_cost_shares_c <- cooltech_cost_shares_c %>%
  gather(msgregion, shares, -c(utype, cooling,
                               investment_million_USD_per_MW_low,
                               investment_million_USD_per_MW_mid,
                               investment_million_USD_per_MW_high,
                               utype_pl)) %>%
  group_by(utype, msgregion) %>%
  mutate(max_shares = max(shares)) %>%
  ungroup() %>%
  group_by(utype) %>%
  mutate(main_tec_gbl = cooling[which.max(shares)]) %>%
  ungroup() %>%
  # change values
  mutate(shares = if_else(max_shares == 0 & cooling == main_tec_gbl, 1, shares)) %>%
  select(-c(max_shares, main_tec_gbl)) %>%
  spread(msgregion, shares) %>% 
  select(-utype_pl,everything(), utype_pl)

# write new file
write.csv(cooltech_cost_shares_c, paste0(data_subf, "/cooltech_cost_and_shares_country.csv"), row.names = FALSE)
