library(readxl)
library(countrycode)
library(tidyverse)

raw.mapping <- read_xlsx('../MESSAGE_region_mapping_R14.xlsx')
raw.trade.FAO <- read.csv('../Comtrade/N fertil trade - FAOSTAT_data_9-25-2019.csv')

trade.FAO <- raw.trade.FAO %>% mutate(ISO = countrycode(Area, 'country.name', 'iso3c')) %>%
  mutate_cond(Area=='Serbia and Montenegro', ISO="SRB") %>% # Will be EEU in the end. So either SRB or MNE
  left_join(raw.mapping) %>%
  mutate(Element=gsub(' Quantity', '', Element))

trade.FAO.R14 <- trade.FAO %>% group_by(msgregion, Element, Year) %>% summarise(Value=sum(Value), Unit=first(Unit)) %>%
  filter(!is.na(msgregion))

trade.FAO.R11 <- trade.FAO %>% mutate_cond(msgregion %in% c('RUS', 'CAS', 'SCS', 'UBM'), msgregion="FSU") %>%
  group_by(msgregion, Element, Year) %>% summarise(Value=sum(Value), Unit=first(Unit)) %>%
  filter(!is.na(msgregion))

write.csv(trade.FAO.R11, '../trade.FAO.R11.csv')
write.csv(trade.FAO.R14, '../trade.FAO.R14.csv')
