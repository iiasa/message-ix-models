# Figures
# This s script produces plots fo governance and water stress data 

# after loading dat.df, water scarcity data and governance

wsi_stat = dat.df %>% select(urban_pop.2010,urban_pop.2020,urban_pop.2030, urban_pop.2040,urban_pop.2050,
                             rural_pop.2020,rural_pop.2030,rural_pop.2040,rural_pop.2050,urban_pop.2060,rural_pop.2060,
                             urban_gdp.2020,  urban_gdp.2030, urban_gdp.2040,
                             urban_gdp.2050,rural_gdp.2020,rural_gdp.2020,rural_gdp.2030,rural_gdp.2030,
                             rural_gdp.2040,rural_gdp.2040,rural_gdp.2050,urban_gdp.2060,rural_gdp.2060,
                             WSI.2020,WSI.2030,WSI.2040,WSI.2050,WSI.2010,WSI.2060,) %>%
  mutate(urban_gdp_pc.2010 = urban_gdp.2020/urban_pop.2010) %>%
  mutate(urban_gdp_pc.2020 = urban_gdp.2020/urban_pop.2020) %>%
  mutate(urban_gdp_pc.2030 = urban_gdp.2020/urban_pop.2030) %>%
  mutate(urban_gdp_pc.2040 = urban_gdp.2020/urban_pop.2040) %>%
  mutate(urban_gdp_pc.2050 = urban_gdp.2020/urban_pop.2050) %>%
  mutate(urban_gdp_pc.2060 = urban_gdp.2060/urban_pop.2060) %>%
  mutate(rural_gdp_pc.2010 = rural_gdp.2020/rural_pop.2010) %>%
  mutate(rural_gdp_pc.2020 = rural_gdp.2020/rural_pop.2020) %>%
  mutate(rural_gdp_pc.2030 = rural_gdp.2020/rural_pop.2030) %>%
  mutate(rural_gdp_pc.2040 = rural_gdp.2020/rural_pop.2040) %>%
  mutate(rural_gdp_pc.2050 = rural_gdp.2020/rural_pop.2050) %>%
  mutate(rural_gdp_pc.2060 = rural_gdp.2060/rural_pop.2060) %>%
  select(-urban_gdp.2020,-rural_gdp.2020,-urban_gdp.2060,-rural_gdp.2060)

wsi_stat[is.na(wsi_stat)] = 0

# N of people in water stress areas in 2020
nppl = wsi_stat %>% filter(WSI.2020 %in% c('High Stress','Extreme Stress')) %>%
  #million people
  summarise(urban_pop.2020 = sum(urban_pop.2020)*1e-6, rural_pop.2020 = sum(rural_pop.2020)*1e-6) %>%
  mutate(year = 2020) %>%
  gather(key = 'urb_rur', value = 'value',1:2) %>% bind_rows(
    wsi_stat %>% filter(WSI.2060 %in% c('High Stress','Extreme Stress')) %>%
      #million people
      summarise(urban_pop.2060 = sum(urban_pop.2060)*1e-6, rural_pop.2060 = sum(rural_pop.2060)*1e-6) %>%
      mutate(year = 2060) %>%
      gather(key = 'urb_rur', value = 'value',1:2)
  ) %>%
  mutate(urb_rur = gsub('\\..*','',urb_rur) )

# GDP per capita of people highly exposed
gdppc =  wsi_stat %>% filter(WSI.2020 %in% c('High Stress','Extreme Stress')) %>%
  #million people
  summarise(urban_gdp_pc.2020 = mean(urban_pop.2020), rural_gdp_pc.2020 = mean(rural_gdp_pc.2020)) %>%
  mutate(year = 2020) %>%
  gather(key = 'urb_rur', value = 'value',1:2) %>% bind_rows(
    wsi_stat %>% filter(WSI.2060 %in% c('High Stress','Extreme Stress')) %>%
      #million people
      summarise(urban_gdp_pc.2060 = mean(urban_gdp_pc.2060), rural_gdp_pc.2060 = mean(rural_gdp_pc.2060)) %>%
      mutate(year = 2060) %>%
      gather(key = 'urb_rur', value = 'value',1:2)
  ) %>%
  mutate(urb_rur = gsub('\\..*','',urb_rur) )

library(ggplot2)
ggplot(nppl)+
  geom_bar(aes(x = year, y = value, fill = urb_rur),stat = "identity", position = "stack")+
  theme_bw()+ggtitle('Population with high/extreme water stress')+
  xlab('Million people')

ggplot(gdppc)+
  geom_bar(aes(x = year, y = value, fill = urb_rur),stat = "identity", position = "dodge")+
  theme_bw()+ggtitle('GDP per capita of people with high/extreme water stress')+
  xlab('Thousands $')


# MAps  of global population-WSI-governance-GDP
dat_2050.df = dat.df %>%
  group_by(country) %>%
  summarise(population = sum(rural_pop.2050 + urban_pop.2050, na.rm = T),
            GDP = sum(rural_gdp.2050 + urban_gdp.2050, na.rm = T),
            WSI = mean(WSI.2050, na.rm = T),
            gov = mean(gov.2050, na.rm = T)) %>%
  mutate(GDPpc = GDP/population/1000) # 1000 $ per capita

#### from marina ####
library(ggplot2)
library(countrycode)
library(viridis)
library(ggthemes)
library(maps)
library(rworldmap)
library(mapproj)

# Merge the country data with maps

map <- joinCountryData2Map(dat_2050.df, joinCode = "ISO3", nameJoinColumn = "country")
map_poly <-  fortify(map) %>%
  merge(map@data, by.x="id", by.y="ADMIN", all.x=T) %>%
  arrange(id, order)


# Governance
ggplot(map_poly, aes( x = long, y = lat, group = group )) +
  coord_map(projection = 'mollweide', xlim = c(-180, 180), ylim = c(-60, 75))  + # Remove antarctica
  geom_polygon(aes(fill = gov)) +
  scale_fill_viridis() +
  labs(fill = 'Value'
       ,title = 'Governance in 2050 - SSP2'   # Change the title of the map
       ,x = NULL
       ,y = NULL) +
  theme(text = element_text(family = 'Helvetica', color = 'gray40')
        ,plot.title = element_text(size = 18)
        ,axis.ticks = element_blank()
        ,axis.text = element_blank()
        ,axis.line = element_blank()
        ,panel.grid = element_blank()
        ,panel.background = element_rect(fill = 'white')
        ,plot.background = element_rect(fill = 'white')
        ,legend.position = c(.08,.26)
        ,legend.background = element_blank()
        ,legend.key = element_blank()
  ) +
  annotate(geom = 'text'
           ,label = ''
           ,x = 18, y = -55
           ,size = 3
           ,family = 'Helvetica'
           ,color = 'gray50'
           ,hjust = 'left'
  )

# population
ggplot(map_poly, aes( x = long, y = lat, group = group )) +
  coord_map(projection = 'mollweide', xlim = c(-180, 180), ylim = c(-60, 75))  + # Remove antarctica
  geom_polygon(aes(fill = population/1e6)) +
  scale_fill_viridis() +
  labs(fill = 'Million people'
       ,title = 'Population in 2050 - SSP2'   # Change the title of the map
       ,x = NULL
       ,y = NULL) +
  theme(text = element_text(family = 'Helvetica', color = 'gray40')
        ,plot.title = element_text(size = 18)
        ,axis.ticks = element_blank()
        ,axis.text = element_blank()
        ,axis.line = element_blank()
        ,panel.grid = element_blank()
        ,panel.background = element_rect(fill = 'white')
        ,plot.background = element_rect(fill = 'white')
        ,legend.position = c(.08,.26)
        ,legend.background = element_blank()
        ,legend.key = element_blank()
  ) +
  annotate(geom = 'text'
           ,label = ''
           ,x = 18, y = -55
           ,size = 3
           ,family = 'Helvetica'
           ,color = 'gray50'
           ,hjust = 'left'
  )

# GDP per capita
ggplot(map_poly, aes( x = long, y = lat, group = group )) +
  coord_map(projection = 'mollweide', xlim = c(-180, 180), ylim = c(-60, 75))  + # Remove antarctica
  geom_polygon(aes(fill = GDPpc)) +
  scale_fill_viridis() +
  labs(fill = 'Thousands $ pc'
       ,title = 'GDP per capita in 2050 - SSP2'   # Change the title of the map
       ,x = NULL
       ,y = NULL) +
  theme(text = element_text(family = 'Helvetica', color = 'gray40')
        ,plot.title = element_text(size = 18)
        ,axis.ticks = element_blank()
        ,axis.text = element_blank()
        ,axis.line = element_blank()
        ,panel.grid = element_blank()
        ,panel.background = element_rect(fill = 'white')
        ,plot.background = element_rect(fill = 'white')
        ,legend.position = c(.08,.26)
        ,legend.background = element_blank()
        ,legend.key = element_blank()
  ) +
  annotate(geom = 'text'
           ,label = ''
           ,x = 18, y = -55
           ,size = 3
           ,family = 'Helvetica'
           ,color = 'gray50'
           ,hjust = 'left'
  )

# WSI

ggplot(map_poly, aes( x = long, y = lat, group = group )) +
  coord_map(projection = 'mollweide', xlim = c(-180, 180), ylim = c(-60, 75))  + # Remove antarctica
  geom_polygon(aes(fill = WSI)) +
  scale_fill_viridis() +
  labs(fill = 'Value'
       ,title = 'WSI in 2050 - SSP2'   # Change the title of the map
       ,x = NULL
       ,y = NULL) +
  theme(text = element_text(family = 'Helvetica', color = 'gray40')
        ,plot.title = element_text(size = 18)
        ,axis.ticks = element_blank()
        ,axis.text = element_blank()
        ,axis.line = element_blank()
        ,panel.grid = element_blank()
        ,panel.background = element_rect(fill = 'white')
        ,plot.background = element_rect(fill = 'white')
        ,legend.position = c(.08,.26)
        ,legend.background = element_blank()
        ,legend.key = element_blank()
  ) +
  annotate(geom = 'text'
           ,label = ''
           ,x = 18, y = -55
           ,size = 3
           ,family = 'Helvetica'
           ,color = 'gray50'
           ,hjust = 'left'
  )
