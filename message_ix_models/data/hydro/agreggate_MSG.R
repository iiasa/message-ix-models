# Aggregate with preferred MESSAGE resolution
library(tidyverse)
library(yaml)

rm( list = ls( ) )

# getwd()
# setwd()
setwd("P:/ene.model/NEST/energy_potentials_Gernaat/MESSAGE")

source( paste(getwd(), 'load_data.R', sep = '/' ) ) 

# message-ix-models path
msg_ix_model = Sys.getenv("MESSAGE-IX-MODELS")

# extract the mapping for R11 and R12 MESSAGE configurations
for (rr in c(11,12)){
  reg = paste0('R',rr)
  file = paste0(msg_ix_model,'/message_ix_models/data/node/',reg,'.yaml')
  from_yaml = read_yaml(file, fileEncoding = "UTF-8" )
  names_loop = names(from_yaml)[names(from_yaml) != 'World']
  reg_map.df = data.frame()
  for (r in names_loop){
    reg_map.df = rbind(reg_map.df,data.frame(region = r, country = from_yaml[[r]]$child))
    assign(paste0('reg_map_R',rr,".df"), reg_map.df, pos = 1)
  }
}

# regional max potential (first for R11)
# simple aggregation: sum of max potential
rr = 11

# unit: kWh/y
reg_max_pot.df = max_pot.df %>% left_join(get(paste0('reg_map_R',rr,".df")) ) %>% 
  group_by(region,RCP,climate_mod,period,tec) %>% 
  summarise(max_pot = sum(value)) %>% ungroup()
                         
reg_max_pot.calc = reg_max_pot.df %>% 
  spread(key = RCP,value = max_pot) %>% 
  mutate(cc_increase = `6p0`-`2p6`,
         pc_inc = cc_increase/`6p0`) %>% 
  filter(abs(pc_inc) < 1)

ggplot(reg_max_pot.calc %>% mutate(region = as.factor(region)),
       aes(x = region,y = pc_inc*100,fill = region,color = period))+
  # geom_point(position = position_dodge(width = 0.5))+
  geom_boxplot(position = position_dodge(width = 0.5))+
  geom_hline(aes(yintercept = 0),color = 'grey60',size = 0.2 )+
  facet_wrap(~tec)+theme_bw()+
  theme(axis.text = element_text(size = 6,angle = 90))+
  xlab('Climate model')+ylab('variation from rcp 2p6 to 6p0 [%]')+
  ggtitle('Maximum potential' )

# cost curves
# unit: $/kWh
reg_cost_curve.df = cost_curve.df %>% left_join(get(paste0('reg_map_R',rr,".df")) ) %>% 
  left_join(max_pot.df %>% rename(max_pot = value)) %>% 
  filter(!is.na(max_pot)) %>% 
  # calculate the regional max pot, to use as scaling factor
  group_by(region,RCP,climate_mod,period,tec,fract_pot) %>% 
  mutate(max_pot_reg = sum(max_pot)) %>% 
  summarise(avg_cost1 = weighted.mean(value,max_pot) ) %>% ungroup() %>% 
  # take frac_pot 50%
  filter(fract_pot == 0.5)

reg_cost_curve.calc = reg_cost_curve.df %>% 
  spread(key = RCP,value = avg_cost1) %>% 
  mutate(cc_increase = `6p0`-`2p6`,
         pc_inc = cc_increase/`6p0`) %>% 
  filter(abs(pc_inc) < 1)

ggplot(reg_cost_curve.calc %>% mutate(region = as.factor(region)),
       aes(x = region,y = pc_inc*100,fill = region,color = period))+
  # geom_point(position = position_dodge(width = 0.5))+
  geom_boxplot(position = position_dodge(width = 0.5))+
  geom_hline(aes(yintercept = 0),color = 'grey60',size = 0.2 )+
  facet_wrap(~tec)+theme_bw()+
  theme(axis.text = element_text(size = 6,angle = 90))+
  xlab('Climate model')+ylab('variation from rcp 2p6 to 6p0 [%]')+
  ggtitle('Average cost increase' )
