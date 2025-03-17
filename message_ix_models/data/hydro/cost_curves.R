rm(list = ls())
setwd("H:/MyDocuments/MESSAGE/Hydro-multi")
library(readxl)
library(tidyverse)
library(rpart)
library(zoo)
library(xlsx)
library(countrycode)
library(devtools)
source_url("https://raw.githubusercontent.com/ggrothendieck/gsubfn/master/R/list.R")


source("functions_Fit.R")

### Run environment set up
res <- "R12" #"R11" #  "Nat" ##
# nregion = length(region)

# Decide # of Steps
n.step <- 8 # 5 # 14 # 11 # 
n.substep <- 4


### max_pot: all regions maximum potential in GWa
### data.combined: Intermediate tibble having all cost & LF information
list[data.combined, cost_curves.sprd, max_pot, reg.names]  <- ReadIMAGEData(res)

focus.res <- reg.names # c("EEU","FSU") # "RUS" 

### final_cost_df0: yval (inv_cost), xval (relative potential, 0 to 1), msg_reg (region name)
final_cost_df0 <- GenerateInvCostCurve(focus.res, n.step, cost_curves.sprd)
list[sub_LF.all, org_LF.all] <- GenerateCapacityFactorCurve(res, focus.res, n.substep, data.combined, max_pot) # lf.ordered for plotting
# org_LF.all <- org_LF.all %>% slice(1) %>% mutate(xval=0, x=0) %>% rbind(org_LF.all) %>% arrange(msg_reg, xval)

### final dfs for plotting and exporting ###
# a = data.frame(msg_reg = focus.res, xval = 0)
a = expand.grid(code = unique(data.combined$code), msg_reg = focus.res) %>% mutate(xval = 0)
# max_pot.t = as.data.frame(t(max_pot %>% select(!!focus.res))) %>% rownames_to_column(var="msg_reg") #%>% as.numeric()

final_cost_df_pl <- full_join(a, final_cost_df0) %>% 
  group_by(code, msg_reg) %>% 
  filter(msg_reg %in% focus.res) %>%
  left_join(max_pot) %>%
  mutate(yval = na.locf(yval, fromLast = T), 
         xval = xval*pot_agg) %>%
  rename(avgIC = yval) %>% select(-pot_agg) %>% arrange(code, msg_reg, xval) %>%
  mutate(xval = round(xval, 4)) # Decimal numbers need to match for a correct join below.

final_LF_df_pl <- sub_LF.all %>%
  mutate(xval = round(xval, 4)) %>%
  group_by(code, msg_reg) %>%
  left_join(final_cost_df_pl %>% select(code, msg_reg, xval) %>% 
              mutate(x.CC=1)) %>%  # have x.CC to mark the step positions
  mutate(commodity = lag(cumsum(!is.na(x.CC)))) %>% # x=0 is not important.
  # mutate(commodity=ifelse(is.na(commodity), 1, commodity)) %>%
  mutate(commodity = paste0("hydro_c", commodity)) %>%
  mutate(x.interval=xval-lag(xval)) %>% select(-x.CC)
  
# mutate(xval = xval*(max_pot %>% select(!!focus.res) %>% as.numeric())) %>% # Already taken care of
# mutate(commodity=cut(xval, xval[x.CC==1], labels=paste0("hydro_c", 1:nstep))) %>%     # To mark the technology levels to LF curve
# mutate(x.interval=xval-lag(xval))






### PLOT ORIGINAL AND FITTED CURVES ###


# Making it to MESSAGE format (JM) w/ incremental intervals
# final_LF_df_pl <- final_LF_df_pl %>% mutate(xval=xval-lag(xval)) %>% slice(-1) %>% 
#   mutate(xval=xval*max_pot %>% select(RUS) %>% as.numeric()) #%>% rename(potential=xval)
# final_cost_df_pl <- final_cost_df_pl %>% mutate(xval=xval-lag(xval)) %>% slice(-1) %>% 
#   mutate(xval=xval*max_pot %>% select(RUS) %>% as.numeric()) #%>% rename(potential=xval)

# save(final_LF_df_pl, file=paste0("LoadFac_", focus.res, "_", nstep, "main_", n.substep, "subs.rda"))
# save(final_cost_df_pl, file=paste0("CapCost_", focus.res, "_", nstep, "main_", n.substep, "subs.rda"))
  
# export_reg = focus.res[4] # FSU

for (rg in focus.res) {
  write.csv(final_LF_df_pl %>% filter(msg_reg==rg), file=paste0("LoadFac_", rg, "_", n.step, "main_", n.substep, "subs_update.csv"), row.names = FALSE)
  write.csv(final_cost_df_pl %>% filter(msg_reg==rg), file=paste0("CapCost_", rg, "_", n.step, "main_", n.substep, "subs_update.csv"), row.names = FALSE)
}


plot_reg = focus.res[4]
plot_code = unique(data.combined$code)[4]
cap_cost <- data.combined %>% select(-LF_agg) %>% filter(code==plot_code, msg_reg==plot_reg) %>%
  left_join(max_pot) %>% # Original cost curve
  mutate(xval=x * pot_agg)

# inv_cost
ggplot()+
  geom_step(data=final_cost_df_pl %>% filter(code==plot_code, msg_reg %in% plot_reg),aes(x=xval,y=avgIC,color=msg_reg), size=1, direction = 'vh')+
  geom_line(data=cap_cost, aes(x=xval,y=cost_agg,color=msg_reg))

# cap_fac vs. inv_cost
ggplot()+
  geom_step(data=final_LF_df_pl %>% filter(code==plot_code, msg_reg==plot_reg), 
            aes(x=xval, y=avgLF, color=msg_reg), size=1, direction = 'vh')+
  # geom_line(data=load_fact[load_fact$msg_reg %in% plot_reg,],aes(x=x,y=LF_agg,color=msg_reg))+
  geom_line(data=org_LF.all %>% filter(code==plot_code, msg_reg==plot_reg), 
            aes(x=x, y=LF_agg, color=msg_reg))+
  geom_step(data=final_cost_df_pl %>% filter(code==plot_code, msg_reg==plot_reg),
            aes(x=xval,y=avgIC/20000,color=msg_reg),size=1,linetype = "dotted",direction = 'vh') # Arbitrary scaling factor: 20000



