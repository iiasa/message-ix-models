################################################################################
# include libraries
################################################################################

options(java.parameters = "-Xmx8g")

library(dplyr)
library(tidyr)
library(readxl)
library(imputeTS) # for time series interpolation of NA

data.path = "C:/Users/krey/Documents/git/message_data_https/data/material"

################################################################################
# read data
################################################################################

setwd(data.path)

# read LCA data from ADVANCE LCA tool
col.types = c(rep("text", 9), rep("numeric", 3))
data.lca = read_xlsx(path = "NTNU_LCA_coefficients.xlsx", sheet = "environmentalImpacts", col_types = col.types)

# technology mapping
technology.mapping = read_xlsx('MESSAGE_global_model_technologies.xlsx', sheet = 'technology')

# region mapping
region.mapping = read_xlsx('LCA_region_mapping.xlsx', sheet = 'region')

# commodity mapping
commodity.mapping = read_xlsx('LCA_commodity_mapping.xlsx', sheet = 'commodity') 

################################################################################
# process data
################################################################################

# filter relevant scenario, technology variant and remove operation phase (and remove duplicates)
data.lca = data.lca %>% filter(scenario == 'Baseline' & `technology variant` == 'mix' & phase != 'Operation')

# add intermediate time steps and turn into long table format
data.lca = data.lca %>% mutate(`2015` = NA, `2020` = NA, `2025` = NA, `2035` = NA, `2040` = NA, `2045` = NA) %>% pivot_longer(cols = c("2010":"2045"), names_to = 'year', values_to = 'value') %>% mutate(value = as.numeric(value))
#data.lca$year = factor(data.lca$year, levels = as.character(seq(2010, 2050, 5)))

# apply technology, commodity/impact and region mappings to MESSAGE
data.lca = data.lca %>% inner_join(region.mapping, by = c("region" = "THEMIS")) %>% inner_join(technology.mapping, by = c("technology" = "LCA mapping")) %>%
  inner_join(commodity.mapping, by = c("impact" = "impact")) %>%
  filter(!is.na(`MESSAGEix-GLOBIOM_1.1`)) %>%
  select(node = `MESSAGEix-GLOBIOM_1.1`, technology = `Type of Technology`, phase, commodity, level, year, unit, value) #%>% unique() 

temp = c()
for (n in unique(data.lca$node)) for (t in unique(data.lca$technology)) for (c in unique(data.lca$commodity)) for (p in unique(data.lca$phase)){
  temp = rbind(temp, data.lca %>% filter(node == n & technology == t & commodity == c & phase == p) %>% na_interpolation())
}
data.lca = temp

################################################################################
# ixmp setup
################################################################################

# existing model and scenario name in ixmp to add material intensities to
modelName <- "Material_Global"
scenarioName <- "NoPolicy"

# new model and scenario name in ixmp
newmodelName <- "Material_Global"
newscenarioName <- "NoPolicy_lca_material"

# comment for commit
comment <- "adding LCA-based material intensity coefficients to electricity generation technologies in MESSAGEix-Materials"

# load required packages 
library(rmessageix)

# specify python binaries and environment under which messageix is installed
use_python("C:/Users/krey/anaconda3/envs/message/")
use_condaenv("message")

# launch the IX modeling platform using the default database
mp <- ixmp$Platform()

################################################################################
# load and clone scenario and extract structural information 
################################################################################

# load existing policy baseline scenario
ixScenarioOriginal = message_ix$Scenario(mp, modelName, scenarioName)

# clone original policy baseline scenario with new scenario name
ixScenario = ixScenarioOriginal$clone(newmodelName, newscenarioName, comment, keep_solution=FALSE)

# checkout scenario
ixScenario$check_out()

# read inv.cost data
inv.cost = ixScenarioOriginal$par('inv_cost')

# read node, technology, commodity and level from existing scenario
node = ixScenarioOriginal$set('node') %>% data.frame()
year = ixScenarioOriginal$set('year') %>% data.frame()
technology = ixScenarioOriginal$set('technology') %>% data.frame()
commodity = ixScenarioOriginal$set('commodity') %>% data.frame()
level = ixScenarioOriginal$set('level') %>% data.frame()

# extract node, technology, commodity, level, and year list from LCA data set
node.list = unique(data.lca$node)
year.list = unique(data.lca$year)
tec.list = unique(data.lca$technology)
com.list = unique(data.lca$commodity)
lev.list = unique(data.lca$level)
# add scrap as commodity level
lev.list = c(lev.list, "old_scrap")

# check whether set members exist in scenario and add in case not
for (n in 1:length(node.list)) if (!node.list[n] %in% node$.) ixScenario$add_set('node', node.list[n])
for (n in 1:length(year.list)) if (!year.list[n] %in% year$.) ixScenario$add_set('year', year.list[n])
for (n in 1:length(tec.list)) if (!tec.list[n] %in% technology$.) ixScenario$add_set('technology', tec.list[n])
for (n in 1:length(com.list)) if (!com.list[n] %in% commodity$.) ixScenario$add_set('commodity', com.list[n])
for (n in 1:length(lev.list)) if (!lev.list[n] %in% level$.) ixScenario$add_set('level', lev.list[n])

# check whether needed units are registered on ixmp and add if not the case
unit = mp$units() %>% data.frame()
if (!('t/kW' %in% unit$.)) mp$add_unit('t/kW', 'tonnes (of commodity) per kW of capacity')

################################################################################
# create data frames for material intensitty input/output parameters
################################################################################

# new data frames for parameters
input_cap_new <- input_cap_ret <- output_cap_ret <- data.frame()

for (n in node.list) for (t in tec.list) for (c in com.list) {
  year_vtg.list = filter(inv.cost, node_loc == n & technology == t)$year_vtg %>% unique() # & year_vtg >= min(as.numeric(year.list))
  for (y in year_vtg.list) {
    # for years after maximum year in data set use values for maximum year, similarly for years before minimum year in data set use values for minimum year
    if (y > max(year.list)) yeff = max(year.list) else if (y < min(year.list)) yeff = min(year.list) else yeff = y
    input_cap_new = rbind(input_cap_new, data.frame(node_loc = n, technology = t, year_vtg = as.character(y), node_origin = n, commodity = c, level = "product", time_origin = "year", value = filter(data.lca, node == n & technology == t & phase == 'Construction' & commodity == c & year == yeff)$value * 1e-3, unit = 't/kW'))
    input_cap_ret = rbind(input_cap_ret, data.frame(node_loc = n, technology = t, year_vtg = as.character(y), node_origin = n, commodity = c, level = "product", time_origin = "year", value = filter(data.lca, node == n & technology == t & phase == 'End-of-life' & commodity == c & year == yeff)$value * 1e-3, unit = 't/kW'))
    output_cap_ret = rbind(output_cap_ret, data.frame(node_loc = n, technology = t, year_vtg = as.character(y), node_dest = n, commodity = c, level = "old_scrap", time_dest = "year", value = filter(data.lca, node == n & technology == t & phase == 'Construction' & commodity == c & year == yeff)$value * 1e-3, unit = 't/kW'))
  }
}

################################################################################
# create new parameters and add new data to scenario
################################################################################

# create new parameters input_cap_new, output_cap_new, input_cap_ret, output_cap_ret, input_cap and output_cap if they don't exist 
if (!ixScenario$has_par('input_cap_new')) 
  ixScenario$init_par('input_cap_new', idx_sets = c('node', 'technology', 'year', 'node', 'commodity', 'level', 'time'), idx_names = c('node_loc', 'technology', 'year_vtg', 'node_origin', 'commodity', 'level', 'time_origin'))
if (!ixScenario$has_par('output_cap_new')) 
  ixScenario$init_par('output_cap_new', idx_sets = c('node', 'technology', 'year', 'node', 'commodity', 'level', 'time'), idx_names = c('node_loc', 'technology', 'year_vtg', 'node_dest', 'commodity', 'level', 'time_dest'))
if (!ixScenario$has_par('input_cap_ret')) 
  ixScenario$init_par('input_cap_ret', idx_sets = c('node', 'technology', 'year', 'node', 'commodity', 'level', 'time'), idx_names = c('node_loc', 'technology', 'year_vtg', 'node_origin', 'commodity', 'level', 'time_origin'))
if (!ixScenario$has_par('output_cap_ret')) 
  ixScenario$init_par('output_cap_ret', idx_sets = c('node', 'technology', 'year', 'node', 'commodity', 'level', 'time'), idx_names = c('node_loc', 'technology', 'year_vtg', 'node_dest', 'commodity', 'level', 'time_dest'))
if (!ixScenario$has_par('input_cap')) 
  ixScenario$init_par('input_cap', idx_sets = c('node', 'technology', 'year', 'year', 'node', 'commodity', 'level', 'time'), idx_names = c('node_loc', 'technology', 'year_vtg', 'year_act', 'node_origin', 'commodity', 'level', 'time_origin'))
if (!ixScenario$has_par('output_cap')) 
  ixScenario$init_par('output_cap', idx_sets = c('node', 'technology', 'year', 'year', 'node', 'commodity', 'level', 'time'), idx_names = c('node_loc', 'technology', 'year_vtg', 'year_act', 'node_dest', 'commodity', 'level', 'time_dest'))

ixScenario$add_par('input_cap_new', input_cap_new)
ixScenario$add_par('input_cap_ret', input_cap_ret)
ixScenario$add_par('output_cap_ret', output_cap_ret)

################################################################################
# add dummy material production technologies (only needed for model variants without material sector)
################################################################################

technology.material = data.frame(tec = c("material_aluminum", "material_cement", "material_steel", "scrap_aluminum", "scrap_cement", "scrap_steel"), com = c("aluminum", "cement", "steel", "aluminum", "cement", "steel"), lev = c("product", "product", "product", "old_scrap", "old_scrap", "old_scrap"))

for (n in 1:length(technology.material$tec)) if (!technology.material$tec[n] %in% technology$.) ixScenario$add_set('technology', technology.material$tec[n])

# new data frames for parameters
output <- var_cost <- data.frame()

for (n in node.list) for (t in 1:length(technology.material$tec)) {
  year_vtg.list = filter(year, . > 2010)$.
  for (y in year_vtg.list) {
    output = rbind(output, data.frame(node_loc = n, technology = technology.material$tec[t], year_vtg = as.character(y), year_act = as.character(y), mode = 'M1', node_dest = n, commodity = technology.material$com[t], level = technology.material$lev[t], time = 'year', time_dest = "year", value = 1.0, unit = 't'))
    var_cost = rbind(var_cost, data.frame(node_loc = n, technology = technology.material$tec[t], year_vtg = as.character(y), year_act = as.character(y), mode = 'M1', time = "year", value = 1.0, unit = 'USD'))
  }
}

ixScenario$add_par('output', output)
ixScenario$add_par('var_cost', var_cost)

################################################################################
# commit new scenario and solve model 
################################################################################

# commit scenario to platform and set as default
ixScenario$commit(comment)
ixScenario$set_as_default()

# run MESSAGE scenario in GAMS and import results in ix platform
ixScenario$solve("MESSAGE")
