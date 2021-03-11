################################################################################
# include libraries
################################################################################

options(java.parameters = "-Xmx8g")

library(dplyr)
library(tidyr)
library(readxl)
library(imputeTS) # for time series interpolation of NA

################################################################################
# functions
################################################################################

read_material_intensities <- function(data_path, node, year, technology, commodity, level, inv_cost) {

  ################################################################################
  # read data
  ################################################################################
  
  # read LCA data from ADVANCE LCA tool
  col_types = c(rep("text", 9), rep("numeric", 3))
  data_lca = read_xlsx(path = paste(data_path, "/NTNU_LCA_coefficients.xlsx", sep = ''), sheet = "environmentalImpacts", col_types = col_types)

  # read technology, region and commodity mappings
  technology_mapping = read_xlsx(paste(data_path, "/MESSAGE_global_model_technologies.xlsx", sep = ''), sheet = 'technology')
  region_mapping = read_xlsx(paste(data_path, "/LCA_region_mapping.xlsx", sep = ''), sheet = 'region')
  commodity_mapping = read_xlsx(paste(data_path, "/LCA_commodity_mapping.xlsx", sep = ''), sheet = 'commodity') 

  ################################################################################
  # process data
  ################################################################################

  # filter relevant scenario, technology variant (residue for biomass, mix for others) and remove operation phase (and remove duplicates)
  data_lca = data_lca %>% filter(scenario == 'Baseline' & `technology variant` %in% c('mix', 'residue') & phase != 'Operation')

  # add intermediate time steps and turn into long table format
  data_lca = data_lca %>% mutate(`2015` = NA, `2020` = NA, `2025` = NA, `2035` = NA, `2040` = NA, `2045` = NA) %>% pivot_longer(cols = c("2010":"2045"), names_to = 'year', values_to = 'value') %>% mutate(value = as.numeric(value))
  #data_lca$year = factor(data_lca$year, levels = as.character(seq(2010, 2050, 5)))

  # apply technology, commodity/impact and region mappings to MESSAGEix
  data_lca = data_lca %>% inner_join(region_mapping, by = c("region" = "THEMIS")) %>% inner_join(technology_mapping, by = c("technology" = "LCA mapping")) %>%
    inner_join(commodity_mapping, by = c("impact" = "impact")) %>%
    filter(!is.na(`MESSAGEix-GLOBIOM_1.1`)) %>%
    select(node = `MESSAGEix-GLOBIOM_1.1`, technology = `Type of Technology`, phase, commodity, level, year, unit, value) #%>% unique() 

  temp = c()
  for (n in unique(data_lca$node)) for (t in unique(data_lca$technology)) for (c in unique(data_lca$commodity)) for (p in unique(data_lca$phase)){
    temp = rbind(temp, data_lca %>% filter(node == n & technology == t & commodity == c & phase == p) %>% na_interpolation())
  }
  # return data frame
  data_lca = temp

  # extract node, technology, commodity, level, and year list from LCA data set
  node_list = unique(data_lca$node)
  year_list = unique(data_lca$year)
  tec_list = unique(data_lca$technology)
  com_list = unique(data_lca$commodity)
  lev_list = unique(data_lca$level)
  # add scrap as commodity level
  lev_list = c(lev_list, 'end_of_life')
  
  # check whether set members exist in scenario and add in case not
  #for (n in 1:length(node.list)) if (!node.list[n] %in% node$.) ixScenario$add_set('node', node.list[n])
  #for (n in 1:length(year.list)) if (!year.list[n] %in% year$.) ixScenario$add_set('year', year.list[n])
  #for (n in 1:length(tec.list)) if (!tec.list[n] %in% technology$.) ixScenario$add_set('technology', tec.list[n])
  #for (n in 1:length(com.list)) if (!com.list[n] %in% commodity$.) ixScenario$add_set('commodity', com.list[n])
  #for (n in 1:length(lev.list)) if (!lev.list[n] %in% level$.) ixScenario$add_set('level', lev.list[n])

  ################################################################################
  # create data frames for material intensity input/output parameters
  ################################################################################
  
  # new data frames for parameters
  input_cap_new <- input_cap_ret <- output_cap_ret <- data.frame()
  
  for (n in node_list) for (t in tec_list) for (c in com_list) {
    year_vtg_list = filter(inv_cost, node_loc == n & technology == t)$year_vtg %>% unique() # & year_vtg >= min(as.numeric(year.list))
    for (y in year_vtg_list) {
      # for years after maximum year in data set use values for maximum year, similarly for years before minimum year in data set use values for minimum year
      if (y > max(year_list)) yeff = max(year_list) else if (y < min(year_list)) yeff = min(year_list) else yeff = y
      input_cap_new = rbind(input_cap_new, data.frame(node_loc = n, technology = t, year_vtg = as.character(y), node_origin = n, commodity = c, level = 'product', time_origin = "year", value = filter(data_lca, node == n & technology == t & phase == 'Construction' & commodity == c & year == yeff)$value * 1e-3, unit = 't/kW'))
      input_cap_ret = rbind(input_cap_ret, data.frame(node_loc = n, technology = t, year_vtg = as.character(y), node_origin = n, commodity = c, level = 'product', time_origin = "year", value = filter(data_lca, node == n & technology == t & phase == 'End-of-life' & commodity == c & year == yeff)$value * 1e-3, unit = 't/kW'))
      output_cap_ret = rbind(output_cap_ret, data.frame(node_loc = n, technology = t, year_vtg = as.character(y), node_dest = n, commodity = c, level = 'end_of_life', time_dest = "year", value = filter(data_lca, node == n & technology == t & phase == 'Construction' & commodity == c & year == yeff)$value * 1e-3, unit = 't/kW'))
    }
  }
  
  # return parameter (other parameters currently not returned)
  input_cap_new    
  
}
