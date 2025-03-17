#-------------------------------------------------------------------------------
# script that reads in MESSAGE useful energy demands from IAMC-style template,
# imports them into ix platform, sets a cumulative carbon constraint, runs the 
# model and imports results into ixWorkDB
#-------------------------------------------------------------------------------

#-------------------------------------------------------------------------------                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
library(dplyr)
library(tidyr)
options(java.parameters = "-Xmx16g")
library(xlsx)
library(RCurl)
library(reticulate)
library(rixmp)
library(rmessageix)
# library(retixmp)
# ixmp <- import('ixmp')
# message_ix <- import('message_ix')

# -------------------------------------------------------------------------------
# functions

# upload file to MESSAGEix_WorkDB
database_upload <- function(filename, path = getwd(), user = "DbCopy", password = "badpassword", verbose = FALSE) {
  
  # set working directory
  setwd(path)
  
  # setup curl options
  curl = getCurlHandle()
  curlSetOpt(cookiejar="",  useragent = "Mozilla/5.0", verbose = verbose, followlocation = TRUE, curl = curl)
  
  # login
  loginUrl <- paste('https://db1.ene.iiasa.ac.at/MESSAGEix_WorkDB/dsd?Action=loginform&usr=', user, '&pwd=', password, sep = '')
  html.response1 <- getURLContent(loginUrl, curl = curl)
  if (verbose) write(rawToChar(html.response1), file = 'html_response1.html')
  
  # file upload
  uploadUrl <- 'https://db1.ene.iiasa.ac.at/MESSAGEix_WorkDB/dsd?Action=uploadFile'
  html.reponse2 <- postForm(uploadUrl, curl = curl, "name" = "file1", "filedata" = fileUpload(filename))
  if (verbose) write( html.response2, file = 'html_response2.html')
}

#-------------------------------------------------------------------------------
# Update a Message datastructure 
# using the Message Java Toolbox
#-------------------------------------------------------------------------------

#-------------------------------------------------------------------------------
# Steps for using this script
#
# (1) Change the values for 'newscenarioName' and 'dbscenarioName'
# (2) Change the cumulative carbon budget => see the command "ixScenario$add_par("bound_emission"..."
# (3) Run the script all the way down to the "ixScenario$solve" command
# (4) Run the "print(paste("python iamc_report.py..." command at the bottom (either for bash or Windows terminal), in order to spit out a string to the R terminal. Next, copy that string to the Bash/Windows terminal in order to execute the post-processing script.
#
#-------------------------------------------------------------------------------

# details for existing datastructure to be updated and annotation log in database 
modelName <- "BZ_MESSAGE_RU" #"CD_Links_SSP2" #
scenarioName <- "test_core" #'baseline' #
ver.base <- 121

# new model name in ix platform
newmodelName <- "BZ_MESSAGE_RU_Hydro"
newscenarioName <- "Hydro_test"

# new model name in scenario database
dbmodelName <- "BZ_MESSAGE_RU_Hydro"

### 2C-ish scenarios ###
#dbscenarioName <- "SSP2_demand_2017-09-08_2C_NPi2020_noBECCS_V5"
dbscenarioName <- "Hydro_test"

comment <- "BZ_MESSAGE_RU test for new hydro potentials"

#-------------------------------------------------------------------------------

# Set path to message_ix working copy
# message_ix_path = Sys.getenv("MESSAGE_IX_PATH")

# load packages and launch the IX modeling platform
# source(file.path(Sys.getenv("IXMP_R_PATH"), "ixmp.R"))

# launch the IX modeling platform using the default central ORACLE database
# platform <- ixmp.Platform(dbtype='HSQLDB')
platform <- ixmp.Platform()
platform$scenario_list(model="BZ_MESSAGE_RU")

#-------------------------------------------------------------------------------

  
Sce.base <- platform$Scenario(model=modelName, scen=scenarioName, version = ver.base)

# clone data structure with new scenario name
Sce.hyd <- Sce.base$clone(annotation="test_JM")

# check out cloned scenario
# Sce.hyd$remove_sol()
Sce.hyd$check_out()

# Sce.hyd$set_as_default()

# Browse this Scenario
paste(Sce.hyd$model, Sce.hyd$scenario, Sce.hyd$scheme, Sce.hyd$version())

# Sce.hyd$par_list()
Sce.hyd$set_list()
# Sce1$cat_list("emission")
# Sce1$var_list()
# Sce1$set("technology") 
# sce1.input <- Sce1$par("input")
# sce1.fix_cost <- Sce1$par("fix_cost")

num.step <- 8
new.tech.names <- paste0("hydro_", 1:num.step)
new.comm.names <- paste0("hydro_c", 1:num.step)

# Sets to modify
technology <- Sce.hyd$set("technology")
commodity <- Sce.hyd$set("commodity")
cat_tec <- Sce.hyd$set("cat_tec")

technology <- c(technology[-grep("hydro_", technology)], new.tech.names)
commodity <- c(commodity[-grep("^hydro$", commodity)], new.comm.names) # avoid "hydrogen"
cat_tec <- cat_tec %>% filter(!grepl("hydro_", technology)) %>% rbind(data.frame(type_tec="powerplant", technology=new.tech.names))

Sce.hyd$add_set("technology", adapt_to_ret(technology))
Sce.hyd$add_set("technology", r_to_py(technology))
Sce.hyd$add_set("technology", new.tech.names)
Sce.hyd$add_set("commodity", adapt_to_ret(commodity))
Sce.hyd$add_set("cat_tec", adapt_to_ret(cat_tec))

# Params to modify

ren_pot <- Sce.hyd$par("renewable_potential")
ren_capfac <- Sce.hyd$par("renewable_capacity_factor")
inv_cost <- Sce.hyd$par("inv_cost") 

ren_pot %>% filter(commodity=="hydro")

temp <- load(file="LoadFac_RUS_8main_4subs.rda") #final_LF_df_pl
temp <- load(file="CapCost_RUS_8main_4subs.rda") #final_cost_df_pl

scenario$add_par("renewable_potential", adapt_to_ret(ren_pot))
scenario$add_par("renewable_capacity_factor", adapt_to_ret(ren_capfac))




Sce1$commit(".")

Sce1$solve(model = "MESSAGE", case = modelName)




tech <- Sce1$set("technology") 
inp <- Sce1$par("input") %>% filter(technology=="Population")

Sce$add_par( "technical_lifetime", # parameter name
             paste(lapply(a[1,1:3], as.character), sep='.', collapse = "."), # set key
             a[1,]$value, # parameter value
             "y")
Sce$par("technical_lifetime")
#-------------------------------------------------------------------------------
# read demands from xlsx file

path.data = "P:/ene.model/ALPS_1.5C_demand/demands_inp_files"
setwd(path.data)
data.demand <- read.xlsx("MESSAGE_demand_v3_20170913.xlsx", sheetName = "data")

names(data.demand) = c("MODEL", "SCENARIO", "REGION", "VARIABLE", "UNIT", "2000", "2005", "2010", "2020", "2030", "2040", "2050", "2060", "2070", "2080", "2090", "2100", "2110")

demand.mapping = data.frame(VARIABLE = c("Demand|Feedstocks", "Demand|Industrial Specific", "Demand|Industrial Thermal", "Demand|Non-Commercial", "Demand|Residential and Commercial Specific", "Demand|Residential and Commercial Thermal", "Demand|Transportation", "Demand|Shipping"), 
                            DEMAND = c("i_feed","i_spec", "i_therm","non-comm", "rc_spec", "rc_therm", "transport", "shipping"))


# turn demand table into long table format (relational database like) and convert from EJ to GWa as used in MESSAGE
data.table = inner_join(data.demand, demand.mapping) %>% select(one_of("DEMAND", "REGION"), starts_with("2")) %>% gather(key = "YEAR", value = "VALUE", 3:15) %>% mutate(VALUE = VALUE * 1000 / (8.76*3.6))

# nodes, periods and demand commodities to add to the model
node_list = levels(data.table$REGION)
node_list = node_list[node_list != "R11_GLB"]  ## remove the "R11_GLB" region from the list, since this is only for the shipping demand commodity and we want to treat this separately in the code below
period_list = seq(2020, 2110, 10)  ## use this command if we want to change model parameters from 2020 onward
##period_list = seq(2030, 2110, 10)  ## use this command if we want to change model parameters from 2030 onward (i.e., if a slice was used up to 2020 in the scenario that this one is cloned from)
commodity_list = levels(data.table$DEMAND)
commodity_list = commodity_list[commodity_list != "shipping"]  ## remove the "shipping" commodity from the list, since this is only for the "R11_GLB" region and we want to treat this separately in the code below

#-------------------------------------------------------------------------------

# read technology investment costs from xlsx file
path.data.invcosts = "P:/ene.model/ALPS_1.5C_demand/technology_inp_files"
setwd(path.data.invcosts)

data.invcosts <- read.xlsx("granular-techs_cost_comparison_20170831_revAG_SDS.XLSX", sheetName = "NewCosts_fixed")

names(data.invcosts) = c("TECHNOLOGY", "REGION", "2005", "2010", "2020", "2030", "2040", "2050", "2060", "2070", "2080", "2090", "2100", "2110")

# turn demand table into long table format (relational database like)
data.table.invcosts = data.invcosts %>% select(one_of("TECHNOLOGY", "REGION"), starts_with("2")) %>% gather(key = "YEAR", value = "VALUE", 3:14)

# No need to update the nodes or periods because they are defined already above. However, the technology list is something new that needs to be read in here.
##node_list = levels(data.invcosts$REGION)
##period_list = seq(2030, 2110, 10)
technology_list = levels(data.invcosts$TECHNOLOGY)


#-------------------------------------------------------------------------------

# read technology fixed O&M costs from xlsx file
path.data.fomcosts = "P:/ene.model/ALPS_1.5C_demand/technology_inp_files"
setwd(path.data.fomcosts)

data.fomcosts <- read.xlsx("granular-techs_cost_comparison_20170831_revAG_SDS.XLSX", sheetName = "NewFOMCosts_fixed")

names(data.fomcosts) = c("TECHNOLOGY", "REGION", "2005", "2010", "2020", "2030", "2040", "2050", "2060", "2070", "2080", "2090", "2100", "2110")

# turn demand table into long table format (relational database like)
data.table.fomcosts = data.fomcosts %>% select(one_of("TECHNOLOGY", "REGION"), starts_with("2")) %>% gather(key = "YEAR", value = "VALUE", 3:14)

# No need to update the nodes or periods because they are defined already above. However, the technology list is something new that needs to be read in here.
##node_list = levels(data.fomcosts$REGION)
##period_list = seq(2030, 2110, 10)
technology_list6 = levels(data.fomcosts$TECHNOLOGY)

#-------------------------------------------------------------------------------

# read technology variable O&M costs from xlsx file
path.data.vomcosts = "P:/ene.model/ALPS_1.5C_demand/technology_inp_files"
setwd(path.data.vomcosts)

data.vomcosts <- read.xlsx("granular-techs_cost_comparison_20170831_revAG_SDS.XLSX", sheetName = "NewVOMCosts_fixed")

names(data.vomcosts) = c("TECHNOLOGY", "REGION", "2005", "2010", "2020", "2030", "2040", "2050", "2060", "2070", "2080", "2090", "2100", "2110")

# turn demand table into long table format (relational database like)
data.table.vomcosts = data.vomcosts %>% select(one_of("TECHNOLOGY", "REGION"), starts_with("2")) %>% gather(key = "YEAR", value = "VALUE", 3:14)

# No need to update the nodes or periods because they are defined already above. However, the technology list is something new that needs to be read in here.
##node_list = levels(data.vomcosts$REGION)
##period_list = seq(2030, 2110, 10)
technology_list7 = levels(data.vomcosts$TECHNOLOGY)

#-------------------------------------------------------------------------------

# read solar and wind intermittency assumptions from xlsx file
path.data.steps = "P:/ene.model/ALPS_1.5C_demand/technology_inp_files"
setwd(path.data.steps)

data.steps <- read.xlsx("solar_wind_intermittency_20170831.XLSX", sheetName = "steps_NEW")

names(data.steps) = c("TECHNOLOGY", "RELATION", "REGION", "2005", "2010", "2020", "2030", "2040", "2050", "2060", "2070", "2080", "2090", "2100", "2110")

# turn demand table into long table format (relational database like)
data.table.steps = data.steps %>% select(one_of("TECHNOLOGY", "RELATION", "REGION"), starts_with("2")) %>% gather(key = "YEAR", value = "VALUE", 4:15)

# No need to update the nodes or periods because they are defined already above. However, the technology list is something new that needs to be read in here.
##node_list = levels(data.steps$REGION)
##period_list = seq(2030, 2110, 10)
technology_list2 = levels(data.steps$TECHNOLOGY)
relation_list2 = levels(data.steps$RELATION)

#-------------------------------------------------------------------------------

# read solar and wind intermittency assumptions from xlsx file
path.data.oper = "P:/ene.model/ALPS_1.5C_demand/technology_inp_files"
setwd(path.data.oper)

data.oper <- read.xlsx("solar_wind_intermittency_20170831.XLSX", sheetName = "oper_NEW")

names(data.oper) = c("TECHNOLOGY", "RELATION", "REGION", "2005", "2010", "2020", "2030", "2040", "2050", "2060", "2070", "2080", "2090", "2100", "2110")

# turn demand table into long table format (relational database like)
data.table.oper = data.oper %>% select(one_of("TECHNOLOGY", "RELATION", "REGION"), starts_with("2")) %>% gather(key = "YEAR", value = "VALUE", 4:15)

# No need to update the nodes or periods because they are defined already above. However, the technology list is something new that needs to be read in here.
##node_list = levels(data.oper$REGION)
##period_list = seq(2030, 2110, 10)
technology_list3 = levels(data.oper$TECHNOLOGY)
relation_list3 = levels(data.oper$RELATION)

#-------------------------------------------------------------------------------

# read solar and wind intermittency assumptions from xlsx file
path.data.resm = "P:/ene.model/ALPS_1.5C_demand/technology_inp_files"
setwd(path.data.resm)

data.resm <- read.xlsx("solar_wind_intermittency_20170831.XLSX", sheetName = "resm_NEW")

names(data.resm) = c("TECHNOLOGY", "RELATION", "REGION", "2005", "2010", "2020", "2030", "2040", "2050", "2060", "2070", "2080", "2090", "2100", "2110")

# turn demand table into long table format (relational database like)
data.table.resm = data.resm %>% select(one_of("TECHNOLOGY", "RELATION", "REGION"), starts_with("2")) %>% gather(key = "YEAR", value = "VALUE", 4:15)

# No need to update the nodes or periods because they are defined already above. However, the technology list is something new that needs to be read in here.
##node_list = levels(data.resm$REGION)
##period_list = seq(2030, 2110, 10)
technology_list4 = levels(data.resm$TECHNOLOGY)
relation_list4 = levels(data.resm$RELATION)

#-------------------------------------------------------------------------------

# read useful level fuel potential contribution assumptions from xlsx file
path.data.uefuel = "P:/ene.model/ALPS_1.5C_demand/technology_inp_files"
setwd(path.data.uefuel)

data.uefuel <- read.xlsx("useful_level_fuel_potential_contribution_20170907.XLSX", sheetName = "UE_constraints_NEW")

names(data.uefuel) = c("TECHNOLOGY", "RELATION", "REGION", "2005", "2010", "2020", "2030", "2040", "2050", "2060", "2070", "2080", "2090", "2100", "2110")

# turn demand table into long table format (relational database like)
data.table.uefuel = data.uefuel %>% select(one_of("TECHNOLOGY", "RELATION", "REGION"), starts_with("2")) %>% gather(key = "YEAR", value = "VALUE", 4:15)

# No need to update the nodes or periods because they are defined already above. However, the technology list is something new that needs to be read in here.
##node_list = levels(data.uefuel$REGION)
##period_list = seq(2030, 2110, 10)
technology_list5 = levels(data.uefuel$TECHNOLOGY)
relation_list5 = levels(data.uefuel$RELATION)

#-------------------------------------------------------------------------------

# insert new demands into the model - 7 standard demands in 11 standard regions
for (node in node_list) {
  for (commodity in commodity_list) {
    for (year in period_list) {
      # ixScenario$add_par("demand",paste(node,commodity,"useful",as.character(year),"year",sep='.'),filter(data.table, DEMAND == commodity & REGION == node & YEAR == as.character(year))$VALUE,"GWa")
      print(paste(node,commodity,"useful",as.character(year),"year",sep='.'))
    }
  }
}


# insert new demands into the model - shipping demands in GLB region only
for (year in period_list) {
  ixScenario$add_par("demand",paste("R11_GLB","shipping","useful",as.character(year),"year",sep='.'),filter(data.table, DEMAND == "shipping" & REGION == "R11_GLB" & YEAR == as.character(year))$VALUE,"GWa")
}


      
# insert emission constraint leading to different carbon budgets (2011-2100)
#
# But actually in MESSAGEix, the budget is defined from the starting year up to 2110, and in terms of annual average emissions over that timeframe
# 400 GtCO2 (1.5 deg C): 1800 MtCeq is for 2011-2110
# 400 GtCO2 (1.5 deg C): 433 MtCeq is for 2021-2110
#
#ixScenario$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 1150, "tC")
#ixScenario$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 450, "tC")  ## This budget (450 MtC; for 2021-2110) produced an infeasible solution on 2017-08-09 using Simon's initial demands from 2017-08-06.
#ixScenario$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 1800, "tC")  ## This budget (for 2021-2110) was run in scenario '2C_NPi2020_noBECCS_V1' to test that a more relaxed budget would be feasible in the current set-up (low demands from Simon on 2017-08-06 and also no BECCS).
#ixScenario$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 1200, "tC")  ## This budget (for 2021-2110) was run in scenario 'b2C_NPi2020_noBECCS_V1' to test that a more relaxed budget would be feasible in the current set-up (low demands from Simon on 2017-08-06 and also no BECCS).
##ixScenario$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 800, "tC")  ## Budget of 800 MtC annual average per year (for 2021-2110) was found to be too tight in an earlier run of scenario 'b2C_NPi2020_noBECCS_V2'. Infeasible solution; model did not solve.
#ixScenario$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 1000, "tC")  ## This budget (for 2021-2110) was run in scenarios 'b2C_NPi2020_noBECCS_V2' and similar up to 'V6' to get something around 1.5C. (The same budget was attempted for the initial V7 run, but the removal of Fossil CCS made it infeasible.) Median temperature is around 1.57C in 2050 and 1.31C in 2100. Median radiative forcing (total) is 1.81 W/m2 in 2100. (low demands from Simon on 2017-08-06 and also no BECCS).
#ixScenario$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 1200, "tC")  ## This budget (for 2021-2110) was run in scenarios 'b2C_NPi2020_noBECCS_V7' to get something around 1.5C. (A budget of 1000 was initially attempted for V7, but the lack of Fossil CCS made it infeasible.) Median temperature is around 1.60C in 2050 and 1.34 in 2100. Median radiative forcing (total) is 1.89 W/m2 in 2100. (low demands from Simon on 2017-08-31 and also no BECCS and no Fossil CCS).
ixScenario$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 1400, "tC")  ## This budget (for 2021-2110) was run in scenarios 'b2C_NPi2020_noBECCS_V7a' to 'V8' to get something around 1.5C. (A budget of 1200 was initially used for V7, but the carbon prices were ridiculously higher.) Median temperature is around 1.61C in 2050 and 1.37 in 2100. Median radiative forcing (total) is 1.94 W/m2 in 2100. (low demands from Simon on 2017-09-08 and also no BECCS and no Fossil CCS).
#ixScenario$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 2500, "tC")  ## This budget (for 2021-2110) was run in scenarios '2C_NPi2020_noBECCS_V2' to try and get something nearer to 2C by end of century. Median temperature is around 1.66C in 2050 and 1.53C in 2100. Median radiative forcing (total) is 2.22 W/m2 in 2100. (low demands from Simon on 2017-08-06 and also no BECCS).
#ixScenario$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 3500, "tC")  ## This budget (for 2021-2110) was run in scenarios '2C_NPi2020_noBECCS_V3' to try and get something nearer to 2C by end of century. Median temperature is around 1.71C in 2050 and 1.67C in 2100. Median radiative forcing (total) is 2.48 W/m2 in 2100. (low demands from Simon on 2017-08-06 and also no BECCS).
#ixScenario$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 4000, "tC")  ## This budget (for 2021-2110) was run in scenarios '2C_NPi2020_noBECCS_V4' to try and get something nearer to 2C by end of century. Median temperature is around 1.73C in 2050, peaking at 1.79C in 2070/2080, and then coming back down to 1.74C in 2100. Median radiative forcing (total) is 2.6 W/m2 in 2100. (low demands from Simon on 2017-08-06 and also no BECCS).

# exclude BECCS by constrainig activity of 'bco2_tr_dis' technology to 0 in all years
for (node in node_list) {
  for (year in period_list) {
    ixScenario$add_par("bound_activity_up",paste(node, "bco2_tr_dis", as.character(year), "M1", "year", sep='.'), 0, "GWa")
  }
}

# exclude Fossil CCS by constrainig activity of 'co2_tr_dis' technology to 0 in all years
for (node in node_list) {
  for (year in period_list) {
    ixScenario$add_par("bound_activity_up",paste(node, "co2_tr_dis", as.character(year), "M1", "year", sep='.'), 0, "GWa")
  }
}

# adjust limit to electrify transport to XX% of useful energy (negative number, i.e. -0.9 refers to 90%)
for (node in node_list) {
  for (year in period_list) {
    #ixScenario$add_par("relation_activity",paste("UE_transport_electric", node, as.character(year), node, "useful_transport", as.character(year), "M1", sep='.'), -0.9, "-")
    ixScenario$add_par("relation_activity",paste("UE_transport_electric", node, as.character(year), node, "useful_transport", as.character(year), "M1", sep='.'), -0.8, "-")
  }
}
    
# adjust limit for hydrogen in transport to 100% of useful energy (negative number, i.e. -1.0 refers to 100%)
for (node in node_list) {
  for (year in period_list) {
    ixScenario$add_par("relation_activity",paste("UE_transport_fc", node, as.character(year), node, "useful_transport", as.character(year), "M1", sep='.'), -1.0, "-")
  }
}
  
# adjust efficiency of the hydrogen transport technology to make it more efficient; a value of 0.45 means that the hydrogen transport technology is 2.22 times more efficient than the liquid fuel combustion technology (0.45 = 1/2.22)
# but only change parameters for vintage-activity year combinations that are currently specified for this technology in the model
technology = "h2_fc_trp"
for (node in node_list) {
 for (year_vtg in period_list) {
    if (any(class(tryCatch((ixScenario$years_active(node, technology, year_vtg)), error = function(e) e)) == "error")) {
      print(paste(node, technology, as.character(year_vtg), sep='.'))
      print('Error: technology does not exist in this region for this vintage year')
    } else {
      years_tec_active = ixScenario$years_active(node, technology, year_vtg)
      for (year_act in years_tec_active) {
        #print(ixScenario$add_par("input",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", node, "lh2", "final", "year", "year", sep='.'), 0.45, "GWa"))
        ixScenario$add_par("input",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", node, "lh2", "final", "year", "year", sep='.'), 0.45, "GWa")
      }
    }
 }
}
  
# # adjust investment cost of the hydrogen transport technology to make it less expensive; 'h2_fc_trp' is the only end-use transport technology with an explicit 'inv_cost' on it; all others have an indirect cost that comes in through the 'weight_trp' relation
# # but only change parameters for vintage years that are currently specified for this technology in the model
# technology = "h2_fc_trp"
# for (node in node_list) {
#   for (year_vtg in period_list) {
#     if (any(class(tryCatch((ixScenario$years_active(node, technology, year_vtg)), error = function(e) e)) == "error")) {
#       print(paste(node, technology, as.character(year_vtg), sep='.'))
#       print('Error: technology does not exist in this region for this vintage year')
#     } else {
#       ixScenario$add_par("inv_cost",paste(node, technology, as.character(year_vtg), sep='.'), 1.0, "USD/GWa")  # specify units in 'USD/GWa', even though they are actually $/kW within the model
#     }
#   }
# }



# adjust investment costs of a set of technologies that are defined in another XLS file
# but only change parameters for vintage years that are currently specified for this technology in the model
#technology = "h2_fc_trp"
for (node in node_list) {
  for (year_vtg in period_list) {
    for (technology in technology_list) {
      if (any(class(tryCatch((ixScenario$years_active(node, technology, year_vtg)), error = function(e) e)) == "error")) {
        print(paste(node, technology, as.character(year_vtg), sep='.'))
        print('Error: technology does not exist in this region for this vintage year')
      } else {
        ixScenario$add_par("inv_cost",paste(node, technology, as.character(year_vtg), sep='.'), (filter(data.table.invcosts, REGION == node & TECHNOLOGY == technology & YEAR == as.character(year_vtg))$VALUE), "USD/GWa")  # specify units in 'USD/GWa', even though they are actually $/kW within the model
        #print(paste(node, technology, as.character(year_vtg), sep='.'),(filter(data.table.invcosts, REGION == node & TECHNOLOGY == technology & YEAR == as.character(year_vtg))$VALUE), digits=4, quote = TRUE)
      }
    }
  }
}


# adjust fixed O&M costs of a set of technologies that are defined in another XLS file
# but only change parameters for vintage years that are currently specified for this technology in the model
for (node in node_list) {
  for (year_vtg in period_list) {
    for (technology in technology_list6) {
      if (any(class(tryCatch((ixScenario$years_active(node, technology, year_vtg)), error = function(e) e)) == "error")) {
        print(paste(node, technology, as.character(year_vtg), sep='.'))
        print('Error: technology does not exist in this region for this vintage-activity year combination')
      } else {
        #ixScenario$add_par("fix_cost",paste(node, technology, as.character(year_vtg), as.character(year_act), sep='.'), (filter(data.table.fomcosts, REGION == node & TECHNOLOGY == technology & YEAR == as.character(year_act))$VALUE), "USD/GWa")  # specify units in 'USD/GWa', even though they are actually $/kWyr/yr within the model
        years_tec_active = ixScenario$years_active(node, technology, year_vtg)
        for (year_act in years_tec_active) {
          #print(ixScenario$add_par("fix_cost",paste(node, technology, as.character(year_vtg), as.character(year_act), sep='.'), (filter(data.table.fomcosts, REGION == node & TECHNOLOGY == technology & YEAR == as.character(year_act))$VALUE), "USD/GWa"))
          ixScenario$add_par("fix_cost",paste(node, technology, as.character(year_vtg), as.character(year_act), sep='.'), (filter(data.table.fomcosts, REGION == node & TECHNOLOGY == technology & YEAR == as.character(year_act))$VALUE), "USD/GWa")
        }
      }
    }
  }
}



# adjust variable O&M costs of a set of technologies that are defined in another XLS file
# but only change parameters for vintage years that are currently specified for this technology in the model
for (node in node_list) {
  for (year_vtg in period_list) {
    for (technology in technology_list7) {
      if (any(class(tryCatch((ixScenario$years_active(node, technology, year_vtg)), error = function(e) e)) == "error")) {
        print(paste(node, technology, as.character(year_vtg), sep='.'))
        print('Error: technology does not exist in this region for this vintage-activity year combination')
      } else {
        #ixScenario$add_par("var_cost",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", "year", sep='.'), (filter(data.table.fomcosts, REGION == node & TECHNOLOGY == technology & YEAR == as.character(year_act))$VALUE), "USD/GWa")  # specify units in 'USD/GWa', even though they are actually $/kWyr/yr within the model
        years_tec_active = ixScenario$years_active(node, technology, year_vtg)
        for (year_act in years_tec_active) {
          #print(ixScenario$add_par("var_cost",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", "year", sep='.'), (filter(data.table.fomcosts, REGION == node & TECHNOLOGY == technology & YEAR == as.character(year_act))$VALUE), "USD/GWa"))
          ixScenario$add_par("var_cost",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", "year", sep='.'), (filter(data.table.fomcosts, REGION == node & TECHNOLOGY == technology & YEAR == as.character(year_act))$VALUE), "USD/GWa")
        }
      }
    }
  }
}



# adjust wind and solar PV resource steps (contribution to total electricity generation) for a set of technologies that are defined in another XLS file
# these changes allow for greater contribution of intermittent solar and wind to total electricity generation
for (node in node_list) {
  for (year_act in period_list) {
    for (relation in relation_list2) {
      for (technology in technology_list2) {
        ixScenario$add_par("relation_activity",paste(relation, node, as.character(year_act), node, technology, as.character(year_act), "M1", sep='.'), (filter(data.table.steps, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE), "-")
        #print(paste(node, technology, relation, as.character(year_act), sep='.'),(filter(data.table.steps, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE))

        # if (any(class(tryCatch((ixScenario$years_active(node, technology, year_act)), error = function(e) e)) == "error")) {
        #   print(paste(node, technology, as.character(year_act), sep='.'))
        #   print('Error: technology does not exist in this region for this activity year')
        # } else {
        #   ixScenario$add_par("relation_activity",paste(relation, node, as.character(year_act), node, technology, as.character(year_act), "M1", sep='.'), (filter(data.table.steps, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE), "-")
        #   #print(paste(node, technology, relation, as.character(year_act), sep='.'),(filter(data.table.steps, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE))
        # }

      }
    }
  }
}


# adjust wind and solar PV operating reserve requirements (amount of flexible generation that needs to be run for every unit of intermittent solar and wind => variable renewables <0, non-dispatchable thermal 0, flexible >0); done for a set of technologies that are defined in another XLS file
# also adjust the contribution of electric transport technologies to the operating reserves, increasing the amount they can contribute (vehicle-to-grid)
# these changes reduce the effective cost of building and running intermittent solar and wind plants, since the amount of back-up capacity built is less than before
for (node in node_list) {
  for (year_act in period_list) {
    for (relation in relation_list3) {
      for (technology in technology_list3) {
        ixScenario$add_par("relation_activity",paste(relation, node, as.character(year_act), node, technology, as.character(year_act), "M1", sep='.'), (filter(data.table.oper, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE), "-")
        #print(paste(node, technology, relation, as.character(year_act), sep='.'),(filter(data.table.oper, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE))

        # if (any(class(tryCatch((ixScenario$years_active(node, technology, year_act)), error = function(e) e)) == "error")) {
        #   print(paste(node, technology, as.character(year_act), sep='.'))
        #   print('Error: technology does not exist in this region for this activity year')
        # } else {
        #   #ixScenario$add_par("relation_activity",paste(relation, node, as.character(year_act), node, technology, as.character(year_act), "M1", sep='.'), (filter(data.table.oper, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE), "-")
        #   print(paste(node, technology, relation, as.character(year_act), sep='.'),(filter(data.table.oper, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE))
        # }

      }
    }
  }
}



# adjust wind and solar PV reserve margin requirements (amount of firm capacity that needs to be run to meet peak load and contingencies; intermittent solar and wind do not contribute a full 100% to the reserve margin); done for a set of technologies that are defined in another XLS file
# these changes allow for greater contribution of intermittent solar and wind to total electricity generation
for (node in node_list) {
  for (year_act in period_list) {
    for (relation in relation_list4) {
      for (technology in technology_list4) {
        # ixScenario$add_par("relation_activity",paste(relation, node, as.character(year_act), node, technology, as.character(year_act), "M1", sep='.'), 
        #                    (filter(data.table.resm, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE), 
        #                    "-")
        print(#paste(node, technology, relation, as.character(year_act), sep='.'),
              (filter(data.table.resm, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE))
      }
    }
  }
}




# adjust limits to potential fuel-specific contributions at useful energy level (in each end-use sector separately); done for a set of technologies and relations that are defined in another XLS file
for (node in node_list) {
  for (year_act in period_list) {
    for (relation in relation_list5) {
      for (technology in technology_list5) {
        #ixScenario$add_par("relation_activity",paste(relation, node, as.character(year_act), node, technology, as.character(year_act), "M1", sep='.'), (filter(data.table.uefuel, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE), "-")
        #print(paste(node, technology, relation, as.character(year_act), sep='.'),(filter(data.table.uefuel, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE))
        #print(paste(node, technology, relation, as.character(year_act), sep='.'))

        if (any(class(tryCatch(ixScenario$add_par("relation_activity",paste(relation, node, as.character(year_act), node, technology, as.character(year_act), "M1", sep='.'), (filter(data.table.uefuel, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE), "-"), error = function(e) e)) == "error")) {
          #print(paste(node, technology, relation, as.character(year_act), sep='.'))
          #print('Error: technology either does not exist in this region for this activity year or it does not write into this relation')
        } else {
          ixScenario$add_par("relation_activity",paste(relation, node, as.character(year_act), node, technology, as.character(year_act), 
                                                       "M1", sep='.'), (filter(data.table.uefuel, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE), "-")
        }
        
      }
    }
  }
}




### Remove any "bound_activity_up" values that might exist for the technology 'h2_fc_trp'; it seems that some are floating around in the base CD-LINKS scenario that we use as a starting point for our work.
# To get this working properly and without errors, I add an upper bound on the technology in all regions and years, and then remove it immediately afterwards (in all regions and years)
technology = "h2_fc_trp"
for (node in node_list) {
  for (year in period_list) {
    #print(paste(node, technology, as.character(year), "M1", "year", sep='.'))
    ixScenario$add_par("bound_activity_up",paste(node, technology, as.character(year), "M1", "year", sep='.'), 0, "GWa")
    ixScenario$remove_par("bound_activity_up",paste(node, technology, as.character(year), "M1", "year", sep='.'))
  }
}
### This block of code should have worked in the for loop above for removing parameters, but I couldn't get it working.    
    # if (any(class(tryCatch((ixScenario$remove_par("bound_activity_up",paste(node, technology, as.character(year), "M1", "year", sep='.'))), error = function(e) print('...'))) == "error")) {
    #   print(paste(node, technology, as.character(year), "M1", "year", sep='.'))
    #   print('Error: technology does not have an upper bound in this region for this vintage year, thus none added')
    # } else {
    #   ixScenario$remove_par("bound_activity_up",paste(node, technology, as.character(year), "M1", "year", sep='.'))
    # }


### Some commands for testing. Can be deleted.
#ixScenario$remove_par("bound_activity_up",paste(node, "h2_fc_trp", as.character(year), "M1", "year", sep='.'))
#ixScenario$remove_par("bound_activity_up",paste("R11_AFR", "h2_fc_trp", as.character(2060), "M1", "year", sep='.'))
#any(class(tryCatch((ixScenario$remove_par("bound_activity_up",paste("R11_AFR", technology, 2030, "M1", "year", sep='.'))), error = function(e) print('...'))) == "error")

# Exclude ethanol fuel cell transport technology by constraining activity to 0 in all years
# Need to do this even though the Web UI does not show that there are upper bounds on this technology in any years.
for (node in node_list) {
  for (year in period_list) {
    ixScenario$add_par("bound_activity_up",paste(node, "eth_fc_trp", as.character(year), "M1", "year", sep='.'), 0, "GWa")
  }
}

# Exclude methanol fuel cell transport technology by constraining activity to 0 in all years
# Need to do this even though the Web UI does not show that there are upper bounds on this technology in any years.
for (node in node_list) {
  for (year in period_list) {
    ixScenario$add_par("bound_activity_up",paste(node, "meth_fc_trp", as.character(year), "M1", "year", sep='.'), 0, "GWa")
  }
}


# Increase the initial starting point value for activity growth bounds on the electric transport technology
years_subset = c(2030)
for (node in node_list) {
  for (year in years_subset) {
    ixScenario$add_par("initial_activity_up",paste(node, "elec_trp", as.character(year), "year", sep='.'), 90, "GWa")
  }
}


# Increase the initial starting point value for activity growth bounds on the hydrogen fuel cell technology in transport
years_subset = c(2030, 2040, 2050)
technology = "h2_fc_trp"
for (node in node_list) {
  for (year in years_subset) {
    #ixScenario$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa")

    #if (any(class(tryCatch(ixScenario$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa"), error = function(e) e)) == "error")) {
    if (any(class(tryCatch((ixScenario$years_active(node, technology, year)), error = function(e) e)) == "error")) {
      print(paste(node, technology, as.character(year), sep='.'))
      print('Error: technology does not exist in this region for this activity year')
    } else {
      ixScenario$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa")
    }

  }
}


# Increase the initial starting point value for activity growth bounds on the hydrogen fuel cell technology in industry
years_subset = c(2030, 2040, 2050)
technology = "h2_fc_I"
for (node in node_list) {
  for (year in years_subset) {
    #ixScenario$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa")
    
    #if (any(class(tryCatch(ixScenario$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa"), error = function(e) e)) == "error")) {
    if (any(class(tryCatch((ixScenario$years_active(node, technology, year)), error = function(e) e)) == "error")) {
      print(paste(node, technology, as.character(year), sep='.'))
      print('Error: technology does not exist in this region for this activity year')
    } else {
      ixScenario$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa")
    }
    
  }
}


# Increase the initial starting point value for activity growth bounds on the hydrogen fuel cell technology in industry
years_subset = c(2030, 2040, 2050)
technology = "h2_fc_RC"
for (node in node_list) {
  for (year in years_subset) {
    #ixScenario$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa")
    
    #if (any(class(tryCatch(ixScenario$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa"), error = function(e) e)) == "error")) {
    if (any(class(tryCatch((ixScenario$years_active(node, technology, year)), error = function(e) e)) == "error")) {
      print(paste(node, technology, as.character(year), sep='.'))
      print('Error: technology does not exist in this region for this activity year')
    } else {
      ixScenario$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa")
    }
    
  }
}



# adjust efficiency of the hydrogen fuel cell co-generation technology (electricity + heat) in the RES-COM sector (satisfying both RC-specific and RC-thermal useful demands), in order to make it more efficient; a value of 2.1 means the technology has a combined efficiency (electricity + heat) of 0.86 (2.1 units of hydrogen input produces 1.0 units of electricity and 0.80 units of heat => thus 1.8/2.1 = 0.86)
# but only change parameters for vintage-activity year combinations that are currently specified for this technology in the model
technology = "h2_fc_RC"
for (node in node_list) {
  for (year_vtg in period_list) {
    if (any(class(tryCatch((ixScenario$years_active(node, technology, year_vtg)), error = function(e) e)) == "error")) {
      print(paste(node, technology, as.character(year_vtg), sep='.'))
      print('Error: technology does not exist in this region for this vintage year')
    } else {
      years_tec_active = ixScenario$years_active(node, technology, year_vtg)
      for (year_act in years_tec_active) {
        #print(ixScenario$add_par("input",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", node, "hydrogen", "final", "year", "year", sep='.'), 2.1, "GWa"))
        ixScenario$add_par("input",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", node, "hydrogen", "final", "year", "year", sep='.'), 2.1, "GWa")
      }
    }
  }
}


# adjust efficiency of the hydrogen fuel cell co-generation technology (electricity + heat) in the IND sector (satisfying both IND-specific and IND-thermal useful demands), in order to make it more efficient; a value of 2.0 means the technology has a combined efficiency (electricity + heat) of 0.86 (2.0 units of hydrogen input produces 1.0 units of electricity and 0.71 units of heat => thus 1.71/2.0 = 0.86)
# but only change parameters for vintage-activity year combinations that are currently specified for this technology in the model
technology = "h2_fc_I"
for (node in node_list) {
  for (year_vtg in period_list) {
    if (any(class(tryCatch((ixScenario$years_active(node, technology, year_vtg)), error = function(e) e)) == "error")) {
      print(paste(node, technology, as.character(year_vtg), sep='.'))
      print('Error: technology does not exist in this region for this vintage year')
    } else {
      years_tec_active = ixScenario$years_active(node, technology, year_vtg)
      for (year_act in years_tec_active) {
        #print(ixScenario$add_par("input",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", node, "hydrogen", "final", "year", "year", sep='.'), 2.1, "GWa"))
        ixScenario$add_par("input",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", node, "hydrogen", "final", "year", "year", sep='.'), 2.1, "GWa")
      }
    }
  }
}


# Increase the initial starting point value for activity growth bounds on the solar PV technology (centralized generation)
# Only do this for a subset of the regions for which there are currently "bound_activity_up" (formerly "mpa up") values defined. We don't want to specify an "initial_activity_up" for a technology that does not have a "bound_activity_up".
years_subset = c(2030, 2040, 2050)
node_subset = c("R11_CPA","R11_FSU","R11_LAM","R11_MEA","R11_NAM","R11_PAS")
technology = "solar_pv_ppl"
for (node in node_subset) {
  for (year in years_subset) {
    #ixScenario$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa")
    
    #if (any(class(tryCatch(ixScenario$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa"), error = function(e) e)) == "error")) {
    if (any(class(tryCatch((ixScenario$years_active(node, technology, year)), error = function(e) e)) == "error")) {
      print(paste(node, technology, as.character(year), sep='.'))
      print('Error: technology does not exist in this region for this activity year')
    } else {
      #print(paste(node, technology, as.character(year), sep='.'))
      ixScenario$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa")
    }
    
  }
}


# Increase the initial starting point value for capacity growth bounds on the solar PV technology (centralized generation)
years_subset = c(2030, 2040, 2050)
technology = "solar_pv_ppl"
for (node in node_list) {
  for (year in years_subset) {
    #ixScenario$add_par("initial_new_capacity_up",paste(node, technology, as.character(year), sep='.'), 10, "GW")
    
    #if (any(class(tryCatch(ixScenario$add_par("initial_new_capacity_up",paste(node, technology, as.character(year), sep='.'), 10, "GW"), error = function(e) e)) == "error")) {
    if (any(class(tryCatch((ixScenario$years_active(node, technology, year)), error = function(e) e)) == "error")) {
      print(paste(node, technology, as.character(year), sep='.'))
      print('Error: technology does not exist in this region for this vintage year')
    } else {
      #print(paste(node, technology, as.character(year), sep='.'))
      ixScenario$add_par("initial_new_capacity_up",paste(node, technology, as.character(year), sep='.'), 10, "GW")
    }
    
  }
}

###****************************************************************************************************
## for various sensitivity analyisis, setting up as loops so that no need to modify mannually

## for different demand levels
demand.list = seq(0.75, 1.25, by = 0.05)
for (demand_factor in demand.list) {

  # new names for scenario in ix platform and database
  ## the names of "newscenarioName" and "newdbscenarioName" need to to modified in the two below lines for differnt loops
  newscenarioName = paste(scenarioName, "_", demand_factor,"_", sep = '')
  newdbscenarioName = paste(dbscenarioName, "_", demand_factor,"_", sep = '')
  comment = paste("demand sensitivity of +", demand_factor - 1, "%", sep = '')

  # clone data structure with new scenario name
  ixScenario = ixScenarioOriginal$clone(new_model = newmodelName, new_scen = newscenarioName, annotation = comment, keep_sol = FALSE)

  # check cloned scenario out
  ixScenario$check_out()

  # insert new demands into the model - 7 standard demands in 11 standard regions
  for (node in node_list) {
    for (commodity in commodity_list) {
      for (year in period_list) {
      ixScenario$add_par("demand",paste(node,commodity,"useful",as.character(year),"year",sep='.'),filter(data.table, DEMAND == commodity & REGION == node & YEAR == as.character(year))$VALUE * demand_factor,"GWa")
      }
    }
  }

#----------------------------------------------------- 
  # include BECCS by constrainig activity of 'bco2_tr_dis' technology to 1E6 in all years
  for (node in node_list) {
   for (year in period_list) {
    ixScenario$add_par("bound_activity_up",paste(node, "bco2_tr_dis", as.character(year), "M1", "year", sep='.'), 1E6, "GWa")
   }
  }
#-----------------------------------------------------
  # include Fossil CCS by constrainig activity of 'co2_tr_dis' technology to 1E6 in all years
  for (node in node_list) {
   for (year in period_list) {
    ixScenario$add_par("bound_activity_up",paste(node, "co2_tr_dis", as.character(year), "M1", "year", sep='.'), 1E6, "GWa")
   }
 }
#------------------------------------------------------ 
  #phasing-out nuclear power plants by constrianing activities of "nuc_lc" and "nuc_hc" to 0
  for (node in node_list) {
    for (year in period_list) {
    ixScenario$add_par("bound_new_capacity_up",paste(node, "nuc_lc", as.character(year), "M1", "year", sep='.'), 0, "GWa")
    }
  }
#------------------------------------------------------
  # Phasing-out biomass by constrianing activities of "fixd_land" to 0
  for (node in node_list) {
    for (year in period_list) {
    ixScenario$add_par("bound_activity_up",paste(node, "fixed_land", as.character(year), "M1", "year", sep='.'), 0, "GWa")
    }
  }
#------------------------------------------------------
###*****************************************************************************************************

####****************************************************************************************************
  ## commit scenario to platform and set as default
  ixScenario$commit(comment)
  ixScenario$set_as_default()

  ## run MESSAGE scenario in GAMS and import results in ix platform
  setwd(modelpath)
  ixScenario$solve(model = "MESSAGE", case = newdbscenarioName)

  # start Python-based reporting script
  setwd(paste(message_data_path, "/post-processing/reporting", sep = ''))
  system(paste("python iamc_report.py --scenario ",'"', newscenarioName,'"', " --scenario_out ",'"', dbscenarioName,'"', " --model ",'"', newmodelName,'"', " --model_out ",'"', dbmodelName,'"', "", sep = ''))

 ## read reporting files with "historical" periods
 # path.history = "P:/ene.model/ALPS_1.5C_demand/scenario_output"
 # setwd(path.history)
 # data.nopolicy <- read.xlsx("CD_Links_R2.3.1_20170906_upload.xlsx", sheetName = "data_NoPolicy_V3")
 # data.npi <- read.xlsx("CD_Links_R2.3.1_20170906_upload.xlsx", sheetName = "data_NPi_V3")

 ## solution file
 # path.solution = "C:/krey/git/message_ix/data_utils/upload"
 # file.solution = paste(dbmodelName, "_", dbscenarioName, ".xlsx", sep = "")
 # setwd(path.solution)
 # data.solution <- read.xlsx(file.solution, sheetName = "data")
 setwd(paste(message_data_path, "/post-processing/output", sep = ''))
 file.solution = paste(dbmodelName, "_", newdbscenarioName, ".xlsx", sep = "")


 ## merge reporting xlsx file with other files with "historical" periods
 # data.merged = inner_join(inner_join(select(data.nopolicy, Region, Variable, X2000, X2005, X2010), select(data.npi, Region, Variable, X2020)), select(data.solution, Model, Scenario, Region, Variable, Unit, X2030, X2040, X2050, X2060, X2070, X2080, X2090, X2100, X2110)) %>% select(Model, Scenario, Region, Variable, Unit, X2000, X2005, X2010, X2020, X2030, X2040, X2050, X2060, X2070, X2080, X2090, X2100, X2110)
 # names(data.merged) = c("Model", "Scenario", "Region", "Variable", "Unit", "2000", "2005", "2010", "2020", "2030", "2040", "2050", "2060", "2070", "2080", "2090", "2100", "2110")
 # file.merged = paste(dbmodelName, "_", dbscenarioName, "_merged.xlsx", sep = "")
 # write.xlsx(data.merged, file.merged, sheetName = "data", row.names = FALSE)

 # upload file to database
 database_upload(file.merged, path = getwd(), user = "guof", password = "badpassword")
}

