#Creates 4 new IAM scenarios including a representation of
# Solves the models on the local machine
# Uploads the solutions to the ix DB

# NOTE: Requires core model configured with full commodity balance

rm(list=ls())
graphics.off()

options(java.parameters = "-Xmx16g")

library("rixmp")
library("rmessageix")

require(rgdal)
require(raster)
require(rgeos)
require(countrycode)
require(RCurl)
require(xlsx)
require(tidyr)
require(dplyr)

## Set path ixmp folder in message_ix working copy
message_ix_path = Sys.getenv("MESSAGE_IX_PATH")

## Set path data folder in message_ix working copy
data_path = Sys.getenv("IIASA_DATA_PATH")

# Country region mapping key
country_region_map_key.df = data.frame( read.csv( paste( data_path, 'Water/water_demands/country_region_map_key.csv',  sep = '/' ), stringsAsFactors=FALSE) )

# SSP scenario - currently only configured for SSP2
ssss = 'SSP2'

# Rounding parameter for decimals
rnd = 6

# list of model-scenario combinations
model_scenarios = data.frame( 	model = c( "MESSAGE-GLOBIOM CD-LINKS R2.3.1", "MESSAGE-GLOBIOM CD-LINKS R2.3.1" , "MESSAGE-GLOBIOM CD-LINKS R2.3.1" , "MESSAGE-GLOBIOM CD-LINKS R2.3.1" ,"MESSAGE-GLOBIOM CD-LINKS R2.3.1" ),
               scenario = c( "baseline", "baseline", "baseline", "baseline", "baseline" ),
               newscenarioName = c( "baseline_globiom_base_watbaseline", "baseline_globiom_SDG_sdg6eff", "baseline_globiom_SDG_sdg6supp", "baseline_globiom_SDG_sdg6supp2", "baseline_globiom_SDG_sdg6led" )
               )

# launch the IX modeling platform using the default central ORCALE database
ix_platform = ixmp.Platform( dbprops = 'ixmp.properties' )

for( sc in 1:nrow(model_scenarios) )
 {

 gc()

 # existing model and scenario names to start with
 modelName = as.character( unlist( model_scenarios$model[sc] ) )
 scenarioName = as.character( unlist( model_scenarios$scenario[sc] ) )

 # new model and scenario name for ix platform
 newmodelName = as.character( unlist( model_scenarios$model[sc] ) )
 newscenarioName = as.character( unlist( model_scenarios$newscenarioName[sc] ) )

 # Comment
 comment = paste("water infrastructure included", sep = '')

 # check if basis for scenarip exists
 scen_exists = tryCatch( ix_platform$Scenario( model=modelName, scen=paste(scenarioName,'watbasis',sep='_') ), error = function(e){} )
 if( is.null(scen_exists) )
   {

   print('no basis  - creating from existing scenario')

   ixDStemp = ix_platform$Scenario( model=modelName, scen=scenarioName )

   # clone data structure with new scenario name
   ixDS0 = ixDStemp$clone( new_model = modelName, new_scen = paste(scenarioName,'watbasis',sep='_'), annotation = comment, keep_sol = FALSE )

   # check out and remove original datastructure
   ixDS0$check_out()

   # Remove some of the old water implementation - this takes about an hour
   chk = tryCatch( ixDS0$par('bound_activity_up', list(technology = 'extract__saline_supply')), error = function(e){} )
   if( !is.null(chk) ){ ret = lapply( 1:nrow(chk), function(r){ ixDS0$remove_par( 'bound_activity_up', paste( as.character( as.matrix(unlist(chk[r, 1:(which(names(chk)=='value')-1)])) ), collapse = '.' ) ) } ) }
   chk = tryCatch( ixDS0$par('input', list(commodity = 'saline_supply')), error = function(e){} )
   if( !is.null(chk) ){ ret = lapply( 1:nrow(chk), function(r){ ixDS0$remove_par( 'input', paste( as.character( as.matrix(unlist(chk[r, 1:(which(names(chk)=='value')-1)])) ), collapse = '.' ) ) } ) }
   chk = tryCatch( ixDS0$par('emission_factor', list(emission = c('fresh_wastewater','saline_wastewater'))), error = function(e){} )
   if( !is.null(chk) ){ ret = lapply( 1:nrow(chk), function(r){ ixDS0$remove_par( 'emission_factor', paste( as.character( as.matrix(unlist(chk[r, 1:(which(names(chk)=='value')-1)])) ), collapse = '.' ) ) } ) }

   ixDS0$commit(paste(comment,'basis for water policies',sep=' '))
   ixDS0$set_as_default()

   rm(ixDStemp)
   rm(ixDS0)

   }

 ixDSoriginal = ix_platform$Scenario( model=modelName, scen=paste(scenarioName,'watbasis',sep='_') )

 print('basis loaded successfully')

 # Branching points for energy efficiency and cost of water technologies
 parameter_levels = 'mid'
 if( model_scenarios$newscenarioName[sc] %in% c("baseline_globiom_SDG_sdg6supp","baseline_globiom_SDG_sdg6supp2","baseline_globiom_SDG_sdg6eff","baseline_globiom_SDG_sdg6led") ){ water_policies = 'SDG6' }else{ water_policies = 'NoWatPol' }
 if( model_scenarios$newscenarioName[sc] %in% c("baseline_globiom_SDG_sdg6eff","baseline_globiom_SDG_sdg6led") ){ parameter_levels2 = 'high' }else{ parameter_levels2 = 'mid' }

 #-------------------------------------------------------------------------------
 # Load the MESSAGE-GLOBIOM SSP baseline scenario
 #-------------------------------------------------------------------------------

 # model and scenario names for database
 comment = paste( "Water infrastructure and policies", newscenarioName, sep = '' )

 # clone data structure with new scenario name
 ixDS = ixDSoriginal$clone( new_model = modelName, new_scen = newscenarioName, annotation = comment, keep_sol = FALSE )

 # check out
 ixDS$check_out()

 #-------------------------------------------------------------------------------------------------------
 # Compare the data in the database to the data in the csv files to determine the cooling technologies to include
 #-------------------------------------------------------------------------------------------------------

 # Import raw cooling water data:
 # Input data file 1: CSV containing the water use coefficients (incl. parasitic electricity) for each MESSAGE technology
 # Source for power plant cooling water use: Meldrum et al. 2013
 # Source for hydropower water use: Taken as an average across Grubert et al 2016 and Scherer and Pfister 2016.
 # Parasitic electricity requirements estimated from Loew et al. 2016.
 # All other water coefficients come from Fricko et al 2016.
 # To compile the data, a complete list of technologies from MESSAGE was initially output to CSV.
 # The water parameters were then checked and entered manually based on data reported in the sources above.
 tech_water_performance_ssp_msg_raw.df = data.frame(read.csv(paste(data_path,'water/water_parameters_message_ix','tech_water_performance_ssp_msg.csv',sep='/'),stringsAsFactors=FALSE))

 # Input data file 2: CSV containing the regional shares of each cooling technology and the investment costs
 # The regional shares are estimated using the dataset from Raptis and Pfister 2016 and country boundaries from the GADM dataset
 # Each plant type in the Raptis and Pfister dataset is mapped to the message technologies. The fraction is calculated using
 # the total capacity identified for each cooling technology in each country.  These results are aggregated into the message 11 regions.
 # The costs are estimated from Loew et al. 2016.
 cooltech_cost_and_shares_ssp_msg_raw.df = data.frame(read.csv(paste(data_path,'water/water_parameters_message_ix','cooltech_cost_and_shares_ssp_msg.csv',sep='/'),stringsAsFactors=FALSE))

 # Define alternate id for cooling technologies from csv file
 cooltech_cost_and_shares_ssp_msg_raw.df$alt_id = apply( cbind(cooltech_cost_and_shares_ssp_msg_raw.df[,1:2]), 1, paste, collapse='__' )

 # Get the names of the technologies in the MESSAGE IAM from the database
 technologies_in_message = ixDS$set( 'technology' )

 # Get the name of the regions
 region = as.character( ixDS$set('cat_node')$node )

 # Get the name of the commodities
 commodity = ixDS$set( 'commodity' )

 # Technology categories
 cat_tec = ixDS$set('cat_tec')

 # Whether or not to use initial shares from Davies et al 2013 - alternatively uses shares estimated with the dataset from Raptis et al 2016.
 use_davies_shares = FALSE

 # Fraction of desal demand
 desal_fraction = 1

 # Freshwater extraction average electricity input
 fw_electricity_input = round( data.frame( low = 0.1 * ( 1e3 / (24 * 365) ) , mid = 0.2 * ( 1e3 / (24 * 365) ), high = 0.3 * ( 1e3 / (24 * 365) ) ), digits = 4 )

 # Model years
 model_years = ixDS$set('year')
 dmd_yrs = model_years[ which( model_years >= min(as.numeric(as.character(ixDS$par('demand',list(node = 'R11_NAM'))[,'year']))) & model_years <= max(as.numeric(as.character(ixDS$par('demand',list(node = 'R11_NAM'))[,'year']))) ) ]
 firstmodelyear = as.numeric( as.character( ixDS$set('cat_year')[which(as.character(unlist(ixDS$set('cat_year')[,'type_year']))=='firstmodelyear'),'year'] ) )
 lastmodelyear = as.numeric( as.character( ixDS$set('cat_year')[which(as.character(unlist(ixDS$set('cat_year')[,'type_year']))=='lastmodelyear'),'year'] ) )

 # Model timeslices
 model_time = ixDS$set('time')

 # Which technologies in MESSAGE are also included in csv files?
 message_technologies_with_water_data = as.character(tech_water_performance_ssp_msg_raw.df$technology_name)[ which( as.character(tech_water_performance_ssp_msg_raw.df$technology_name) %in% technologies_in_message )]
 message_technologies_without_water_data = as.character(tech_water_performance_ssp_msg_raw.df$technology_name)[ which( !(as.character(tech_water_performance_ssp_msg_raw.df$technology_name) %in% technologies_in_message ) )]
 message_technologies_with_water_data_alt = as.character(technologies_in_message)[ which(  technologies_in_message %in% as.character(tech_water_performance_ssp_msg_raw.df$technology_name) )]
 message_technologies_without_water_data_alt = as.character(technologies_in_message)[ which(  !(technologies_in_message %in% as.character(tech_water_performance_ssp_msg_raw.df$technology_name) )) ]

 # Define a common mode using the existing DB settings
 mode_common = ixDS$set('mode')[1] # Common mode name from set list

 # Define hydropower technologies
 hydro_techs = c('hydro_hc','hydro_lc')

 #-------------------------------------------------------------------
 # Add LED demands from Gruebler et al. 2018 - only if LED scenario
 #-------------------------------------------------------------------

 if( as.character( unlist( model_scenarios$newscenarioName[sc] ) ) == "baseline_globiom_SDG_sdg6led" )
   {

   # read demands from xlsx file

   path.data = paste( data_path, '/water/water_demands/ALPS_1.5C_demand/demands_inp_files', sep = '' )
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
   ##period_list = seq(2020, 2110, 10)  ## use this command if we want to change model parameters from 2020 onward
   period_list = seq(2030, 2110, 10)  ## use this command if we want to change model parameters from 2030 onward (i.e., if a slice was used up to 2020 in the scenario that this one is cloned from)
   commodity_list = levels(data.table$DEMAND)
   commodity_list = commodity_list[commodity_list != "shipping"]  ## remove the "shipping" commodity from the list, since this is only for the "R11_GLB" region and we want to treat this separately in the code below

   #-------------------------------------------------------------------------------

   # read technology investment costs from xlsx file
   path.data.invcosts = paste( data_path, '/water/water_demands/ALPS_1.5C_demand/technology_inp_files', sep='' )
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
   path.data.fomcosts = paste( data_path, '/water/water_demands/ALPS_1.5C_demand/technology_inp_files', sep = '' )
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
   path.data.vomcosts = paste( data_path, '/water/water_demands/ALPS_1.5C_demand/technology_inp_files', sep = '' )
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
   path.data.steps = paste( data_path, '/water/water_demands/ALPS_1.5C_demand/technology_inp_files', sep='' )
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
   path.data.oper = paste( data_path, '/water/water_demands/ALPS_1.5C_demand/technology_inp_files', sep = '' )
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
   path.data.resm = paste( data_path, '/water/water_demands/ALPS_1.5C_demand/technology_inp_files', sep = '' )
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
   path.data.uefuel = paste( data_path, '/water/water_demands/ALPS_1.5C_demand/technology_inp_files', sep = '' )
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
       ixDS$add_par("demand",paste(node,commodity,"useful",as.character(year),"year",sep='.'),filter(data.table, DEMAND == commodity & REGION == node & YEAR == as.character(year))$VALUE,"GWa")
     }
     }
   }


   # insert new demands into the model - shipping demands in GLB region only
   for (year in period_list) {
     ixDS$add_par("demand",paste("R11_GLB","shipping","useful",as.character(year),"year",sep='.'),filter(data.table, DEMAND == "shipping" & REGION == "R11_GLB" & YEAR == as.character(year))$VALUE,"GWa")
   }


   # insert emission constraint leading to different carbon budgets (2011-2100)
   #
   # But actually in MESSAGEix, the budget is defined from the starting year up to 2110, and in terms of annual average emissions over that timeframe
   # 400 GtCO2 (1.5 deg C): 1800 MtCeq is for 2011-2110
   # 400 GtCO2 (1.5 deg C): 433 MtCeq is for 2021-2110
   #
   #ixDS$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 1150, "tC")
   #ixDS$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 450, "tC")  ## This budget (450 MtC; for 2021-2110) produced an infeasible solution on 2017-08-09 using Simon's initial demands from 2017-08-06.
   #ixDS$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 1800, "tC")  ## This budget (for 2021-2110) was run in scenario '2C_NPi2020_noBECCS_V1' to test that a more relaxed budget would be feasible in the current set-up (low demands from Simon on 2017-08-06 and also no BECCS).
   #ixDS$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 1200, "tC")  ## This budget (for 2021-2110) was run in scenario 'b2C_NPi2020_noBECCS_V1' to test that a more relaxed budget would be feasible in the current set-up (low demands from Simon on 2017-08-06 and also no BECCS).
   ##ixDS$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 800, "tC")  ## Budget of 800 MtC annual average per year (for 2021-2110) was found to be too tight in an earlier run of scenario 'b2C_NPi2020_noBECCS_V2'. Infeasible solution; model did not solve.
   #ixDS$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 1000, "tC")  ## This budget (for 2021-2110) was run in scenarios 'b2C_NPi2020_noBECCS_V2' and similar up to 'V6' to get something around 1.5C. (The same budget was attempted for the initial V7 run, but the removal of Fossil CCS made it infeasible.) Median temperature is around 1.57C in 2050 and 1.31C in 2100. Median radiative forcing (total) is 1.81 W/m2 in 2100. (low demands from Simon on 2017-08-06 and also no BECCS).
   #ixDS$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 1200, "tC")  ## This budget (for 2021-2110) was run in scenarios 'b2C_NPi2020_noBECCS_V7' to get something around 1.5C. (A budget of 1000 was initially attempted for V7, but the lack of Fossil CCS made it infeasible.) Median temperature is around 1.60C in 2050 and 1.34 in 2100. Median radiative forcing (total) is 1.89 W/m2 in 2100. (low demands from Simon on 2017-08-31 and also no BECCS and no Fossil CCS).
   #ixDS$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 1400, "tC")  ## This budget (for 2021-2110) was run in scenarios 'b2C_NPi2020_noBECCS_V7a' to 'V8' to get something around 1.5C. (A budget of 1200 was initially used for V7, but the carbon prices were ridiculously higher.) Median temperature is around 1.61C in 2050 and 1.37 in 2100. Median radiative forcing (total) is 1.94 W/m2 in 2100. (low demands from Simon on 2017-09-08 and also no BECCS and no Fossil CCS).
   #ixDS$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 2500, "tC")  ## This budget (for 2021-2110) was run in scenarios '2C_NPi2020_noBECCS_V2' to try and get something nearer to 2C by end of century. Median temperature is around 1.66C in 2050 and 1.53C in 2100. Median radiative forcing (total) is 2.22 W/m2 in 2100. (low demands from Simon on 2017-08-06 and also no BECCS).
   #ixDS$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 3500, "tC")  ## This budget (for 2021-2110) was run in scenarios '2C_NPi2020_noBECCS_V3' to try and get something nearer to 2C by end of century. Median temperature is around 1.71C in 2050 and 1.67C in 2100. Median radiative forcing (total) is 2.48 W/m2 in 2100. (low demands from Simon on 2017-08-06 and also no BECCS).
   #ixDS$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 4000, "tC")  ## This budget (for 2021-2110) was run in scenarios '2C_NPi2020_noBECCS_V4' to try and get something nearer to 2C by end of century. Median temperature is around 1.73C in 2050, peaking at 1.79C in 2070/2080, and then coming back down to 1.74C in 2100. Median radiative forcing (total) is 2.6 W/m2 in 2100. (low demands from Simon on 2017-08-06 and also no BECCS).

   # exclude BECCS by constrainig activity of 'bco2_tr_dis' technology to 0 in all years
   for (node in node_list) {
     for (year in period_list) {
     ixDS$add_par("bound_activity_up",paste(node, "bco2_tr_dis", as.character(year), "M1", "year", sep='.'), 0, "GWa")
     }
   }

   # exclude Fossil CCS by constrainig activity of 'co2_tr_dis' technology to 0 in all years
   for (node in node_list) {
     for (year in period_list) {
     ixDS$add_par("bound_activity_up",paste(node, "co2_tr_dis", as.character(year), "M1", "year", sep='.'), 0, "GWa")
     }
   }

   # adjust limit to electrify transport to XX% of useful energy (negative number, i.e. -0.9 refers to 90%)
   for (node in node_list) {
     for (year in period_list) {
     #ixDS$add_par("relation_activity",paste("UE_transport_electric", node, as.character(year), node, "useful_transport", as.character(year), "M1", sep='.'), -0.9, "-")
     ixDS$add_par("relation_activity",paste("UE_transport_electric", node, as.character(year), node, "useful_transport", as.character(year), "M1", sep='.'), -0.8, "-")
     }
   }

   # adjust limit for hydrogen in transport to 100% of useful energy (negative number, i.e. -1.0 refers to 100%)
   for (node in node_list) {
     for (year in period_list) {
     ixDS$add_par("relation_activity",paste("UE_transport_fc", node, as.character(year), node, "useful_transport", as.character(year), "M1", sep='.'), -1.0, "-")
     }
   }

   # adjust efficiency of the hydrogen transport technology to make it more efficient; a value of 0.45 means that the hydrogen transport technology is 2.22 times more efficient than the liquid fuel combustion technology (0.45 = 1/2.22)
   # but only change parameters for vintage-activity year combinations that are currently specified for this technology in the model
   technology = "h2_fc_trp"
   for (node in node_list) {
    for (year_vtg in period_list) {
     if (any(class(tryCatch((ixDS$years_active(node, technology, year_vtg)), error = function(e) e)) == "error")) {
       print(paste(node, technology, as.character(year_vtg), sep='.'))
       print('Error: technology does not exist in this region for this vintage year')
     } else {
       years_tec_active = ixDS$years_active(node, technology, year_vtg)
       for (year_act in years_tec_active) {
       #print(ixDS$add_par("input",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", node, "lh2", "final", "year", "year", sep='.'), 0.45, "GWa"))
       ixDS$add_par("input",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", node, "lh2", "final", "year", "year", sep='.'), 0.45, "GWa")
       }
     }
    }
   }

   # # adjust investment cost of the hydrogen transport technology to make it less expensive; 'h2_fc_trp' is the only end-use transport technology with an explicit 'inv_cost' on it; all others have an indirect cost that comes in through the 'weight_trp' relation
   # # but only change parameters for vintage years that are currently specified for this technology in the model
   # technology = "h2_fc_trp"
   # for (node in node_list) {
   #   for (year_vtg in period_list) {
   #     if (any(class(tryCatch((ixDS$years_active(node, technology, year_vtg)), error = function(e) e)) == "error")) {
   #       print(paste(node, technology, as.character(year_vtg), sep='.'))
   #       print('Error: technology does not exist in this region for this vintage year')
   #     } else {
   #       ixDS$add_par("inv_cost",paste(node, technology, as.character(year_vtg), sep='.'), 1.0, "USD/GWa")  # specify units in 'USD/GWa', even though they are actually $/kW within the model
   #     }
   #   }
   # }

   # adjust investment costs of a set of technologies that are defined in another XLS file
   # but only change parameters for vintage years that are currently specified for this technology in the model
   #technology = "h2_fc_trp"
   for (node in node_list) {
     for (year_vtg in period_list) {
     for (technology in technology_list) {
       if (any(class(tryCatch((ixDS$years_active(node, technology, year_vtg)), error = function(e) e)) == "error")) {
       print(paste(node, technology, as.character(year_vtg), sep='.'))
       print('Error: technology does not exist in this region for this vintage year')
       } else {
       ixDS$add_par("inv_cost",paste(node, technology, as.character(year_vtg), sep='.'), (filter(data.table.invcosts, REGION == node & TECHNOLOGY == technology & YEAR == as.character(year_vtg))$VALUE), "USD/GWa")  # specify units in 'USD/GWa', even though they are actually $/kW within the model
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
       if (any(class(tryCatch((ixDS$years_active(node, technology, year_vtg)), error = function(e) e)) == "error")) {
       print(paste(node, technology, as.character(year_vtg), sep='.'))
       print('Error: technology does not exist in this region for this vintage-activity year combination')
       } else {
       #ixDS$add_par("fix_cost",paste(node, technology, as.character(year_vtg), as.character(year_act), sep='.'), (filter(data.table.fomcosts, REGION == node & TECHNOLOGY == technology & YEAR == as.character(year_act))$VALUE), "USD/GWa")  # specify units in 'USD/GWa', even though they are actually $/kWyr/yr within the model
       years_tec_active = ixDS$years_active(node, technology, year_vtg)
       for (year_act in years_tec_active) {
         #print(ixDS$add_par("fix_cost",paste(node, technology, as.character(year_vtg), as.character(year_act), sep='.'), (filter(data.table.fomcosts, REGION == node & TECHNOLOGY == technology & YEAR == as.character(year_act))$VALUE), "USD/GWa"))
         ixDS$add_par("fix_cost",paste(node, technology, as.character(year_vtg), as.character(year_act), sep='.'), (filter(data.table.fomcosts, REGION == node & TECHNOLOGY == technology & YEAR == as.character(year_act))$VALUE), "USD/GWa")
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
       if (any(class(tryCatch((ixDS$years_active(node, technology, year_vtg)), error = function(e) e)) == "error")) {
       print(paste(node, technology, as.character(year_vtg), sep='.'))
       print('Error: technology does not exist in this region for this vintage-activity year combination')
       } else {
       #ixDS$add_par("var_cost",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", "year", sep='.'), (filter(data.table.fomcosts, REGION == node & TECHNOLOGY == technology & YEAR == as.character(year_act))$VALUE), "USD/GWa")  # specify units in 'USD/GWa', even though they are actually $/kWyr/yr within the model
       years_tec_active = ixDS$years_active(node, technology, year_vtg)
       for (year_act in years_tec_active) {
         #print(ixDS$add_par("var_cost",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", "year", sep='.'), (filter(data.table.fomcosts, REGION == node & TECHNOLOGY == technology & YEAR == as.character(year_act))$VALUE), "USD/GWa"))
         ixDS$add_par("var_cost",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", "year", sep='.'), (filter(data.table.fomcosts, REGION == node & TECHNOLOGY == technology & YEAR == as.character(year_act))$VALUE), "USD/GWa")
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
       ixDS$add_par("relation_activity",paste(relation, node, as.character(year_act), node, technology, as.character(year_act), "M1", sep='.'), (filter(data.table.steps, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE), "-")
       #print(paste(node, technology, relation, as.character(year_act), sep='.'),(filter(data.table.steps, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE))

       # if (any(class(tryCatch((ixDS$years_active(node, technology, year_act)), error = function(e) e)) == "error")) {
       #   print(paste(node, technology, as.character(year_act), sep='.'))
       #   print('Error: technology does not exist in this region for this activity year')
       # } else {
       #   ixDS$add_par("relation_activity",paste(relation, node, as.character(year_act), node, technology, as.character(year_act), "M1", sep='.'), (filter(data.table.steps, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE), "-")
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
       ixDS$add_par("relation_activity",paste(relation, node, as.character(year_act), node, technology, as.character(year_act), "M1", sep='.'), (filter(data.table.oper, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE), "-")
       #print(paste(node, technology, relation, as.character(year_act), sep='.'),(filter(data.table.oper, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE))

       # if (any(class(tryCatch((ixDS$years_active(node, technology, year_act)), error = function(e) e)) == "error")) {
       #   print(paste(node, technology, as.character(year_act), sep='.'))
       #   print('Error: technology does not exist in this region for this activity year')
       # } else {
       #   #ixDS$add_par("relation_activity",paste(relation, node, as.character(year_act), node, technology, as.character(year_act), "M1", sep='.'), (filter(data.table.oper, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE), "-")
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
       ixDS$add_par("relation_activity",paste(relation, node, as.character(year_act), node, technology, as.character(year_act), "M1", sep='.'), (filter(data.table.resm, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE), "-")
       #print(paste(node, technology, relation, as.character(year_act), sep='.'),(filter(data.table.resm, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE))
       }
     }
     }
   }

   # adjust limits to potential fuel-specific contributions at useful energy level (in each end-use sector separately); done for a set of technologies and relations that are defined in another XLS file
   for (node in node_list) {
     for (year_act in period_list) {
     for (relation in relation_list5) {
       for (technology in technology_list5) {
       #ixDS$add_par("relation_activity",paste(relation, node, as.character(year_act), node, technology, as.character(year_act), "M1", sep='.'), (filter(data.table.uefuel, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE), "-")
       #print(paste(node, technology, relation, as.character(year_act), sep='.'),(filter(data.table.uefuel, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE))
       #print(paste(node, technology, relation, as.character(year_act), sep='.'))

       if (any(class(tryCatch(ixDS$add_par("relation_activity",paste(relation, node, as.character(year_act), node, technology, as.character(year_act), "M1", sep='.'), (filter(data.table.uefuel, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE), "-"), error = function(e) e)) == "error")) {
         #print(paste(node, technology, relation, as.character(year_act), sep='.'))
         #print('Error: technology either does not exist in this region for this activity year or it does not write into this relation')
       } else {
         ixDS$add_par("relation_activity",paste(relation, node, as.character(year_act), node, technology, as.character(year_act), "M1", sep='.'), (filter(data.table.uefuel, REGION == node & TECHNOLOGY == technology & RELATION == relation & YEAR == as.character(year_act))$VALUE), "-")
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
     ixDS$add_par("bound_activity_up",paste(node, technology, as.character(year), "M1", "year", sep='.'), 0, "GWa")
     ixDS$remove_par("bound_activity_up",paste(node, technology, as.character(year), "M1", "year", sep='.'))
     }
   }
   ### This block of code should have worked in the for loop above for removing parameters, but I couldn't get it working.
     # if (any(class(tryCatch((ixDS$remove_par("bound_activity_up",paste(node, technology, as.character(year), "M1", "year", sep='.'))), error = function(e) print('...'))) == "error")) {
     #   print(paste(node, technology, as.character(year), "M1", "year", sep='.'))
     #   print('Error: technology does not have an upper bound in this region for this vintage year, thus none added')
     # } else {
     #   ixDS$remove_par("bound_activity_up",paste(node, technology, as.character(year), "M1", "year", sep='.'))
     # }

   ### Some commands for testing. Can be deleted.
   #ixDS$remove_par("bound_activity_up",paste(node, "h2_fc_trp", as.character(year), "M1", "year", sep='.'))
   #ixDS$remove_par("bound_activity_up",paste("R11_AFR", "h2_fc_trp", as.character(2060), "M1", "year", sep='.'))
   #any(class(tryCatch((ixDS$remove_par("bound_activity_up",paste("R11_AFR", technology, 2030, "M1", "year", sep='.'))), error = function(e) print('...'))) == "error")

   # Exclude ethanol fuel cell transport technology by constraining activity to 0 in all years
   # Need to do this even though the Web UI does not show that there are upper bounds on this technology in any years.
   for (node in node_list) {
     for (year in period_list) {
     ixDS$add_par("bound_activity_up",paste(node, "eth_fc_trp", as.character(year), "M1", "year", sep='.'), 0, "GWa")
     }
   }

   # Exclude methanol fuel cell transport technology by constraining activity to 0 in all years
   # Need to do this even though the Web UI does not show that there are upper bounds on this technology in any years.
   for (node in node_list) {
     for (year in period_list) {
     ixDS$add_par("bound_activity_up",paste(node, "meth_fc_trp", as.character(year), "M1", "year", sep='.'), 0, "GWa")
     }
   }


   # Increase the initial starting point value for activity growth bounds on the electric transport technology
   years_subset = c(2030)
   for (node in node_list) {
     for (year in years_subset) {
     ixDS$add_par("initial_activity_up",paste(node, "elec_trp", as.character(year), "year", sep='.'), 90, "GWa")
     }
   }

   # Increase the initial starting point value for activity growth bounds on the hydrogen fuel cell technology in transport
   years_subset = c(2030, 2040, 2050)
   technology = "h2_fc_trp"
   for (node in node_list) {
     for (year in years_subset) {
     #ixDS$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa")

     #if (any(class(tryCatch(ixDS$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa"), error = function(e) e)) == "error")) {
     if (any(class(tryCatch((ixDS$years_active(node, technology, year)), error = function(e) e)) == "error")) {
       print(paste(node, technology, as.character(year), sep='.'))
       print('Error: technology does not exist in this region for this activity year')
     } else {
       ixDS$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa")
     }

     }
   }

   # Increase the initial starting point value for activity growth bounds on the hydrogen fuel cell technology in industry
   years_subset = c(2030, 2040, 2050)
   technology = "h2_fc_I"
   for (node in node_list) {
     for (year in years_subset) {
     #ixDS$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa")

     #if (any(class(tryCatch(ixDS$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa"), error = function(e) e)) == "error")) {
     if (any(class(tryCatch((ixDS$years_active(node, technology, year)), error = function(e) e)) == "error")) {
       print(paste(node, technology, as.character(year), sep='.'))
       print('Error: technology does not exist in this region for this activity year')
     } else {
       ixDS$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa")
     }

     }
   }

   # Increase the initial starting point value for activity growth bounds on the hydrogen fuel cell technology in industry
   years_subset = c(2030, 2040, 2050)
   technology = "h2_fc_RC"
   for (node in node_list) {
     for (year in years_subset) {
     #ixDS$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa")

     #if (any(class(tryCatch(ixDS$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa"), error = function(e) e)) == "error")) {
     if (any(class(tryCatch((ixDS$years_active(node, technology, year)), error = function(e) e)) == "error")) {
       print(paste(node, technology, as.character(year), sep='.'))
       print('Error: technology does not exist in this region for this activity year')
     } else {
       ixDS$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa")
     }

     }
   }

   # adjust efficiency of the hydrogen fuel cell co-generation technology (electricity + heat) in the RES-COM sector (satisfying both RC-specific and RC-thermal useful demands), in order to make it more efficient; a value of 2.1 means the technology has a combined efficiency (electricity + heat) of 0.86 (2.1 units of hydrogen input produces 1.0 units of electricity and 0.80 units of heat => thus 1.8/2.1 = 0.86)
   # but only change parameters for vintage-activity year combinations that are currently specified for this technology in the model
   technology = "h2_fc_RC"
   for (node in node_list) {
     for (year_vtg in period_list) {
     if (any(class(tryCatch((ixDS$years_active(node, technology, year_vtg)), error = function(e) e)) == "error")) {
       print(paste(node, technology, as.character(year_vtg), sep='.'))
       print('Error: technology does not exist in this region for this vintage year')
     } else {
       years_tec_active = ixDS$years_active(node, technology, year_vtg)
       for (year_act in years_tec_active) {
       #print(ixDS$add_par("input",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", node, "hydrogen", "final", "year", "year", sep='.'), 2.1, "GWa"))
       ixDS$add_par("input",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", node, "hydrogen", "final", "year", "year", sep='.'), 2.1, "GWa")
       }
     }
     }
   }

   # adjust efficiency of the hydrogen fuel cell co-generation technology (electricity + heat) in the IND sector (satisfying both IND-specific and IND-thermal useful demands), in order to make it more efficient; a value of 2.0 means the technology has a combined efficiency (electricity + heat) of 0.86 (2.0 units of hydrogen input produces 1.0 units of electricity and 0.71 units of heat => thus 1.71/2.0 = 0.86)
   # but only change parameters for vintage-activity year combinations that are currently specified for this technology in the model
   technology = "h2_fc_I"
   for (node in node_list) {
     for (year_vtg in period_list) {
     if (any(class(tryCatch((ixDS$years_active(node, technology, year_vtg)), error = function(e) e)) == "error")) {
       print(paste(node, technology, as.character(year_vtg), sep='.'))
       print('Error: technology does not exist in this region for this vintage year')
     } else {
       years_tec_active = ixDS$years_active(node, technology, year_vtg)
       for (year_act in years_tec_active) {
       #print(ixDS$add_par("input",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", node, "hydrogen", "final", "year", "year", sep='.'), 2.1, "GWa"))
       ixDS$add_par("input",paste(node, technology, as.character(year_vtg), as.character(year_act), "M1", node, "hydrogen", "final", "year", "year", sep='.'), 2.1, "GWa")
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
     #ixDS$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa")

     #if (any(class(tryCatch(ixDS$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa"), error = function(e) e)) == "error")) {
     if (any(class(tryCatch((ixDS$years_active(node, technology, year)), error = function(e) e)) == "error")) {
       print(paste(node, technology, as.character(year), sep='.'))
       print('Error: technology does not exist in this region for this activity year')
     } else {
       #print(paste(node, technology, as.character(year), sep='.'))
       ixDS$add_par("initial_activity_up",paste(node, technology, as.character(year), "year", sep='.'), 90, "GWa")
     }

     }
   }


   # Increase the initial starting point value for capacity growth bounds on the solar PV technology (centralized generation)
   years_subset = c(2030, 2040, 2050)
   technology = "solar_pv_ppl"
   for (node in node_list) {
     for (year in years_subset) {
     #ixDS$add_par("initial_new_capacity_up",paste(node, technology, as.character(year), sep='.'), 10, "GW")

     #if (any(class(tryCatch(ixDS$add_par("initial_new_capacity_up",paste(node, technology, as.character(year), sep='.'), 10, "GW"), error = function(e) e)) == "error")) {
     if (any(class(tryCatch((ixDS$years_active(node, technology, year)), error = function(e) e)) == "error")) {
       print(paste(node, technology, as.character(year), sep='.'))
       print('Error: technology does not exist in this region for this vintage year')
     } else {
       #print(paste(node, technology, as.character(year), sep='.'))
       ixDS$add_par("initial_new_capacity_up",paste(node, technology, as.character(year), sep='.'), 10, "GW")
     }

     }
   }

   # read demands from xlsx file

   path.data = paste( data_path, '/water/water_demands/ALPS_1.5C_demand/demands_inp_files', sep='' )
   wds = getwd()
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
   ##period_list = seq(2020, 2110, 10)  ## use this command if we want to change model parameters from 2020 onward
   period_list = seq(2030, 2110, 10)  ## use this command if we want to change model parameters from 2030 onward (i.e., if a slice was used up to 2020 in the scenario that this one is cloned from)
   commodity_list = levels(data.table$DEMAND)
   commodity_list = commodity_list[commodity_list != "shipping"]  ## remove the "shipping" commodity from the list, since this is only for the "R11_GLB" region and we want to treat this separately in the code below

   demand_factor = 1

   # insert new demands into the model - 7 standard demands in 11 standard regions
   res = lapply( node_list, function(node){

     lapply( commodity_list, function(commodity){

       lapply( period_list, function( year ){

         ixDS$add_par("demand",paste(node,commodity,"useful",as.character(year),"year",sep='.'),filter(data.table, DEMAND == commodity & REGION == node & YEAR == as.character(year))$VALUE * demand_factor,"GWa")

         } )

       } )

     } )

   setwd(wds)

   }

 #-------------------------------------------------------------------------------------------------------
 # Add water technologies using historical ppl data and pre-defined regional shares
 #-------------------------------------------------------------------------------------------------------

 print('Initializing water technologies')

 # Cooling technologies:
 # Once through (fresh and saline), closed-loop (fresh) and air cooled options considered
 # No air cooling options for nuclear and CCS
 # Only consider the cooling technologies that correspond to power plant technologies in the MESSAGE model.
 cooling_technologies_to_consider = c( apply( cooltech_cost_and_shares_ssp_msg_raw.df[ which( as.character( cooltech_cost_and_shares_ssp_msg_raw.df$utype ) %in% as.character( message_technologies_with_water_data )  ), c('utype','cooling') ], 1, paste, collapse='__') )

 # Cooling commodities by power plant type - output commodity for cooling technology
 cooling_commodities = unlist( lapply( 1:length( unique( cooltech_cost_and_shares_ssp_msg_raw.df$utype[ as.numeric( names(cooling_technologies_to_consider) )  ] ) ), function(x){ paste( 'cooling', unique( cooltech_cost_and_shares_ssp_msg_raw.df$utype[ as.numeric(names(cooling_technologies_to_consider)) ] )[x], sep='__' ) } ) )
 ret = lapply( 1:length(cooling_commodities), function(x){ ixDS$add_set( 'commodity', as.character( cooling_commodities[x] ) ) } ) # Add commodity to ix DB

 # Water sources - input commodity for cooling technology
 water_supply_type = unique(tech_water_performance_ssp_msg_raw.df$water_supply_type[ which( as.character(tech_water_performance_ssp_msg_raw.df$technology_name) %in% message_technologies_with_water_data | as.character(tech_water_performance_ssp_msg_raw.df$technology_group) == 'cooling' ) ] )
 water_supply_type = c( water_supply_type[-1*which(is.na(water_supply_type))] )
 ret = lapply( c( as.character( water_supply_type ), 'saline_supply_ppl' ), function(x){ ixDS$add_set( 'commodity', x ) } ) # Add commodity to ix DB

 # Water supply and cooling as a new level
 water_supply_level = 'water_supply'
 water_treat_level = 'water_treat'
 ret = ixDS$add_set( 'level', water_supply_level ) # Add level to ix DB
 ret = ixDS$add_set( 'level', water_treat_level ) # Add level to ix DB
 cooling_level = 'cooling'
 ret = ixDS$add_set( 'level', cooling_level ) # Add level to ix DB

 # Wastewater as an emission - could alternatively be included as commodity balance
 ret = ixDS$add_set( 'emission', 'fresh_wastewater' )
 ret = ixDS$add_set( 'emission', 'unfresh_wastewater' )
 ret = ixDS$add_set( 'emission', 'saline_wastewater' ) # some of these should alternatively be defined in the input files

 # Thermal pollution as an emission - consider oceans and rivers
 ret = ixDS$add_set( 'emission', 'fresh_thermal_pollution' )
 ret = ixDS$add_set( 'emission', 'saline_thermal_pollution' )

 # Initialize urban, rural and irrigation water demands
 water_end_use_commodities = c( 'urban_mw', 'urban_dis', 'rural_mw', 'rural_dis' )
 ret = lapply( water_end_use_commodities, function(x){ ixDS$add_set( 'commodity', x ) } ) # Add commodity to ix DB

 # Initialize wastewater commodities
 wastewater_commodities = c( 'urban_collected_wst', 'rural_collected_wst', 'urban_uncollected_wst', 'rural_uncollected_wst' )
 ret = lapply( wastewater_commodities, function(x){ ixDS$add_set( 'commodity', x ) } ) # Add commodity to ix DB

 # Water source extraction technologies - add to technology and type sets
 # Currently distinguishes: freshwater_instream (hydropower), freshwater_supply (all techs using freshwater), saline_supply (ocean and brackish resources) and upstream_landuse (globiom accounting)
 water_source_extraction_techs =  unlist( lapply( 1:length(water_supply_type), function(x){ paste( 'extract', as.character( water_supply_type[x] ), sep='__' ) } ) )
 ret = lapply( 1:length(water_supply_type), function(x){ ixDS$add_set( 'technology', water_source_extraction_techs[x] ) } )
 water_resource_extraction_tech_type = 'water_resource_extraction'
 ret = ixDS$add_set( 'type_tec', water_resource_extraction_tech_type ) # Add to technology types
 ret = lapply( 1:length(water_supply_type), function(x){ ixDS$add_set( 'cat_tec', paste( water_resource_extraction_tech_type , water_source_extraction_techs[x], sep='.') ) } )
 ret = lapply( 1:length(water_supply_type), function(x){ ixDS$add_set( 'cat_tec', paste( 'investment_other' , water_source_extraction_techs[x], sep='.') ) } )

 # Power plant cooling technologies as technology types - will set technology names later
 power_plant_cooling_tech_type = 'power_plant_cooling'
 ixDS$add_set( 'type_tec', power_plant_cooling_tech_type )

 # Create data.frame that stores the relevant cost data from the csv files
 cooling_technology_costs = data.frame( 	inv_costs = unlist( lapply( cooling_technologies_to_consider, function(techs2check){ as.numeric( cooltech_cost_and_shares_ssp_msg_raw.df$investment_million_USD_per_MW_mid[ which( as.character( cooltech_cost_and_shares_ssp_msg_raw.df$alt_id ) == as.character( techs2check ) ) ] ) } ) ),
                     fixed_costs = 0,#unlist( lapply( cooling_technologies_to_consider, function(techs2check){ as.numeric( cooltech_cost_and_shares_ssp_msg_raw.df$fixed_million_USD_per_MW[ which( as.character( cooltech_cost_and_shares_ssp_msg_raw.df$alt_id ) == as.character( techs2check ) ) ] ) } ) ),
                     var_costs = 0,#unlist( lapply( cooling_technologies_to_consider, function(techs2check){ as.numeric( cooltech_cost_and_shares_ssp_msg_raw.df$variable_million_USD_per_MW[ which( as.character( cooltech_cost_and_shares_ssp_msg_raw.df$alt_id ) == as.character( techs2check ) ) ] ) } ) ),
                     row.names = cooling_technologies_to_consider )

 # Recover the names of the cooled technologies in MESSAGE (i.e., the thermal power plants)
 cooled_technologies_in_message = unique( as.character( unlist(data.frame(strsplit( cooling_technologies_to_consider, '__'))[1,]) ) )

 # Manually set efficiencies - data not readily extracted from the DB - currently manually read from .inp file :(
 manually_set_efficiencies = list( 	geo_hpl = 0.850,
                   geo_ppl = 0.385,
                   nuc_hc = 0.326, # couldn't find these numbers in the .inp file so using average heat rate of 3.065 kWh heat per kWh electricity from EIA
                   nuc_lc = 0.326, # couldn't find these numbers in the .inp file so using average heat rate of 3.065 kWh heat per kWh electricity from EIA
                   solar_th_ppl = 0.385 )

 #----------------------------------------------------------------------------------------------------------------------
 # Initialize the full commodity balance set and commodities, as well as the associated equation added to the core model
 #----------------------------------------------------------------------------------------------------------------------

 ixDS$init_set( "full_balance", c( "commodity" ) )
 res = lapply( wastewater_commodities, function( www ){ ixDS$add_set( "full_balance", www ) } )
 ixDS$init_equ( "COMMODITY_BALANCE_FULL", c( "node", "commodity", "level", "year", "time" ) )

 #-------------------------------------------------------------------------------------------------------
 # Add urban and rural demands / return flows as exogenous inputs
 #-------------------------------------------------------------------------------------------------------

 # Import urban and rural demands from csv - convert mcm/year to km3/year
 if( as.character( model_scenarios$newscenarioName[sc] ) == 'baseline_globiom_SDG_sdg6supp' ){ pth = 'sdg6_supp'; ipth = 'baseline' }
 if( as.character( model_scenarios$newscenarioName[sc] ) == 'baseline_globiom_SDG_sdg6eff' ){ pth = 'sdg6_eff'; ipth = 'sdg6_eff' }
 if( as.character( model_scenarios$newscenarioName[sc] ) == 'baseline_globiom_base_watbaseline' ){ pth = 'baseline'; ipth = 'baseline' }
 if( as.character( model_scenarios$newscenarioName[sc] ) == "baseline_globiom_SDG_sdg6led" ){ pth = 'sdg6_eff'; ipth = 'sdg6_eff' }
 if( as.character( model_scenarios$newscenarioName[sc] ) == 'baseline_globiom_SDG_sdg6supp2' ){ pth = 'sdg6_supp'; ipth = 'sdg6_eff' }
 urban_withdrawal.df = 1e-3 * data.frame( read.csv( paste( data_path, '/water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_urban_withdrawal', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
 rural_withdrawal.df = 1e-3 * data.frame( read.csv( paste( data_path, '/water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_rural_withdrawal', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
 urban_return.df = 1e-3 * data.frame( read.csv( paste( data_path, '/water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_urban_return', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
 rural_return.df = 1e-3 * data.frame( read.csv( paste( data_path, '/water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_rural_return', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
 urban_connected_fraction.df = data.frame( read.csv( paste( data_path, '/water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_urban_connection_rate', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
 rural_connected_fraction.df = data.frame( read.csv( paste( data_path, '/water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_rural_connection_rate', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
 urban_treated_fraction.df = data.frame( read.csv( paste( data_path, '/water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_urban_treatment_rate', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
 rural_treated_fraction.df = data.frame( read.csv( paste( data_path, '/water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_rural_treatment_rate', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
 urban_desal_fraction.df = data.frame( read.csv( paste( data_path, '/water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_urban_desalination_rate', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
 urban_reuse_fraction.df = data.frame( read.csv( paste( data_path, '/water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_urban_recycling_rate', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
 irrigation_withdrawal.df = 1e-3 * data.frame( read.csv( paste( data_path, '/water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_irrigation_withdrawal', ipth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )

 # # Import irrigation demands from GLOBIOM and harmonize
 # irrig = ixDSoriginal$par('land_output',list(commodity='Water|Withdrawal|Irrigation'))
 # land = ixDSoriginal$var('LAND')
 # irrigation_withdrawal.df = 1e-3 * data.frame( do.call( cbind, lapply( names( urban_reuse_fraction.df ), function(rr){ sapply( c( row.names( urban_reuse_fraction.df ), '2100' ), function(yy){
   # out = irrig[ which( grepl( rr, irrig$node ) & grepl( as.numeric( yy ), irrig$year ) ) ,  ]
   # lnd = land[ which( grepl( rr, land$node ) & grepl( as.numeric( yy ), land$year ) ) , ]
   # cmb = merge( out, lnd, by = 'land_scenario')
   # res = max( sum( cmb$value * cmb$level.y ), 0, na.rm=TRUE )
   # return(res)
   # } ) } ) ) )
 # names( irrigation_withdrawal.df ) = names( urban_reuse_fraction.df )

 # Import and harmonize historical municipal and manufacturing demands from Floerke et al. 'Domestic and industrial water uses as a mirror for socioeconomic development'
 watergap_mw_hist.df = data.frame( read.csv( paste( data_path, '/water/water_demands/watergap_historical_water_use_mw.csv', sep=''), stringsAsFactors = FALSE ) )
 watergap_mw_hist.df$country = countrycode(watergap_mw_hist.df$iso, 'iso3n', 'iso3c')
 watergap_mw_hist.df$region = sapply( watergap_mw_hist.df$country, function(x){ return( country_region_map_key.df$Eleven_region[ which( country_region_map_key.df$Region == x ) ] ) } )
 watergap_mf_hist.df = data.frame( read.csv( paste( data_path, '/water/water_demands/watergap_historical_water_use_mf.csv', sep=''), stringsAsFactors = FALSE ) )
 watergap_mf_hist.df$country = countrycode(watergap_mf_hist.df$iso, 'iso3n', 'iso3c')
 watergap_mf_hist.df$region = sapply( watergap_mf_hist.df$country, function(x){ return( country_region_map_key.df$Eleven_region[ which( country_region_map_key.df$Region == x ) ] ) } )

 # Add exogenous water demands to the db
 ret = lapply( region, function(rr){ lapply( dmd_yrs, function(yy){

   ## Add urban and rural demands for all historical and future years to db
   if( yy %in% as.numeric( row.names( urban_withdrawal.df ) ) )
     {

     ixDS$add_par( 	'demand', # parameter name
             paste( rr, 'urban_mw', 'final', yy, model_time, sep = '.' ), # set key
             round( c( urban_withdrawal.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] * urban_connected_fraction.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2),
             '-' )

     ixDS$add_par( 	'demand', # parameter name
             paste( rr, 'urban_dis', 'final', yy, model_time, sep = '.' ), # set key
             round( c( urban_withdrawal.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] * (1-urban_connected_fraction.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2),
             '-' )

     ixDS$add_par( 	'demand', # parameter name
             paste( rr, 'rural_mw', 'final', yy, model_time, sep = '.' ), # set key
             round( c( rural_withdrawal.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] * rural_connected_fraction.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2), # value
             '-' )	# unit

     ixDS$add_par( 	'demand', # parameter name
             paste( rr, 'rural_dis', 'final', yy, model_time, sep = '.' ), # set key
             round( c( rural_withdrawal.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] * (1-rural_connected_fraction.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2), # value
             '-' )	# unit

     ixDS$add_par( 	'demand', # parameter name
             paste( rr, 'urban_collected_wst', 'final', yy, model_time, sep = '.' ), # set key
             -1 * round( c( urban_return.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] * urban_treated_fraction.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2),
             '-' )

     ixDS$add_par( 	'demand', # parameter name
             paste( rr, 'rural_collected_wst', 'final', yy, model_time, sep = '.' ), # set key
             -1 * round( c( rural_return.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] * rural_treated_fraction.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2), # value
             '-' )	# unit

     ixDS$add_par( 	'demand', # parameter name
             paste( rr, 'urban_uncollected_wst', 'final', yy, model_time, sep = '.' ), # set key
             -1 * round( c( urban_return.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] * (1-urban_treated_fraction.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2),
             '-' )

     ixDS$add_par( 	'demand', # parameter name par[a,s,d,f,g]
             paste( rr, 'rural_uncollected_wst', 'final', yy, model_time, sep = '.' ), # set key
             -1 * round( c( rural_return.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] * (1-rural_treated_fraction.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2), # value
             '-' )	# unit

     }else # year outside range of projections either historical or beyond proejections - use average growth rates to estimate
     {

     if ( yy < min( as.numeric( row.names( urban_withdrawal.df ) ) ) )
       {

       dmd2010 = round( c( urban_withdrawal.df[ as.character( 2010 ), unlist( strsplit( rr, '_') )[2] ] * urban_connected_fraction.df[ as.character( 2010 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2)
       dmd2020 = round( c( urban_withdrawal.df[ as.character( 2020 ), unlist( strsplit( rr, '_') )[2] ] * urban_connected_fraction.df[ as.character( 2020 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2)
       delta = ( dmd2020/dmd2010 )^(1/10) - 1
       dmdhist = round( dmd2010*( (1 + delta)^(-1*(2010 - as.numeric(yy))) ), digits = 2)
       ixDS$add_par( 	'demand', # parameter name
             paste( rr, 'urban_mw', 'final', yy, model_time, sep = '.' ), # set key
             dmdhist,
             '-' )

       dmd2010 = round( c( urban_withdrawal.df[ as.character( 2010 ), unlist( strsplit( rr, '_') )[2] ] * (1-urban_connected_fraction.df[ as.character( 2010 ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2)
       dmd2020 = round( c( urban_withdrawal.df[ as.character( 2020 ), unlist( strsplit( rr, '_') )[2] ] * (1-urban_connected_fraction.df[ as.character( 2020 ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2)
       delta = ( dmd2020/dmd2010 )^(1/10) - 1
       dmdhist =  round( dmd2010*( (1 + delta)^(-1*(2010 - as.numeric(yy))) ), digits = 2)
       ixDS$add_par( 	'demand', # parameter name
               paste( rr, 'urban_dis', 'final', yy, model_time, sep = '.' ), # set key
               dmdhist,
               '-' )

       dmd2010 = round( c( rural_withdrawal.df[ as.character( 2010 ), unlist( strsplit( rr, '_') )[2] ] * rural_connected_fraction.df[ as.character( 2010 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2)
       dmd2020 = round( c( rural_withdrawal.df[ as.character( 2020 ), unlist( strsplit( rr, '_') )[2] ] * rural_connected_fraction.df[ as.character( 2020 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2)
       delta = ( dmd2020/dmd2010 )^(1/10) - 1
       dmdhist =  round( dmd2010*( (1 + delta)^(-1*(2010 - as.numeric(yy))) ), digits = 2)
       ixDS$add_par( 	'demand', # parameter name
             paste( rr, 'rural_mw', 'final', yy, model_time, sep = '.' ), # set key
             dmdhist, # value
             '-' )	# unit

       dmd2010 = round( c( rural_withdrawal.df[ as.character( 2010 ), unlist( strsplit( rr, '_') )[2] ] * (1-rural_connected_fraction.df[ as.character( 2010 ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2)
       dmd2020 = round( c( rural_withdrawal.df[ as.character( 2020 ), unlist( strsplit( rr, '_') )[2] ] * (1-rural_connected_fraction.df[ as.character( 2020 ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2)
       delta = ( dmd2020/dmd2010 )^(1/10) - 1
       dmdhist =  round( dmd2010*( (1 + delta)^(-1*(2010 - as.numeric(yy))) ), digits = 2)
       ixDS$add_par( 'demand', # parameter name
             paste( rr, 'rural_dis', 'final', yy, model_time, sep = '.' ), # set key
             dmdhist,
             '-' )

       dmd2010 = -1 * round( c( urban_return.df[ as.character( 2010 ), unlist( strsplit( rr, '_') )[2] ] * urban_treated_fraction.df[ as.character( 2010 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2)
       dmd2020 = -1 * round( c( urban_return.df[ as.character( 2020 ), unlist( strsplit( rr, '_') )[2] ] * urban_treated_fraction.df[ as.character( 2020 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2)
       delta = ( dmd2020/dmd2010 )^(1/10) - 1
       dmdhist =  round( dmd2010*( (1 + delta)^(-1*(2010 - as.numeric(yy))) ), digits = 2)
       ixDS$add_par( 'demand', # parameter name
               paste( rr, 'urban_collected_wst', 'final', yy, model_time, sep = '.' ), # set key
               dmdhist,
               '-' )

       dmd2010 = -1 * round( c( urban_return.df[ as.character( 2010 ), unlist( strsplit( rr, '_') )[2] ] * (1-urban_treated_fraction.df[ as.character( 2010 ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2 )
       dmd2020 = -1 * round( c( urban_return.df[ as.character( 2020 ), unlist( strsplit( rr, '_') )[2] ] * (1-urban_treated_fraction.df[ as.character( 2020 ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2 )
       delta = ( dmd2020/dmd2010 )^(1/10) - 1
       dmdhist =  round( dmd2010*( (1 + delta)^(-1*(2010 - as.numeric(yy))) ), digits = 2)
       ixDS$add_par( 'demand', # parameter name
               paste( rr, 'urban_uncollected_wst', 'final', yy, model_time, sep = '.' ), # set key
               dmdhist,
               '-' )

       dmd2010 = -1 * round( c( rural_return.df[ as.character( 2010 ), unlist( strsplit( rr, '_') )[2] ] * rural_treated_fraction.df[ as.character( 2010 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2)
       dmd2020 = -1 * round( c( rural_return.df[ as.character( 2020 ), unlist( strsplit( rr, '_') )[2] ] * rural_treated_fraction.df[ as.character( 2020 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2)
       delta = ( dmd2020/dmd2010 )^(1/10) - 1
       dmdhist =  round( dmd2010*( (1 + delta)^(-1*(2010 - as.numeric(yy))) ), digits = 2)
       ixDS$add_par( 'demand', # parameter name
               paste( rr, 'rural_collected_wst', 'final', yy, model_time, sep = '.' ), # set key
               dmdhist,
               '-' )

       dmd2010 = -1 * round( c( rural_return.df[ as.character( 2010 ), unlist( strsplit( rr, '_') )[2] ] * (1-rural_treated_fraction.df[ as.character( 2010 ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2)
       dmd2020 = -1 * round( c( rural_return.df[ as.character( 2020 ), unlist( strsplit( rr, '_') )[2] ] * (1-rural_treated_fraction.df[ as.character( 2020 ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2)
       delta = ( dmd2020/dmd2010 )^(1/10) - 1
       dmdhist =  round( dmd2010*( (1 + delta)^(-1*(2010 - as.numeric(yy))) ), digits = 2)
       ixDS$add_par( 'demand', # parameter name
               paste( rr, 'rural_uncollected_wst', 'final', yy, model_time, sep = '.' ), # set key
               dmdhist,
               '-' )

       }else # beyond projections - assume previous decadal growth / decay rate
       {

       dmd2090 = round( c( urban_withdrawal.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ] * urban_connected_fraction.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2 )
       dmd2080 = round( c( urban_withdrawal.df[ as.character( 2080 ), unlist( strsplit( rr, '_') )[2] ] * urban_connected_fraction.df[ as.character( 2080 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2 )
       delta = ( dmd2090/dmd2080 )^(1/10) - 1
       if(is.nan(delta)){delta=0}
       dmdfut = round( dmd2090*( (1 + delta)^((as.numeric(yy) - 2090)) ), digits = 2 )
       ixDS$add_par( 'demand', # parameter name
               paste( rr, 'urban_mw', 'final', yy, model_time, sep = '.' ), # set key
               dmdfut,
               '-' )

       dmd2090 = round( c( urban_withdrawal.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ] * (1-urban_connected_fraction.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2 )
       dmd2080 = round( c( urban_withdrawal.df[ as.character( 2080 ), unlist( strsplit( rr, '_') )[2] ] * (1-urban_connected_fraction.df[ as.character( 2080 ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2 )
       delta = ( dmd2090/dmd2080 )^(1/10) - 1
       if(is.nan(delta)){delta=0}
       dmdfut = round( dmd2090*( (1 + delta)^((as.numeric(yy) - 2090)) ), digits = 2 )
       ixDS$add_par( 'demand', # parameter name
               paste( rr, 'urban_dis', 'final', yy, model_time, sep = '.' ), # set key
               dmdfut,
               '-' )

       dmd2090 = round( c( rural_withdrawal.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ] * rural_connected_fraction.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2 )
       dmd2080 = round( c( rural_withdrawal.df[ as.character( 2080 ), unlist( strsplit( rr, '_') )[2] ] * rural_connected_fraction.df[ as.character( 2080 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2 )
       delta = ( dmd2090/dmd2080 )^(1/10) - 1
       if(is.nan(delta)){delta=0}
       dmdfut = round( dmd2090*( (1 + delta)^((as.numeric(yy) - 2090)) ), digits = 2 )
       ixDS$add_par( 'demand', # parameter name
               paste( rr, 'rural_mw', 'final', yy, model_time, sep = '.' ), # set key
               dmdfut,
               '-' )

       dmd2090 = round( c( rural_withdrawal.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ] * rural_connected_fraction.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2 )
       dmd2080 = round( c( rural_withdrawal.df[ as.character( 2080 ), unlist( strsplit( rr, '_') )[2] ] * rural_connected_fraction.df[ as.character( 2080 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2 )
       delta = ( dmd2090/dmd2080 )^(1/10) - 1
       if(is.nan(delta)){delta=0}
       dmdfut = round( dmd2090*( (1 + delta)^((as.numeric(yy) - 2090)) ), digits = 2 )
       ixDS$add_par( 'demand', # parameter name
               paste( rr, 'rural_dis', 'final', yy, model_time, sep = '.' ), # set key
               dmdfut,
               '-' )

       dmd2090 = -1 * round( c( urban_return.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ] * urban_treated_fraction.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2)
       dmd2080 = -1 * round( c( urban_return.df[ as.character( 2080 ), unlist( strsplit( rr, '_') )[2] ] * urban_treated_fraction.df[ as.character( 2080 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2)
       delta = ( dmd2090/dmd2080 )^(1/10) - 1
       if(is.nan(delta)){delta=0}
       dmdfut = round( dmd2090*( (1 + delta)^((as.numeric(yy) - 2090)) ), digits = 2 )
       ixDS$add_par( 'demand', # parameter name
               paste( rr, 'urban_collected_wst', 'final', yy, model_time, sep = '.' ), # set key
               dmdfut,
               '-' )

       dmd2090 = -1 * round( c( urban_return.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ] * (1-urban_treated_fraction.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2)
       dmd2080 = -1 * round( c( urban_return.df[ as.character( 2080 ), unlist( strsplit( rr, '_') )[2] ] * (1-urban_treated_fraction.df[ as.character( 2080 ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2)
       delta = ( dmd2090/dmd2080 )^(1/10) - 1
       if(is.nan(delta)){delta=0}
       dmdfut = round( dmd2090*( (1 + delta)^((as.numeric(yy) - 2090)) ), digits = 2 )
       ixDS$add_par( 'demand', # parameter name
               paste( rr, 'urban_uncollected_wst', 'final', yy, model_time, sep = '.' ), # set key
               dmdfut,
               '-' )

       dmd2090 = -1 * round( c( rural_return.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ] * rural_treated_fraction.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2)
       dmd2080 = -1 * round( c( rural_return.df[ as.character( 2080 ), unlist( strsplit( rr, '_') )[2] ] * rural_treated_fraction.df[ as.character( 2080 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2)
       delta = ( dmd2090/dmd2080 )^(1/10) - 1
       if(is.nan(delta)){delta=0}
       dmdfut = round( dmd2090*( (1 + delta)^((as.numeric(yy) - 2090)) ), digits = 2 )
       ixDS$add_par( 'demand', # parameter name
               paste( rr, 'rural_collected_wst', 'final', yy, model_time, sep = '.' ), # set key
               dmdfut,
               '-' )

       dmd2090 = -1 * round( c( rural_return.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ] * (1-rural_treated_fraction.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2)
       dmd2080 = -1 * round( c( rural_return.df[ as.character( 2080 ), unlist( strsplit( rr, '_') )[2] ] * (1-rural_treated_fraction.df[ as.character( 2080 ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2)
       delta = ( dmd2090/dmd2080 )^(1/10) - 1
       if(is.nan(delta)){delta=0}
       dmdfut = round( dmd2090*( (1 + delta)^((as.numeric(yy) - 2090)) ), digits = 2 )
       ixDS$add_par( 'demand', # parameter name
               paste( rr, 'rural_uncollected_wst', 'final', yy, model_time, sep = '.' ), # set key
               dmdfut,
               '-' )
       }
     }

   # Add irrigation demands for all historical and future years to db - writes directly into the freshwater supply level
   if( yy %in% as.numeric( row.names( irrigation_withdrawal.df ) ) )
     {

     ixDS$add_par( 	'demand', # parameter name
             paste( rr, 'freshwater_supply', water_supply_level, yy, model_time, sep = '.' ), # set key
             round( irrigation_withdrawal.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ], digits = 2),
             '-' )

     }else # year outside range of projections either historical or beyond proejections
     {

     if ( yy < min( as.numeric( row.names( urban_withdrawal.df ) ) ) )
       {
       ixDS$add_par( 	'demand', # parameter name
               paste( rr, 'freshwater_supply', water_supply_level, yy, model_time, sep = '.' ), # set key
               round( irrigation_withdrawal.df[ as.character( min( as.numeric( row.names( irrigation_withdrawal.df ) ) ) ), unlist( strsplit( rr, '_') )[2] ], digits = 2),
               '-' )
       }else
       {

       di = irrigation_withdrawal.df[ as.character( max( as.numeric( row.names( irrigation_withdrawal.df ) ) ) ), unlist( strsplit( rr, '_') )[2] ] / irrigation_withdrawal.df[ ( which.max( as.numeric( row.names( irrigation_withdrawal.df ) ) ) - 1 ), unlist( strsplit( rr, '_') )[2] ]

       ixDS$add_par( 'demand', # parameter name
               paste( rr, 'freshwater_supply', water_supply_level, yy, model_time, sep = '.' ), # set key
               round( ( irrigation_withdrawal.df[ as.character( max( as.numeric( row.names( irrigation_withdrawal.df ) ) ) ), unlist( strsplit( rr, '_') )[2] ] * ( di^( which(dmd_yrs == yy) - which( dmd_yrs == as.character( max( as.numeric( row.names( irrigation_withdrawal.df ) ) ) ) ) ) ) ), digits = 2),
               '-' )
       }

     }

   } ) } )

 # Base year urban water demands for senstivity
 # ssdm = do.call( cbind, lapply( region, function(rr){ data.frame( sapply( dmd_yrs, function(yy){ unlist( ixDS$par( 'demand', list( node = rr, commodity = 'urban_mw', level = 'final', year = yy, time  = model_time) )['value'] ) } ) ) } ) )
 # row.names(ssdm) = dmd_yrs
 # names(ssdm) = region
 # ssdw = do.call( cbind, lapply( region, function(rr){ data.frame( sapply( dmd_yrs, function(yy){ unlist( ixDS$par( 'demand', list( node = rr, commodity = 'urban_collected_wst', level = 'final', year = yy, time = model_time) )['value'] ) } ) ) } ) )
 # row.names(ssdw) = dmd_yrs
 # names(ssdw) = region
 # ssdi = do.call( cbind, lapply( region, function(rr){ data.frame( sapply( dmd_yrs, function(yy){ unlist( ixDS$par( 'demand', list( node = rr, commodity = 'freshwater_supply', level = 'water_supply', year = yy, time  = model_time) )['value'] ) } ) ) } ) )
 # row.names(ssdi) = dmd_yrs
 # names(ssdi) = region
 # print(ssdm)
 # print(ssdw)
 # print(ssdi)

 # ------------------------------
 # Add technologies that distribute water to end use - allows tracking (estimating) investments in distribution infrastructure
 # ------------------------------

 ret = ixDS$add_set( 'type_tec', 'water_distribution' )
 d_tech = data.frame( urban_mw = 'urban_t_d', urban_dis = 'urban_unconnected', rural_mw = 'rural_t_d', rural_dis = 'rural_unconnected', urban_collected_wst = 'urban_sewerage' )

 # distribution cost parameters
 # Estimated by taking the per capita connection costs from Table 4 in 'Evaluation of the Costs and Benefits of Water and Sanitation Improvements at the Global Level'
 # Component for piping and pretreatment estimated from breakdown across cost components presented in 'The Economic Costs and Benefits of Investments in Municipal Water and Sanitation Infrastructure: A Global Perspective'
 # The per capita level is converted to per m3 considering a 'decent living' water requirement of 50 liters per day per person (18.25 m3 per year) from Gleick 1996
 # 'household connection' to represent the urban distribution and 'borehole' to represent the rural distribution
 # Irrigation infrastructure optimized in GLOBIOM - not considered in this implementation but irrigation demands accounted for in the freshwater balance
 urban_mw_par.list = list( 	investment = round( 0.45 * 1.1 * 1e3 * data.frame( low = ( 55 / 18.25 ), mid = ( 102 / 18.25 ), high = ( 144 / 18.25 ) ) ),
               fixed_cost = round( 0.25 * 0.45 * 1.1 * 1e3 * data.frame( low = ( 55 / 18.25 ), mid = ( 102 / 18.25 ), high = ( 144 / 18.25 ) ) ),
               var_cost = data.frame( low = 0, mid = 0, high = 0 ),
               lifetime = data.frame( low = 50, mid = 40, high = 30 ) )
 urban_dis_par.list = list( 	investment = data.frame( low = 0, mid = 0, high = 0 ),
               fixed_cost = data.frame( low = 0, mid = 0, high = 0 ),
               var_cost = data.frame( low = 0, mid = 0, high = 0 ),
               lifetime = data.frame( low = 1, mid = 1, high = 1 ) )
 rural_mw_par.list = list( 	investment = round( 0.65 * 1.1 * 1e3 * data.frame( low = ( 17 / 18.25 ), mid = ( 23 / 18.25 ), high = ( 55 / 18.25 ) ) ),
               fixed_cost = round( 0.05 * 0.65 * 1.1 * 1e3 * data.frame( low = ( 17 / 18.25 ), mid = ( 23 / 18.25 ), high = ( 55 / 18.25 ) ) ),
               var_cost = data.frame( low = 0, mid = 0, high = 0 ),
               lifetime = data.frame( low = 30, mid = 20, high = 10 ) )
 rural_dis_par.list = list( 	investment = data.frame( low = 0, mid = 0, high = 0 ),
               fixed_cost = data.frame( low = 0, mid = 0, high = 0 ),
               var_cost = data.frame( low = 0, mid = 0, high = 0 ),
               lifetime = data.frame( low = 1, mid = 1, high = 1 ) )
 urban_collected_wst_par.list = list( 	investment = round( 0.35 * 1.1 * 1e3 * data.frame( low = ( 55 / 18.25 ), mid = ( 102 / 18.25 ), high = ( 144 / 18.25 ) ) ),
                     fixed_cost = round( 0.25 * 0.35 * 1.1 * 1e3 * data.frame( low = ( 55 / 18.25 ), mid = ( 102 / 18.25 ), high = ( 144 / 18.25 ) ) ),
                     var_cost = data.frame( low = 0, mid = 0, high = 0 ),
                     lifetime = data.frame( low = 50, mid = 40, high = 30 ) )

 ret = lapply( names( d_tech ), function(tt){

   ixDS$add_set( 'technology', as.character( d_tech[[ tt ]] ) )
   ixDS$add_set( 'cat_tec', paste( 'investment_other' , as.character( d_tech[[ tt ]] ), sep='.' ) ) # Add technology to list of water system investments
   ixDS$add_set( 'cat_tec', paste( 'water_distribution', as.character( d_tech[[ tt ]] ), sep='.' ) ) # Add technology to list of desal techs

   par.list = get( paste( tt, 'par.list', sep= '_') )

   lapply( region, function(rr){

     tst = as.numeric( as.character( unlist(ixDS$par('demand',list( node = rr, commodity = tt ))['year']) ) )

     lapply( tst , function(yy){

       # Add the investment cost
       ixDS$add_par( 'inv_cost', # parameter name
           paste(rr, as.character( d_tech[[ tt ]] ), yy, sep = '.' ), # set key
           round( unlist( par.list[[ 'investment' ]][ parameter_levels ] ), digits = 3 ),
           '-' )

       # Add the technical lifetime
       ixDS$add_par( 'technical_lifetime', # parameter name
             paste(rr, as.character( d_tech[[ tt ]] ), yy, sep = '.' ), # set key
             unlist( par.list[[ 'lifetime' ]][ parameter_levels ] ),
             'y' )

       # Add the construction time
       ixDS$add_par( 'construction_time', # parameter name
             paste( rr, as.character( d_tech[[ tt ]] ), yy, sep = '.' ), # set key
             1,
             'y' )

       # Add the historical capacity and activity
       if( yy <= 2010 )
         {

         ixDS$add_par( 'historical_activity', paste(  rr, as.character( d_tech[[ tt ]] ), yy, mode_common , model_time, sep = '.' ), abs(unlist(ixDS$par('demand',list( node = rr, commodity = tt, year = yy )))['value']), '-' )

         if( yy == min( tst ) )
           {
           ixDS$add_par( 	'historical_new_capacity', # parameter name
                   paste(  rr, as.character( d_tech[[ tt ]] ), yy, sep = '.' ), # set key
                   round(1/10*abs(unlist(ixDS$par('demand',list( node = rr, commodity = tt, year = yy )))['value'])/3, digits=rnd),
                   '-' )
           }else
           {
           ixDS$add_par( 	'historical_new_capacity', # parameter name
                   paste(  rr, as.character( d_tech[[ tt ]] ), yy, sep = '.' ), # set key
                   round(max( 0, 1/5*c( abs(unlist(ixDS$par('demand',list( node = rr, commodity = tt, year = yy )))['value']) - abs(unlist( ixDS$par( 'demand',list( node = rr, commodity = tt, year = tst[ ( which( tst == yy ) - 1 ) ] ) ) )['value'] ) ) ),digits=rnd),
                   '-' )
           }

         }

       # Add the historical capacity and activity for locked-in future years in the mitigation and no policy scenarios (e.g., 2030, 2040)
       if( yy > 2010 & yy < firstmodelyear )
         {
         ixDS$add_par( 'historical_activity', paste(  rr, as.character( d_tech[[ tt ]] ), yy, mode_common , model_time, sep = '.' ), max(0, base_hist_act[ which( as.character( unlist( base_hist_act$tec ) ) == as.character( d_tech[[ tt ]] ) & as.numeric( unlist( base_hist_act$year_all ) ) == yy & as.character( unlist( base_hist_act$node ) ) == rr ), 'val' ], na.rm=TRUE), '-' )
         ixDS$add_par( 'historical_new_capacity', paste(  rr, as.character( d_tech[[ tt ]] ), yy, sep = '.' ), max(0, base_hist_ncap[ which( as.character( unlist( base_hist_ncap$tec ) ) == as.character( d_tech[[ tt ]] ) & as.numeric( unlist( base_hist_ncap$year_all ) ) == yy & as.character( unlist( base_hist_ncap$node ) ) == rr ), 'val' ], na.rm=TRUE), '-' )
         }

       # Add i-o parameters
       lapply( model_years[ which( model_years >= as.numeric(yy) & model_years <= ( as.numeric(yy) + unlist( par.list[['lifetime']][parameter_levels] ) ) ) ], function(aa){

         # Add the input efficiency ratio
         if(tt != 'urban_collected_wst')
           {
           incmd = 'freshwater_supply'
           inlvl = water_supply_level
           outlvl = 'final'
           }else
           {
           incmd = tt
           inlvl = 'final'
           outlvl = water_treat_level
           }

         # Add the output efficiency ratio
         ixDS$add_par( 'output', # parameter name
           paste( rr, as.character( d_tech[[ tt ]] ), yy, aa, mode_common, rr, tt, outlvl, model_time, model_time, sep = '.' ), # set key
           1, # parameter value
           '-' )


         ixDS$add_par( 'input', # parameter name
           paste( rr, as.character( d_tech[[ tt ]] ), yy, aa, mode_common, rr, incmd, inlvl, model_time, model_time, sep = '.' ), # set key
           1,
           '-' )

         # Add the variable cost
         ixDS$add_par( 'var_cost', # parameter name
           paste( rr, as.character( d_tech[[ tt ]] ), yy, aa, mode_common, model_time, sep = '.' ), # set key
           unlist( par.list[['var_cost']][parameter_levels] ),
           '-' ) # units

         # Add fixed costs
         ixDS$add_par( 'fix_cost', # parameter name
           paste( rr, as.character( d_tech[[ tt ]] ), yy, aa, sep = '.' ), # set key
           unlist( par.list[['fixed_cost']][parameter_levels] ),
           '-' )

         # Add the capacity factor
         ixDS$add_par( 'capacity_factor', # parameter name
           paste( rr, as.character( d_tech[[ tt ]] ), yy, aa, model_time, sep = '.' ), # set key
           1,
           '-' )

         } )

       } )

     } )

   } )

 # -------------------------------------------------------------------
 # Add simplified conservation curves as technologies representing step functions
 # -------------------------------------------------------------------

 if( as.character( model_scenarios$newscenarioName[sc] ) == 'baseline_globiom_SDG_sdg6supp' ){ flg = 1 }
 if( as.character( model_scenarios$newscenarioName[sc] ) == 'baseline_globiom_SDG_sdg6eff' ){ flg = 1 }
 if( as.character( model_scenarios$newscenarioName[sc] ) == 'baseline_globiom_base_watbaseline' ){ flg = 0 }
 if( as.character( model_scenarios$newscenarioName[sc] ) == "baseline_globiom_SDG_sdg6led" ){ flg = 1 }
 if( as.character( model_scenarios$newscenarioName[sc] ) == 'baseline_globiom_SDG_sdg6supp2' ){ flg = 1 }


 if( water_policies == 'SDG6' ){ scld = 0.05 }else{ scld = 0 } # Assume 10% more efficiency for SDG6 scenario
 ret = ixDS$add_set( 'type_tec', 'water_efficiency' )
 d_tech = data.frame( 	urban_low = 'ueff1', urban_mid = 'ueff2', urban_high = 'ueff3',
             rural_low = 'reff1', rural_mid = 'reff2', rural_high = 'reff3',
             irrig_low = 'ieff1', irrig_mid = 'ieff2', irrig_high = 'ieff3' )

 # Backstop costs - variable cost associated with backstop supply technology to set the threshold for investment into efficiency / price responsive demand
 # Mid-range case from Ward et al. 2010, Partial costs of global climate change adaptation for water supply... using 0.45 to achieve average (mid-point) costs of 0.3 USD per m3
 # Lower from A.A. Keller "Water Scarcity and the Role of Storage in Development", backstop technology ranges estimated from Table 5. In USD per m3
 # Upper from, Ouda (2013) "Review of Saudi Arabia Municipal Water Tariff", report a production cost for desalination of 1.09 USD per m3.
 cbu = 1e3 * data.frame( low = 0.2, mid = 0.45, high = 1 ) # units are converted to million USD / km3 / yr
 cbr = 1e3 * data.frame( low = 0.2, mid = 0.45, high = 1 )
 cbi = 1e3 * data.frame( low = 0.2, mid = 0.45, high = 1 )

 # Water conservation cost parameters
 urban_low_par.list = list( 	investment = data.frame( low = 0, mid = 0, high = 0 ) ,
               fixed_cost  = data.frame( low = 0, mid = 0, high = 0 ) ,
               var_cost = data.frame( low = (1/3) * cbu['low'], mid = (1/3) * cbu['mid'], high = (1/3) * cbu['high'] ),
               max_response = data.frame( low = (1/3) * ( scld + 0.35 ), mid = (1/3) * ( scld + 0.25 ), high = (1/3) * ( scld + 0.15 ) ) )
 urban_mid_par.list = list( 	investment = data.frame( low = 0, mid = 0, high = 0 ) ,
               fixed_cost  = data.frame( low = 0, mid = 0, high = 0 ) ,
               var_cost = data.frame( low = (2/3) * cbu['low'], mid = (2/3) * cbu['mid'], high = (2/3) * cbu['high'] ),
               max_response = data.frame( low = (1/3) * ( scld + 0.35 ), mid = (1/3) * ( scld + 0.25 ), high = (1/3) *( scld + 0.15 ) ) )
 urban_high_par.list = list( investment = data.frame( low = 0, mid = 0, high = 0 ) ,
               fixed_cost  = data.frame( low = 0, mid = 0, high = 0 ) ,
               var_cost = data.frame( low = cbu['low'], mid = cbu['mid'], high = cbu['high'] ) ,
               max_response = data.frame( low = (1/3) * ( scld + 0.35 ), mid = (1/3) * ( scld + 0.25 ), high = (1/3) * ( scld + 0.15 ) ) )
 rural_low_par.list = list( 	investment = data.frame( low = 0, mid = 0, high = 0 ) ,
               fixed_cost  = data.frame( low = 0, mid = 0, high = 0 ) ,
               var_cost = data.frame( low = (1/3) * cbr['low'], mid = (1/3) * cbr['mid'], high = (1/3) * cbr['high'] ) ,
               max_response = data.frame( low = (1/3) * ( scld + 0.35 ), mid = (1/3) * ( scld + 0.25 ), high = (1/3) * ( scld + 0.15 ) ) )
 rural_mid_par.list = list( 	investment = data.frame( low = 0, mid = 0, high = 0 ) ,
               fixed_cost  = data.frame( low = 0, mid = 0, high = 0 ) ,
               var_cost = data.frame( low = (2/3) * cbr['low'], mid = (2/3) * cbr['mid'], high = (2/3) * cbr['high'] ) ,
               max_response = data.frame( low = (1/3) * ( scld + 0.35 ), mid = (1/3) * ( scld + 0.25 ), high = (1/3) * ( scld + 0.15 ) ) )
 rural_high_par.list = list( investment = data.frame( low = 0, mid = 0, high = 0 ) ,
               fixed_cost  = data.frame( low = 0, mid = 0, high = 0 ) ,
               var_cost = data.frame( low = cbr['low'], mid = cbr['mid'], high = cbr['high'] ) ,
               max_response = data.frame( low = (1/3) * ( scld + 0.35 ), mid = (1/3) * ( scld + 0.25 ), high = (1/3) * ( scld + 0.15 ) ) )
 irrig_low_par.list = list( 	investment = data.frame( low = 0, mid = 0, high = 0 ) ,
               fixed_cost  = data.frame( low = 0, mid = 0, high = 0 ) ,
               var_cost = data.frame( low = (1/3) * cbi['low'], mid = (1/3) * cbi['mid'], high = (1/3)* cbi['high'] ) ,
               max_response = flg * data.frame( low = (1/3) * 0.35, mid = (1/3) * 0.25, high = (1/3) * 0.25 ) )
 irrig_mid_par.list = list( 	investment = data.frame( low = 0, mid = 0, high = 0 ) ,
               fixed_cost  = data.frame( low = 0, mid = 0, high = 0 ) ,
               var_cost = data.frame( low = (2/3) * cbi['low'], mid = (2/3) * cbi['mid'], high = (2/3) * cbi['high'] ) ,
               max_response = flg * data.frame( low = (1/3) * 0.35, mid = (1/3) * 0.25, high = (1/3) * 0.25 ) )
 irrig_high_par.list = list( investment = data.frame( low = 0, mid = 0, high = 0 ) ,
               fixed_cost  = data.frame( low = 0, mid = 0, high = 0 ) ,
               var_cost = data.frame( low = cbi['low'], mid = cbi['mid'], high = cbi['high'] ) ,
               max_response = flg * data.frame( low = (1/3) * 0.35, mid = (1/3) * 0.25, high = (1/3) * 0.25 ) )

 ret = lapply( names( d_tech ), function(tt){

   ixDS$add_set( 'technology', as.character( d_tech[[ tt ]] ) )
   ixDS$add_set( 'cat_tec', paste( 'investment_other' , as.character( d_tech[[ tt ]] ), sep='.' ) ) # Add technology to list of water system investments
   ixDS$add_set( 'cat_tec', paste( 'water_efficiency', as.character( d_tech[[ tt ]] ), sep='.' ) ) # Add technology to list of efficiency techs

   par.list = get( paste( tt, 'par.list', sep= '_') )

   if( unlist(strsplit(tt,'_'))[1] == 'urban' ){cmdtys=c('urban_mw','urban_collected_wst')}else if(unlist(strsplit(tt,'_'))[1] == 'rural'){cmdtys=c('rural_mw','rural_collected_wst')}else{cmdtys=c('freshwater_supply',NA)}
   if( unlist(strsplit(tt,'_'))[1] == 'urban' ){lvl=c('final')}else if(unlist(strsplit(tt,'_'))[1] == 'rural'){lvl=c('final')}else{lvl=water_supply_level}

   lapply( region, function(rr){

     tst = as.numeric( as.character( unlist(ixDS$par('demand',list( node = rr, commodity = cmdtys[1] ))['year']) ) )
     tst = tst[tst>2010]

     lapply( tst , function(yy){

       # Add the investment cost
       ixDS$add_par( 'inv_cost', # parameter name
           paste(rr, as.character( d_tech[[ tt ]] ), yy, sep = '.' ), # set key
           0,
           '-' )

       # Add the technical lifetime
       ixDS$add_par( 'technical_lifetime', # parameter name
             paste(rr, as.character( d_tech[[ tt ]] ), yy, sep = '.' ), # set key
             10,
             'y' )

       # Add the construction time
       ixDS$add_par( 'construction_time', # parameter name
             paste( rr, as.character( d_tech[[ tt ]] ), yy, sep = '.' ), # set key
             0,
             'y' )

       # Add i-o parameters
       aa = yy

       # Add the output efficiency ratio
       ixDS$add_par( 	'output', # parameter name
               paste( rr, as.character( d_tech[[ tt ]] ), yy, aa, mode_common, rr, cmdtys[1], lvl, model_time, model_time, sep = '.' ), # set key
               1,
               '-' )

       if( !is.na(cmdtys[2]) )
         {
         # Add the output efficiency ratio for wastewater - it is assumed efficiency measures reduce both withdrawals and return flows symmetrically.
         ixDS$add_par( 	'input', # parameter name
                 paste( rr, as.character( d_tech[[ tt ]] ), yy, aa, mode_common, rr, cmdtys[2], lvl, model_time, model_time, sep = '.' ), # set key
                 min( abs( round( unlist( ixDS$par('demand',list(node=rr,commodity=cmdtys[2],year=aa))['value'] ) / unlist( ixDS$par('demand',list(node=rr,commodity=cmdtys[1],year=aa))['value'] ), digits = rnd ) ), 0, na.rm=TRUE ),
                 '-' )
         }

       # Add the variable cost
       ixDS$add_par( 'var_cost', # parameter name
         paste( rr, as.character( d_tech[[ tt ]] ), yy, aa, mode_common, model_time, sep = '.' ), # set key
         unlist( par.list[['var_cost']][parameter_levels] ),
         '-' ) # units

       # Add fixed costs
       ixDS$add_par( 'fix_cost', # parameter name
         paste( rr, as.character( d_tech[[ tt ]] ), yy, aa, sep = '.' ), # set key
         unlist( par.list[['fixed_cost']][parameter_levels] ),
         '-' )

       # Add the capacity factor
       ixDS$add_par( 'capacity_factor', # parameter name
         paste( rr, as.character( d_tech[[ tt ]] ), yy, aa, model_time, sep = '.' ), # set key
         1,
         '-' )

       # Add upper bounds
       ixDS$add_par( 'bound_activity_up', # parameter name
         paste( rr, as.character( d_tech[[ tt ]] ), yy, mode_common, model_time, sep = '.' ), # set key
         round( c( unlist( par.list[['max_response']][parameter_levels] ) * unlist( ixDS$par('demand',list(node=rr,commodity=cmdtys[1],year=aa))['value'] ) ) , digits = 3 ),
         '-' )

       } )

     } )

   } )

 #-------------------------------------------------------------------------------------------------------
 # Add wastewater treatment technologies
 #-------------------------------------------------------------------------------------------------------

 ret = ixDS$add_set( 'type_tec', 'wastewater_treatment' )
 wwt_techs = c( 'urban_treatment', 'urban_untreated', 'urban_recycle', 'rural_treatment','rural_untreated' )

 wwt_techs_wst = list( urban_treatment = 'urban_collected_wst', urban_untreated = 'urban_uncollected_wst', urban_recycle = 'urban_collected_wst', rural_treatment = 'rural_collected_wst', rural_untreated = 'rural_uncollected_wst' )
 wwt_techs_wout = list( urban_treatment = NA, urban_untreated = NA, urban_recycle = 'freshwater_supply', rural_treatment = NA, rural_untreated = NA )
 wwt_techs_wems = list( urban_treatment = 'fresh_wastewater', urban_untreated = 'unfresh_wastewater', urban_recycle = NA, rural_treatment = 'fresh_wastewater', rural_untreated = 'unfresh_wastewater' )

 wwt_tech_par.list = list(

   # Investment costs taken from Gonzalez-Serrano et al. 2005 'Cost of reclaimed municipal wastewater for applications in seasonally stressed semi-arid regions'
   urban_untreated = data.frame(	recovery_rate = data.frame( low = 1, mid = 1, high = 1 ),
                   electricity_input = data.frame( low = 0 , mid = 0, high = 0 ),
                   investment = data.frame( low = 0, mid = 0, high = 0 ),
                   fixed_cost = data.frame( low = 0, mid = 0, high = 0 ),
                   var_cost = data.frame( low = 0, mid = 0, high = 0 ),
                   capacity_factor = data.frame( low = 1, mid = 1, high = 1 ),
                   lifetime = data.frame( low = 1, mid = 1, high = 1 ) ),

   # Using the 'T1' configuration option from Gonzalez-Serrano et al. 2005 in table 4 which represents discharge to surface or sea. Converted to USD using 1.27 USD / euro exchange rate
   # Electricity intensity follows Kajenthira et al. 2014 Table 1 Appendix A: 'A New Case for Wastewater Reuse in Saudi Arabia'
   urban_treatment = data.frame(	recovery_rate = data.frame( low = 0.95, mid = 0.9, high = 0.85 ),
                   electricity_input = round( data.frame( low = 0.13 * ( 1e3 / (24 * 365) ) , mid = 0.38 * ( 1e3 / (24 * 365) ), high = 0.64 * ( 1e3 / (24 * 365) ) ), digits = 4 ),
                   investment = round(data.frame( low = 303*(1/0.365), mid = 429*(1/0.365), high = 627*(1/0.365) )), # converted usd/m3/day -> million usd/km3/yr
                   fixed_cost = round(data.frame( low = 21*(1/0.365), mid = 35*(1/0.365), high = 67*(1/0.365) )),
                   var_cost = data.frame( low = 0, mid = 0, high = 0 ),
                   capacity_factor = data.frame( low = 0.95, mid = 0.9, high = 0.85 ),
                   lifetime = data.frame( low = 40, mid = 30, high = 25 ) ) ,

   # Using the 'T4' configuration option from Gonzalez-Serrano et al. 2005 which is for direct injection into aquifers for domestic uses
   # Electricity intensity follows Kajenthira et al. 2014 Table 1 Appendix A: 'A New Case for Wastewater Reuse in Saudi Arabia'
   urban_recycle = data.frame(		recovery_rate = data.frame( low = 0.9, mid = 0.8, high = 0.7 ),
                   electricity_input = round( data.frame( low = 0.8 * ( 1e3 / (24 * 365) ) , mid = 1 * ( 1e3 / (24 * 365) ), high = 1.5 * ( 1e3 / (24 * 365) ) ), digits = 4),  # converted kWh / m3 -> GW-yr/km3
                   investment = round(data.frame( low = 1008*(1/0.365), mid = 1352*(1/0.365), high = 2010*(1/0.365) )), # Converted from $/m3/day to million $ / km3 / yr
                   fixed_cost = round(data.frame( low = 63*(1/0.365), mid = 98*(1/0.365), high = 175*(1/0.365) )),
                   var_cost = data.frame( low = 0, mid = 0, high = 0 ),
                   capacity_factor = data.frame( low = 0.95, mid = 0.9, high = 0.85 ),
                   lifetime = data.frame( low = 40, mid = 30, high = 25 ) ) ,

   # Rural treatment: Using Septic system costs stated in per capita  from The Costs of Meeting the 2030 Sustainable Development Goal Targets on Drinking Water, Sanitation, and Hygiene
   # The per capita level is converted to per m3 considering an average per capita wastewater requirement of 50 cubic meters of return flow per year
   rural_treatment = data.frame(	recovery_rate = data.frame( low = 0.97, mid = 0.95, high = 0.9 ),
                   electricity_input = data.frame( low = 0 , mid = 0, high = 0 ),
                   investment = round( data.frame( low = ( 33 / ( 50 * 0.001 ) ) , mid = ( 104 / ( 50 * 0.001 ) ), high = ( 178 / ( 50 * 0.001 ) ) )),
                   fixed_cost = round(data.frame( low = 0.1 * ( 33 / ( 50 * 0.001 ) ) , mid = 0.1 * ( 104 / ( 50 * 0.001 ) ), high = 0.1 * ( 178 / ( 50 * 0.001 ) ) )),
                   var_cost = data.frame( low = 0, mid = 0, high = 0 ),
                   capacity_factor = data.frame( low = 0.98, mid = 0.96, high = 0.94 ),
                   lifetime = data.frame( low = 30, mid = 25, high = 20 ) ) ,

   rural_untreated = data.frame(	recovery_rate = data.frame( low = 1, mid = 1, high = 1 ),
                   electricity_input = data.frame( low = 0 , mid = 0, high = 0 ),
                   investment = data.frame( low = 0, mid = 0, high = 0 ),
                   fixed_cost = data.frame( low = 0, mid = 0, high = 0 ),
                   var_cost = data.frame( low = 0, mid = 0, high = 0 ),
                   capacity_factor = data.frame( low = 1, mid = 1, high = 1 ),
                   lifetime = data.frame( low = 1, mid = 1, high = 1 ) )

   )

 inv_cost_recycle = round( data.frame( urban_recycle = (c(1301,1293,1244)/1301)* unlist( wwt_tech_par.list[['urban_recycle']][paste('investment',parameter_levels2,sep='.')] ) ), digits = 3 )
 row.names(inv_cost_recycle) = c( 2020, 2030, 2050 )

 ret = lapply( names( wwt_tech_par.list ), function(tt){

   # Add to set list
   ixDS$add_set( 'technology', tt )
   ixDS$add_set( 'cat_tec', paste( 'investment_other' , tt, sep='.') ) # Add technology to list of water system investments
   ixDS$add_set( 'cat_tec', paste( 'wastewater_treatment', tt, sep='.' ) ) # Add technology to list of desal techs

   # Performance parameters - across all regions, and historical and future years
   lapply( region, function(rr){

     tst = as.numeric( as.character( unlist(ixDS$par('demand',list( node = rr, commodity = wwt_techs_wst[[ tt ]] ))['year']) ) )

     lapply( tst, function(yy){

       if( tt == 'urban_recycle' ){ cst = inv_cost_recycle[ which.min( (as.numeric(yy) - as.numeric(row.names(inv_cost_recycle)))^2 ),tt ] }else{ cst = round( unlist( wwt_tech_par.list[[tt]][paste('investment',parameter_levels2,sep='.')] ), digits = 3 ) }
       # Add the investment cost
       ixDS$add_par( 'inv_cost', # parameter name
           paste(rr, tt, yy, sep = '.' ), # set key
           cst,
           '-' )

       # Add the technical lifetime
       ixDS$add_par( 'technical_lifetime', # parameter name
             paste(rr, tt, yy, sep = '.' ), # set key
             round( unlist( wwt_tech_par.list[[tt]][paste('lifetime',parameter_levels,sep='.')] ), digits = 3 ),
             'y' )

       # Add the construction time
       ixDS$add_par( 'construction_time', # parameter name
             paste(rr, tt, yy, sep = '.' ), # set key
             3,
             'y' )

       # Add the historical capacity and activity
       if( yy <= 2010 )
         {

         if( tt == 'urban_recycle' | tt == 'urban_treatment' )
           {

           if( tt == 'urban_recycle' )
             {
             ixDS$add_par( 	'historical_activity', # parameter name
                     paste(  rr, tt, yy, mode_common , model_time, sep = '.' ), # set key
                     round( abs( unlist(ixDS$par('demand',list( node = rr, commodity = 'urban_collected_wst', year = yy )))['value'] * urban_connected_fraction.df[ '2010', unlist( strsplit( rr, '_') )[2] ] * urban_reuse_fraction.df[ '2010', unlist( strsplit( rr, '_') )[2] ] ), digits = 2),
                     '-' )
             if( yy == min( tst ) )
               {
               ixDS$add_par( 	'historical_new_capacity', # parameter name
                       paste(  rr, tt, yy, sep = '.' ), # set key
                       round( 1/10*abs( unlist(ixDS$par('demand',list( node = rr, commodity = 'urban_collected_wst', year = yy )))['value'] * urban_connected_fraction.df[ '2010', unlist( strsplit( rr, '_') )[2] ] * urban_reuse_fraction.df[ '2010', unlist( strsplit( rr, '_') )[2] ] ), digits = 2),
                       '-' )
               }else
               {
               ixDS$add_par( 	'historical_new_capacity', # parameter name
                       paste(  rr, tt, yy, sep = '.' ), # set key
                       max( 0, 1/5*c( round( abs( unlist(ixDS$par('demand',list( node = rr, commodity = 'urban_collected_wst', year = yy )))['value'] * urban_connected_fraction.df[ '2010', unlist( strsplit( rr, '_') )[2] ] * urban_reuse_fraction.df[ '2010', unlist( strsplit( rr, '_') )[2] ] ), digits = 2) - round( abs( unlist(ixDS$par('demand',list( node = rr, commodity = 'urban_collected_wst', year = tst[ ( which( tst == yy ) - 1 ) ] )))['value'] * urban_connected_fraction.df[ '2010', unlist( strsplit( rr, '_') )[2] ] * urban_reuse_fraction.df[ '2010', unlist( strsplit( rr, '_') )[2] ] ), digits = 2) ) ),
                       '-' )
               }
             }else # urban_treatment - need to subtract recycled shared
             {
             ixDS$add_par( 	'historical_activity', # parameter name
                     paste(  rr, tt, yy, mode_common , model_time, sep = '.' ), # set key
                     round( abs( unlist(ixDS$par('demand',list( node = rr, commodity = wwt_techs_wst[[ tt ]], year = yy )))['value'] - round( c( unlist(ixDS$par('demand',list( node = rr, commodity = 'urban_collected_wst', year = yy )))['value'] * urban_connected_fraction.df[ '2010', unlist( strsplit( rr, '_') )[2] ] * urban_reuse_fraction.df[ '2010', unlist( strsplit( rr, '_') )[2] ] ), digits = 2) ), digits = 2),
                     '-' )
             if( yy == min( tst ) )
               {
               ixDS$add_par( 	'historical_new_capacity', # parameter name
                       paste(  rr, tt, yy, sep = '.' ), # set key
                       round( 1/10 * abs( unlist(ixDS$par('demand',list( node = rr, commodity = wwt_techs_wst[[ tt ]], year = yy )))['value'] - round( c( unlist(ixDS$par('demand',list( node = rr, commodity = 'urban_collected_wst', year = yy )))['value'] * urban_connected_fraction.df[ '2010', unlist( strsplit( rr, '_') )[2] ] * urban_reuse_fraction.df[ '2010', unlist( strsplit( rr, '_') )[2] ] ), digits = 2) ), digits = 2),
                       '-' )
               }else
               {
               ixDS$add_par( 	'historical_new_capacity', # parameter name
                       paste(  rr, tt, yy, sep = '.' ), # set key
                       round( 1/5 * abs( max( 0, abs( unlist(ixDS$par('demand',list( node = rr, commodity = wwt_techs_wst[[ tt ]], year = yy )))['value'] - unlist( ixDS$par( 'demand',list( node = rr, commodity = wwt_techs_wst[[ tt ]], year = tst[ ( which( tst == yy ) - 1 ) ] ) ) )['value'] ) ) - max( 0, abs( round( c( unlist(ixDS$par('demand',list( node = rr, commodity = 'urban_collected_wst', year = yy )))['value'] * urban_connected_fraction.df[ '2010', unlist( strsplit( rr, '_') )[2] ] * urban_reuse_fraction.df[ '2010', unlist( strsplit( rr, '_') )[2] ] ), digits = 2) - round( abs( unlist(ixDS$par('demand',list( node = rr, commodity = 'urban_collected_wst', year = tst[ ( which( tst == yy ) - 1 ) ] )))['value'] * urban_connected_fraction.df[ '2010', unlist( strsplit( rr, '_') )[2] ] * urban_reuse_fraction.df[ '2010', unlist( strsplit( rr, '_') )[2] ] ), digits = 2) ) ) ), digits = 2),
                       '-' )
               }
             }

           }else
           {

           ixDS$add_par( 	'historical_activity', # parameter name
                   paste(  rr, tt, yy, mode_common , model_time, sep = '.' ), # set key
                   abs(unlist(ixDS$par('demand',list( node = rr, commodity = wwt_techs_wst[[ tt ]], year = yy )))['value']),
                   '-' )

           if( yy == min( tst ) )
             {
             ixDS$add_par( 	'historical_new_capacity', # parameter name
                     paste(  rr, tt, yy, sep = '.' ), # set key
                     round( 1/10 * abs(unlist(ixDS$par('demand',list( node = rr, commodity = wwt_techs_wst[[ tt ]], year = yy )))['value']), digits = rnd ),
                     '-' )
             }else
             {
             ixDS$add_par( 	'historical_new_capacity', # parameter name
                     paste(  rr, tt, yy, sep = '.' ), # set key
                     round( 1/5 * max( 0, c( abs(unlist(ixDS$par('demand',list( node = rr, commodity = wwt_techs_wst[[ tt ]], year = yy )))['value']) - abs(unlist( ixDS$par( 'demand',list( node = rr, commodity = wwt_techs_wst[[ tt ]], year = tst[ ( which( tst == yy ) - 1 ) ] ) ) )['value']) ) ), digits = rnd ),
                     '-' )
             }

           }

         }

       # Add the historical capacity and activity for locked-in future years in the mitigation and no policy scenarios (e.g., 2030, 2040)
       if( yy > 2010 & yy < firstmodelyear )
         {
         ixDS$add_par( 'historical_activity', paste(  rr, as.character( tt ), yy, mode_common , model_time, sep = '.' ), max(0, base_hist_act[ which( as.character( unlist( base_hist_act$tec ) ) == as.character( d_tech[[ tt ]] ) & as.numeric( unlist( base_hist_act$year_all ) ) == yy & as.character( unlist( base_hist_act$node ) ) == rr ), 'val' ], na.rm=TRUE), '-' )
         ixDS$add_par( 'historical_new_capacity', paste(  rr, as.character( tt ), yy, sep = '.' ), max(0, base_hist_ncap[ which( as.character( unlist( base_hist_ncap$tec ) ) == as.character( d_tech[[ tt ]] ) & as.numeric( unlist( base_hist_ncap$year_all ) ) == yy & as.character( unlist( base_hist_ncap$node ) ) == rr ), 'val' ], na.rm=TRUE), '-' )
         }

       # # Add upper bounds to reflect availability of return flows
       # if( tt %in% c('urban_recycle') & yy >= 2010 )
         # {
         # mxp = 0.6
         # mx = mxp * abs( unlist(ixDS$par('demand',list( node = rr, commodity = wwt_techs_wst[[ tt ]], year = yy ))['value']) )
         # ixDS$add_par( 'bound_activity_up', # parameter name
                 # paste(  rr, tt, yy, mode_common , model_time, sep = '.' ), # set key
                 # round( mx, digits = 2 ),
                 # '-' )
         # }

       # Add activity bounds for urban recycling technologies to reflect demand in water stressed regions
       if( yy > 2010 & tt == 'urban_recycle' )
         {
         if( yy <  max( as.numeric( row.names( urban_withdrawal.df ) ) ) )
           {
           ixDS$add_par( 'bound_activity_lo', # parameter name
                 paste(  rr, tt, yy, mode_common , model_time, sep = '.' ), # set key
                 round( c( urban_return.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] * urban_treated_fraction.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] * urban_reuse_fraction.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2),
                 '-' )
           }else
           {
           ixDS$add_par( 'bound_activity_lo', # parameter name
                 paste(  rr, tt, yy, mode_common , model_time, sep = '.' ), # set key
                 round( c( urban_return.df[ as.character( max( as.numeric( row.names( urban_withdrawal.df ) ) ) ), unlist( strsplit( rr, '_') )[2] ] * urban_treated_fraction.df[ as.character( max( as.numeric( row.names( urban_withdrawal.df ) ) ) ), unlist( strsplit( rr, '_') )[2] ] * urban_reuse_fraction.df[ as.character( max( as.numeric( row.names( urban_withdrawal.df ) ) ) ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2),
                 '-' )
           }
         }
       # if( yy > 2010 & tt == 'urban_treatment' )
         # {
         # if( yy <  max( as.numeric( row.names( urban_withdrawal.df ) ) ) )
           # {
           # ixDS$add_par( 'bound_activity_lo', # parameter name
                 # paste(  rr, tt, yy, mode_common , model_time, sep = '.' ), # set key
                 # round( c( urban_return.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] * urban_treated_fraction.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] * (1-urban_reuse_fraction.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2),
                 # '-' )
           # }else
           # {
           # ixDS$add_par( 'bound_activity_lo', # parameter name
                 # paste(  rr, tt, yy, mode_common , model_time, sep = '.' ), # set key
                 # round( c( urban_return.df[ as.character( max( as.numeric( row.names( urban_withdrawal.df ) ) ) ), unlist( strsplit( rr, '_') )[2] ] * urban_treated_fraction.df[ as.character( max( as.numeric( row.names( urban_withdrawal.df ) ) ) ), unlist( strsplit( rr, '_') )[2] ] * (1-urban_reuse_fraction.df[ as.character( max( as.numeric( row.names( urban_withdrawal.df ) ) ) ), unlist( strsplit( rr, '_') )[2] ]) ), digits = 2),
                 # '-' )
           # }
         # }

       # Add i-o parameters
       ret = lapply( tst[ which( tst >= as.numeric(yy) & tst <= ( as.numeric(yy) + unlist( wwt_tech_par.list[[tt]][paste('lifetime',parameter_levels,sep='.')]  ) ) ) ], function(aa){


         if( tt %in% c('urban_treatment','urban_recycle')){inlvl = water_treat_level}else{inlvl = 'final'}

         # Add the input water efficiency ratio
         ixDS$add_par( 'input', # parameter name
           paste( rr, tt, yy, aa, mode_common, rr, wwt_techs_wst[[ tt ]], inlvl, model_time, model_time, sep = '.' ), # set key
           1, # parameter value
           '-' )


         # Add the input efficiency ratio - electricity
         ixDS$add_par( 'input', # parameter name
           paste( rr, tt, yy, aa, mode_common, rr, 'electr', 'final', model_time, model_time, sep = '.' ), # set key
           round( unlist( wwt_tech_par.list[[tt]][paste('electricity_input',parameter_levels2,sep='.')] ), digits = 5 ), # parameter value
           '-' )

         # Add the output efficiency ratio - recycled water, and bound the activity based on the available return flow
         if( !is.na( wwt_techs_wout[[ tt ]] ) )
           {
           ixDS$add_par( 'output', # parameter name
             paste( rr, tt, yy, aa, mode_common, rr, wwt_techs_wout[[ tt ]], water_supply_level, model_time, model_time, sep = '.' ), # set key
             round( unlist( wwt_tech_par.list[[tt]][paste('recovery_rate',parameter_levels2,sep='.')] ), digits = 3 ), # parameter value
             '-' )
           }

         if( !is.na( wwt_techs_wems[[ tt ]] ) )
           {

           # Add treated wastewater as emission
           ixDS$add_par( 'emission_factor', # parameter name
               paste(  rr, tt, yy, aa, mode_common, wwt_techs_wems[[ tt ]], sep = '.' ), # set key
               round( unlist( wwt_tech_par.list[[tt]][paste('recovery_rate',parameter_levels2,sep='.')] ), digits = 3 ) ,
               '-' )
           }

         # Add the variable cost
         ixDS$add_par( 'var_cost', # parameter name
           paste( rr, tt, yy, aa, mode_common, model_time, sep = '.' ), # set key
           unlist( wwt_tech_par.list[[tt]][paste('var_cost',parameter_levels2,sep='.')] ),
           '-' ) # units

         # Add fixed costs
         ixDS$add_par( 'fix_cost', # parameter name
           paste( rr, tt, yy, aa, sep = '.' ), # set key
           unlist( wwt_tech_par.list[[tt]][paste('fixed_cost',parameter_levels2,sep='.')] ),
           '-' )

         # Add the capacity factor
         ixDS$add_par( 'capacity_factor', # parameter name
           paste( rr, tt, yy, aa, model_time, sep = '.' ), # set key
           unlist( wwt_tech_par.list[[tt]][paste('capacity_factor',parameter_levels,sep='.')] ),
           '-' )

         } )

       } )

     } )

   } )

 #-------------------------------------------------------------------------------------------------------
 # Add stand-alone desalination technologies and desalination demand
 #-------------------------------------------------------------------------------------------------------

 # Initialize general technology and commodity parameters for desalination
 desalination_level = 'desalination_supply'
 ret = ixDS$add_set( 'level', desalination_level ) # Add level to ix DB
 desalination_tech_type = 'desalination'
 ret = ixDS$add_set( 'type_tec', desalination_tech_type )
 desalination_commodity = 'desalinated_water'
 ret = lapply( desalination_commodity, function(x){ ixDS$add_set( 'commodity', as.character( x ) ) } ) # Add commodity to ix DB

 # Stand-alone desalination technologies to be added
 desalination_technologies = c( 'membrane_desal',
                  'distillation_desal' )

 # Define performance range - assuming freshwater output in km3 for desal tech is 1 to 1 with the activity level
 # Energy intensities from Ghaffour et al (2013) 'Technical review and evaluation of the economics of water desalination'
 # Thermal balances from 'Desalination and advanced wastewater treatment: Economics and Financing' by Corrado Sommariva
 desal_par.list = list(

   distillation = data.frame(	recovery_rate = data.frame( low = 0.4, mid = 0.35, high = 0.3 ),
                 cooling_water_withdrawal = data.frame( low = 2.4, mid = 5.2, high = 7.1 ), # Estimated from Table 6.2 in 'Desalination and advanced wastewater treatment: Economics and Financing' by Corrado Sommariva
                 cooling_water_return = data.frame( low = 2.3, mid = 5.1, high = 7.0 ), # from
                 electricity_input = data.frame( low = 0.171 , mid = 0.342, high = 0.456 ), # Converted from kWh/m3 to GW-yr/km3
                 heat_input = data.frame( low = 0.456, mid = 1.026, high = 1.368 ), # Converted from kWh/m3 to GW-yr/km3
                 thermal_pollution = data.frame( low = 0.360, mid = 0.952, high = 1.081 ), # Estimated from Figure 6.1 in 'Desalination and advanced wastewater treatment: Economics and Financing' by Corrado Sommariva
                 investment = round(data.frame( low = 1750*(1/0.365), mid = 1900*(1/0.365), high = 2200*(1/0.365) )),
                 fixed_cost = round(data.frame( low = 0.1*1350*(1/0.365), mid = 0.1*1550*(1/0.365), high = 0.1*2000*(1/0.365) )),
                 var_cost = data.frame( low = 0, mid = 0, high = 0 ),
                 capacity_factor = data.frame( low = 0.9, mid = 0.85, high = 0.8 ),
                 lifetime = data.frame( low = 50, mid = 30, high = 25 ) ) ,

   membrane = data.frame(	recovery_rate = data.frame( low = 0.45, mid = 0.4, high = 0.35 ), # inverse of recovery rate in km3/km3
               cooling_water_withdrawal = data.frame( low = 0, mid = 0, high = 0 ), #
               cooling_water_return = data.frame( low = 0, mid = 0, high = 0 ), #
               electricity_input = data.frame( low = 0.342, mid = 0.456, high = 0.571 ), # Converted from kWh/m3 to GW-yr/km3
               heat_input = data.frame( low = 0, mid = 0, high = 0 ), #
               thermal_pollution = data.frame( low = 0, mid = 0, high = 0 ), #
               investment = round( data.frame( low = 1300*(1/0.365), mid = 1550*(1/0.365), high = 1800*(1/0.365) ) ), # Converted from $/m3/day to million $ / km3 / yr
               fixed_cost = round( 0.1 * data.frame( low = 1300*(1/0.365), mid = 1650*(1/0.365), high = 1800*(1/0.365) ) ),
               var_cost = data.frame( low = 0, mid = 0, high = 0 ),
               capacity_factor = data.frame( low = 0.9, mid = 0.85, high = 0.8 ),
               lifetime = data.frame( low = 50, mid = 30, high = 25) )

   )

 inv_cost_desal = round( data.frame( membrane = ( (c(1301,1293,1244)/1301)* unlist( desal_par.list[['membrane']][paste('investment',parameter_levels2,sep='.')] ) ), distillation = ( (c(1301,1293,1244)/1301)* unlist( desal_par.list[['distillation']][paste('investment',parameter_levels2,sep='.')] ) ) ), digits = 3 )
 row.names(inv_cost_desal) = c(2020,2030,2050)

 # Load in the cleaned desal database from Hanasaki et al 2016 and add MESSAGE regions
 reg.spdf = readOGR('P:/ene.model/data/desalination','REGION_dissolved',verbose=FALSE)
 global_desal.spdf = spTransform( readOGR('P:/ene.model/data/desalination','global_desalination_plants'), crs(reg.spdf) )
 global_desal.spdf@data$region = over(global_desal.spdf,reg.spdf[,which(names(reg.spdf) == 'REGION')])
 global_desal.spdf = global_desal.spdf[ -1*which( is.na(global_desal.spdf@data$region) ), ]
 global_desal.spdf@data$msg_vintage = sapply( global_desal.spdf@data$online, function(x){ as.numeric( model_years[ which.min( ( as.numeric( model_years ) - x )^2 ) ] ) } )
 global_desal.spdf@data$technology_2 = sapply( global_desal.spdf@data$technology, function(x){ if( grepl('MSF',x)|grepl('MED',x) ){ return('distillation') }else{ return('membrane') }} )

 # From : Chart 1.1 in 'Executive Summary Desalination Technology Markets Global Demand Drivers, Technology Issues, Competitive Landscape, and Market Forecasts'
 # Global desalination capacity in 2010 was approx. 24 km3 / year
 global_desal.spdf@data$m3_per_day = global_desal.spdf@data$m3_per_day  * ( 24  / ( sum( global_desal.spdf@data$m3_per_day ) * 365 / 1e9 ) )

 # Match to message vintaging and regions
 historical_desal_capacity.list = lapply( c('membrane','distillation'), function(tt){
   temp = data.frame( do.call(cbind, lapply( region, function(reg){ sapply( unique(global_desal.spdf@data$msg_vintage)[order(unique(global_desal.spdf@data$msg_vintage))], function(y){ (365/1e9) * max( 0,  sum( global_desal.spdf@data$m3_per_day[ which( global_desal.spdf@data$msg_vintage == y & global_desal.spdf@data$region == unlist(strsplit(reg,'_'))[2] & global_desal.spdf@data$technology_2 == tt ) ] , na.rm=TRUE ), na.rm=TRUE ) } ) } ) ) )
   names(temp) = region
   row.names(temp) = unique(global_desal.spdf@data$msg_vintage)[order(unique(global_desal.spdf@data$msg_vintage))]
   return(temp)
   } )
 names(historical_desal_capacity.list) = c('membrane','distillation')

 # Add existing infrastructure and performance parameters for stand alone desal techs
 ret = lapply( names(historical_desal_capacity.list), function(tt){

   # Add to set list
   ixDS$add_set( 'technology', tt )
   ixDS$add_set( 'cat_tec', paste( 'investment_other' , tt, sep='.') ) # Add technology to list of water system investments
   ixDS$add_set( 'cat_tec', paste( desalination_tech_type, tt, sep='.') ) # Add technology to list of desal techs

   # Performance parameters - across all historical and future years
   lapply( region, function(rr){ lapply( model_years[ which( model_years >= min( as.numeric( row.names( historical_desal_capacity.list[[tt]] ) ) ) ) ], function(yy){

     cst = inv_cost_desal[ which.min( (as.numeric(yy) - as.numeric(row.names(inv_cost_desal)))^2 ),tt ]
     # Add the investment cost
     ixDS$add_par( 'inv_cost', # parameter name
         paste(rr, tt, yy, sep = '.' ), # set key
         unlist( cst ),
         '-' )

     # Add the technical lifetime
     ixDS$add_par( 'technical_lifetime', # parameter name
           paste(rr, tt, yy, sep = '.' ), # set key
           unlist( desal_par.list[[tt]][paste('lifetime',parameter_levels2,sep='.')] ),
           'y' )

     # Add the construction time
     ixDS$add_par( 'construction_time', # parameter name
           paste(rr, tt, yy, sep = '.' ), # set key
           3,
           'y' )

     # Add the historical capacity and activity for locked-in future years in the mitigation and no policy scenarios (e.g., 2030, 2040)
     if( yy > 2010 & yy < firstmodelyear )
       {
       ixDS$add_par( 'historical_activity', paste(  rr, as.character( tt ), yy, mode_common , model_time, sep = '.' ), max(0, base_hist_act[ which( as.character( unlist( base_hist_act$tec ) ) == as.character( d_tech[[ tt ]] ) & as.numeric( unlist( base_hist_act$year_all ) ) == yy & as.character( unlist( base_hist_act$node ) ) == rr ), 'val' ], na.rm=TRUE), '-' )
       ixDS$add_par( 'historical_new_capacity', paste(  rr, as.character( tt ), yy, sep = '.' ), max(0, base_hist_ncap[ which( as.character( unlist( base_hist_ncap$tec ) ) == as.character( d_tech[[ tt ]] ) & as.numeric( unlist( base_hist_ncap$year_all ) ) == yy & as.character( unlist( base_hist_ncap$node ) ) == rr ), 'val' ], na.rm=TRUE), '-' )
       }

     # Add i-o parameters
     lapply( model_years[ which( model_years >= as.numeric(yy) & model_years <= ( as.numeric(yy) + unlist( desal_par.list[[tt]][paste('lifetime',parameter_levels,sep='.')]  ) ) ) ], function(aa){

       # Add the output efficiency ratio
       ixDS$add_par( 'output', # parameter name
         paste( rr, tt, yy, aa, mode_common, rr, 'desalinated_water', desalination_level, model_time, model_time, sep = '.' ), # set key
         1, # parameter value
         '-' )

       # Add the input efficiency ratio - seawater
       ixDS$add_par( 'input', # parameter name
         paste( rr, tt, yy, aa, mode_common, rr, 'saline_supply', water_supply_level, model_time, model_time, sep = '.' ), # set key
         round( c( unlist( desal_par.list[[tt]][ paste('cooling_water_withdrawal',parameter_levels2,sep='.') ] ) + unlist( desal_par.list[[tt]][paste('recovery_rate',parameter_levels,sep='.')] )^-1 ), digits = 5), # parameter value
         '-' )

       # Add the input efficiency ratio - electricity
       ixDS$add_par( 'input', # parameter name
         paste( rr, tt, yy, aa, mode_common, rr, 'electr', 'final', model_time, model_time, sep = '.' ), # set key
         round( unlist( desal_par.list[[tt]][paste('electricity_input',parameter_levels2,sep='.')] ), digits = 5 ), # parameter value
         '-' )

       # Add the input efficiency ratio - heat
       ixDS$add_par( 'input', # parameter name
         paste( rr, tt, yy, aa, mode_common, rr, 'd_heat', 'final', model_time, model_time, sep = '.' ), # set key
         round( unlist( desal_par.list[[tt]][paste('heat_input',parameter_levels2,sep='.')] ), digits = 5 ), # parameter value
         '-' )

       # Add thermal pollution emission
       ixDS$add_par( 'emission_factor', # parameter name
           paste(  rr, tt, yy, aa, mode_common, 'saline_thermal_pollution', sep = '.' ), # set key
           round( unlist( desal_par.list[[tt]][paste('thermal_pollution',parameter_levels2,sep='.')] ), digits = 5 ),
           '-' )

       # Add wastewater (brine)
       ixDS$add_par( 'emission_factor', # parameter name
           paste(  rr, tt, yy, aa, mode_common, 'saline_wastewater', sep = '.' ), # set key
           round( c( unlist( desal_par.list[[tt]][paste('cooling_water_return',parameter_levels2,sep='.')] + (1 - desal_par.list[[tt]][paste('recovery_rate',parameter_levels,sep='.')] ) ) ), digits = 5) ,
           '-' )

       # Add the variable cost
       ixDS$add_par( 'var_cost', # parameter name
         paste( rr, tt, yy, aa, mode_common, model_time, sep = '.' ), # set key
         unlist( desal_par.list[[tt]][paste('var_cost',parameter_levels2,sep='.')] ),
         '-' ) # units

       # Add fixed costs
       ixDS$add_par( 'fix_cost', # parameter name
         paste( rr, tt, yy, aa, sep = '.' ), # set key
         unlist( desal_par.list[[tt]][paste('fixed_cost',parameter_levels2,sep='.')] ),
         '-' )

       # Add the capacity factor
       ixDS$add_par( 'capacity_factor', # parameter name
         paste( rr, tt, yy, aa, model_time, sep = '.' ), # set key
         unlist( desal_par.list[[tt]][paste('capacity_factor',parameter_levels2,sep='.')] ),
         '-' )

       } )

     } ) } )

   # Add existing infrastructure and calibrate demands
   lapply( region, function(rr){ lapply( row.names( historical_desal_capacity.list[[tt]] ), function(yy){

     # Add the historical capacity
     ixDS$add_par( 'historical_new_capacity',
             paste( rr, tt, yy, sep = '.' ),
             round( 1/5*unlist( historical_desal_capacity.list[[tt]][as.character(yy),rr] ), digits = 5),
             '-' )

     # Add the historical activity, calibrate the demands and bound lower activities of historical capacity to ensure utilization in future years
     lapply( model_years[ which( model_years >= as.numeric(yy) & model_years <= ( as.numeric(yy)+ 5 + unlist( desal_par.list[[tt]][paste('lifetime',parameter_levels,sep='.')]  ) ) ) ], function(aa){

       # Add the historical activity and demands
       if( as.numeric( aa ) <= 2010 )
         {
         chk =  tryCatch( ixDS$par( 'historical_activity', list( node_loc = rr, technology = tt, year_act = aa, mode = mode_common, time = model_time ) ), error = function(e){} )
         if( is.null( chk ) )
           {
           ixDS$add_par( 'historical_activity', # parameter name
             paste(  rr, tt, aa, mode_common , model_time, sep = '.' ), # set key
             round( unlist( desal_par.list[[tt]][paste('capacity_factor',parameter_levels,sep='.')] ) * unlist( historical_desal_capacity.list[[tt]][as.character(yy),rr] ), digits = 5 ), # parameter value
             '-' )
           }else
           {
           ixDS$add_par( 'historical_activity', # parameter name
             paste(  rr, tt, aa, mode_common , model_time, sep = '.' ), # set key
             round( unlist( chk['value'] ) + unlist( desal_par.list[[tt]][paste('capacity_factor',parameter_levels,sep='.')] ) * unlist( historical_desal_capacity.list[[tt]][as.character(yy),rr] ), digits = 5 ), # parameter value
             '-' )
           }
         }else
         {
         # Add lower activity bounds in future years to calibrate to make sure capacity is utilized - added cumulatively to account for vintaging
         chk =  tryCatch( ixDS$par( 'bound_activity_lo', list( node_loc = rr, technology = tt, year_act = aa, mode = mode_common, time = model_time ) ), error = function(e){} )
         if( is.null( chk ) )
           {
           ixDS$add_par( 'bound_activity_lo', # parameter name
             paste(  rr, tt, aa, mode_common , model_time, sep = '.' ), # set key
             round( unlist( desal_par.list[[tt]][paste('capacity_factor',parameter_levels,sep='.')] ) * unlist( historical_desal_capacity.list[[tt]][as.character(yy),rr] ), digits = 5 ), # parameter value
             '-' )
           }else
           {
           ixDS$add_par( 'bound_activity_lo', # parameter name
             paste(  rr, tt, aa, mode_common , model_time, sep = '.' ), # set key
             round( unlist( chk['value'] ) + unlist( desal_par.list[[tt]][paste('capacity_factor',parameter_levels,sep='.')] ) * unlist( historical_desal_capacity.list[[tt]][as.character(yy),rr] ), digits = 5 ), # parameter value
             '-' )
           }
         }

       # calibrate energy demands
       chk =  tryCatch( ixDS$par( 'demand', list( node = rr, commodity = 'i_spec', level = 'useful', year = aa, time = model_time ) ), error = function(e){} )
       if(!is.null( chk ) )
         {
         ixDS$add_par( 'demand', # parameter name
           paste( rr, 'i_spec', 'useful', aa, model_time, sep = '.' ), # set key
           round( c( max( 0,  unlist( chk['value'] ) - unlist( desal_par.list[[tt]][paste('capacity_factor',parameter_levels,sep='.')] ) * unlist( historical_desal_capacity.list[[tt]][as.character(yy),rr] ) * unlist( desal_par.list[[tt]][paste('electricity_input',parameter_levels,sep='.')] ) ) ),  digits = 5),
           'GWa' )
         }

       chk =  tryCatch( ixDS$par( 'demand', list( node = rr, commodity = 'i_therm', level = 'useful', year = aa, time = model_time ) ), error = function(e){} )
       if(!is.null( chk ) )
         {
         ixDS$add_par( 'demand', # parameter name
           paste( rr, 'i_therm', 'useful', aa, model_time, sep = '.' ), # set key
           round( c( max( 0,  ( unlist( chk['value'] ) - unlist( desal_par.list[[tt]][paste('capacity_factor',parameter_levels,sep='.')] ) * unlist( historical_desal_capacity.list[[tt]][as.character(yy),rr] ) * unlist( desal_par.list[[tt]][paste('heat_input',parameter_levels,sep='.')] ) ) ) ), digits = 5),
           'GWa' )
         }

       } )

     } ) } )

   } )

 #### Aggregate the desalination production to allow for activity constraints covering all types of this technology group

 # Add to set list
 tt = 'desal_t_d'
 ixDS$add_set( 'technology', tt )
 ixDS$add_set( 'cat_tec', paste( 'investment_other' , tt, sep='.') ) # Add technology to list of water system investments
 ixDS$add_set( 'cat_tec', paste( desalination_tech_type, tt, sep='.') ) # Add technology to list of desal techs

 # Performance parameters - across all historical and future years
 ret = lapply( region, function(rr){ lapply( model_years[ which( model_years >= min( as.numeric( row.names( historical_desal_capacity.list[[1]] ) ) ) ) ], function(yy){

   # Add the investment cost
   ixDS$add_par( 'inv_cost', # parameter name
       paste(rr, tt, yy, sep = '.' ), # set key
       0,
       '-' )

   # Add the technical lifetime
   ixDS$add_par( 'technical_lifetime', # parameter name
         paste(rr, tt, yy, sep = '.' ), # set key
         1,
         'y' )

   # Add the construction time
   ixDS$add_par( 'construction_time', # parameter name
         paste(rr, tt, yy, sep = '.' ), # set key
         1,
         'y' )

   # Add i-o parameters
   lapply( model_years[ which( model_years >= as.numeric(yy) & model_years <= ( as.numeric(yy) + unlist( desal_par.list[[1]][paste('lifetime',parameter_levels,sep='.')]  ) ) ) ], function(aa){

     # Add the output efficiency ratio
     ixDS$add_par( 'output', # parameter name
       paste( rr, tt, yy, aa, mode_common, rr, 'freshwater_supply', water_supply_level, model_time, model_time, sep = '.' ), # set key
       1, # parameter value
       '-' )

     # Add the input efficiency ratio
     ixDS$add_par( 'input', # parameter name
       paste( rr, tt, yy, aa, mode_common, rr, 'desalinated_water', desalination_level, model_time, model_time, sep = '.' ), # set key
       1,
       '-' )

     # Add the variable cost
     ixDS$add_par( 'var_cost', # parameter name
       paste( rr, tt, yy, aa, mode_common, model_time, sep = '.' ), # set key
       0,
       '-' ) # units

     # Add fixed costs
     ixDS$add_par( 'fix_cost', # parameter name
       paste( rr, tt, yy, aa, sep = '.' ), # set key
       0,
       '-' )

     # Add the capacity factor
     ixDS$add_par( 'capacity_factor', # parameter name
       paste( rr, tt, yy, aa, model_time, sep = '.' ), # set key
       1,
       '-' )

     } )

   } ) } )

 # Add existing infrastructure
 ret = lapply( names(historical_desal_capacity.list), function(dd){ lapply( region, function(rr){ lapply( row.names( historical_desal_capacity.list[[dd]] ), function(yy){

   # Add the historical capacity
   chk =  tryCatch( ixDS$par( 'historical_new_capacity', list( node_loc = rr, technology = tt, year = yy ) ), error = function(e){} )
     if( is.null( chk ) )
       {
       ixDS$add_par( 'historical_new_capacity',
           paste( rr, tt, yy, sep = '.' ),
           round( 1/5 * unlist( historical_desal_capacity.list[[dd]][as.character(yy),rr] ), digits = 5),
           '-' )
       }else
       {
       ixDS$add_par( 'historical_new_capacity',
           paste( rr, tt, yy, sep = '.' ),
           round( unlist( chk['value'] ) + 1/5 * unlist( historical_desal_capacity.list[[dd]][as.character(yy),rr] ), digits = 5),
           '-' )
       }

   # Add the historical activity
   tstp = model_years[ which( model_years >= as.numeric(yy) & model_years <= 2010 ) ]
   if( length(tstp) > 0 )
     {
     lapply( tstp, function(aa){

       chk =  tryCatch( ixDS$par( 'historical_activity', list( node_loc = rr, technology = tt, year_act = aa, mode = mode_common, time = model_time ) ), error = function(e){} )

       if( is.null( chk ) )
         {
         ixDS$add_par( 'historical_activity', # parameter name
           paste(  rr, tt, aa, mode_common , model_time, sep = '.' ), # set key
           round( unlist( desal_par.list[[dd]][paste('capacity_factor',parameter_levels,sep='.')] ) * unlist( historical_desal_capacity.list[[dd]][as.character(yy),rr] ), digits = 5 ), # parameter value
           '-' )
         }else
         {
         ixDS$add_par( 'historical_activity', # parameter name
           paste(  rr, tt, aa, mode_common , model_time, sep = '.' ), # set key
           round( unlist( chk['value'] ) + unlist( desal_par.list[[dd]][paste('capacity_factor',parameter_levels,sep='.')] ) * unlist( historical_desal_capacity.list[[dd]][as.character(yy),rr] ), digits = 5 ), # parameter value
           '-' )
         }

       } )

     }

   } ) } ) } )

 # # Add future desalination demands based on projections developed previously  - set as activity bound on aggregate desal tech
 ret = lapply( region, function(rr){ lapply( model_years[ which( model_years > 2010 ) ], function(yy){
   if( as.numeric(yy) < 2100 )
     {
     ixDS$add_par( 'bound_activity_lo', # parameter name
         paste(  rr, tt, yy, mode_common , model_time, sep = '.' ), # set key
         round( c( urban_desal_fraction.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] * urban_withdrawal.df[ as.character( yy ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2), # parameter value
         '-' )
     }else
     {
     ixDS$add_par( 'bound_activity_lo', # parameter name
         paste(  rr, tt, yy, mode_common , model_time, sep = '.' ), # set key
         round( c( urban_desal_fraction.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ] * urban_withdrawal.df[ as.character( 2090 ), unlist( strsplit( rr, '_') )[2] ] ), digits = 2), # parameter value
         '-' )
     }
   } ) } )

 #-------------------------------------------------------------------------------------------------------
 # Add water withdrawal and return flow for thermal ppl cooling technologies
 #-------------------------------------------------------------------------------------------------------

 # Technologies with historical capacity and activity
 all_historical_new_capacity = ixDS$par( 'historical_new_capacity' )
 all_historical_activity = ixDS$par( 'historical_activity' )

 # Add the cooling technology data to the db
 skipped_tech_reg = NULL
 ret = lapply( cooled_technologies_in_message, function(tech){

   # Status update
   print(paste( round( 100 * ( ( which( cooled_technologies_in_message == tech ) - 1 ) / ( length(cooled_technologies_in_message) ) ) ), ' % complete', sep=''))

   # Need to grab the entire data series for these variables to check whether they exist in all regions: there is probably a more efficient way to do this.
   all_output = ixDS$par( 'output', list(technology = tech) )
   all_technical_lifetime = ixDS$par( 'technical_lifetime', list(technology = tech) )
   all_construction_time = ixDS$par( 'construction_time', list(technology = tech) )
   all_fix_cost = ixDS$par( 'fix_cost', list(technology = tech) )

   all_inv_cost = ixDS$par( 'inv_cost', list(technology = tech) )

   lapply( region, function(reg){ # go through each region

     # Retrieve the output for this particular cooled technology in MESSAGE
     if(length(which(as.character(all_output$node_loc) == reg))>0)
       {

       # Grab the output activity ratio
       output = ixDS$par( 'output', list( node_loc = reg, technology = tech ) )

       # Check if multiple modes - only need one
       if( length(unique(output$mode)) > 1 ){ output = output[ which( output$mode == unique(output$mode)[1] ) , ] }

       # Check if multiple commodities - only need one
       if( length(unique(output$commodity)) > 1 ){ output = output[ which( output$commodity == unique(output$commodity)[1] ) , ] }

       # Define the vintaging and time slicing parameters for the cooling technologies to match the cooled MESSAGE techs in the db
       ind = data.frame( 	year_vtg = as.character( output$year_vtg ),
                 year_act = as.character( output$year_act ),
                 mode = as.character( output$mode ),
                 time = as.character( output$time ) 	)

       # Retrieve the input for this particular cooled technology in MESSAGE
       if( !( tech %in% names( manually_set_efficiencies ) ) )
         {

         # Grab the input activity ratio
         input = ixDS$par( 'input', list( node_loc = reg, technology = tech ) )

         # Check if multiple modes
         if( length(unique(input$mode)) > 1 ){ input = input[ which( input$mode == unique(input$mode)[1] ) , ] }

         # Check if multiple commodities
         if( length(unique(input$commodity)) > 1 ){ input = input[ which( input$commodity == unique(input$commodity)[1] ) , ] }

         # Check if output and input matrices of different length
         if( nrow(ind) == nrow(input) ){ input_vec = input$value }else{ print('output and input different lengths') }

         }else{ input_vec = rep( (1/manually_set_efficiencies[[ tech ]]), nrow(ind) ) } # Use the manually set values where applicable

       # Retrieve the historical capacity for this particular cooled technology in MESSAGE
       if(length(which( as.character(all_historical_new_capacity$node_loc) == reg & as.character(all_historical_new_capacity$technology) == tech ))>0)
         {
         historical_new_capacity = ixDS$par( 'historical_new_capacity', list( node_loc = reg, technology = tech ) )
         }else
         {
         historical_new_capacity = NULL
         }

       # Retrieve the historical activity for this particular cooled technology in MESSAGE
       if(length(which( as.character(all_historical_activity$node_loc) == reg & as.character(all_historical_activity$technology) == tech ))>0)
         {
         historical_activity = ixDS$par( 'historical_activity', list( node_loc = reg, technology = tech ) )
         }else
         {
         historical_activity = NULL
         }

       # Technical lifetime
       if(length(which(as.character(all_technical_lifetime$node_loc) == reg))>0){ technical_lifetime = ixDS$par( 'technical_lifetime', list( node_loc = reg, technology = tech ) ) }

       # Investment costs
       if(length(which(as.character(all_inv_cost$node_loc) == reg))>0){ inv_cost = ixDS$par( 'inv_cost', list( node_loc = reg, technology = tech ) ) }

       # Fixed costs
       if(length(which(as.character(all_fix_cost$node_loc) == reg))>0){ fix_cost = ixDS$par( 'fix_cost', list( node_loc = reg, technology = tech ) ) }

       # Construction time
       if(length(which(as.character(all_construction_time$node_loc) == reg))>0){ construction_time = ixDS$par( 'construction_time', list( node_loc = reg, technology = tech ) ) }

       # Get the name of the cooling technologies for this particular cooled MESSAGE technology
       techs_to_update = cooling_technologies_to_consider[ which( as.character( unlist(data.frame(strsplit( cooling_technologies_to_consider, '__'))[1,]) )  == as.character(tech) ) ]
       id2 = apply(cbind(as.character( unlist(data.frame(strsplit( techs_to_update, '__'))[2,]) ),as.character( unlist(data.frame(strsplit( techs_to_update, '__'))[1,]) )),1,paste,collapse='_') # alternate ID in csv file

       # Go through each vintage and cooling option and add the corresponding data to the DB
       ret = lapply( 1:length(ind$year_vtg), function(v){ lapply( 1:length(techs_to_update), function(ttt){

         # Add the technology to the set list - only need to for one region
         if(reg == region[1])
           {
           ixDS$add_set( 'technology', as.character( techs_to_update[ ttt ] ) )
           ixDS$add_set( 'cat_tec', paste( power_plant_cooling_tech_type , as.character( techs_to_update[ ttt ] ), sep='.') ) # Add technology to list of cooling technology types
           ixDS$add_set( 'cat_tec', paste( 'investment_electricity' , as.character( techs_to_update[ ttt ] ), sep='.') ) # Add technology to list of electricity system investments
           }

         # Set the input commodity using the name from the csv file, define output commodity using names generated and initialized previously
         cmdty_in = as.character( tech_water_performance_ssp_msg_raw.df$water_supply_type[ which( as.character(tech_water_performance_ssp_msg_raw.df$technology_name) == id2[ ttt ] ) ] )
         if( !is.na(cmdty_in) ){ if( cmdty_in == 'saline_supply' ){ cmdty_in = 'saline_supply_ppl' } }
         cmdty_out = cooling_commodities[ which( unlist( strsplit( cooling_commodities, '__') )[seq(2, length(unlist( strsplit( cooling_commodities, '__') )), by=2)] == tech ) ]

         ## Set the water withdrawal, return flow, thermal pollution and parasitic electricity use for this cooling technology (as intensities)

           # Get the heat lost to emissions and electricity production and use to compute cooling fraction
           emissions_heat_fraction = as.numeric(tech_water_performance_ssp_msg_raw.df$emissions_heat_fraction[ which( as.character(tech_water_performance_ssp_msg_raw.df$technology_name) == tech  ) ]) # fraction of heat lost through emissions
           cooling_fraction = input_vec * ( 1 - emissions_heat_fraction / max(input_vec) ) - 1  # scale heat lost to emission proportionally to the changes in the cooling fraction

           # Scale historical withdrawal to follow heat rate improvements
           water_withdrawal = round( ( 60 * 60 * 24 * 365 ) * ( 1e-9 ) * ( cooling_fraction / max(cooling_fraction) ) * as.numeric( tech_water_performance_ssp_msg_raw.df[ which( as.character(tech_water_performance_ssp_msg_raw.df$technology_name) == id2[ttt]  ), which(as.character(names(tech_water_performance_ssp_msg_raw.df)) == paste('water_withdrawal_',parameter_levels,'_m3_per_output',sep='') ) ] ), digits = rnd )

           # Return flow using consumption intensity
           return_flow = round( water_withdrawal * ( 1 - as.numeric( tech_water_performance_ssp_msg_raw.df[ which( as.character(tech_water_performance_ssp_msg_raw.df$technology_name) == id2[ ttt ]  ), which(as.character(names(tech_water_performance_ssp_msg_raw.df)) == paste('water_consumption_',parameter_levels,'_m3_per_output',sep='') ) ] ) / as.numeric( tech_water_performance_ssp_msg_raw.df[ which( as.character(tech_water_performance_ssp_msg_raw.df$technology_name) == id2[ttt]  ), which(as.character(names(tech_water_performance_ssp_msg_raw.df)) == paste('water_withdrawal_',parameter_levels,'_m3_per_output',sep='') ) ] ) ), digits = rnd )

           # Parasitic electricity consumption
           parasitic_electricity = as.numeric( tech_water_performance_ssp_msg_raw.df[ which( as.character(tech_water_performance_ssp_msg_raw.df$technology_name) == id2[ ttt ]  ), 'parasitic_electricity_demand_fraction' ] )

         ##
         # Add the variable cost
         ixDS$add_par( 'var_cost', # parameter name
           paste( reg, techs_to_update[ ttt ], as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ), as.character( ind$mode[ v ] ), as.character( ind$time[ v ] ), sep = '.' ), # set key
           round( ( 1e3 ) * cooling_technology_costs$var_costs[ which(as.character(row.names(cooling_technology_costs)) == as.character(techs_to_update[ ttt ]) ) ],digits = rnd), # parameter value
           'USD/GWa' ) # units

         # Add the capacity factor
         ixDS$add_par( 'capacity_factor', # parameter name
           paste( reg, techs_to_update[ ttt ], as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ),  as.character( ind$time[ v ] ), sep = '.' ), # set key
           1, # Assume for now that cooling technologies are always available
           '-' ) # units

         # Add the output efficiency ratio
         ixDS$add_par( 'output', # parameter name
           paste( reg, techs_to_update[ ttt ], as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ), as.character( ind$mode[ v ] ), reg, cmdty_out, cooling_level, as.character( ind$time[ v ] ), as.character( ind$time[ v ] ), sep = '.' ), # set key
           1, # parameter value
           '-' )

         if( unlist(strsplit(id2[ttt],'_'))[1] != 'air' ) # Don't need to add water use inputs for air cooling technologies
           {

           # Add the input efficiency ratio - water withdrawal
           ixDS$add_par( 'input', # parameter name
             paste( reg, techs_to_update[ ttt ], as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ), as.character( ind$mode[ v ] ), reg, cmdty_in, water_supply_level, as.character( ind$time[ v ] ), as.character( ind$time[ v ] ), sep = '.' ), # set key
             water_withdrawal[ v ], # parameter value
             '-' )

           # Add the thermal pollution emission factor
           if( unlist(strsplit(id2[ttt],'_'))[1] == 'ot' ) # only for once through cooling technologies
             {
             if( unlist(strsplit(id2[ttt],'_'))[2] == 'fresh' ){ emis = 'fresh_thermal_pollution' }else{ emis = 'saline_thermal_pollution' }
             ixDS$add_par( 'emission_factor', # parameter name
             paste( reg, techs_to_update[ ttt ], as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ), as.character( ind$mode[ v ] ), emis, sep = '.' ), # set key
             round( cooling_fraction[ v ], digits = rnd ), # parameter value
             '-' )
             }

           # Add the wastewater emission factor
           if( unlist(strsplit(id2[ttt],'_'))[2] == 'fresh' ){ emis = 'fresh_wastewater' }else{ emis = 'saline_wastewater' }
           ixDS$add_par( 'emission_factor', # parameter name
           paste( reg, techs_to_update[ ttt ], as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ), as.character( ind$mode[ v ] ), emis, sep = '.' ), # set key
           return_flow[ v ], # parameter value
           '-' )

           }

         # Add the input efficiency ratio - parasitic electricity consumption
         if( parasitic_electricity > 0 ) # only for some cooling technologies
           {
           ixDS$add_par( 'input', # parameter name
           paste( reg, techs_to_update[ ttt ], as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ), as.character( ind$mode[ v ] ), reg, 'electr', 'secondary', as.character( ind$time[ v ] ), as.character( ind$time[ v ] ), sep = '.' ), # set key
           parasitic_electricity, # parameter value
           '-' )
           }

         # Add the cooling commodity to the cooled message technology input list
         if( ttt == 1 ) # only need to do once
           {
           ixDS$add_par( 'input', # parameter name
             paste( reg, tech, as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ), as.character( ind$mode[ v ] ), reg, cmdty_out, cooling_level, as.character( ind$time[ v ] ), as.character( ind$time[ v ] ), sep = '.' ), # set key
             1, # parameter value
             '-' )
           }

         } ) } )

       # Go through each vintage and cooling option and add the corresponding data to the DB for investement costs
       ret = lapply( 1:length(inv_cost$year_vtg), function(v){ lapply( 1:length(techs_to_update), function(ttt){
         # Add the investment cost
         ixDS$add_par( 'inv_cost', # parameter name
             paste( reg, techs_to_update[ ttt ], as.character( inv_cost$year_vtg[ v ] ), sep = '.' ), # set key
             round( ( 1e3 ) * cooling_technology_costs$inv_costs[ which(as.character(row.names(cooling_technology_costs)) == as.character(techs_to_update[ ttt ]) ) ],digits = rnd), # parameter value (convert from mill USD/MW to USD/GWa)
             'USD/GWa' )
         } ) } )

       # Go through each vintage and cooling option and add the corresponding data to the DB for investement costs
       ret = lapply( 1:length(fix_cost$year_vtg), function(v){ lapply( 1:length(techs_to_update), function(ttt){
         # Add the fixed cost
         ixDS$add_par( 'fix_cost', # parameter name
           paste( reg, techs_to_update[ ttt ], as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ), sep = '.' ), # set key
           round( ( 1e3 ) * cooling_technology_costs$fixed_costs[ which(as.character(row.names(cooling_technology_costs)) == as.character(techs_to_update[ ttt ]) ) ],digits = rnd), # parameter value
           'USD/GWa' )
         } ) } )

       # Go through each vintage and cooling option and add the corresponding data to the DB for historical capacity
       ret = lapply( 1:length(historical_new_capacity$year_vtg), function(v){ lapply( 1:length(techs_to_update), function(ttt){

         # Add the historical capacity
         if( !is.null(historical_new_capacity) ) # Check if historical capacity exists
           {
           # The output of the power plant cooling technologies are defined in terms of the electric power output supported
           # Use the historical capacity for each cooled power plant type and the share of each cooling technology to estimate the historical capacity
           if(use_davies_shares)
             {
             shr = cooltech_cost_and_shares_ssp_msg_raw.df[ which( ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$utype) == unlist(strsplit(techs_to_update[ ttt ],'__'))[1] ) & ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$cooling) == unlist(strsplit(techs_to_update[ ttt ],'__'))[2] ) ), paste('mix',unlist(strsplit(reg,'_'))[2],'Davies_2013',sep='_') ]
             }else
             {
             shr = cooltech_cost_and_shares_ssp_msg_raw.df[ which( ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$utype) == unlist(strsplit(techs_to_update[ ttt ],'__'))[1] ) & ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$cooling) == unlist(strsplit(techs_to_update[ ttt ],'__'))[2] ) ), paste('mix',unlist(strsplit(reg,'_'))[2],sep='_') ]
             }
           cap = round( shr * historical_new_capacity[ which( as.character( historical_new_capacity$year_vtg ) == as.character( historical_new_capacity$year_vtg[v] )  ) ,  'value' ], digits = rnd )

           # Add the ix db
           ixDS$add_par( 'historical_new_capacity',
             paste( reg, techs_to_update[ ttt ], as.character( historical_new_capacity$year_vtg[ v ] ), sep = '.' ),
             cap,
             as.character( historical_new_capacity$unit[1] ) )
           }

         } ) } )

       # Go through each year and cooling option and add the corresponding data to the DB for historical activity
       ret = lapply( 1:length(historical_activity$year_act), function(v){ lapply( 1:length(techs_to_update), function(ttt){

         # Add the historical_activity
         if( !is.null(historical_activity) ) # Check if historical_activity exists
           {

           # The output of the power plant cooling technologies are defined in terms of the electric power output supported
           # Use the historical_activity for each cooled power plant type and the share of each cooling technology to estimate the historical activity
           if(use_davies_shares) # whether or not to use the share from Davies or Raptis
             {
             shr = cooltech_cost_and_shares_ssp_msg_raw.df[ which( ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$utype) == unlist(strsplit(techs_to_update[ ttt ],'__'))[1] ) & ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$cooling) == unlist(strsplit(techs_to_update[ ttt ],'__'))[2] ) ), paste('mix',unlist(strsplit(reg,'_'))[2],'Davies_2013',sep='_') ]
             }else
             {
             shr = cooltech_cost_and_shares_ssp_msg_raw.df[ which( ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$utype) == unlist(strsplit(techs_to_update[ ttt ],'__'))[1] ) & ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$cooling) == unlist(strsplit(techs_to_update[ ttt ],'__'))[2] ) ), paste('mix',unlist(strsplit(reg,'_'))[2],sep='_') ]
             }
           act = round( shr * historical_activity[ which( as.character( historical_activity$year_act ) == as.character( historical_activity$year_act[v] )  ) ,  'value' ], digits = rnd )

           # Add the ix db
           ixDS$add_par( 'historical_activity',
             paste( reg, techs_to_update[ ttt ], as.character( historical_activity$year_act[ v ] ), as.character( historical_activity$mode[ v ] ) , as.character( historical_activity$time[ v ] ), sep = '.' ),
             act,
             as.character( historical_activity$unit[1] ) )

           }
         } ) } )

       ret = lapply( 1:length(technical_lifetime$year_vtg), function(v){ lapply( 1:length(techs_to_update), function(ttt){
         # Add the technical lifetime
         ixDS$add_par( 'technical_lifetime', # parameter name
             paste( reg, techs_to_update[ ttt ], as.character( technical_lifetime$year_vtg[ v ] ), sep = '.' ), # set key
             technical_lifetime[ v ,  'value' ],
             'y' )
         } ) } )

       ret = lapply( 1:length(construction_time$year_vtg), function(v){ lapply( 1:length(techs_to_update), function(ttt){
         # Add the construction time
         ixDS$add_par( 'construction_time', # parameter name
             paste( reg, techs_to_update[ ttt ], as.character( construction_time$year_vtg[ v ] ), sep = '.' ), # set key
             construction_time[ v ,  'value' ],
             as.character( construction_time[ v ,  'unit' ] ) )
         } ) } )

       }else{ skipped_tech_reg = rbind( skipped_tech_reg, data.frame( technology = tech, region = reg, comb = paste(tech,reg,sep='_') ) ) } # add to list of skipped techs
     } )
   } )

 #-------------------------------------------------------------------------------------------------------
 # Add water withdrawal and return flow for non-cooling technologies
 #-------------------------------------------------------------------------------------------------------

 print('Adding water coefficients for non-cooling technologies')

 # Could speed this up a bit by allocating the ppl water use during the previous step.

 if(ssss == 'SSP3')
   {
   skipped_techs = c('igcc_co2scr','gfc_co2scr','cfc_co2scr','h2_co2_scrub','h2b_co2_scrub','gas_htfc','h2_bio','h2_bio_ccs','h2_smr','h2_smr_ccs','h2_coal','h2_coal_ccs','h2_elec','solar_pv_ppl','wind_ppl')
   }else
   {
   skipped_techs = c('igcc_co2scr','gfc_co2scr','cfc_co2scr','h2_co2_scrub','h2b_co2_scrub','gas_htfc','solar_pv_ppl','wind_ppl')
   }
 message_technologies_with_water_data2 = message_technologies_with_water_data[which(!(message_technologies_with_water_data %in% skipped_techs))]
 ret = lapply( message_technologies_with_water_data2, function(tech){

   # Status update
   print(paste( round( 100 * ( ( which( message_technologies_with_water_data2 == tech ) - 1 ) / ( length(message_technologies_with_water_data2) ) ) ), ' % complete', sep=''))

   all_output = ixDS$par( 'output', list(technology = tech) )

   lapply( region, function(reg){

     if( length(which(as.character(all_output$node_loc) == reg))>0 )
       {

       output = ixDS$par( 'output', list( node_loc = reg, technology = tech ) )

       # Check if multiple modes - only need one
       if( length(unique(output$mode)) > 1 ){ output = output[ which( output$mode == unique(output$mode)[1] ) , ] }

       # Check if multiple commodities - only need one
       if( length(unique(output$commodity)) > 1 ){ output = output[ which( output$commodity == unique(output$commodity)[1] ) , ] }

       # Define the vintaging and time slicing parameters
       ind = data.frame( 	year_vtg = as.character( output$year_vtg ),
                 year_act = as.character( output$year_act ),
                 mode = as.character( output$mode ),
                 time = as.character( output$time ) 	)

       # Input commodity
       cmdty_in = as.character( tech_water_performance_ssp_msg_raw.df$water_supply_type[ which( as.character(tech_water_performance_ssp_msg_raw.df$technology_name) == tech ) ] )
       if( !is.na(cmdty_in) ){ if( cmdty_in == 'saline_supply' ){ cmdty_in = 'saline_supply_ppl' } }

       # Add the water coefficients for each vintage and time slice
       ret = lapply( 1:nrow( ind ), function(v){

         # Using data from input csv files and converted from m3 / GJ to km3 / GWa
         water_withdrawal = round( ( 60 * 60 * 24 * 365 ) * ( 1e-9 ) * as.numeric( tech_water_performance_ssp_msg_raw.df[ which( as.character(tech_water_performance_ssp_msg_raw.df$technology_name) == tech  ), which(as.character(names(tech_water_performance_ssp_msg_raw.df)) == paste('water_withdrawal_',parameter_levels,'_m3_per_output',sep='') ) ] ), digits = rnd )
         return_flow = round( water_withdrawal - ( 60 * 60 * 24 * 365 ) * ( 1e-9 ) * as.numeric( tech_water_performance_ssp_msg_raw.df[ which( as.character(tech_water_performance_ssp_msg_raw.df$technology_name) == tech  ), which(as.character(names(tech_water_performance_ssp_msg_raw.df)) == paste('water_consumption_',parameter_levels,'_m3_per_output',sep='') ) ] ), digits = rnd )

         # Add the withdrawal to db as an input
         ixDS$add_par( 'input', # parameter name
           paste( reg, tech, as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ), as.character( ind$mode[ v ] ), reg, cmdty_in, water_supply_level, as.character( ind$time[ v ] ), as.character( ind$time[ v ] ), sep = '.' ), # set key
           water_withdrawal,
           '-' )

         # Add the return flow to the emission factors
         if( !( tech %in% hydro_techs ) ) # no wastewater for hydropower / instream technologies sads
           {
           ixDS$add_par( 'emission_factor', # parameter name
             paste( reg, tech, as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ), as.character( ind$mode[ v ] ), 'fresh_wastewater', sep = '.' ),
             return_flow,
             '-' )
           }
         } )
       }
     } )
   } )

 #-------------------------------------------------------------------------------------------------------
 # Add water resource extraction technologies
 #-------------------------------------------------------------------------------------------------------

 print('Adding water source extraction technologies')

 # List of technologies with historical water use
 hist_techs = list( 	freshwater_supply = c( as.character( message_technologies_with_water_data2 )[which( as.character( message_technologies_with_water_data2  ) %in% as.character( unique( all_historical_activity$technology ) ) & !( as.character( message_technologies_with_water_data2 )  %in% c( tech_water_performance_ssp_msg_raw.df$technology_name[which(tech_water_performance_ssp_msg_raw.df$water_supply_type %in% c('freshwater_instream','upstream_landuse'))]  ) ) )], cooling_technologies_to_consider[ which( unlist(lapply(cooling_technologies_to_consider,function(zzz){unlist(strsplit( zzz, '__' ))[1]})) %in% unique( all_historical_activity$technology ) &  unlist(lapply( unlist(lapply(cooling_technologies_to_consider,function(zzz){unlist(strsplit( zzz, '__' ))[2]})), function(xxx){ unlist(strsplit( xxx, '_' ))[2] }  ) ) == 'fresh' ) ] ),
           saline_supply = cooling_technologies_to_consider[ which( c( unlist(lapply( unlist(lapply(cooling_technologies_to_consider,function(zzz){unlist(strsplit( zzz, '__' ))[2]})), function(xxx){ unlist(strsplit( xxx, '_' ))[2] }  ) ) == 'saline' ) ) ],
           freshwater_instream = c( tech_water_performance_ssp_msg_raw.df$technology_name[which(tech_water_performance_ssp_msg_raw.df$water_supply_type == 'freshwater_instream')]  ),
           upstream_landuse = c( tech_water_performance_ssp_msg_raw.df$technology_name[which(tech_water_performance_ssp_msg_raw.df$water_supply_type == 'upstream_landuse')] ) )

 # Technologies with historical capacity and activity - update
 all_historical_new_capacity = ixDS$par( 'historical_new_capacity' )
 all_historical_activity = ixDS$par( 'historical_activity' )

 ret = lapply( water_source_extraction_techs, function(tech){

   # Status update
   print( paste( round( 100 * ( ( which( water_source_extraction_techs == tech ) - 1 ) / ( length(water_source_extraction_techs) ) ) ), ' % complete', sep='') )

   lapply( region, function(reg){ # across each region

     # Get the commodity for the source type (freshwater or saline or instream)
     cmdty_out = as.character( unlist(strsplit(tech,'__'))[2] )

     lapply( model_years, function( yy ){ # go through the years

       # Add the output for each timeslice
       ret = lapply( model_time, function(tm){

         ixDS$add_par( 'output', # parameter name
           paste( reg, tech, as.character( yy ), as.character( yy ), mode_common, reg, cmdty_out, water_supply_level, as.character( tm ), as.character( tm ), sep = '.' ), # set key
           1, # parameter value
           '-' )

         } )

       # Add the electricity usage for freshwater supply
       if( tech == 'extract__freshwater_supply' )
         {
         ret = lapply( model_time, function(tm){
             ixDS$add_par( 'input', # parameter name
             paste( reg, tech, as.character( yy ), as.character( yy ), mode_common, reg, 'electr', 'final', as.character( tm ), as.character( tm ), sep = '.' ), # set key
             unlist( fw_electricity_input[ parameter_levels ] ), # parameter value
             '-' )
           } )
         }

       # Add the investment cost
       ixDS$add_par( 'inv_cost', # parameter name
         paste( reg, tech, as.character( yy ), sep = '.' ), # set key
         0,
         '-' )

       # Add the fixed cost
       # print(ixDS$get_par_set('fix_cost'))
       ixDS$add_par( 'fix_cost', # parameter name
         paste( reg, tech, as.character( yy ), as.character( yy ), sep = '.' ), # set key
         0,
         '-' )

       # Average water supply expansion costs
       # Medium-scale storage costs from A.A. Keller et al. assuming long-term storage costs - converted from USD / m3 to million USD / km3
       #fwc = 1e3 * data.frame(low = 0.01, mid = 0.02, high = 0.1)
       fwc = 1e3 * data.frame(low = 0, mid = 0, high = 0)
       if( tech == 'extract__freshwater_supply' ){ vc = unlist(fwc[ parameter_levels ]) }else{ vc = 0 }
       # Add the variable costs
       # print(ixDS$get_par_set('var_cost'))
       ret = lapply( model_time, function(tm){
         ixDS$add_par( 'var_cost', # parameter name
           paste( reg, tech, as.character( yy ), as.character( yy ), as.character( mode_common ), as.character( tm ), sep = '.' ), # set key
           vc, # default for now
           '-' )
         } )

       # Add the capacity factor
       # print(ixDS$get_par_set('capacity_factor'))
       ret = lapply( model_time, function(tm){
         ixDS$add_par( 'capacity_factor', # parameter name
           paste( reg, tech, as.character( yy ), as.character( yy ), as.character( tm ), sep = '.' ), # set key
           1,
           '-' )
         } )

       # Add the technical lifetime
       if(yy < model_years[length(model_years)]){tl = as.numeric(model_years[which(model_years==yy)+1]) - as.numeric(yy)}else{tl = as.numeric(model_years[length(model_years)]) - as.numeric(model_years[length(model_years)-1])}
       ixDS$add_par( 'technical_lifetime', # parameter name
         paste( reg, tech, as.character( yy ), sep = '.' ), # set key
         tl,
         'y' )

       # Add the contruction time
       ixDS$add_par( 'construction_time', # parameter name
         paste( reg, tech, as.character( yy ), sep = '.' ), # set key
         0,
         'y' )

       # Add capacity in locked in years for mitigation and no policy scenarios
       if( yy > 2010 & yy < firstmodelyear )
         {
         ixDS$add_par( 'historical_activity', paste(  reg, as.character( tt ), yy, mode_common , model_time, sep = '.' ), max(0, base_hist_act[ which( as.character( unlist( base_hist_act$tec ) ) == as.character( d_tech[[ tt ]] ) & as.numeric( unlist( base_hist_act$year_all ) ) == yy & as.character( unlist( base_hist_act$node ) ) == reg ), 'val' ], na.rm=TRUE), '-' )
         ixDS$add_par( 'historical_new_capacity', paste(  reg, as.character( tt ), yy, sep = '.' ), max(0, base_hist_ncap[ which( as.character( unlist( base_hist_ncap$tec ) ) == as.character( d_tech[[ tt ]] ) & as.numeric( unlist( base_hist_ncap$year_all ) ) == yy & as.character( unlist( base_hist_ncap$node ) ) == reg ), 'val' ], na.rm=TRUE), '-' )
         }

       } )

     # Historical activity of extraction techs determined from
     # the historical activity of the MESSAGE techs
     techs_hist = unlist( hist_techs[ cmdty_out ] )
     temp = lapply(techs_hist, function(ttt){
       hist_act = all_historical_activity[which( as.character(all_historical_activity$technology) == ttt  & as.character(all_historical_activity$node_loc) == reg ),]
       hist_act_yy = unique(hist_act$year_act)
       if( length(hist_act_yy) > 0)
         {
         if( cmdty_out == 'saline_supply' ){ cmdty_out2 = 'saline_supply_ppl' }else{ cmdty_out2 = cmdty_out }
         ret1 = data.frame( do.call(rbind, lapply( hist_act_yy, function(yyy){
           yr = as.character(yyy)
           inp = mean( ixDS$par( 'input', list( node_loc = reg, technology = ttt, year_act = as.character(hist_act$year_act[which(hist_act$year_act == yyy)]), commodity = cmdty_out2 ) )[,'value'], na.rm=TRUE )
           act = hist_act$value[which(hist_act$year_act == yyy)]
           return( c(yr, inp * sum(act)) ) # multiply input by activity to estimate historical demands
           } ) ) )
         }else{ ret1 = NULL }
       return(ret1)
       })
     names(temp) = techs_hist

     # total historical is summed across the demands from all technologies
     hist_years = unique(unlist(lapply( 1:length(techs_hist), function(xxx){ if(!is.null(temp[[xxx]])){ unlist( as.character( temp[[xxx]][,1] ) )}})))
     hist_tot_act = unlist( lapply( 1:length(hist_years), function(yyy){ sum( unlist( lapply( 1:length(techs_hist), function(xxx){
       temp2 = temp[[xxx]]
       if( hist_years[yyy] %in% as.character( temp2[,1] ) )
         {
         return( as.numeric( as.character( temp2[ which( as.character( temp2[,1] ) == as.character( hist_years[yyy] ) ), 2 ] ) ) )
         }else{ return(NULL) }
       } ) ), na.rm = TRUE ) } ) )
     names( hist_tot_act ) = hist_years

     # total historical demand from desalination
     if( cmdty_out == 'saline_supply' )
       {
       techs_hist = c('membrane','distillation')
       temp = lapply(techs_hist, function(ttt){
         hist_act = all_historical_activity[which( as.character(all_historical_activity$technology) == ttt  & as.character(all_historical_activity$node_loc) == reg ),]
         hist_act_yy = unique(hist_act$year_act)
         if( length(hist_act_yy) > 0)
           {
           ret1 = data.frame( do.call(rbind, lapply( hist_act_yy, function(yyy){
             yr = as.character(yyy)
             inp = mean( ixDS$par( 'input', list( node_loc = reg, technology = ttt, year_act = as.character(hist_act$year_act[which(hist_act$year_act == yyy)]), commodity = cmdty_out ) )[,'value'], na.rm=TRUE )
             act = hist_act$value[which(hist_act$year_act == yyy)]
             return( c(yr, inp * sum(act)) ) # multiply input by activity to estimate historical demands
             } ) ) )
           }else{ ret1 = NULL }
         return(ret1)
         })
       names(temp) = techs_hist

       # total historical is summed across the demands from all technologies
       hist_years = unique(unlist(lapply( 1:length(techs_hist), function(xxx){ if(!is.null(temp[[xxx]])){ unlist( as.character( temp[[xxx]][,1] ) )}})))
       hist_tot_act_ds = unlist( lapply( 1:length(hist_years), function(yyy){ sum( unlist( lapply( 1:length(techs_hist), function(xxx){
         temp2 = temp[[xxx]]
         if( hist_years[yyy] %in% as.character( temp2[,1] ) )
           {
           return( as.numeric( as.character( temp2[ which( as.character( temp2[,1] ) == as.character( hist_years[yyy] ) ), 2 ] ) ) )
           }else{ return(NULL) }
         } ) ), na.rm = TRUE ) } ) )
       names( hist_tot_act_ds ) = hist_years
       hist_tot_act = sapply( unique( names( hist_tot_act_ds ), ( hist_tot_act ) ), function(xxx){ return( max( 0, hist_tot_act_ds[ xxx ], na.rm = TRUE ) + max( 0, hist_tot_act[ xxx ], na.rm = TRUE ) ) } )
       names( hist_tot_act ) = unique( names( hist_tot_act_ds ), ( hist_tot_act ) )
       }

     # Add the ix db
     ret = lapply( names( hist_tot_act ), function(yyy){
       wtdm = hist_tot_act[ yyy ] + max( 0, tryCatch( sum( ixDS$par( 'demand', list( node = reg, commodity = c('freshwater_supply','urban_mw','urban_dis','rural_mw','rural_dis'), year = yyy ) )[ 'value' ], na.rm=TRUE ), error = function(e){} ), na.rm = TRUE )
       ixDS$add_par( 'historical_activity',
         paste( reg, tech, yyy, as.character( mode_common ) , model_time, sep = '.' ),
         round(wtdm, digits=rnd),
         '-' )
       } )

     # Calibrate the electricity demands	for freshwater supply
     if( tech == 'extract__freshwater_supply' )
       {
       hist_elec_2010.df = data.frame( hist_elec_2010 = unlist(ixDS$par( 'historical_activity', list( technology = tech, node_loc=reg ) )['value']) * unlist( fw_electricity_input[ parameter_levels ] ) )
       row.names( hist_elec_2010.df ) = unlist(ixDS$par( 'historical_activity', list( technology = tech, node_loc=reg ) )['year_act'])
       ret = lapply( row.names(hist_elec_2010.df), function(aaa){
         chk =  tryCatch( ixDS$par( 'demand', list( node = reg, commodity = 'i_spec', level = 'useful', year = aaa, time = model_time ) ), error = function(e){} )
         if(!is.null( chk ) )
           {
           ixDS$add_par( 'demand', # parameter name
             paste( reg, 'i_spec', 'useful', aaa, model_time, sep = '.' ), # set key
             round( c( max( 0,  c( unlist( chk['value'] ) - unlist( hist_elec_2010.df[ aaa , ] ) ) ) ),  digits = 5),
             'GWa' )
           }
         } )
       ret = lapply( as.character( model_years[ which(model_years > 2010) ] ), function(aaa){
         chk =  tryCatch( ixDS$par( 'demand', list( node = reg, commodity = 'i_spec', level = 'useful', year = aaa, time = model_time ) ), error = function(e){} )
         if(!is.null( chk ) )
           {
           ixDS$add_par( 'demand', # parameter name
             paste( reg, 'i_spec', 'useful', aaa, model_time, sep = '.' ), # set key
             round( c( max( 0,  c( unlist( chk['value'] ) - unlist( hist_elec_2010.df[ '2010' , ] ) ) ) ),  digits = 5),
             'GWa' )
           }
         } )
       }
     } )
   } )

 #-------------------------------------------------------------------------------------------------------
 # Add reservoir technologies
 #-------------------------------------------------------------------------------------------------------

 print('Adding reservoir technologies')

 ret = ixDS$add_set( 'commodity', 'yield_freshwater_supply' )
 ret = ixDS$add_set( 'technology', 'storage__freshwater_supply' )
 ret = ixDS$add_set( 'type_tec', 'water_resource_storage' ) # Add to technology types
 ret = ixDS$add_set( 'cat_tec', paste( 'water_resource_storage' , 'storage__freshwater_supply', sep='.') )
 ret = ixDS$add_set( 'cat_tec', paste( 'investment_other', 'storage__freshwater_supply', sep='.') )

 # Medium-scale storage costs from Table 5 in A.A. Keller et al. assuming long-term storage costs - converted from USD / m3 to million USD / km3
 storage_yield_efficiency = list( low = 0.7, mid = 1, high = 1.3 )
 inv_storage__freshwater_supply = list( low = 170, mid = 420, high = 2140 )
 fix_storage__freshwater_supply = list( low = 0.02*170, mid = 0.02*420, high = 0.02*2140 )
 lifetime_storage__freshwater_supply = list( low = 90, mid = 80, high = 50 )
 constime_storage__freshwater_supply = list( low = 1, mid = 1, high = 1 )

 ret = lapply( region, function(reg){ # across each region

   # Get the commodity for the source type (freshwater or saline or instream)
   cmdty_out = 'yield_freshwater_supply'
   tech = 'storage__freshwater_supply'

   lapply( model_years, function( yy ){ # go through the years

     # Add the output for storage each timeslice
     ret = lapply( model_time, function(tm){

       ixDS$add_par( 'output', # parameter name
         paste( reg, tech, as.character( yy ), as.character( yy ), mode_common, reg, cmdty_out, water_supply_level, as.character( tm ), as.character( tm ), sep = '.' ), # set key
         1, # parameter value
         '-' )

       } )

     # Add the input for extraction technologies each timeslice
     ret = lapply( model_time, function(tm){

       ixDS$add_par( 'input', # parameter name
         paste( reg, 'extract__freshwater_supply', as.character( yy ), as.character( yy ), mode_common, reg, cmdty_out, water_supply_level, as.character( tm ), as.character( tm ), sep = '.' ), # set key
         storage_yield_efficiency[[ parameter_levels ]], # parameter value
         '-' )

       } )

     # Add the investment cost
     ixDS$add_par( 'inv_cost', # parameter name
       paste( reg, tech, as.character( yy ), sep = '.' ), # set key
       inv_storage__freshwater_supply[[ parameter_levels ]],
       '-' )

     # Add the fixed cost
     # print(ixDS$get_par_set('fix_cost'))
     ixDS$add_par( 'fix_cost', # parameter name
       paste( reg, tech, as.character( yy ), as.character( yy ), sep = '.' ), # set key
       fix_storage__freshwater_supply[[ parameter_levels ]],
       '-' )

     # Add the technical lifetime
     ixDS$add_par( 'technical_lifetime', # parameter name
       paste( reg, tech, as.character( yy ), sep = '.' ), # set key
       lifetime_storage__freshwater_supply[[ parameter_levels ]],
       'y' )

     # Add the contruction time
     ixDS$add_par( 'construction_time', # parameter name
       paste( reg, tech, as.character( yy ), sep = '.' ), # set key
       constime_storage__freshwater_supply[[ parameter_levels ]],
       'y' )

     } )

   # Historical activity of extraction techs determined from
   # the historical activity of the MESSAGE techs
   cmdty_out = 'freshwater_supply'
   techs_hist = unlist( hist_techs[ cmdty_out ] )
   temp = lapply(techs_hist, function(ttt){
     hist_act = all_historical_activity[which( as.character(all_historical_activity$technology) == ttt  & as.character(all_historical_activity$node_loc) == reg ),]
     hist_act_yy = unique(hist_act$year_act)
     if( length(hist_act_yy) > 0)
       {
       if( cmdty_out == 'saline_supply' ){ cmdty_out2 = 'saline_supply_ppl' }else{ cmdty_out2 = cmdty_out }
       ret1 = data.frame( do.call(rbind, lapply( hist_act_yy, function(yyy){
         yr = as.character(yyy)
         inp = mean( ixDS$par( 'input', list( node_loc = reg, technology = ttt, year_act = as.character(hist_act$year_act[which(hist_act$year_act == yyy)]), commodity = cmdty_out2 ) )[,'value'], na.rm=TRUE )
         act = hist_act$value[which(hist_act$year_act == yyy)]
         return( c(yr, inp * sum(act)) ) # multiply input by activity to estimate historical demands
         } ) ) )
       }else{ ret1 = NULL }
     return(ret1)
     })
   names(temp) = techs_hist

   # total historical is summed across the demands from all technologies
   hist_years = unique(unlist(lapply( 1:length(techs_hist), function(xxx){ if(!is.null(temp[[xxx]])){ unlist( as.character( temp[[xxx]][,1] ) )}})))
   hist_tot_act = unlist( lapply( 1:length(hist_years), function(yyy){ sum( unlist( lapply( 1:length(techs_hist), function(xxx){
     temp2 = temp[[xxx]]
     if( hist_years[yyy] %in% as.character( temp2[,1] ) )
       {
       return( as.numeric( as.character( temp2[ which( as.character( temp2[,1] ) == as.character( hist_years[yyy] ) ), 2 ] ) ) )
       }else{ return(NULL) }
     } ) ), na.rm = TRUE ) } ) )
   names( hist_tot_act ) = hist_years


   # Add the ix db
   ret = lapply( names( hist_tot_act ), function(yyy){
     wtdm = hist_tot_act[ yyy ] + max( 0, tryCatch( sum( ixDS$par( 'demand', list( node = reg, commodity = c('freshwater_supply','urban_mw','urban_dis','rural_mw','rural_dis'), year = yyy ) )[ 'value' ], na.rm=TRUE ), error = function(e){} ), na.rm = TRUE )
     ixDS$add_par( 'historical_activity',
       paste( reg, tech, yyy, as.character( mode_common ) , model_time, sep = '.' ),
       round(wtdm, digits=rnd),
       '-' )
     } )

   } )

 #---------------------------------------------------
 # Add consumption as emission from the energy sector
 #---------------------------------------------------

 ## Add consumption as an emission
 ret = ixDS$add_set( "emission", "fresh_consumption" )
 ret = ixDS$add_set( "emission", "saline_consumption" )
 ret = ixDS$add_set( "emission", "instream_consumption" )
 ret = ixDS$add_set( 'type_emission', 'water_consumption' ) # Add to technology types
 ret = ixDS$add_set( 'cat_emission', paste( 'water_consumption', 'fresh_consumption', sep='.' ) )
 consumption_type = data.frame( freshwater_supply = "fresh_consumption", saline_supply = "saline_consumption", saline_supply_ppl = "saline_consumption" )
 res = lapply( c( ixDS$set('node') ), function(nn){

   withdrawal_intensities.df = data.frame( tryCatch( ixDS$par( 'input', list(commodity=c('freshwater_supply','saline_supply_ppl'),level=water_supply_level,node_loc=nn) ), error = function(e){} ) )
   t2r = c('urban_t_d','urban_unconnected','rural_t_d','rural_unconnected')
   withdrawal_intensities.df = withdrawal_intensities.df[ -1*( which( as.character( withdrawal_intensities.df$technology ) %in% t2r ) ), ]

   if(length(withdrawal_intensities.df)>0)
     {
     return_intensities.df = data.frame( tryCatch( ixDS$par( 'emission_factor', list( emission=c('fresh_wastewater','saline_wastewater'), node_loc=nn, technology=unique( as.character(withdrawal_intensities.df$technology)) ) ), error = function(e){} ) )
     res1 = lapply( 1:nrow(withdrawal_intensities.df), function(i){
       ixDS$add_par( 	"emission_factor",
               paste( nn, withdrawal_intensities.df$technology[ i ], as.character( withdrawal_intensities.df$year_vtg[ i ] ), as.character( withdrawal_intensities.df$year_act[ i ] ), as.character( withdrawal_intensities.df$mode[ i ] ), as.character( unlist( consumption_type[ as.character( withdrawal_intensities.df$commodity[ i ] ) ] ) ), sep = '.' ), # set key
               round( c( unlist(withdrawal_intensities.df$value[ i ]) - unlist(return_intensities.df$value[ which( as.character( unlist( return_intensities.df$technology ) ) == as.character( unlist( withdrawal_intensities.df$technology[ i ] ) ) & as.character( unlist( return_intensities.df$year_vtg ) ) == as.character( unlist( withdrawal_intensities.df$year_vtg[ i ] ) ) & as.character( unlist( return_intensities.df$year_act ) ) == as.character( unlist( withdrawal_intensities.df$year_act[ i ] ) ) & as.character( unlist( return_intensities.df$mode ) ) == as.character( unlist( withdrawal_intensities.df$mode[ i ] )) ) ] ) ), digits = 6 ), # parameter value
               '-' )
       } )
     }
   } )

 # Hydropower consumption
 withdrawal_intensities.df = data.frame( tryCatch( ixDS$par( 'input', list(commodity=c('freshwater_instream')) ), error = function(e){} ) )
 res = lapply( 1:nrow(withdrawal_intensities.df), function(i){
   ixDS$add_par( 	"emission_factor",
           paste( as.character( withdrawal_intensities.df$node_loc[ i ] ), withdrawal_intensities.df$technology[ i ], as.character( withdrawal_intensities.df$year_vtg[ i ] ), as.character( withdrawal_intensities.df$year_act[ i ] ), as.character( withdrawal_intensities.df$mode[ i ] ), "instream_consumption", sep = '.' ), # set key
           round( ( withdrawal_intensities.df$value[ i ] ), digits = 6 ), # parameter value
           '-' )
   } )


 #-------------------------------------------------------------------------------------------------------
 # Add baseline cooling technology policy - No new once-through freshwater cooling capacity
 #-------------------------------------------------------------------------------------------------------
 print('Adding once through freshwater baseline cooling policy')

 # Once through freshwater cooled techs and seawater for SDG6 scenarios
 if( model_scenarios$newscenarioName[sc] %in% c("baseline_globiom_SDG_sdg6eff","baseline_globiom_SDG_sdg6led","baseline_globiom_SDG_sdg6supp","baseline_globiom_SDG_sdg6supp2") )
   {
   otc = unlist(lapply( ixDS$set('technology'), function(x) if( length(unlist(strsplit(x,'__'))==2) ){ if( unlist(strsplit(x,'__'))[2] %in% c('ot_fresh','ot_saline') ){ return( x ) } } ))
   }else{
   otc = unlist(lapply( ixDS$set('technology'), function(x) if( length(unlist(strsplit(x,'__'))==2) ){ if( unlist(strsplit(x,'__'))[2] %in% c('ot_fresh') ){ return( x ) } } ))
   }

 # Loop through and bound new capacity additions to zero for otc techs
 ret = lapply( otc, function(tech){

   # status
   print(paste( round( 100 * ( ( which( otc == tech ) - 1 ) / ( length(otc) ) ) ), ' % complete', sep=''))

   # Across all regions
   lapply( region, function(reg){
     if( !( paste(tech,reg,sep='_') %in% skipped_tech_reg$comb ) )
       {
       if( !( unlist(strsplit(tech,'__'))[1] == 'nuc_lc' & reg == 'R11_MEA') )
         {
         vtg = unique( ixDS$par( 'output', list( node_loc = reg, technology = tech ) )$year_vtg )
         lapply( vtg, function(vv){
           ixDS$add_par( 'bound_new_capacity_up',
             paste( reg, tech, vv, sep = '.' ),
             0,
             '-' )
           } )
         }
       }
     } )
   } )

 #-------------------------------------------------------------------------------------------------------
 # Add baseline cooling technology policy - No new once-through seawater cooling capacity beyond existing levels
 #-------------------------------------------------------------------------------------------------------

 print('Adding once through saline water baseline cooling policy')

 #### Aggregate saline water for ppl cooling to allow for activity constraints covering all types of this technology group

 # Add to set list
 tt = 'saline_ppl_t_d'
 ixDS$add_set( 'technology', tt )
 ixDS$add_set( 'cat_tec', paste( 'investment_other' , tt, sep='.') ) # Add technology to list of water system investments

 # List of techs with saline once through
 otc = unlist(lapply( ixDS$set('technology'), function(x) if( length(unlist(strsplit(x,'__'))==2) ){ if( unlist(strsplit(x,'__'))[2] %in% c('ot_saline') ){ return( x ) } } ))
 yb = 2010 # base year

 # Getting the historical activity of all seawater cooling technologies - will use to constrain future years
 nms_hist = sapply(1:nrow( all_historical_activity ),function(fff){ paste(all_historical_activity$technology[fff],all_historical_activity$node_loc[fff],sep='_') })
 ret = do.call( cbind, lapply( otc, function(tech){
   print(paste( round( 100 * ( ( which( otc == tech ) - 1 ) / ( length(otc) ) ) ), ' % complete', sep=''))
   dfs = data.frame( do.call(rbind, lapply( region, function(reg){
     if( !( paste(tech,reg,sep='_') %in% skipped_tech_reg$comb ) & paste(tech,reg,sep='_') %in% nms_hist )
       {
       # Total historical activity of cooled powered plant and share for ocean water cooling
       chk =  tryCatch( ixDS$par( 'historical_activity', list( node_loc = reg, technology = unlist(strsplit(tech,'__'))[1], year_act= 2010  ) ), error = function(e){} )
       if(!is.null(chk))
         {
         hist_act = sum( chk[,'value'] )
         if(use_davies_shares)
           {
           hist_shr = cooltech_cost_and_shares_ssp_msg_raw.df[ which( ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$utype) == unlist(strsplit(tech,'__'))[1] ) & ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$cooling) == unlist(strsplit(tech,'__'))[2] ) ), paste('mix',unlist(strsplit(reg,'_'))[2],'Davies_2013',sep='_') ]
           }else
           {
           hist_shr = cooltech_cost_and_shares_ssp_msg_raw.df[ which( ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$utype) == unlist(strsplit(tech,'__'))[1] ) & ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$cooling) == unlist(strsplit(tech,'__'))[2] ) ), paste('mix',unlist(strsplit(reg,'_'))[2],sep='_') ]
           }

           #return(as.matrix(max(0,hist_act*hist_shr,na.rm=TRUE))) # multiply the activity by the share to estimate the total historical output

         }else
         {
         return(as.matrix(c(0)))
         }
       }else
       {
       return(as.matrix(c(0)))
       }
     } ) ) )
   row.names(dfs) = region
   return(dfs) } ) )
 names(ret) = otc
 ww1 = rowSums(ret)

 # Performance parameters - across all historical and future years
 ret = lapply( region, function(rr){ lapply( model_years, function(yy){

   # Add the investment cost
   ixDS$add_par( 'inv_cost', # parameter name
       paste(rr, tt, yy, sep = '.' ), # set key
       0,
       '-' )

   # Add the technical lifetime
   ixDS$add_par( 'technical_lifetime', # parameter name
         paste(rr, tt, yy, sep = '.' ), # set key
         1,
         'y' )

   # Add the construction time
   ixDS$add_par( 'construction_time', # parameter name
         paste(rr, tt, yy, sep = '.' ), # set key
         1,
         'y' )

   # Add i-o parameters
   lapply( model_years[ which( model_years >= as.numeric(yy) & model_years <= ( as.numeric(yy) + unlist( desal_par.list[[1]][paste('lifetime',parameter_levels,sep='.')]  ) ) ) ], function(aa){

     # Add the output efficiency ratio
     ixDS$add_par( 'output', # parameter name
       paste( rr, tt, yy, aa, mode_common, rr, 'saline_supply_ppl', water_supply_level, model_time, model_time, sep = '.' ), # set key
       1, # parameter value
       '-' )

     # Add the input efficiency ratio
     ixDS$add_par( 'input', # parameter name
       paste( rr, tt, yy, aa, mode_common, rr, 'saline_supply', water_supply_level, model_time, model_time, sep = '.' ), # set key
       1,
       '-' )

     # Add the variable cost
     ixDS$add_par( 'var_cost', # parameter name
       paste( rr, tt, yy, aa, mode_common, model_time, sep = '.' ), # set key
       0,
       '-' ) # units

     # Add fixed costs
     ixDS$add_par( 'fix_cost', # parameter name
       paste( rr, tt, yy, aa, sep = '.' ), # set key
       0,
       '-' )

     # Add the capacity factor
     ixDS$add_par( 'capacity_factor', # parameter name
       paste( rr, tt, yy, aa, model_time, sep = '.' ), # set key
       1,
       '-' )

     } )

   } ) } )

 # Add existing infrastructure
 ret = lapply( region, function(rr){ lapply( model_years, function(yy){

   ixDS$add_par( 'historical_new_capacity',
           paste( rr, tt, yy, sep = '.' ),
           round( ww1[ rr ]/5 , digits = 2 ),
           '-' )

   ixDS$add_par( 'historical_activity',
           paste( rr, tt, yy, mode_common, model_time, sep = '.' ),
           round( ww1[ rr ] , digits = 2 ),
           '-' )

   } ) } )

 # # Add future constraints  - no expansion of seawater cooling beyond current capacity
 ret = lapply( region, function(rr){ lapply( model_years[ which( model_years > 2010 ) ], function(yy){
   ixDS$add_par( 'bound_activity_up', # parameter name
         paste(  rr, tt, yy, mode_common , model_time, sep = '.' ), # set key
         round( ww1[ rr ] , digits = 2 ), # parameter value
         '-' )
   } ) } )

 # Include set of all tecs in the type tec category
 ret = ixDS$add_set( 'type_tec', 'all_tec' ) # Add to technology types
 ret = lapply( unique(ixDSoriginal$set( 'technology' )), function(x){ ixDS$add_set( 'cat_tec', paste( 'all_tec' , x, sep='.' ) ) } )

 #---------------------------------------------------------------------
 # Fix the land-use scenarios that can be used for mitigation
 #---------------------------------------------------------------------

 lsn = ixDS$set('land_scenario')
 lsn = lsn[ which( as.numeric( unlist( strsplit( lsn, 'GHG' ) )[ seq( 2, 2*length(lsn), by = 2 ) ] ) >= 200 ) ]
 if ( length(lsn) > 0 )
   {

   ret = lapply( region, function( rr ){ lapply( model_years[ which( model_years > 2010 ) ], function( yy ){ lapply( lsn, function( ll ){

     ixDS$add_par( 'fixed_land', # parameter name
           paste(  rr, ll, yy, sep = '.' ), # set key
           0, # parameter value
           '%' )

     # ixDS$remove_par( 'fixed_land', paste(  rr, ll, yy, sep = '.' ) )

     } ) } ) } )

   }

 #--------------------------------------------------
 # Commit to DB and set to default
 #--------------------------------------------------

 # Commit the current editions to the db and set as default
 ixDS$commit(paste(comment,water_policies,sep=' '))
 ixDS$set_as_default()
 rm(ixDS)
 gc()

 #----------------------------------------------------
 # Generate water and emission constrained scenarios
 #----------------------------------------------------

 # options(java.parameters = "-Xmx16g")

 # library("rixmp")
 # library("rmessageix")

 # ## Set path ixmp folder in message_ix working copy
 # message_ix_path = Sys.getenv("MESSAGE_IX_PATH")

 # # Country region mapping key
 # country_region_map_key.df = data.frame( read.csv('P:/ene.model/data/Water/demands/country_region_map_key.csv', stringsAsFactors=FALSE) )

 # # SSP scenario
 # ssss = 'SSP2'

 # # Rounding parameter for decimals
 # rnd = 6

 # # list of model-scenario combinations
 # model_scenarios = data.frame( 	model = c( "MESSAGE-GLOBIOM CD-LINKS R2.3.1", "MESSAGE-GLOBIOM CD-LINKS R2.3.1" , "MESSAGE-GLOBIOM CD-LINKS R2.3.1","MESSAGE-GLOBIOM CD-LINKS R2.3.1" ),
                 # scenario = c( "baseline", "baseline", "baseline", "baseline" ),
                 # newscenarioName = c( "baseline_globiom_base_watbaseline", "baseline_globiom_SDG_sdg6supp", "baseline_globiom_SDG_sdg6eff", "baseline_globiom_SDG_sdg6led" ) 	)

 # # launch the IX modeling platform using the default central ORCALE database
 # ix_platform = ixmp.Platform( dbprops = 'ixmp.properties' )

 # sc = 2
 # newscenarioName = as.character( unlist( model_scenarios$newscenarioName[sc] ) )

 # if( model_scenarios$newscenarioName[sc] %in% c("baseline_globiom_SDG_sdg6eff","baseline_globiom_SDG_sdg6led") ){ water_policies = 'SDG6' }else{ water_policies = 'NoWatPol' }

 ixDS = ix_platform$Scenario( model=as.character( unlist( model_scenarios$model[sc] ) ), scen=as.character( unlist( model_scenarios$newscenarioName[sc] ) ) )

 #solve for policy scenarios - make sure to remember that the water constraints depend on baseline (i.e., c0 and w0)
 for( climpol in c( 'c0', '1p5', '2p0' ) )
   {

   # Set water constraint level for energy sector
   if( as.character( model_scenarios$newscenarioName[sc] ) == "baseline_globiom_base_watbaseline" ){ watcon = 'w0' }
   if( as.character( model_scenarios$newscenarioName[sc] ) == "baseline_globiom_SDG_sdg6supp" ){ watcon = 'w5' }
   if( as.character( model_scenarios$newscenarioName[sc] ) == "baseline_globiom_SDG_sdg6supp2" ){ watcon = 'w5' }
   if( as.character( model_scenarios$newscenarioName[sc] ) == "baseline_globiom_SDG_sdg6eff" ){ watcon = 'w10' }
   if( as.character( model_scenarios$newscenarioName[sc] ) == "baseline_globiom_SDG_sdg6led" ){ watcon = 'w10' }

   # Sensitivity cases for alternative regional freshwater constraints
   for( watcon2 in c('w0','w10','w20','w30') )
     {

     # status
     print( paste( 'Working on: ',as.character( model_scenarios$newscenarioName[sc] ),' - ', climpol, ' - ', watcon2, sep = '' ) )

     # Clone basis
     scname = paste(newscenarioName,watcon2,climpol,sep='_')
     ixDS0 = ixDS$clone( new_model = as.character( unlist( model_scenarios$model[sc] ) ), new_scen = scname, annotation = 'constraints sensitivity', keep_sol = FALSE )
     ixDS0$check_out()

     # Constrain withdrawals to fraction of basis
     wcons = as.numeric( strsplit( watcon, 'w' )[[1]][2] ) / 100
     wcons2 = as.numeric( strsplit( watcon2, 'w' )[[1]][2] ) / 100

     if( wcons > 0 )
       {

       require(gdxrrw)
       igdx( 'C:/GAMS/win64/24.8' )
       upath = paste(message_ix_path,'/model/output/',sep='')
       scen = paste('MSGoutput',"baseline_globiom_base_watbaseline_w0_c0",sep='_')
       res.list = lapply( scen, function(fpath){
         vars = c( 'ACT' )
         gdx_res = lapply( vars, function( vv ){
           tmp = rgdx( paste( upath, fpath, sep = '' ), list( name = vv, form = "sparse" ) )
           names(tmp$uels) = tmp$domains
           rs = data.frame( tmp$val )
           names(rs) = c( unlist( tmp$domains ), 'val' )
           rs[ , which( names(rs) != 'val' ) ] = do.call( cbind, lapply( names( rs )[ which( names(rs) != 'val' ) ], function( cc ){ sapply( unlist( rs[ , cc ] ) , function(ii){ return( tmp$uels[[ cc ]][ ii ] ) } ) } ) )
           return(rs)
           } )
         names(gdx_res) = vars
         return(gdx_res)
         } )
       names(res.list) = scen
       res.df = data.frame( res.list[[1]][[1]] )
       res.df = res.df[ which( as.character( unlist( res.df$tec ) ) == "extract__freshwater_supply" & as.numeric( unlist( res.df$year_all ) ) >= 2030 ), ]

       # Upper activity bound to reflect water conservation constraint based on baseline trajectory
       ret = lapply( 1:nrow(res.df), function(ii){ ixDS0$add_par( 'bound_activity_up', paste( res.df[ii,'node'], res.df[ii,'tec'], res.df[ii,'year_all'], ixDS0$set('mode')[1], ixDS0$set('time'), sep = '.' ), round( c( res.df[ii,'val'] * ( 1 - wcons2 ) ) , digits = 3 ), '-' ) } )

       }

     if( wcons2 > 0 )
       {

       require(gdxrrw)
       igdx( 'C:/GAMS/win64/24.8' )
       upath = paste(message_ix_path,'/model/output/',sep='')
       scen = paste('MSGoutput',"baseline_globiom_base_watbaseline_w0_c0",sep='_')

       # Get the consumption trajectory for the baseline scenario
       res.list = lapply( scen, function(fpath){
         vars = c( 'EMISS' )
         gdx_res = lapply( vars, function( vv ){
           tmp = rgdx( paste( upath, fpath, sep = '' ), list( name = vv, form = "sparse" ) )
           names(tmp$uels) = tmp$domains
           rs = data.frame( tmp$val )
           names(rs) = c( unlist( tmp$domains ), 'val' )
           rs[ , which( names(rs) != 'val' ) ] = do.call( cbind, lapply( names( rs )[ which( names(rs) != 'val' ) ], function( cc ){ sapply( unlist( rs[ , cc ] ) , function(ii){ return( tmp$uels[[ cc ]][ ii ] ) } ) } ) )
           return(rs)
           } )
         names(gdx_res) = vars
         return(gdx_res)
         } )
       names( res.list ) = scen
       res.df = data.frame( res.list[[1]][[1]] )
       res.df = res.df[ which( as.character( unlist( res.df$emission ) ) == "fresh_consumption" & as.character( unlist( res.df$node ) ) != "World" & as.character( unlist( res.df$type_tec ) ) == "all_tec" & as.numeric( unlist( res.df$year_all ) ) >= 2030  ), ]

       # Upper activity bound to reflect water conservation constraint based on baseline trajectory
       ret = lapply( 1:nrow(res.df), function(ii){ ixDS0$add_par( 'bound_emission', paste(res.df[ii,'node'],'water_consumption','all',res.df[ii,'year_all'],sep='.'), round( c( res.df[ii,'val'] * ( 1 - wcons ) ) , digits = 3 ) , '-' ) } )

       }

     # Set cumulative emissions bound if 1p5 degree scenario - using emissions bound from Gruebler et al. 2018
     if( climpol == '1p5' )
       {
       ixDS0$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 1400, "tC")  ## This budget (for 2021-2110) was run in scenarios 'b2C_NPi2020_noBECCS_V7a' to 'V8' to get something around 1.5C. (A budget of 1200 was initially used for V7, but the carbon prices were ridiculously higher.) Median temperature is around 1.61C in 2050 and 1.37 in 2100. Median radiative forcing (total) is 1.94 W/m2 in 2100.
       }

     if( climpol == '2p0' )
       {
       ixDS0$add_par("bound_emission",paste("World", "TCE", "all", "cumulative", sep='.'), 2500, "tC")  #
       }

     if( as.character( model_scenarios$newscenarioName[sc] ) == "baseline_globiom_SDG_sdg6led" ){ runm = 'MESSAGE' }else{ runm = 'MESSAGE-MACRO' }

     # Commit, solve and upload solution
     ixDS0$commit(paste(water_policies,sep=' '))
     ixDS0$to_gdx( paste(message_ix_path,'/model/data',sep=''), paste( 'MSGdata_', scname, '.gdx',sep='') )
     current_working_drive = getwd()
     setwd(paste(message_ix_path,'/model',sep='')) # set based on local machine
     cmd = paste( "gams ",runm,"_run.gms --in=data\\MSGdata_",scname,".gdx --out=output\\MSGoutput_",scname,".gdx", sep='' )
     res = system(cmd)
     setwd(current_working_drive)
     if( res == 0 )
       {
       ixDS0$read_sol_from_gdx( 	paste(message_ix_path,'/model/output',sep=''),
                     paste( 'MSGoutput_', scname, '.gdx',sep=''),
                     var_list=NULL,
                     equ_list="COMMODITY_BALANCE_FULL",
                     check_sol=TRUE	)
       }
     ixDS0$set_as_default()
     rm(ixDS0)
     gc()

     }

   }

 }
