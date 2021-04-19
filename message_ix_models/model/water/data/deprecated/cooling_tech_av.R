
# call using e.g.,: source('C:/Users/parkinso/git/message_ix/workflow/water/ssp_water_implement.r')
options( java.parameters = "-Xmx16g" )
rm(list=ls())
graphics.off()

library(reticulate)
path_to_python <- "/anaconda/bin/python"
use_python(path_to_python)
#-------------------------------------------------------------------------------
# load ixToolbox auxiliary functions and get the gateway to the ixToolbox JVM
#-------------------------------------------------------------------------------
print('Initializing functions and parameters')

# launch the IX modeling platform using the default central ORCALE database
import('ixmp')
ixmp <- import('ixmp')
message_ix <- import('message_ix')
#source(file.path(Sys.getenv("IXMP_R_PATH"),"ixmp.r"))
#ix_platform = ixPlatform()
mp = ixmp$Platform(name='ixmp_dev')
scen_list = mp$scenario_list(default = F)

# Set local message_ix and GAMS directories 
wkd_message_ix = "C:/Users/parkinso/git/" # !!!!! Set based on local machine
wkd_GAMS = paste(unlist(strsplit( unlist(strsplit(Sys.getenv("PATH"),';'))[which(
  grepl('GAMS', unlist(strsplit(Sys.getenv("PATH"),';')) ))[1]], '[\\]' )),collapse='/')

SSP_scenarios = c('SSP1') # go through and create a baseline for each scenario

for( ssss in SSP_scenarios ) # this should be parrellized
{
  
  #-------------------------------------------------------------------------------
  # Load the MESSAGE-GLOBIOM SSP baseline scenario 
  # UPDATE: MESSAGE_GLOBIOM scenario give error with the current version of message+ix
  # because the set 'rating' and probably other are missing
  # I will use an CD-Links scenario, which has all SSPs
  #-------------------------------------------------------------------------------
  
  modelName = paste( "CD_Links", ssss, sep = "_" )
  scenarioName = "baseline"
  newscenarioName = "baseline_with_water_2020"
  comment = "Adding water for energy structure and basic data for SSP to existing MESSAGE run"
  ixDSoriginal = message_ix$Scenario(mp, model = modelName, scenario = scenarioName)
  ixDS = ixDSoriginal$clone(modelName, newscenarioName, keep_solution = F)
  ixDS$check_out()
  
  #-------------------------------------------------------------------------------
  # Set some global parameters
  #-------------------------------------------------------------------------------
  
  # Rounding parameter for decimals
  rnd = 6
  
  # low, mid or high parameter settings for water techs
  parameter_levels = 'mid'
  
  # Whether or not to use initial shares from Davies et al 2013 - alternatively uses shares estimated with the dataset from Raptis et al 2016.
  use_davies_shares = TRUE
  
  #-------------------------------------------------------------------------------------------------------
  # Compare the data in the database to the data in the csv files to determine the technologies to include
  #-------------------------------------------------------------------------------------------------------
  
  # Import raw cooling water data: 
  # Input data file 1: CSV containing the water use coefficients (incl. parasitic electricity) for each MESSAGE technology 
  # Source for power plant cooling water use: Meldrum et al. 2013
  # Source for hydropower water use: Taken as an average across Grubert et al 2016 and Scherer and Pfister 2016.
  # Parasitic electricity requirements estimated from Loew et al. 2016.
  # All other water coefficients come from Fricko et al 2016.
  # To compile the data, a complete list of technologies from MESSAGE was initially output to CSV. 
  # The water parameters were then checked and entered manually based on data reported in the sources above.
  
  tech_water_performance_ssp_msg_raw.df = data.frame(read.csv(paste('~/Reps/message_data/data/water/tech_water_performance_ssp_msg.csv' )),stringsAsFactors=FALSE)
  # Input data file 2: CSV containing the regional shares of each cooling technology and the investment costs
  # The regional shares are estimated using the dataset from Raptis and Pfister 2016 and country boundaries from the GADM dataset
  # Each plant type in the Raptis and Pfister dataset is mapped to the message technologies. The fraction is calculated using 
  # the total capacity identified for each cooling technology in each country.  These results are aggregated into the message 11 regions.
  # The costs are estimated from Loew et al. 2016.  
  cooltech_cost_and_shares_ssp_msg_raw.df = data.frame(read.csv(paste(
    '~/Reps/message_data/data/water/cooltech_cost_and_shares_ssp_msg.csv'),stringsAsFactors=FALSE))
  
  # Define alternate id for cooling technologies from csv file 
  cooltech_cost_and_shares_ssp_msg_raw.df$alt_id = apply( 
    cbind(cooltech_cost_and_shares_ssp_msg_raw.df[,1:2]), 1, paste, collapse="__") 
  
  # Get the names of the technologies in the MESSAGE IAM from the database
  technologies_in_message = ixDS$set('technology')
  
  # Get the name of the regions 
  region = as.character( ixDS$set('cat_node')$node )
  
  # Get the name of the commodities
  commodity = ixDS$set('commodity')
  
  # Model years
  model_years = ixDS$set('year')
  
  # Model timeslices
  model_time = ixDS$set('time')
  
  # Which technologies in MESSAGE are also included in csv files?
  message_technologies_with_water_data = as.character(tech_water_performance_ssp_msg_raw.df$technology_name)[ 
    which( as.character(tech_water_performance_ssp_msg_raw.df$technology_name) %in% technologies_in_message )]
  
  message_technologies_without_water_data = as.character(tech_water_performance_ssp_msg_raw.df$technology_name)[ 
    which( !(as.character(tech_water_performance_ssp_msg_raw.df$technology_name) %in% technologies_in_message ) )]
  
  message_technologies_with_water_data_alt = as.character(technologies_in_message)[ 
    which(  technologies_in_message %in% as.character(tech_water_performance_ssp_msg_raw.df$technology_name) )]
  
  message_technologies_without_water_data_alt = as.character(technologies_in_message)[ 
    which(  !(technologies_in_message %in% as.character(tech_water_performance_ssp_msg_raw.df$technology_name) )) ]
  
  # Define a common mode using the existing DB settings
  mode_common = ixDS$set('mode')[1] # Common mode name from set list
  
  # Define hydropower technologies
  hydro_techs = c('hydro_hc','hydro_lc')
  
  #-------------------------------------------------------------------------------------------------------
  # Add cooling technologies using historical ppl data and pre-defined regional shares
  #-------------------------------------------------------------------------------------------------------
  print('Adding cooling technologies')
  
  # Cooling technologies:
  # Once through (fresh and saline), closed-loop (fresh) and air cooled options considered
  # No air cooling options for nuclear and CCS 
  # Only consider the cooling technologies that correspond to power plant technologies in the MESSAGE model. 
  cooling_technologies_to_consider = c( apply( cooltech_cost_and_shares_ssp_msg_raw.df[ which( as.character( cooltech_cost_and_shares_ssp_msg_raw.df$utype ) %in% as.character( message_technologies_with_water_data )  ), c("utype","cooling") ], 1, paste, collapse="__") )
  
  # Cooling commodities by power plant type - output commodity for cooling technology
  cooling_commodities = unlist( lapply( 1:length( unique( cooltech_cost_and_shares_ssp_msg_raw.df$utype[ as.numeric( names(cooling_technologies_to_consider) )  ] ) ), function(x){ paste( 'cooling', unique( cooltech_cost_and_shares_ssp_msg_raw.df$utype[ as.numeric(names(cooling_technologies_to_consider)) ] )[x], sep='__' ) } ) )
  ret = lapply( 1:length(cooling_commodities), function(x){ ixDS$add_set( "commodity", as.character( cooling_commodities[x] ) ) } ) # Add commodity to ix DB
  
  # Water sources - input commodity for cooling technology
  water_supply_type = unique(tech_water_performance_ssp_msg_raw.df$water_supply_type[ which( as.character(tech_water_performance_ssp_msg_raw.df$technology_name) %in% message_technologies_with_water_data | as.character(tech_water_performance_ssp_msg_raw.df$technology_group) == 'cooling' ) ] )
  water_supply_type = water_supply_type[-1*which(is.na(water_supply_type))]
  ret = lapply( 1:length(water_supply_type), function(x){ ixDS$add_set( "commodity", as.character( water_supply_type[x] ) ) } ) # Add commodity to ix DB
  
  # Water supply and cooling as a new level 
  water_supply_level = 'water_supply'
  ret = ixDS$add_set( "level", water_supply_level ) # Add level to ix DB
  cooling_level = 'cooling'
  ret = ixDS$add_set( "level", cooling_level ) # Add level to ix DB
  
  # Wastewater as an emission - could alternatively be included as commodity balance
  ret = ixDS$add_set( "emission", "fresh_wastewater" )
  ret = ixDS$add_set( "emission", "saline_wastewater" ) # some of these should alternatively be defined in the input files
  
  # Thermal pollution as an emission - consider oceans and rivers
  ret = ixDS$add_set( "emission", "fresh_thermal_pollution" )
  ret = ixDS$add_set( "emission", "saline_thermal_pollution" )
  
  # Water source extraction technologies - add to technology and type sets
  # Currently distinguishes: freshwater_instream (hydropower), freshwater_supply (all techs using freshwater), saline_supply (ocean and brackish resources) and upstream_landuse (globiom accounting)
  water_source_extraction_techs =  unlist( lapply( 1:length(water_supply_type), 
                                                   function(x){ paste( 'extract', as.character( water_supply_type[x] ), sep='__' ) } ) )
  ret = lapply( 1:length(water_supply_type), function(x){ ixDS$add_set( "technology", water_source_extraction_techs[x] ) } )
  water_resource_extraction_tech_type = 'water_resource_extraction'
  ret = ixDS$add_set( "type_tec", water_resource_extraction_tech_type ) # Add to technology types
  
  # the way mapping sets are defined is different, remove paste and add c('type_tec','tec')
  ret = lapply( 1:length(water_supply_type), function(x){ ixDS$add_set( "cat_tec", c( water_resource_extraction_tech_type , water_source_extraction_techs[x]) ) } ) 
  ret = lapply( 1:length(water_supply_type), function(x){ ixDS$add_set( "cat_tec", c( 'investment_other' , water_source_extraction_techs[x]) ) } ) 
  
  # Power plant cooling technologies as technology types - will set technology names later
  power_plant_cooling_tech_type = 'power_plant_cooling'
  ixDS$add_set( "type_tec", power_plant_cooling_tech_type )
  
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
                                     solar_th_ppl = 0.385	)
  
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
        if(length(which( as.character(all_historical_new_capacity$node_loc) == reg & 
                         as.character(all_historical_new_capacity$technology) == tech ))>0)
        { 
          historical_new_capacity = ixDS$par( 'historical_new_capacity', list( node_loc = reg, technology = tech ) ) 
        }else
        { 
          historical_new_capacity = NULL
        }
        
        # Retrieve the historical activity for this particular cooled technology in MESSAGE
        if(length(which( as.character(all_historical_activity$node_loc) == reg & 
                         as.character(all_historical_activity$technology) == tech ))>0)
        { 
          historical_activity = ixDS$par( 'historical_activity', list( node_loc = reg, technology = tech ) ) 
        }else
        { 
          historical_activity = NULL
        }	
        
        # Technical lifetime
        if(length(which(as.character(all_technical_lifetime$node_loc) == reg))>0){ 
          technical_lifetime = ixDS$par( 'technical_lifetime', list( node_loc = reg, technology = tech ) ) }	
        
        # Investment costs
        if(length(which(as.character(all_inv_cost$node_loc) == reg))>0){ 
          inv_cost = ixDS$par( 'inv_cost', list( node_loc = reg, technology = tech ) ) }	
        
        # Fixed costs
        if(length(which(as.character(all_fix_cost$node_loc) == reg))>0){ 
          fix_cost = ixDS$par( 'fix_cost', list( node_loc = reg, technology = tech ) ) }
        
        # Construction time
        if(length(which(as.character(all_construction_time$node_loc) == reg))>0){ 
          construction_time = ixDS$par( 'construction_time', list( node_loc = reg, technology = tech ) ) }
        
        # Get the name of the cooling technologies for this particular cooled MESSAGE technology
        techs_to_update = cooling_technologies_to_consider[ which( 
          as.character( unlist(data.frame(strsplit( cooling_technologies_to_consider, '__'))[1,]) )  == as.character(tech) ) ]	
        id2 = apply(cbind(as.character( unlist(data.frame(strsplit( 
          techs_to_update, '__'))[2,]) ),as.character( unlist(data.frame(strsplit( 
            techs_to_update, '__'))[1,]) )),1,paste,collapse='_') # alternate ID in csv file
        
        # Go through each vintage and cooling option and add the corresponding data to the DB
        ret = lapply( 1:length(ind$year_vtg), function(v){ lapply( 1:length(techs_to_update), function(ttt){
          
          # Add the technology to the set list - only need to for one region
          if(reg == region[1])
          {
            ixDS$add_set( "technology", as.character( techs_to_update[ ttt ] ) )
            ixDS$add_set( "cat_tec", c( power_plant_cooling_tech_type , as.character( techs_to_update[ ttt ] )) ) # Add technology to list of cooling technology types
            ixDS$add_set( "cat_tec", c( 'investment_electricity' , as.character( techs_to_update[ ttt ] )) ) # Add technology to list of electricity system investments
          }
          
          # Set the input commodity using the name from the csv file, define output commodity using names generated and initialized previously
          cmdty_in = as.character( tech_water_performance_ssp_msg_raw.df$water_supply_type[ which( as.character(tech_water_performance_ssp_msg_raw.df$technology_name) == id2[ ttt ] ) ] )
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
          parasitic_electricity = as.numeric( tech_water_performance_ssp_msg_raw.df[ which( as.character(tech_water_performance_ssp_msg_raw.df$technology_name) == id2[ ttt ]  ), "parasitic_electricity_demand_fraction" ] )
          
          ##
          # Add the variable cost
          ixDS$add_par( "var_cost", # parameter name
                        c( reg, techs_to_update[ ttt ], as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ), 
                           as.character( ind$mode[ v ] ), as.character( ind$time[ v ] ) ), # set key
                        round( ( 1e9/(60*60*24*365) ) * cooling_technology_costs$var_costs[ which(
                          as.character(row.names(cooling_technology_costs)) == as.character(techs_to_update[ ttt ]) ) ],digits = rnd), # parameter value
                        'USD/GWa' ) # units
          
          # Add the capacity factor
          ixDS$add_par( "capacity_factor", # parameter name
                        c( reg, techs_to_update[ ttt ], as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ),  
                           as.character( ind$time[ v ] )), # set key
                        1, # Assume for now that cooling technologies are always available
                        '-' ) # units
          
          # Add the output efficiency ratio 
          ixDS$add_par( "output", # parameter name
                        c( reg, techs_to_update[ ttt ], as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ), as.character( ind$mode[ v ] ), reg, cmdty_out, cooling_level, as.character( ind$time[ v ] ), as.character( ind$time[ v ] ) ), # set key
                        1, # parameter value
                        '-' )
          
          if( unlist(strsplit(id2[ttt],'_'))[1] != 'air' ) # Don't need to add water use inputs for air cooling technologies
          {
            
            # Add the input efficiency ratio - water withdrawal 
            ixDS$add_par( "input", # parameter name
                          c( reg, techs_to_update[ ttt ], as.character( ind$year_vtg[ v ] ), 
                             as.character( ind$year_act[ v ] ), as.character( ind$mode[ v ] ), 
                             reg, cmdty_in, water_supply_level, as.character( ind$time[ v ] ), as.character( ind$time[ v ] )), # set key
                          water_withdrawal[ v ], # parameter value
                          '-' )
            
            # Add the thermal pollution emission factor
            if( unlist(strsplit(id2[ttt],'_'))[1] == 'ot' ) # only for once through cooling technologies
            {
              if( unlist(strsplit(id2[ttt],'_'))[2] == 'fresh' ){ emis = 'fresh_thermal_pollution' }else{ emis = 'saline_thermal_pollution' }
              ixDS$add_par( "emission_factor", # parameter name
                            c( reg, techs_to_update[ ttt ], as.character( ind$year_vtg[ v ] ), 
                               as.character( ind$year_act[ v ] ), as.character( ind$mode[ v ] ), emis  ), # set key
                            round( cooling_fraction[ v ], digits = rnd ), # parameter value
                            '-' )		
            }
            
            # Add the wastewater emission factor	
            if( unlist(strsplit(id2[ttt],'_'))[2] == 'fresh' ){ emis = 'fresh_wastewater' }else{ emis = 'saline_wastewater' }
            ixDS$add_par( "emission_factor", # parameter name
                          c( reg, techs_to_update[ ttt ], as.character( ind$year_vtg[ v ] ), 
                             as.character( ind$year_act[ v ] ), as.character( ind$mode[ v ] ), emis  ), # set key
                          return_flow[ v ], # parameter value
                          '-' )		
            
          }
          
          # Add the input efficiency ratio - parasitic electricity consumption
          if( parasitic_electricity > 0 ) # only for some cooling technologies
          {
            ixDS$add_par( "input", # parameter name
                          c( reg, techs_to_update[ ttt ], as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ), 
                             as.character( ind$mode[ v ] ), reg, 'electr', 'secondary', as.character( ind$time[ v ] ), 
                             as.character( ind$time[ v ] )  ), # set key
                          parasitic_electricity, # parameter value
                          '-' )		
          }	
          
          # Add the cooling commodity to the cooled message technology input list
          if( ttt == 1 ) # only need to do once
          {
            ixDS$add_par( "input", # parameter name
                          c( reg, tech, as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ), 
                             as.character( ind$mode[ v ] ), reg, cmdty_out, cooling_level, as.character( ind$time[ v ] ), 
                             as.character( ind$time[ v ] )  ), # set key
                          1, # parameter value
                          '-' )
          }
          
        } ) } )
        
        # Go through each vintage and cooling option and add the corresponding data to the DB for investement costs
        ret = lapply( 1:length(inv_cost$year_vtg), function(v){ lapply( 1:length(techs_to_update), function(ttt){
          # Add the investment cost
          ixDS$add_par( "inv_cost", # parameter name
                        c( reg, techs_to_update[ ttt ], as.character( inv_cost$year_vtg[ v ] )  ), # set key
                        round( ( 1e9/(60*60*24*365) ) * cooling_technology_costs$inv_costs[ which(as.character(row.names(cooling_technology_costs)) == as.character(techs_to_update[ ttt ]) ) ],digits = rnd), # parameter value (convert from mill USD/MW to USD/GWa)
                        'USD/GWa' )
        } ) } )	
        
        # Go through each vintage and cooling option and add the corresponding data to the DB for investement costs
        ret = lapply( 1:length(fix_cost$year_vtg), function(v){ lapply( 1:length(techs_to_update), function(ttt){
          # Add the fixed cost
          ixDS$add_par( "fix_cost", # parameter name
                        c( reg, techs_to_update[ ttt ], as.character( fix_cost$year_vtg[ v ] ), as.character( fix_cost$year_act[ v ] )  ), # set key
                        round( ( 1e9/(60*60*24*365) ) * cooling_technology_costs$fixed_costs[ which(as.character(row.names(cooling_technology_costs)) == as.character(techs_to_update[ ttt ]) ) ],digits = rnd), # parameter value
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
              shr = cooltech_cost_and_shares_ssp_msg_raw.df[ 
                which( ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$utype) == 
                           unlist(strsplit(techs_to_update[ ttt ],'__'))[1] ) & ( 
                             as.character(cooltech_cost_and_shares_ssp_msg_raw.df$cooling) == 
                               unlist(strsplit(techs_to_update[ ttt ],'__'))[2] ) ), 
                paste("mix",unlist(strsplit(reg,'_'))[2],'Davies_2013',sep="_") ]
            }else
            {
              shr = cooltech_cost_and_shares_ssp_msg_raw.df[ 
                which( ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$utype) == 
                           unlist(strsplit(techs_to_update[ ttt ],'__'))[1] ) & ( 
                             as.character(cooltech_cost_and_shares_ssp_msg_raw.df$cooling) == 
                               unlist(strsplit(techs_to_update[ ttt ],'__'))[2] ) ), 
                paste("mix",unlist(strsplit(reg,'_'))[2],sep="_") ]
            }
            cap = round( shr * historical_new_capacity[ 
              which( as.character( historical_new_capacity$year_vtg ) == 
                       as.character( historical_new_capacity$year_vtg[v] )  ) ,  'value' ], digits = rnd )
            
            # Add the ix db
            ixDS$add_par( "historical_new_capacity", 
                          c( reg, techs_to_update[ ttt ], as.character( historical_new_capacity$year_vtg[ v ] )  ), 
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
              shr = cooltech_cost_and_shares_ssp_msg_raw.df[ 
                which( ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$utype) == 
                           unlist(strsplit(techs_to_update[ ttt ],'__'))[1] ) & 
                         ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$cooling) == 
                             unlist(strsplit(techs_to_update[ ttt ],'__'))[2] ) ), 
                paste("mix",unlist(strsplit(reg,'_'))[2],'Davies_2013',sep="_") ]
            }else
            {
              shr = cooltech_cost_and_shares_ssp_msg_raw.df[ 
                which( ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$utype) == 
                           unlist(strsplit(techs_to_update[ ttt ],'__'))[1] ) & 
                         ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$cooling) == 
                             unlist(strsplit(techs_to_update[ ttt ],'__'))[2] ) ), 
                paste("mix",unlist(strsplit(reg,'_'))[2],sep="_") ]
            }
            act = round( shr * historical_activity[ 
              which( as.character( historical_activity$year_act ) == 
                       as.character( historical_activity$year_act[v] )  ) ,  'value' ], digits = rnd )
            
            # Add the ix db
            ixDS$add_par( "historical_activity", 
                          c( reg, techs_to_update[ ttt ], as.character( historical_activity$year_act[ v ] ), 
                             as.character( historical_activity$mode[ v ] ) , as.character( historical_activity$time[ v ] )  ), 
                          act, 
                          as.character( historical_activity$unit[1] ) )
            
          }	
        } ) } )	
        
        ret = lapply( 1:length(technical_lifetime$year_vtg), function(v){ lapply( 1:length(techs_to_update), function(ttt){		
          # Add the technical lifetime
          ixDS$add_par( "technical_lifetime", # parameter name
                        c( reg, techs_to_update[ ttt ], as.character( technical_lifetime$year_vtg[ v ] )  ), # set key
                        technical_lifetime[ v ,  'value' ],
                        'y' )
        } ) } )
        
        ret = lapply( 1:length(construction_time$year_vtg), function(v){ lapply( 1:length(techs_to_update), function(ttt){		
          # Add the construction time
          ixDS$add_par( "construction_time", # parameter name
                        c( reg, techs_to_update[ ttt ], as.character( construction_time$year_vtg[ v ] )  ), # set key
                        construction_time[ v ,  'value' ],
                        as.character( construction_time[ v ,  'unit' ] ) )
        } ) } )		
        
      }else{ skipped_tech_reg = rbind( skipped_tech_reg, data.frame( 
        technology = tech, region = reg, comb = paste(tech,reg,sep='_') ) ) } # add to list of skipped techs
    } ) 
  } )	 
  
  #-------------------------------------------------------------------------------------------------------
  # Add water withdrawal and return flow for non-cooling technologies
  #-------------------------------------------------------------------------------------------------------
  
  print('Adding water coefficients for non-cooling technologies')
  
  # Could speed this up a bit by allocating the ppl water use during the previous step.
  
  if(ssss == 'SSP3')
  {
    skipped_techs = c("igcc_co2scr","gfc_co2scr","cfc_co2scr","h2_co2_scrub","h2b_co2_scrub","gas_htfc","h2_bio","h2_bio_ccs","h2_smr","h2_smr_ccs","h2_coal","h2_coal_ccs","h2_elec","solar_pv_ppl","wind_ppl")
  }else
  {
    skipped_techs = c("igcc_co2scr","gfc_co2scr","cfc_co2scr","h2_co2_scrub","h2b_co2_scrub","gas_htfc","solar_pv_ppl","wind_ppl")
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
        
        # Add the water coefficients for each vintage and time slice
        ret = lapply( 1:nrow( ind ), function(v){  
          
          # Using data from input csv files and converted from m3 / GJ to km3 / GWa
          water_withdrawal = round( ( 60 * 60 * 24 * 365 ) * ( 1e-9 ) * as.numeric( tech_water_performance_ssp_msg_raw.df[ which( as.character(tech_water_performance_ssp_msg_raw.df$technology_name) == tech  ), which(as.character(names(tech_water_performance_ssp_msg_raw.df)) == paste('water_withdrawal_',parameter_levels,'_m3_per_output',sep='') ) ] ), digits = rnd )
          return_flow = round( water_withdrawal - ( 60 * 60 * 24 * 365 ) * ( 1e-9 ) * as.numeric( tech_water_performance_ssp_msg_raw.df[ which( as.character(tech_water_performance_ssp_msg_raw.df$technology_name) == tech  ), which(as.character(names(tech_water_performance_ssp_msg_raw.df)) == paste('water_consumption_',parameter_levels,'_m3_per_output',sep='') ) ] ), digits = rnd )
          
          # Add the withdrawal to db as an input
          ixDS$add_par( "input", # parameter name
                        c( reg, tech, as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ), 
                           as.character( ind$mode[ v ] ), reg, cmdty_in, water_supply_level, as.character( ind$time[ v ] ), 
                           as.character( ind$time[ v ] )  ), # set key
                        water_withdrawal,
                        '-' )
          
          # Add the return flow to the emission factors	
          if( !( tech %in% hydro_techs ) ) # no wastewater for hydropower / instream technologies sads
          {
            ixDS$add_par( "emission_factor", # parameter name
                          c( reg, tech, 
                             as.character( ind$year_vtg[ v ] ), as.character( ind$year_act[ v ] ), 
                             as.character( ind$mode[ v ] ), 'fresh_wastewater'  ),
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
    print(paste( round( 100 * ( ( which( water_source_extraction_techs == tech ) - 1 ) / ( length(water_source_extraction_techs) ) ) ), ' % complete', sep=''))
    
    lapply( region, function(reg){ # across each region
      
      # Get the commodity for the source type (freshwater or saline or instream)
      cmdty_out = as.character( unlist(strsplit(tech,'__'))[2] )
      
      lapply( model_years, function( yy ){ # go through the years
        
        
        # Add the output for each timeslice
        ret = lapply( model_time, function(tm){ 
          
          ixDS$add_par( "output", # parameter name
                        c( reg, tech, as.character( yy ), 
                           as.character( yy ), mode_common, reg, cmdty_out, water_supply_level, 
                           as.character( tm ), as.character( tm )  ), # set key
                        1, # parameter value
                        '-' )
          
        } )
        
        # Add the investment cost
        ixDS$add_par( "inv_cost", # parameter name
                      c( reg, tech, as.character( yy )  ), # set key
                      0, 
                      '-' )
        
        # Add the fixed cost
        # print(ixDS$get_par_set('fix_cost'))
        ixDS$add_par( "fix_cost", # parameter name
                      c( reg, tech, as.character( yy ), as.character( yy )  ), # set key
                      0,
                      '-' )
        
        # Add the variable cost
        # print(ixDS$get_par_set('var_cost'))
        ret = lapply( model_time, function(tm){ 
          ixDS$add_par( "var_cost", # parameter name
                        c( reg, tech, as.character( yy ), as.character( yy ), as.character( mode_common ), as.character( tm )  ), # set key
                        0.0001, # default for now 
                        '-' )
        } )	
        
        # Add the capacity factor
        # print(ixDS$get_par_set('capacity_factor'))
        ret = lapply( model_time, function(tm){ 
          ixDS$add_par( "capacity_factor", # parameter name
                        c( reg, tech, as.character( yy ), as.character( yy ), as.character( tm )  ), # set key
                        1,  
                        '-' )
        } )
        
        # Add the technical lifetime
        if(yy < model_years[length(model_years)]){tl = as.numeric(model_years[which(model_years==yy)+1]) - as.numeric(yy)}else{tl = as.numeric(model_years[length(model_years)]) - as.numeric(model_years[length(model_years)-1])}
        ixDS$add_par( "technical_lifetime", # parameter name
                      c( reg, tech, as.character( yy )  ), # set key
                      tl,
                      'y' )
        
        # Add the contruction time
        ixDS$add_par( "construction_time", # parameter name
                      c( reg, tech, as.character( yy )  ), # set key
                      0,
                      'y' )	
        
      } )
      
      # Historical activity of extraction techs determined from 
      # the historical activity of the MESSAGE techs
      techs_hist = unlist( hist_techs[ cmdty_out ] ) 
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
      hist_tot_act = unlist( lapply( 1:length(hist_years), function(yyy){ sum( unlist( lapply( 1:length(techs_hist), function(xxx){    
        temp2 = temp[[xxx]]
        if( hist_years[yyy] %in% as.character( temp2[,1] ) )
        {
          return( as.numeric( as.character( temp2[ which( as.character( temp2[,1] ) == as.character( hist_years[yyy] ) ), 2 ] ) ) )
        }else{ return(NULL) }
      } ) ), na.rm = TRUE ) } ) )
      
      # Add the ix db
      ret = lapply( 1:length(hist_years), function(yyy){
        ixDS$add_par( "historical_activity", 
                      c( reg, tech, as.character( hist_years[ yyy ] ), as.character( mode_common ) , model_time  ), 
                      hist_tot_act[ yyy ], 
                      '-' )
      } )
    } ) 
  } )			
  
  #-------------------------------------------------------------------------------------------------------
  # Add baseline cooling technology policy - No new once-through freshwater cooling capacity
  #-------------------------------------------------------------------------------------------------------
  print('Adding once through freshwater baseline cooling policy')
  
  # Once through freshwater cooled techs
  otc = unlist(lapply( ixDS$set('technology'), function(x) if( length(unlist(strsplit(x,'__'))==2) ){ if( unlist(strsplit(x,'__'))[2] %in% c('ot_fresh') ){ return( x ) } } ))
  
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
                          c( reg, tech, vv  ), 
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
  
  # List of techs with saline once through 
  otc = unlist(lapply( ixDS$set('technology'), function(x) if( length(unlist(strsplit(x,'__'))==2) ){ if( unlist(strsplit(x,'__'))[2] %in% c('ot_saline') ){ return( x ) } } ))
  yb = 2010 # base year
  
  # Getting the historical activity of all seawater cooling technologies - will use to constrain future years
  nms_hist = sapply(1:nrow( all_historical_activity ),function(fff){ 
    paste(all_historical_activity$technology[fff],all_historical_activity$node_loc[fff],sep='_') })
  
  ret = do.call(cbind, lapply( otc, function(tech){ 
    print(paste( round( 100 * ( ( which( otc == tech ) - 1 ) / ( length(otc) ) ) ), ' % complete', sep=''))
    dfs = data.frame( do.call(rbind, lapply( region, function(reg){
      if( !( paste(tech,reg,sep='_') %in% skipped_tech_reg$comb ) & paste(tech,reg,sep='_') %in% nms_hist )
      {
        # Total historical activity of cooled powered plant and share for ocean water cooling
        hist_act = sum( ixDS$par( 'historical_activity', list( node_loc = reg, technology = unlist(strsplit(tech,'__'))[1], year_act= 2010  ) )[,'value'] )
        if(use_davies_shares)
        {
          hist_shr = cooltech_cost_and_shares_ssp_msg_raw.df[ which( ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$utype) == unlist(strsplit(tech,'__'))[1] ) & ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$cooling) == unlist(strsplit(tech,'__'))[2] ) ), paste("mix",unlist(strsplit(reg,'_'))[2],'Davies_2013',sep="_") ]
        }else
        {
          hist_shr = cooltech_cost_and_shares_ssp_msg_raw.df[ which( ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$utype) == unlist(strsplit(tech,'__'))[1] ) & ( as.character(cooltech_cost_and_shares_ssp_msg_raw.df$cooling) == unlist(strsplit(tech,'__'))[2] ) ), paste("mix",unlist(strsplit(reg,'_'))[2],sep="_") ]
        }
        return(as.matrix(max(0,hist_act*hist_shr,na.rm=TRUE))) # multiply the activity by the share to estimate the total historical output
      }else{ return(as.matrix(c(0))) }
    } ) ) )
    row.names(dfs) = region
    return(dfs) } ) )
  names(ret) = otc	
  ww1 = rowSums(ret)	
  
  # Add the constraint to the db by bounding the activity in each year of the regional extraction technology
  ret = lapply( c('extract__saline_supply'), function(tech){ lapply( region, function(reg){
    year_all = seq(2020,2100,by=10)
    lapply( as.character(year_all), function(vv){
      ixDS$add_par( 'bound_activity_up', 
                    c( reg, tech, vv, mode_common, model_time  ), 
                    ww1[reg], 
                    '-' )
    } )
  } ) 
  } )
  
  #-------------------------------------------------------------------------------------------------------
  # Initialize water constraint criteria - an extra technology that outputs into water extraction techs
  #-------------------------------------------------------------------------------------------------------
  
  # Water constraints are implemented using an extra technology (extract__water_constraint) that outputs into water extraction techs
  # This choice enables modeling policies that cover multiple water commodities. E.g., simultaneous reduction targets for saline and freshwater, etc.
  # The water resources to be constrained can be modified by selecting the techs that take in the output commodity from extract__water_constraint
  # Note that the constraint is left disconnected in the baseline script and that the following merely initializes the general structure.
  
  print('Initializing water constraints')
  
  # Add the water constraint technology to the set list
  tech = "extract__water_constraint"
  cmdty_out = "water_constraint"
  water_supply_level_constraint = paste(water_supply_level,'constraint',sep='_')
  ret = ixDS$add_set( "technology", tech )
  ret = ixDS$add_set( "commodity", cmdty_out )
  ret = ixDS$add_set( "level", water_supply_level_constraint ) # Add level to ix DB
  
  # Go through each region and add the technology data
  res = lapply( region, function(reg){ 
    
    # Add the output for each timeslice
    ret = lapply( model_years, function(yy){ 
      
      ixDS$add_par( "output", # parameter name
                    c( reg, tech, as.character( yy ), as.character( yy ), mode_common, reg, cmdty_out, 
                       water_supply_level_constraint, model_time, model_time  ), # set key
                    1, # parameter value
                    '-' )
      
      # Add the investment cost
      ixDS$add_par( "inv_cost", # parameter name
                    c( reg, tech, as.character( yy )  ), # set key
                    0, 
                    '-' )
      
      # Add the fixed cost
      # print(ixDS$get_par_set('fix_cost'))
      ixDS$add_par( "fix_cost", # parameter name
                    c( reg, tech, as.character( yy ), as.character( yy )  ), # set key
                    0,
                    '-' )
      
      # Add the variable cost
      # print(ixDS$get_par_set('var_cost'))
      ret = lapply( model_time, function(tm){ 
        ixDS$add_par( "var_cost", # parameter name
                      c( reg, tech, as.character( yy ), as.character( yy ), as.character( mode_common ), model_time  ), # set key
                      0,  
                      '-' )
      } )	
      
      # Add the capacity factor
      # print(ixDS$get_par_set('capacity_factor'))
      ret = lapply( model_time, function(tm){ 
        ixDS$add_par( "capacity_factor", # parameter name
                      c( reg, tech, as.character( yy ), as.character( yy ), model_time  ), # set key
                      1,  
                      '-' )
      } )
      
      # Add the technical lifetime
      if(yy < model_years[length(model_years)]){tl = as.numeric(model_years[which(model_years==yy)+1]) - as.numeric(yy)}else{tl = as.numeric(model_years[length(model_years)]) - as.numeric(model_years[length(model_years)-1])}
      ixDS$add_par( "technical_lifetime", # parameter name
                    c( reg, tech, as.character( yy )  ), # set key
                    tl,
                    'y' )
      
      # Add the contruction time
      ixDS$add_par( "construction_time", # parameter name
                    c( reg, tech, as.character( yy )  ), # set key
                    0,
                    'y' )	
      
    } )
  } )
  
  #-------------------------------------------------------------------------------------------------------
  # Commit to DB, solve, read GDX and set to default scenario
  #-------------------------------------------------------------------------------------------------------
  # REPLACE SIMPLY with SOLVE
  # 
  # 	# Need to use this library for calling GAMS from R
  # 	require(gdxrrw)
  # 
  # 	# Commit to DB
  ixDS$commit(comment)
  ixDS$set_as_default()
  ixDS$solve()
  # 	fname = paste(ssss, newscenarioName, sep = '_')
  # 	
  # 	# Set the GAMS path and file information
  # 	gdx_path_data = paste(wkd_message_ix,"message_ix/model/data",sep='') # set based on local machine
  # 	gdx_name = paste( 'MSGdata_', fname, ".gdx",sep="")
  # 
  # 	# Output to gdx
  # 	ixDS$to_gdx(gdx_path_data, gdx_name)	
  # 
  # 	# Solve the model using the gdxrrw utilities
  # 	current_working_drive = getwd()
  # 	setwd(paste(wkd_message_ix,'message_ix/model',sep='')) # set based on local machine
  # 	igdx(wkd_GAMS) # set based on local machine
  # 	ret = gams(paste("MESSAGE_master.gms --fname=",fname,sep=""))
  # 	setwd(current_working_drive)		
  # 
  # 	# Upload the gdx to the db and set as default if solved correctly
  # 	ixDS$read_sol_from_gdx(paste(wkd_message_ix,"message_ix/model/output",sep=""),paste("MSGoutput_",fname,".gdx",sep=''))
  # 	ixDS$set_as_default()
  
} # Next SSP	