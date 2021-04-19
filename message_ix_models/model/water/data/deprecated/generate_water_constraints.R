# Clear memory and close all windows
rm(list = ls())
graphics.off()

require(reshape)
require(maptools)
require(countrycode)
require(raster)
require(rgeos)
require(rgdal)
memory.limit(size=1e9)

## Set path data folder in message_ix working copy
msg_data = Sys.getenv("MESSAGE_DATA_PATH")
data_path = path.expand(paste0(msg_data,'\\data\\water'))

# Country region mapping key
country_region_map_key.df = data.frame( read.csv( paste( data_path, '/water_demands/country_region_map_key.csv',  sep = '/' ), stringsAsFactors=FALSE) )

# ssp
ssp = 2

# Load municipal water demands and socioeconomic parameters from local drive
dat.df = merge_recurse( lapply( seq(2010,2090,by=10), function(y){

  dat.df = data.frame( readRDS( paste(data_path,'/harmonized_rcp_ssp_data/water_use_ssp2_rcp2_',y,'_data.Rda',sep='') ) )
  dat.df = cbind( dat.df[ ,c('country_id', paste('xloc',y,sep= '..'), paste('yloc', y, sep= '..'), paste('urban_pop', y, sep= '..'), paste('rural_pop', y, sep= '..'), paste('urban_gdp', y, sep= '..'), paste('rural_gdp', y, sep= '..') ) ],
                  rowSums( do.call( cbind, lapply( 1:12, function(m){ dat.df[ ,paste('urban_withdrawal',y,m,sep= '..') ] } ) ) ),
                  rowSums( do.call( cbind, lapply( 1:12, function(m){ dat.df[ ,paste('rural_withdrawal',y,m,sep= '..') ] } ) ) ),
                  rowSums( do.call( cbind, lapply( 1:12, function(m){ dat.df[ ,paste('urban_return',y,m,sep= '..') ] } ) ) ),
                  rowSums( do.call( cbind, lapply( 1:12, function(m){ dat.df[ ,paste('rural_return',y,m,sep= '..') ] } ) ) ),
                  rowSums( do.call( cbind, lapply( 1:12, function(m){ dat.df[ ,paste('urban_withdrawal',y,m,sep= '..') ] } ) ) ) / dat.df[, paste('urban_pop', y, sep= '..') ],
                  rowSums( do.call( cbind, lapply( 1:12, function(m){ dat.df[ ,paste('rural_withdrawal',y,m,sep= '..') ] } ) ) ) / dat.df[, paste('rural_pop', y, sep= '..') ]	)
  names(dat.df) = c('country_id', 'xloc', 'yloc', paste('urban_pop',y,sep= '..'), paste('rural_pop', y, sep= '..'), paste('urban_gdp', y, sep= '..'), paste('rural_gdp', y, sep= '..'), paste('urban_withdrawal', y, sep= '..'), paste('rural_withdrawal', y, sep= '..'), paste('urban_return', y, sep= '..'), paste('rural_return', y, sep= '..'), paste('urban_per_capita_withdrawal', y, sep= '..'), paste('rural_per_capita_withdrawal', y, sep= '..') )

  # Make sure return flows don't exceed withdrawals
  inds = which( unlist( dat.df[, paste('urban_return',y,sep= '..') ] ) > 0.92 * unlist(dat.df[, paste('urban_withdrawal', y, sep= '..') ] ) )
  if( length(inds)>0 ){ dat.df[ inds, paste('urban_return',y,sep= '..') ] = dat.df[inds, paste('urban_withdrawal', y, sep= '..') ] * 0.92 }
  inds = which( unlist( dat.df[, paste('rural_return',y,sep= '..') ] ) > 0.92 * unlist(dat.df[, paste('rural_withdrawal', y, sep= '..') ] ) )
  if( length(inds)>0 ){ dat.df[ inds, paste('rural_return',y,sep= '..') ] = dat.df[inds, paste('rural_withdrawal', y, sep= '..') ] * 0.92 }

  # Set NA to 0
  dat.df[which(is.nan(dat.df[,12])),12] = 0
  dat.df[which(is.nan(dat.df[,13])),13] = 0
  dat.df[which(is.na(dat.df[,12])),12] = 0
  dat.df[which(is.na(dat.df[,13])),13] = 0

  return(dat.df)

} ), by = c('xloc','yloc','country_id') )

dat.df[is.na(dat.df)] = 0

# year interpolation
var_2_intpl = unique(gsub('\\..*','',names(dat.df)))
var_2_intpl = var_2_intpl[!var_2_intpl %in% c('xloc','yloc','country_id')]
initial_years = unique(gsub('.*\\.','',names(dat.df)))
initial_years = as.numeric(initial_years[!initial_years %in% c('xloc','yloc','country_id')])
yrs = initial_years

# Make spatial
dat.spdf = dat.df
coordinates(dat.spdf) = ~ xloc + yloc

yrs = seq(2010,2090,by=10)

# Add distance to coastline
coast = readShapeLines('P:/ene.model/data/Water/ne_10m_coastline/ne_10m_coastline.shp')
#distance2coast = sapply( 1:nrow(dat.spdf), function(x){  gDistance( dat.spdf[x,], coast ) } )
#write.csv( distance2coast, 'C:/Users/parkinso/Documents/distance2coastssp2.csv', row.names=FALSE )
distance2coast2 = data.frame( read.csv( 'P:/ene.model/data/Water/distance2coastssp2.csv', stringsAsFactors = FALSE ) )
dat.spdf$distance2coast = distance2coast2$x  # in degrees

# a = dat.spdf[,'distance2coast']
# a$distance2coast = as.numeric(a$distance2coast)/0.125
# gridded(a) = TRUE
# windows()
# plot(a )
# plot( coast, add=TRUE)
# rm(a)

#read in raster from ED. ATTENTION 2100 and 2110 missing
nc = nc_open( paste0(data_path,'/water_scarcity/wsi_memean_ssp2_rcp6p0.nc'), verbose=FALSE)
watstress.brick = brick( paste0(data_path,'/water_scarcity/wsi_memean_ssp2_rcp6p0.nc') )

for (i in seq(1:nlayers(watstress.brick)) ){
    r <- raster(watstress.brick, layer=i)
    proj4string(r) = proj4string(dat.spdf)
    yr = as.numeric(gsub('X','',gsub('s','',names(r)) ))
    if (yr %in% initial_years){
      column_name = paste0('WSI.', yr )
      dat.spdf@data[,column_name] = raster::extract(r,dat.spdf)
      # there are 876 Na values, we set them = 0
      dat.spdf@data[column_name][is.na(dat.spdf@data[column_name])] = 0
      } else {}

}

dat.df = bind_cols(as.data.frame(dat.spdf@coords),dat.spdf@data)

## END TESTING

# OLD PART TO REMOVE
# Add water stress level - map to gridded withdrawals

temp = readOGR('P:/ene.model/data/Water','water_scarcity',verbose=FALSE)
temp = SpatialPolygonsDataFrame(as(temp,'SpatialPolygons'), data.frame(temp), match.ID = TRUE)
all_bas = temp
all_bas@data$PID = as.character( all_bas$ECOREGION )
temp = temp[ which(as.character( temp@data$wtr_stress ) %in% c( 'Stress','High stress','Low stress') ), ]
temp@data$ID = 1
temp@data$ID2 = sapply( as.character( temp@data$wtr_stress ), function(st){ if(st == 'Stress'){return(0.75)}; if(st == 'High stress'){return(1)}; if(st == 'Low stress'){return(0.6)} } )
temp = temp[ ,c('ID2') ]
temp@data$ID = as.numeric( temp@data$ID2 )
temp@data$PID = row.names( temp )
proj4string(dat.spdf) = proj4string(temp)
dat.df = cbind( data.frame(dat.spdf), over( dat.spdf,temp ) )
rm( dat.spdf )
dat.df$ID[ which( is.na( dat.df$ID ) ) ] = 0
# temp = readOGR(paste0(data_path,'/water_scarcity'),'water_scarcity',verbose=FALSE)
# temp = SpatialPolygonsDataFrame(as(temp,'SpatialPolygons'), data.frame(temp), match.ID = TRUE)
# all_bas = temp
# all_bas@data$PID = as.character( all_bas$ECOREGION )
# temp = temp[ which(as.character( temp@data$wtr_stress ) %in% c( 'Stress','High stress','Low stress') ), ]
# temp@data$ID = 1
# temp@data$ID2 = sapply( as.character( temp@data$wtr_stress ), function(st){ if(st == 'Stress'){return(0.75)}; if(st == 'High stress'){return(1)}; if(st == 'Low stress'){return(0.6)} } )
# temp = temp[ ,c('ID2') ]
# temp@data$ID = as.numeric( temp@data$ID2 )
# temp@data$PID = row.names( temp )
# proj4string(dat.spdf) = proj4string(temp)
# dat.df = cbind( data.frame(dat.spdf), over( dat.spdf,temp ) )
# rm( dat.spdf )
# dat.df$ID[ which( is.na( dat.df$ID ) ) ] = 0
## END PARt TO REMOVE

# Countries and regions
# Here we can add specification in case of a country model
dat.df$country = sapply( dat.df$country_id, function(cc){ if( cc %in% country_region_map_key.df$UN_Code ){ return( country_region_map_key.df$Region[ which(country_region_map_key.df$UN_Code == cc) ] ) }else{ return(NA) } } )
dat.df$region = sapply( dat.df$country_id, function(cc){ if( cc %in% country_region_map_key.df$UN_Code ){ return( country_region_map_key.df$Eleven_region[ which(country_region_map_key.df$UN_Code == cc) ] ) }else{ return(NA) } } )
dat.df = dat.df[which(!is.na(dat.df$region)),]

length(unique(dat.df$country))
gov.df = read.csv(paste0(data_path,'/governance/governance_obs_project.csv')) %>%
  filter(!is.na(governance)) %>%
  filter(scenario == SSP, year %in% initial_years ) %>%
  select(countrycode, year, governance)
names(gov.df)
mean_gov = mean(gov.df$governance)
# missing countries, just add mean_gov
to_add = data.frame(countrycode = c('AFG','AGO', 'ALB', 'ARE', 'MMR', 'PSE', 'QAT', 'TLS','TWN'), governance = mean_gov) %>%
  crossing(year = unique(gov.df$year)) %>%
  select(countrycode,year,governance)

gov.df = gov.df %>% bind_rows(to_add) %>%
  rename(gov = year, country = countrycode) %>%
  spread(gov,governance, sep = '..')

dat.df = dat.df %>% left_join(gov.df)
#countries that have no governance values
dat.df %>% filter(is.na(gov.2020)) %>%
  distinct(country) # not anymore

# Existing urban wastewater treatment
# Income vs connection from Baum et al 2010 - model fit logistic
income_vs_connection.df = data.frame(level = c('low','middle','upper','high'), max_income = c(1045/2,(4125-1045)/2+1045,(12375-4125)/2+4125,(30000-12375)/2+12375), connection = c(3.6,12.7,53.6,86.8)/100, treatment=c(0.02,2,13.8,78.9)/100)
y = c(0.0001,income_vs_connection.df$connection,0.99)
x = c(100,income_vs_connection.df$max_income,60000)
r = nls(y ~ SSlogis(x,a,m,s))
cp.a = 1
cp.m = coef(r)[2]
cp.s = coef(r)[3]
y = c(0.0001,income_vs_connection.df$treatment,0.99)
r = nls(y ~ SSlogis(x,a,m,s))
tp.a = 1
tp.m = coef(r)[2]
tp.s = coef(r)[3]

# Get the historical data from Baum et al.
ww.df = data.frame(read.csv( paste( data_path, '/wastewater_Baum_2013/sewerage_connection_and_treatment.csv', sep = '' ), header=TRUE, sep=',', stringsAsFactors=F, as.is=T))

ww.df = data.frame(read.csv( paste( data_path, '/wastewater_Baum_2013/sewerage_connection_and_treatment.csv', sep = '' ), header=TRUE, sep=',', stringsAsFactors=F, as.is=T))

ww.df$country[ which( ww.df$country == '' ) ] = 'Venezuela'
ww.df$country_id = as.character(countrycode(ww.df$country, 'country.name', 'iso3c'))
ww.df$income.2010 = sapply( ww.df$country_id, function( ccc ){
  inds = which( dat.df$country == ccc )
  return( sum( dat.df$urban_gdp.2010[ inds ] + dat.df$rural_gdp.2010[ inds ] ) / sum( dat.df$urban_pop.2010[ inds ] + dat.df$rural_pop.2010[ inds ] ) )
} )
ww.df2 = ww.df[ which( ww.df$income.2010 > 0 ), ]

# Plot historical connection levels and model
cpm = data.frame( x = seq( 1, 56000, by = 1000 ), y = SSlogis( seq( 1, 56000, by = 1000 ), 0.99, cp.m, cp.s ) )
tpm = data.frame( x = seq( 1, 56000, by = 1000 ), y = SSlogis( seq( 1, 56000, by = 1000 ), 0.99, tp.m, tp.s ) )
#pdf(paste(getwd(),'/baum_model.pdf',sep=''))
p1 = layout(matrix(c(1,2),1,2,byrow=TRUE),widths=c(0.45,0.45),heights=c(0.9,0.1))
par(mar=c(5,4,5,2), oma = c(2,2,2,2))
plot(cpm$x, 100*cpm$y, col = 'black', type = 'l', lwd = 2, xlab = 'Per Capita Income [ USD2010 ]', ylab = 'Population with Piped Water Access [ % ]', ylim = c(0,100) )
points( ww.df2$income.2010, ww.df2$connected.2010 )
text( 1e3, 99, 'a', cex= 1.2, font = 2 )
text( 3.5e4, 7, expression( y == frac( 1, 1 + ~ "exp[ " ~ frac(  7516.48 - x , 2360.56 ) ~ " ]" ) ), cex = 0.75 )
plot(tpm$x, 100*tpm$y, col = 'black', type = 'l', lwd = 2, xlab = 'Per Capita Income [ USD2010 ]', ylab = 'Population with Wastewater Treatment [ % ]', ylim = c(0,100))
points( ww.df2$income.2010, ww.df2$treated.2010 )
text( 1e3, 99, 'b', cex= 1.2, font = 2 )
text( 3.5e4, 7, expression( y == frac( 1, 1 + ~ "exp[ " ~ frac(  15769.69 - x , 3871.52 ) ~ " ]" ) ), cex = 0.75 )
dev.off()

# store for later
bbbb = dat.df

#rst = lapply( 1:2, function( add_SDG_constrain ){
#for( add_SDG_constrain in 1:3 )
for( scn in c('baseline','sdg6') )
{

  dat.df = bbbb
  #temp
  #scn = 'baseline'
  # Minimum level of daily water demand for decent living - SSP1 manufacturing demands to incorporate expected water efficiency measures in SDG6 scenarios
  if( scn %in% c('sdg6') ){

     fsc = SSP

    # Add manufacturing demands - simple approach where national values generated previously are downscaled to urban areas based on population
    mf_withdrawal.df = data.frame( read.csv( paste0(data_path,'/manufacturing_water_demand_results/national/IIASA_water_withdrawal_manufacturing_Static.csv'), stringsAsFactors = FALSE ) )
    mf_return.df = data.frame( read.csv( paste0(data_path,'/manufacturing_water_demand_results/national/IIASA_water_return_manufacturing_Static.csv'), stringsAsFactors = FALSE ) )
    mf_withdrawal.df = mf_withdrawal.df[which( as.character(mf_withdrawal.df$Scenario) == fsc & !is.na(mf_withdrawal.df$UN_Code)),]
    mf_return.df = mf_return.df[which( as.character(mf_return.df$Scenario) == fsc & !is.na(mf_return.df$UN_Code)),]

    # Downscale based on country
    dat.df = cbind( dat.df, do.call( cbind, lapply( yrs, function(yy){
      cnt = unique( as.character( dat.df$country ) )[ which( unique( as.character( dat.df$country ) ) %in% unique( as.character( mf_withdrawal.df$Country_Code ) ) ) ]
      mfpc = unlist( sapply( cnt, function(cc){ return( mf_withdrawal.df[ which( as.character( mf_withdrawal.df$Scenario ) == fsc & as.character( mf_withdrawal.df$Country_Code ) == cc ), paste('X',yy,sep='') ] / sum(dat.df[ which( dat.df$country == cc ), paste('urban_pop',yy,sep= '..') ], na.rm = TRUE ) ) } ) )
      tmp.df = data.frame( country = dat.df$country )
      tmp.df$res = NA
      for( x in cnt ){ tmp.df$res[ as.character( tmp.df$country ) == x ] = dat.df[ as.character( dat.df$country ) == x, paste('urban_pop',yy,sep= '..') ] * mfpc[x ] }
      res = data.frame( tmp.df$res )
      names(res) = paste('mf_withdrawal',yy,sep= '..')
      row.names(res) = row.names(dat.df)
      return(res) } ) ) )
    dat.df = cbind( dat.df, do.call( cbind, lapply( yrs, function(yy){
      cnt = unique( as.character( dat.df$country ) )[ which( unique( as.character( dat.df$country ) ) %in% unique( as.character( mf_return.df$Country_Code ) ) ) ]
      mfpc = unlist( sapply( cnt, function(cc){ return( mf_return.df[ which( as.character( mf_return.df$Scenario ) == fsc & as.character( mf_return.df$Country_Code ) == cc ), paste('X',yy,sep='') ] / sum(dat.df[ which( dat.df$country == cc ), paste('urban_pop',yy,sep= '..') ], na.rm = TRUE ) ) } ) )
      tmp.df = data.frame( country = dat.df$country )
      tmp.df$res = NA
      for( x in cnt ){ tmp.df$res[ as.character( tmp.df$country ) == x ] = dat.df[ as.character( dat.df$country ) == x, paste('urban_pop',yy,sep= '..') ] * mfpc[x ] }
      res = data.frame( tmp.df$res )
      names(res) = paste('mf_return',yy,sep= '..')
      row.names(res) = row.names(dat.df)
      return(res) } ) ) )

    ch = scn

    # Adjust demands in urban grid cells with less than 100 L per day and for rural cells with less than 50 L per day - only for post-2030
    urb_min_sdg = 100 / 0.8
    rur_min_sdg = 50 / 0.8
    for( yyy in yrs[ yrs >= 2030 ] )
    {

      upo = dat.df[,paste('urban_pop',yyy,sep= '..')]
      rpo = dat.df[,paste('rural_pop',yyy,sep= '..')]

      upc = dat.df[,paste('urban_per_capita_withdrawal',yyy,sep= '..')] * 1e9 / 365 # convert from mcm per year to liters per day
      rpc = dat.df[,paste('rural_per_capita_withdrawal',yyy,sep= '..')] * 1e9 / 365 # convert from mcm per year to liters per day
      upc[is.na(upc)]=0
      rpc[is.na(rpc)]=0

      udxi = density( upc[ which( upc > 0 ) ], weights = upo[ which( upc > 0 ) ] / sum( upo[ which( upc > 0 ) ] ) )
      rdxi = density( rpc[ which( rpc > 0 ) ], weights = rpo[ which( rpc > 0 ) ] / sum( rpo[ which( rpc > 0 ) ] ) )

      upc[ which( upc < urb_min_sdg & upc > 0 ) ] = urb_min_sdg
      rpc[ which( rpc < rur_min_sdg & rpc > 0 ) ] = rur_min_sdg

      udxf = density( upc[ which( upc > 0 ) ], weights = upo[ which( upc > 0 ) ] / sum( upo[ which( upc > 0 ) ] ) )
      rdxf = density( rpc[ which( rpc > 0 ) ], weights = rpo[ which( rpc > 0 ) ] / sum( rpo[ which( rpc > 0 ) ] ) )

      # pdf(paste('C:/Users/parkinso/Documents/mwdens',yyy,'.pdf',sep=''))
      # p1 = layout(matrix(c(1,2),1,2,byrow=TRUE),widths=c(0.45,0.45),heights=c(0.9,0.1))
      # plot( udxi$x * 0.8 , cumsum(udxi$y)/max(cumsum(udxi$y)), type='l', col = 'black', xlab = 'Liters per day', ylab = 'Cumulative Population Distribution', main = paste( 'Urban',yyy, sep=' - ') )
      # lines( udxf$x * 0.8, cumsum(udxf$y)/max(cumsum(udxf$y)), type='l', col = 'red' )
      # plot( rdxi$x * 0.8, cumsum(rdxi$y)/max(cumsum(rdxi$y)), type='l', col = 'black', xlab = 'Liters per day', ylab = 'Cumulative Population Distribution', main = paste( 'Rural',yyy, sep=' - ') )
      # lines( rdxf$x * 0.8, cumsum(rdxf$y)/max(cumsum(rdxf$y)), type='l', col = 'red' )
      # legend( 'bottomright', bty = 'n', legend = c('Baseline', 'SDG6'), lty = 1, col = c('black','red') )
      # dev.off()

      # Convert units to km3 per year
      dat.df[,paste('urban_per_capita_withdrawal',yyy,sep= '..')] = upc * 1e-9 * 365
      dat.df[,paste('rural_per_capita_withdrawal',yyy,sep= '..')] = rpc * 1e-9 * 365

      dat.df[,paste('urban_withdrawal',yyy,sep= '..')] = dat.df[, paste('urban_per_capita_withdrawal', yyy, sep= '..')] * dat.df[, paste('urban_pop', yyy, sep= '..')]
      dat.df[,paste('rural_withdrawal',yyy,sep= '..')] = dat.df[, paste('rural_per_capita_withdrawal', yyy, sep= '..')] * dat.df[, paste('rural_pop', yyy, sep= '..')]

        # previously sdg6_eff
        # Further end-use conservation assumed combining a 10% reduction in withdrawals due to
        # behavioral changes and 10% recycling
        fct = 0.9 * 0.9

        dat.df[,paste('urban_withdrawal',yyy,sep= '..')] = fct * dat.df[, paste('urban_withdrawal', yyy, sep= '..')]
        dat.df[,paste('urban_return',yyy,sep= '..')] = fct * dat.df[, paste('urban_return', yyy, sep= '..')]
        dat.df[,paste('rural_withdrawal',yyy,sep= '..')] = fct * dat.df[, paste('rural_withdrawal', yyy, sep= '..')]
        dat.df[,paste('rural_return',yyy,sep= '..')] = fct * dat.df[, paste('rural_return', yyy, sep= '..')]
        dat.df[,paste('mf_withdrawal',yyy,sep= '..')] = fct * dat.df[, paste('mf_withdrawal', yyy, sep= '..')]
        dat.df[,paste('mf_return',yyy,sep= '..')] = fct * dat.df[, paste('mf_return', yyy, sep= '..')]


      inds = which( unlist( dat.df[, paste('urban_return',yyy,sep= '..') ] ) > 0.92 * unlist(dat.df[, paste('urban_withdrawal', yyy, sep= '..') ] ) )
      if( length(inds)>0 ){ dat.df[ inds, paste('urban_return',yyy,sep= '..') ] = dat.df[inds, paste('urban_withdrawal', yyy, sep= '..') ] * 0.92 }
      inds = which( unlist( dat.df[, paste('rural_return',yyy,sep= '..') ] ) > 0.92 * unlist(dat.df[, paste('rural_withdrawal', yyy, sep= '..') ] ) )
      if( length(inds)>0 ){ dat.df[ inds, paste('rural_return',yyy,sep= '..') ] = dat.df[inds, paste('rural_withdrawal', yyy, sep= '..') ] * 0.92 }
      inds = which( unlist( dat.df[, paste('mf_return',yyy,sep= '..') ] ) > 0.92 * unlist(dat.df[, paste('mf_withdrawal', yyy, sep= '..') ] ) )
      if( length(inds)>0 ){ dat.df[ inds, paste('mf_return',yyy,sep= '..') ] = dat.df[inds, paste('mf_withdrawal', yyy, sep= '..') ] * 0.92 }

    }


  }else{ # baseline


    # Add manufacturing demands - simple approach where national values generated previously are downscaled to urban areas based on population
    mf_withdrawal.df = data.frame( read.csv( paste0(data_path,'/manufacturing_water_demand_results/national/IIASA_water_withdrawal_manufacturing_Static.csv'), stringsAsFactors = FALSE ) )
    mf_return.df = data.frame( read.csv( paste0(data_path,'/manufacturing_water_demand_results/national/IIASA_water_return_manufacturing_Static.csv'), stringsAsFactors = FALSE ) )
    mf_withdrawal.df = mf_withdrawal.df[which( as.character(mf_withdrawal.df$Scenario) == 'SSP2' & !is.na(mf_withdrawal.df$UN_Code)),]
    mf_return.df = mf_return.df[which( as.character(mf_return.df$Scenario) == 'SSP2' & !is.na(mf_return.df$UN_Code)),]

    dat.df = cbind( dat.df, do.call( cbind, lapply( yrs, function(yy){
      cnt = unique( as.character( dat.df$country ) )[ which( unique( as.character( dat.df$country ) ) %in% unique( as.character( mf_withdrawal.df$Country_Code ) ) ) ]
      mfpc = unlist( sapply( cnt, function(cc){ return( mf_withdrawal.df[ which( as.character( mf_withdrawal.df$Scenario ) == 'SSP2' & as.character( mf_withdrawal.df$Country_Code ) == cc ), paste('X',yy,sep='') ] / sum(dat.df[ which( dat.df$country == cc ), paste('urban_pop',yy,sep= '..') ], na.rm = TRUE ) ) } ) )
      tmp.df = data.frame( country = dat.df$country )
      tmp.df$res = NA
      for( x in cnt ){ tmp.df$res[ as.character( tmp.df$country ) == x ] = dat.df[ as.character( dat.df$country ) == x, paste('urban_pop',yy,sep= '..') ] * mfpc[x ] }
      res = data.frame( tmp.df$res )
      names(res) = paste('mf_withdrawal',yy,sep= '..')
      row.names(res) = row.names(dat.df)
      return(res) } ) ) )
    dat.df = cbind( dat.df, do.call( cbind, lapply( yrs, function(yy){
      cnt = unique( as.character( dat.df$country ) )[ which( unique( as.character( dat.df$country ) ) %in% unique( as.character( mf_return.df$Country_Code ) ) ) ]
      mfpc = unlist( sapply( cnt, function(cc){ return( mf_return.df[ which( as.character( mf_return.df$Scenario ) == 'SSP2' & as.character( mf_return.df$Country_Code ) == cc ), paste('X',yy,sep='') ] / sum(dat.df[ which( dat.df$country == cc ), paste('urban_pop',yy,sep= '..') ], na.rm = TRUE ) ) } ) )
      tmp.df = data.frame( country = dat.df$country )
      tmp.df$res = NA
      for( x in cnt ){ tmp.df$res[ as.character( tmp.df$country ) == x ] = dat.df[ as.character( dat.df$country ) == x, paste('urban_pop',yy,sep= '..') ] * mfpc[x ] }
      res = data.frame( tmp.df$res )
      names(res) = paste('mf_return',yy,sep= '..')
      row.names(res) = row.names(dat.df)
      return(res) } ) ) )

    ch = scn

  }
  # CHECK TILL HERE
  # piped water access / sewerage connection
  #### ATTENTION HERE IT SEEMS IT WORK WIT 10 Y TIME STEPS, see below
  national_connection_rate.df = do.call( rbind, lapply( yrs , function(yy){

    ret1 = data.frame( do.call( rbind, lapply( unique( dat.df$country_id )[ which( unique( dat.df$country_id ) %in% as.numeric( country_region_map_key.df$UN_Code ) ) ], function(cc){

      ret2 = do.call( rbind, lapply( c('urban','rural'), function(tt){


        # Use the gridded income data and logistics model to  estimate connection levels.
        # Assume convergence to the model along exponential path
        inc_o = max( 0, sum( dat.df[ which(  dat.df$country_id == cc ) , c(  paste( tt, '_gdp.', 2010, sep='' ) ) ], na.rm=TRUE ) / sum( dat.df[ which(  dat.df$country_id == cc ) , c(  paste( tt, '_pop.', 2010, sep='' ) ) ], na.rm=TRUE ) , na.rm=TRUE )
        inc_f = max( 0, sum( dat.df[ which(  dat.df$country_id == cc ) , c(  paste( tt, '_gdp.', yy, sep='' ) ) ], na.rm=TRUE ) / sum( dat.df[ which(  dat.df$country_id == cc ) , c(  paste( tt, '_pop.', yy, sep='' ) ) ], na.rm=TRUE  ) , na.rm=TRUE )
        ind = which( ww.df$country_id == as.character( country_region_map_key.df$Region[ which( as.numeric( country_region_map_key.df$UN_Code ) == cc ) ] ) )
        if( length( ind ) > 0 )
        {
          c0 = ww.df$connected.2010[ ind ] / 100
          t0 = ww.df$treated.2010[ ind ] / 100
        }else
        {
          if(inc_f > 0)
          {
            c0 = c( SSlogis(inc_o,cp.a,cp.m,cp.s) )
            t0 = c( SSlogis(inc_o,tp.a,tp.m,tp.s) )
          }else
          {
            c0 = 0
            t0 = 0
          }
        }
        #### ATTENTION HERE IT SEEMS IT WORK WIT 10 Y TIME STEPS
        decay = max( 0, 1 / length(yrs) * log( 1 / 0.01 ), na.rm=TRUE )
        if( yy > 2010 )
        {
          c_mod = c( SSlogis( inc_f, cp.a, cp.m, cp.s ) )
          cp = ( 1 +  ( c0 / c_mod - 1 ) * exp( -1 * decay * ( which(yrs == yy) - 1 ) ) ) * c_mod
          if( scn %in% c('sdg6') & yy >= 2030 & cp < 0.99 ){ cp = 0.99 }

          t_mod = c( SSlogis( inc_f, tp.a, tp.m, tp.s ) )
          tp = ( 1 +  ( t0 / t_mod - 1 ) * exp( -1 * decay * ( which(yrs == yy) - 1 ) ) ) * t_mod
          if( scn %in% c('sdg6') & yy >= 2030 & tp < 0.5 ){ tp = 0.5 }

        }else
        {
          cp = c0
          tp = t0
        }

        ret3  = data.frame( cp = cp, tp = tp )
        names(ret3) = paste(tt,names(ret3),sep= '..')

        return( as.matrix(c(cp,tp)) )

      } ) )

      ret2 = data.frame(t(ret2))
      names(ret2) = c( 'urban.cp', 'urban.tp', 'rural.cp', 'rural.tp' )
      row.names(ret2) = paste(country_region_map_key.df$Region[ which( as.numeric( country_region_map_key.df$UN_Code ) == cc ) ],yy,sep= '..')
      return(ret2)

    } ) ) )

    return(ret1)

  } ) )

  assign( paste( 'national_connection_rate', ch, sep='_' ), national_connection_rate.df )

  # Limit diffusion in low-income regions according to logistic model fit saturating at per capita income for Israel
  national_advtech_rate.df = do.call( cbind, lapply( yrs , function(yy){
    ret1 = data.frame( do.call( rbind, lapply( unique( dat.df$country_id ), function(cc){
      inc_f = max( 0, sum( dat.df[ which(  dat.df$country_id == cc ) , c(  paste( 'urban', '_gdp.', yy, sep='' ) ) ], na.rm=TRUE ) / sum( dat.df[ which(  dat.df$country_id == cc ) , c(  paste( 'urban', '_pop.', yy, sep='' ) ) ], na.rm=TRUE  ) , na.rm=TRUE )
      return( c( round( SSlogis( inc_f, 1, 15000, 1000 ), digits = 2 ) ) )
    } ) ) )
    names(ret1) = yy
    row.names(ret1) = sapply( unique( dat.df$country_id ), function(cc){ as.character( country_region_map_key.df$Region[ which( as.numeric( country_region_map_key.df$UN_Code ) == cc ) ] ) } )
    return(ret1)
  } ) )

  assign( paste( 'national_advtech_rate', ch, sep='_' ), national_advtech_rate.df )

  # Downscale
  dat.df = cbind( dat.df, do.call(cbind, lapply(yrs, function(yy){
    ret = data.frame( res = sapply( dat.df$country, function(cc){ national_connection_rate.df[paste( cc, yy, sep = '..'), 'urban.cp' ] } ) )
    ret$res[ which( !( unlist( dat.df[ , paste( 'urban_pop', yy, sep = '..') ] ) > 0 ) ) ] = 0
    names(ret) = paste( 'urban_connection_rate', yy, sep = '..')
    return(ret)
  } ) ) )
  dat.df = cbind( dat.df, do.call(cbind, lapply(yrs, function(yy){
    ret = data.frame( res = sapply( dat.df$country, function(cc){ national_connection_rate.df[paste( cc, yy, sep = '..'), 'urban.tp' ] } ) )
    ret$res[ which( !( unlist( dat.df[ , paste( 'urban_pop', yy, sep = '..') ] ) > 0 ) ) ] = 0
    names(ret) = paste( 'urban_treated_rate', yy, sep = '..')
    return(ret)
  } ) ) )
  dat.df = cbind( dat.df, do.call(cbind, lapply(yrs, function(yy){
    ret = data.frame( res = sapply( dat.df$country, function(cc){ national_connection_rate.df[paste( cc, yy, sep = '..'), 'rural.cp' ] } ) )
    ret$res[ which( !( unlist( dat.df[ , paste( 'rural_pop', yy, sep = '..') ] ) > 0 ) ) ] = 0
    names(ret) = paste( 'rural_connection_rate', yy, sep = '..')
    return(ret)
  } ) ) )
  dat.df = cbind( dat.df, do.call(cbind, lapply(yrs, function(yy){
    ret = data.frame( res = sapply( dat.df$country, function(cc){ national_connection_rate.df[paste( cc, yy, sep = '..'), 'rural.tp' ] } ) )
    ret$res[ which( !( unlist( dat.df[ , paste( 'rural_pop', yy, sep = '..') ] ) > 0 ) ) ] = 0
    names(ret) = paste( 'rural_treated_rate', yy, sep = '..')
    return(ret)
  } ) ) )

  dat.df[ is.na(dat.df) ] = 0

  # Some testing parameters ####
  # Pop connected and pop treated
  # tst = dat.df[,c('xloc','yloc','urban_connection_rate.2010','urban_treated_rate.2010','urban_pop.2010',
  #                 'rural_connection_rate.2010','rural_treated_rate.2010','rural_pop.2010','urban_connection_rate.2030'
  #                 ,'urban_treated_rate.2030','urban_pop.2030','rural_connection_rate.2030','rural_treated_rate.2030',
  #                 'rural_pop.2030')]
  # mxc = 0.9
  # tst$urban_connection_rate.2010[ tst$urban_connection_rate.2010 > mxc ] = 1
  # tst$urban_connection_rate.2030[ tst$urban_connection_rate.2030 > mxc ] = 1
  # tst$rural_connection_rate.2010[ tst$rural_connection_rate.2010 > mxc ] = 1
  # tst$rural_connection_rate.2030[ tst$rural_connection_rate.2030 > mxc ] = 1
  # tst$pop_connected.2010 =  tst$urban_connection_rate.2010 * tst$urban_pop.2010 + tst$rural_connection_rate.2010 * tst$rural_pop.2010
  # tst$pop_connected.2030 =  tst$urban_connection_rate.2030 * tst$urban_pop.2030 + tst$rural_connection_rate.2030 * tst$rural_pop.2030
  # tst$px = tst$pop_connected.2030 - tst$pop_connected.2010
  # tst$pop_treated.2010 =  tst$urban_treated_rate.2010 * tst$urban_pop.2010 + tst$rural_treated_rate.2010 * tst$rural_pop.2010
  # tst$pop_treated.2030 =  tst$urban_treated_rate.2030 * tst$urban_pop.2030 + tst$rural_treated_rate.2030 * tst$rural_pop.2030
  # tst$tx = tst$pop_treated.2030 - tst$pop_treated.2010
  #
  # # Some tracking parameters for plotting
  # coordinates(tst) = ~xloc+yloc
  # gridded(tst) = TRUE
  # a=raster(tst[,'pop_connected.2030'])-raster(tst[,'pop_connected.2010'])
  # a[is.na(a)]=0
  # a[a<=1000]=NA
  # b=raster(tst[,'pop_treated.2030'])-raster(tst[,'pop_treated.2010'])
  # b[is.na(b)]=0
  # b[b<=1000]=NA
  # assign( paste( 'a', scn, sep='_'), a )
  # assign( paste( 'b', scn, sep='_'), b )
  #
  # # # plot check
  # NNN = 10000
  #
  # tcnt = sapply( seq(2010,2090,by=10), function(yy){
  # ct = c( unlist( dat.df[ which( unlist( dat.df[ , paste( 'urban_pop', yy, sep = '.' ) ] ) > 0 ), paste( 'urban_connection_rate', yy, sep = '.' ) ] ),  unlist( dat.df[ which( unlist( dat.df[ , paste( 'rural_pop', yy, sep = '.' ) ] ) > 0 ), paste( 'rural_connection_rate', yy, sep = '.' ) ] ) )
  # pp = c( unlist( dat.df[ which( unlist( dat.df[ , paste( 'urban_pop', yy, sep = '.' ) ] ) > 0 ), paste( 'urban_pop', yy, sep = '.' ) ] ),  unlist( dat.df[ which( unlist( dat.df[ , paste( 'rural_pop', yy, sep = '.' ) ] ) > 0 ), paste( 'rural_pop', yy, sep = '.' ) ] ) )
  # return( sum( ct * pp , na.rm = TRUE ) / sum( pp, na.rm = TRUE )  )
  # } )
  # ttrt = sapply( seq(2010,2090,by=10), function(yy){
  # tt = c( unlist( dat.df[ which( unlist( dat.df[ , paste( 'urban_pop', yy, sep = '.' ) ] ) > 0 ), paste( 'urban_treated_rate', yy, sep = '.' ) ] ),  unlist( dat.df[ which( unlist( dat.df[ , paste( 'rural_pop', yy, sep = '.' ) ] ) > 0 ), paste( 'rural_treated_rate', yy, sep = '.' ) ] ) )
  # pp = c( unlist( dat.df[ which( unlist( dat.df[ , paste( 'urban_pop', yy, sep = '.' ) ] ) > 0 ), paste( 'urban_pop', yy, sep = '.' ) ] ),  unlist( dat.df[ which( unlist( dat.df[ , paste( 'rural_pop', yy, sep = '.' ) ] ) > 0 ), paste( 'rural_pop', yy, sep = '.' ) ] ) )
  # return( sum( tt * pp , na.rm = TRUE ) / sum( pp, na.rm = TRUE )  )
  # } )
  #
  # assign( paste( 'tcnt', ch, sep = '_' ), tcnt )
  # assign( paste( 'ttrt', ch, sep = '_' ), ttrt )
  #
  # cnt_2010 = c( unlist( dat.df[ which( unlist( dat.df[ , paste( 'urban_pop', 2010, sep = '.' ) ] ) > 0 ), paste( 'urban_connection_rate', 2010, sep = '.' ) ] ),  unlist( dat.df[ which( unlist( dat.df[ , paste( 'rural_pop', 2010, sep = '.' ) ] ) > 0 ), paste( 'rural_connection_rate', 2010, sep = '.' ) ] ) )
  # trt_2010 = c( unlist( dat.df[ which( unlist( dat.df[ , paste( 'urban_pop', 2010, sep = '.' ) ] ) > 0 ), paste( 'urban_treated_rate', 2010, sep = '.' ) ] ),  unlist( dat.df[ which( unlist( dat.df[ , paste( 'rural_pop', 2010, sep = '.' ) ] ) > 0 ), paste( 'rural_treated_rate', 2010, sep = '.' ) ] ) )
  # pop_2010 = c( unlist( dat.df[ which( unlist( dat.df[ , paste( 'urban_pop', 2010, sep = '.' ) ] ) > 0 ), paste( 'urban_pop', 2010, sep = '.' ) ] ),  unlist( dat.df[ which( unlist( dat.df[ , paste( 'rural_pop', 2010, sep = '.' ) ] ) > 0 ), paste( 'rural_pop', 2010, sep = '.' ) ] ) )
  #
  # cnt_2030 = c( unlist( dat.df[ which( unlist( dat.df[ , paste( 'urban_pop', 2030, sep = '.' ) ] ) > 0 ), paste( 'urban_connection_rate', 2030, sep = '.' ) ] ),  unlist( dat.df[ which( unlist( dat.df[ , paste( 'rural_pop', 2030, sep = '.' ) ] ) > 0 ), paste( 'rural_connection_rate', 2030, sep = '.' ) ] ) )
  # trt_2030 = c( unlist( dat.df[ which( unlist( dat.df[ , paste( 'urban_pop', 2030, sep = '.' ) ] ) > 0 ), paste( 'urban_treated_rate', 2030, sep = '.' ) ] ),  unlist( dat.df[ which( unlist( dat.df[ , paste( 'rural_pop', 2030, sep = '.' ) ] ) > 0 ), paste( 'rural_treated_rate', 2030, sep = '.' ) ] ) )
  # pop_2030 = c( unlist( dat.df[ which( unlist( dat.df[ , paste( 'urban_pop', 2030, sep = '.' ) ] ) > 0 ), paste( 'urban_pop', 2030, sep = '.' ) ] ),  unlist( dat.df[ which( unlist( dat.df[ , paste( 'rural_pop', 2030, sep = '.' ) ] ) > 0 ), paste( 'rural_pop', 2030, sep = '.' ) ] ) )
  #
  # require(weights)
  # tmp = wtd.hist( cnt_2010, NNN, weight = pop_2010, plot = FALSE )
  # assign( paste( 'd10_cnt', ch, sep = '_' ), data.frame( x = tmp$mids, y = cumsum( tmp$count ) / sum( tmp$count ) ) )
  # tmp = wtd.hist( cnt_2030, NNN, weight = pop_2030, plot = FALSE )
  # assign( paste( 'd30_cnt', ch, sep = '_' ), data.frame( x = tmp$mids, y = cumsum( tmp$count ) / sum( tmp$count ) ) )
  # tmp = wtd.hist( trt_2010, NNN, weight = pop_2010, plot = FALSE )
  # assign( paste( 'd10_trt', ch, sep = '_' ), data.frame( x = tmp$mids, y = cumsum( tmp$count ) / sum( tmp$count ) ) )
  # tmp = wtd.hist( trt_2030, NNN, weight = pop_2030, plot = FALSE )
  # assign( paste( 'd30_trt', ch, sep = '_' ), data.frame( x = tmp$mids, y = cumsum( tmp$count ) / sum( tmp$count ) ) )

  # TEMPORARILY COMMENTED, need to find another way to add WSI into the adaptation capacity ####
  # Desalination and wastewater recycling rates
  # national_advtech_rate.df$country_id = sapply( row.names(national_advtech_rate.df), function(cc){ return( unlist( country_region_map_key.df$UN_Code[ which( as.character( country_region_map_key.df$Region ) == cc ) ] ) ) } )
  # conv1 = 1/( which(yrs == 2070) )
  # conv2 = 1/( which(yrs == 2030) )
  # max_recycle = 0.8
  # dat.df = cbind( dat.df, do.call(cbind, lapply(yrs, function(yy){
  #   if( scn %in% c('sdg6_supp','sdg6_eff') & yy > 2010 ){ conv = min(1,( which(yrs==yy)*conv2 ) ) }else{ conv = min(1,( which(yrs==yy)*conv1 ) ) }
  #   av = rep( 0, nrow(dat.df) )
  #   for( cc in unique(dat.df$country_id) ){ av[which(dat.df$country_id == cc)] = national_advtech_rate.df[ which( national_advtech_rate.df$country_id == cc  ) , as.character( yy ) ]  }
  #   #ws = rep( 0, nrow(dat.df) )
  #   #ws[ which( dat.df$ID == 1 & unlist( dat.df[,paste('urban_treated_rate',yy,sep='.')] ) > 0  ) ] = 1
  #   ws = as.numeric(dat.df$ID)
  #   res = av * ws
  #   res[res>conv]=conv
  #   res[res>max_recycle] = max_recycle
  #   ret = data.frame(res = res)
  #   row.names(ret) = row.names(dat.df)
  #   names(ret) = paste( 'recycling_rate', yy, sep = '.' )
  #   return(ret)
  # } ) ) )
  # dat.df = cbind( dat.df, do.call(cbind, lapply(yrs, function(yy){
  #   if( scn %in% c('sdg6_supp','sdg6_eff') & yy > 2010 ){ conv = min(1,( which(yrs==yy)*conv2 ) ) }else{ conv = min(1,( which(yrs==yy)*conv1 ) ) }
  #   av = rep( 0, nrow(dat.df) )
  #   for( cc in unique(dat.df$country_id) ){ av[which(dat.df$country_id == cc)] = national_advtech_rate.df[ which( national_advtech_rate.df$country_id == cc  ) , as.character( yy ) ]  }
  #   ws = rep( 0, nrow(dat.df) )
  #   ws[ which( dat.df$distance2coast <= 1.5 ) ] = 1
  #   ws = ws * as.numeric(dat.df$ID)
  #   res = av * ws
  #   res[res>conv]=conv
  #   ret = data.frame(res = res)
  #   row.names(ret) = row.names(dat.df)
  #   names(ret) = paste( 'desalination_rate', yy, sep = '.' )
  #   return(ret)
  # } ) ) )
  # dat.df = cbind( dat.df, do.call(cbind, lapply(yrs, function(yy){
  #   ur = unlist( dat.df[,paste('urban_return',yy,sep='.')] )
  #   ur[ is.na(ur) ] = 0
  #   mr = unlist( dat.df[,paste('mf_return',yy,sep='.')] )
  #   mr[ is.na(mr) ] = 0
  #   ret = data.frame( res = ( unlist( dat.df[,paste('recycling_rate',yy,sep='.')] ) *
  #                               unlist( dat.df[,paste('urban_treated_rate',yy,sep='.')] ) * ( ur + mr ) ) )
  #   row.names(ret) = row.names(dat.df)
  #   names(ret) = paste( 'recycled', yy, sep = '.' )
  #   return(ret)
  # } ) ) )
  # #desalinated water: desalination rate* (
  # # urban connection rate (urban wd + manufactirung wd)-
  # # (0.8 * recycling rate + urban treated rate * (uw +mw) ) )
  # dat.df = cbind( dat.df, do.call(cbind, lapply(yrs, function(yy){
  #   uw = unlist( dat.df[,paste('urban_withdrawal',yy,sep='.')] )
  #   uw[ is.na(uw) ] = 0
  #   mw = unlist( dat.df[,paste('mf_withdrawal',yy,sep='.')] )
  #   mw[ is.na(mw) ] = 0
  #   ur = unlist( dat.df[,paste('urban_return',yy,sep='.')] )
  #   ur[ is.na(ur) ] = 0
  #   mr = unlist( dat.df[,paste('mf_return',yy,sep='.')] )
  #   mr[ is.na(mr) ] = 0
  #   ret = data.frame( res = unlist( dat.df[ , paste('desalination_rate',yy,sep='.') ] ) *
  #                       (  unlist( dat.df[,paste('urban_connection_rate',yy,sep='.')] ) * ( uw + mw ) -
  #                            0.8 * ( unlist( dat.df[,paste('recycling_rate',yy,sep='.')] ) *
  #                                      unlist( dat.df[,paste('urban_treated_rate',yy,sep='.')] ) * ( ur + mr ) ) )  )
  #   row.names(ret) = row.names(dat.df)
  #   names(ret) = paste( 'desalinated', yy, sep = '.' )
  #   return(ret)
  # } ) ) )
  #
  # library(rasterVis)
  # a = dat.df[,c('xloc','yloc','recycled.2030')]
  # coordinates(a) = ~ xloc + yloc
  # gridded(a) = TRUE
  # f = raster(a)
  # f[f[]==0]=NA
  # g = dat.df[,c('xloc','yloc','desalinated.2030')]
  # coordinates(g) = ~ xloc + yloc
  # gridded(g)=TRUE
  # g=raster(g)
  # g[g[]==0]=NA
  # # pdf(paste('C:/Users/parkinso/Documents/desalination_recycling_2030_',ch,'.pdf',sep=''))
  # # print( levelplot(stack(f,g),zscaleLog=TRUE,margin=FALSE,names.attr=c('Recycling [ million cubic meters ]','Desalination  [ million cubic meters ]'))+layer_(sp.polygons(coast,col=alpha('grey31',0.3))) )
  # # dev.off()
  # assign( paste( 'advtch', ch, sep='_' ), stack(f,g) )
  # END COMMENTED PART ON ADAPTATION
#### format OUTPUT ####
  ysr_full = c(yrs)
  #including manufacturing
  regional_urban_withdrawal.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum(unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_withdrawal', yy, sep= '..') ) ] ) , na.rm=TRUE ) + sum(unlist(dat.df[which(dat.df$region == rr ) , c(paste('mf_withdrawal', yy, sep= '..') ) ] ) , na.rm=TRUE ) } ) } ) ) )
  names(regional_urban_withdrawal.df) = unique( dat.df$region )
  row.names(regional_urban_withdrawal.df) = ysr_full
  regional_urban_withdrawal.df = round( regional_urban_withdrawal.df, digits = 3 )
  var_list = list('regional_urban_withdrawal' = regional_urban_withdrawal.df)

  # urban return flows
  regional_urban_return.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum(unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_return', yy, sep= '..') ) ] ) , na.rm=TRUE ) + sum(unlist(dat.df[which(dat.df$region == rr ) , c(paste('mf_return', yy, sep= '..') ) ] ) , na.rm=TRUE ) } ) } ) ) )
  names(regional_urban_return.df) = unique( dat.df$region )
  row.names(regional_urban_return.df) = ysr_full
  regional_urban_return.df = round( regional_urban_return.df, digits = 3 )
  var_list = append(var_list,list('regional_urban_return' = regional_urban_return.df) )

  # without manufacturing
  regional_urban_withdrawal2.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum(unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_withdrawal', yy, sep= '..') ) ] ) , na.rm=TRUE ) } ) } ) ) )
  names(regional_urban_withdrawal2.df) = unique( dat.df$region )
  row.names(regional_urban_withdrawal2.df) = ysr_full
  regional_urban_withdrawal2.df = round( regional_urban_withdrawal2.df, digits = 3 )
  var_list = append(var_list,list('regional_urban_withdrawal2' = regional_urban_withdrawal2.df))

  regional_urban_return2.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum(unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_return', yy, sep= '..') ) ] ) , na.rm=TRUE ) } ) } ) ) )
  names(regional_urban_return2.df) = unique( dat.df$region )
  row.names(regional_urban_return2.df) = ysr_full
  regional_urban_return2.df = round( regional_urban_return2.df, digits = 3 )
  var_list = append(var_list,list('regional_urban_return2' = regional_urban_return2.df))

  # manufacturing withdrawals
  regional_manufacturing_withdrawal.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum(unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'mf_withdrawal', yy, sep= '..') ) ] ) , na.rm=TRUE ) } ) } ) ) )
  names(regional_manufacturing_withdrawal.df) = unique( dat.df$region )
  row.names(regional_manufacturing_withdrawal.df) = ysr_full
  regional_manufacturing_withdrawal.df = round( regional_manufacturing_withdrawal.df, digits = 3 )
  var_list = append(var_list,list('regional_manufacturing_withdrawal' = regional_manufacturing_withdrawal.df))

  # manufacturing return flows
  regional_manufacturing_return.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){ sum(unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'mf_return', yy, sep= '..') ) ] ) , na.rm=TRUE ) } ) } ) ) )
  names(regional_manufacturing_return.df) = unique( dat.df$region )
  row.names(regional_manufacturing_return.df) = ysr_full
  regional_manufacturing_return.df = round( regional_manufacturing_return.df, digits = 3 )
  var_list = append(var_list,list('regional_manufacturing_return' = regional_manufacturing_return.df))

  # urban connection rate
  regional_urban_connection_rate.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){ sum(c( unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_pop', yy, sep= '..') ) ] )  * unlist(dat.df[which(dat.df$region == rr ) , c(paste('urban_connection_rate', yy, sep= '..') ) ] ) ) , na.rm=TRUE ) / sum(c(unlist(dat.df[which(dat.df$region == rr ) , c(paste('urban_pop', yy, sep= '..') ) ] ) ) , na.rm = TRUE ) } ) } ) ) )
  names(regional_urban_connection_rate.df) = unique( dat.df$region )
  row.names(regional_urban_connection_rate.df) = ysr_full
  regional_urban_connection_rate.df = round( regional_urban_connection_rate.df, digits = 3 )
  var_list = append(var_list,list('regional_urban_connection_rate' = regional_urban_connection_rate.df))

  # COMMENTED FOR NOW
  # urban treatment rate
  # regional_urban_treatment_rate.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum( c( unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_pop', yy, sep='.' ) ) ] )  * unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_treated_rate', yy, sep='.' ) ) ] ) ) , na.rm=TRUE ) / sum( c( unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_pop', yy, sep='.' ) ) ] ) ) , na.rm = TRUE ) } ) } ) ) )
  # names(regional_urban_treatment_rate.df ) = unique( dat.df$region )
  # row.names(regional_urban_treatment_rate.df) = ysr_full
  # regional_urban_treatment_rate.df = round( regional_urban_treatment_rate.df, digits = 3 )
  # var_list = append(var_list,list('regional_urban_treatment_rate' = regional_urban_treatment_rate.df))
  #
  # regional_urban_desalination_rate.df =  data.frame( round(  do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){ sum( unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'desalinated', yy, sep='.' ) ) ] ) , na.rm=TRUE ) / sum( unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_withdrawal', yy, sep='.' ) ) ] ) + unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'mf_withdrawal', yy, sep='.' ) ) ] ) , na.rm=TRUE ) } ) } ) ), digits = 3 ) )
  # names(regional_urban_desalination_rate.df) = unique( dat.df$region )
  # row.names(regional_urban_desalination_rate.df) = ysr_full
  # regional_urban_desalination_rate.df = round( regional_urban_desalination_rate.df, digits = 3 )
  # regional_urban_desalination_rate.df[ regional_urban_desalination_rate.df < 0.005 ] = 0.005
  # var_list = append(var_list,list('regional_urban_desalination_rate' = regional_urban_desalination_rate.df))
  # # END COMMENTED PART

  # urban recycling rate
  regional_urban_recycling_rate.df = data.frame( round(do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){ sum(unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'recycled', yy, sep= '..') ) ] ) , na.rm=TRUE ) / sum(unlist(dat.df[which(dat.df$region == rr ) , c(paste('urban_withdrawal', yy, sep= '..') ) ] ) + unlist(dat.df[which(dat.df$region == rr ) , c(paste('mf_withdrawal', yy, sep= '..') ) ] ) , na.rm=TRUE ) } ) } ) ), digits = 3 ) )
  names(regional_urban_recycling_rate.df) = unique( dat.df$region )
  row.names(regional_urban_recycling_rate.df) = ysr_full
  regional_urban_recycling_rate.df = round( regional_urban_recycling_rate.df, digits = 3 )
  regional_urban_recycling_rate.df[ regional_urban_recycling_rate.df < 0.005 ] = 0.005
  var_list = append(var_list,list( 'regional_urban_recycling_rate' = regional_urban_recycling_rate.df))

  # rural connection rate
  regional_rural_connection_rate.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum(c( unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'rural_pop', yy, sep= '..') ) ] )  * unlist(dat.df[which(dat.df$region == rr ) , c(paste('rural_connection_rate', yy, sep= '..') ) ] ) ) , na.rm=TRUE ) / sum(c(unlist(dat.df[which(dat.df$region == rr ) , c(paste('rural_pop', yy, sep= '..') ) ] ) ) , na.rm = TRUE ) } ) } ) ) )
  names(regional_rural_connection_rate.df) = unique( dat.df$region )
  row.names(regional_rural_connection_rate.df) = ysr_full
  regional_rural_connection_rate.df = round( regional_rural_connection_rate.df, digits = 3 )
  var_list = append(var_list,list('regional_rural_connection_rate' = regional_rural_connection_rate.df))

  # rural treatment rates
  regional_rural_treatment_rate.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum(c( unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'rural_pop', yy, sep= '..') ) ] )  * unlist(dat.df[which(dat.df$region == rr ) , c(paste('rural_treated_rate', yy, sep= '..') ) ] ) ) , na.rm=TRUE ) / sum(c(unlist(dat.df[which(dat.df$region == rr ) , c(paste('rural_pop', yy, sep= '..') ) ] ) ) , na.rm = TRUE ) } ) } ) ) )
  names(regional_rural_treatment_rate.df) = unique( dat.df$region )
  row.names(regional_rural_treatment_rate.df) = ysr_full
  regional_rural_treatment_rate.df = round( regional_rural_treatment_rate.df, digits = 3 )
  var_list = append(var_list,list('regional_rural_treatment_rate' = regional_rural_treatment_rate.df))

  # rural withdrawals
  regional_rural_withdrawal.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum(unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'rural_withdrawal', yy, sep= '..') ) ] ) , na.rm=TRUE ) } ) } ) ) )
  names(regional_rural_withdrawal.df) = unique( dat.df$region )
  row.names(regional_rural_withdrawal.df) = ysr_full
  regional_rural_withdrawal.df = round( regional_rural_withdrawal.df, digits = 3 )
  var_list = append(var_list,list('regional_rural_withdrawal' = regional_rural_withdrawal.df))

  # return flows, only SDG6
  regional_rural_return.df = data.frame(
    do.call( cbind, lapply( unique( dat.df$region ), function(rr){
      sapply( ysr_full, function(yy){
        sum(unlist( dat.df[ which( dat.df$region == rr ) ,
                             c( paste( 'rural_return', yy, sep= '..') ) ] ) , na.rm=TRUE ) } ) } ) ) )
  names(regional_rural_return.df) = unique( dat.df$region )
  row.names(regional_rural_return.df) = ysr_full
  regional_rural_return.df = round( regional_rural_return.df, digits = 3 )
  var_list = append(var_list,list('regional_rural_return' = regional_rural_return.df))

  # ADDING FINAL TIME STEPS AND SAVE csv #
  for (v in names(var_list)){
    df = as.data.frame(var_list[v][[1]])

    for_lm = df %>%
      mutate(year = row.names(.)) %>%
      select(year,everything()) %>%
      filter(year >= 2060) %>%
      gather(key = 'region',value = 'value',2:12) %>%
      group_by(region) %>%
      mutate(int = lm(value ~ year)$coefficients[1],
             coef = lm(value ~ year)$coefficients[2]) %>%
      ungroup()
    # add 2100, 2110
    new_vals = for_lm %>% select(-year,-value) %>%
      distinct() %>%
      mutate(`2100` := int + (2100-2060)/10 * coef,
             `2110` := int + (2110-2060)/10 * coef) %>%
      gather(key = year, value = 'value',`2100`,`2110`)
    # add back to df
    final_df = df %>% bind_rows(
      new_vals %>% select(year,region,value) %>%
        spread(region,value) %>%
        tibble::column_to_rownames(var = 'year')
    )

    write.csv( final_df,
               paste0( data_path, '/water_demands/harmonized/ssp',ssp,'_',
                      v,'_', ch, '.csv' ), row.names = TRUE )

  }

}

# NEED to adjust list and remove write pdf in previous part


# Plot - compare SDG pathways

# px = a_1 - a_2
# tx = b_1 - b_2
# px[px<5]=NA
# tx[tx<5]=NA

# r = raster()
# res(r) = 0.05
# r = crop(r,extent(tx))

# # pxb = resample(px,r,method='bilinear')
# # txb = resample(tx,r,method='bilinear')

# require(rasterVis)
# myTheme =YlOrRdTheme
# # myTheme$regions$col = rev(myTheme$regions$col)
# # myTheme$panel.background$col = 'black'

# xxxx = stack(px,tx)

# xxxx[xxxx>0&xxxx<10] = 10

# gcoast = gSimplify( coast, tol = 0.2 )
# windows()
# print(levelplot(xxxx,zscaleLog=TRUE,par.settings=myTheme,margin=FALSE,colorkey=list(space="bottom"),names.attr=c("Piped Freshwater & Wastewater Collection","Wastewater Treatment")) + layer_(sp.polygons(gcoast,col=alpha('grey31',0.3))))


# temp = readOGR('P:/ene.general/Water/global_basin_modeling/basin_delineation','subbasins_by_country',verbose=FALSE)
# basin_test = "Zambezi"
# basin.sp = gUnaryUnion(  spTransform(temp[which(as.character(temp$BASIN) == basin_test),],CRS("+proj=longlat")) )
# basin.sp = do.call(rbind, lapply( 1:length(basin.sp), function(xxx){
# tmp = SpatialPolygons(list(Polygons(Filter(function(f){f@ringDir==1},basin.sp@polygons[[xxx]]@Polygons),ID=1)))
# row.names(tmp) =  row.names(basin.sp)[xxx]
# return(tmp)
# } ) )
# zambezi.sp = basin.sp
# basin_test = "Indus"
# basin.sp = gUnaryUnion(  spTransform(temp[which(as.character(temp$BASIN) == basin_test),],CRS("+proj=longlat")) )
# basin.sp = do.call(rbind, lapply( 1:length(basin.sp), function(xxx){
# tmp = SpatialPolygons(list(Polygons(Filter(function(f){f@ringDir==1},basin.sp@polygons[[xxx]]@Polygons),ID=1)))
# row.names(tmp) =  row.names(basin.sp)[xxx]
# return(tmp)
# } ) )
# indus.sp = basin.sp
# rm(temp)
# gc()


# ind = crop( a_1, extent( indus.sp ), snap="out" ) * rasterize( indus.sp, crop( a_1, extent( indus.sp ), snap="out"	) )
# zam = crop( a_1, extent( zambezi.sp ), snap="out" ) * rasterize( zambezi.sp, crop( a_1, extent( zambezi.sp ), snap="out"	) )

# By country
# countries.spdf = readOGR('P:/ene.general/Water/global_basin_modeling/basin_delineation/data/country_delineation/gadm/output_data', 'gadm_country_boundaries', verbose=FALSE)
# countries.df = data.frame(countries.spdf)
# countries.sp = as(countries.spdf,'SpatialPolygons')
# basins.spdf = all_bas
# basins.sp = as(basins.spdf,'SpatialPolygons')
# pxc = sapply( 1:length(countries.spdf), function(f){ print(f); return( sum( unlist( extract( crop( px, countries.sp[f] ), countries.sp[f] ) ), na.rm = TRUE ) ) } )
# txc = sapply( 1:length(countries.spdf), function(f){ sum( unlist( extract( crop( tx, countries.sp[f] ), countries.sp[f] ) ), na.rm = TRUE ) } )
# pxr = sapply( 1:length(basins.spdf), function(f){ print(f); return( sum( unlist( extract( crop( px, basins.sp[f] ), basins.sp[f] ) ), na.rm = TRUE ) ) } )
# txr = sapply( 1:length(basins.spdf), function(f){ print(f); return( sum( unlist( extract( crop( tx, basins.sp[f] ), basins.sp[f] ) ), na.rm = TRUE ) ) } )

# pxr2 = pxr[ which( as.character( basins.spdf@data$wtr_stress ) %in% c( 'Stress','High stress','Low stress') ) ]
# bpxr2 = as.character( basins.spdf@data$PID )[ which(as.character( basins.spdf@data$wtr_stress ) %in% c( 'Stress','High stress','Low stress') ) ]
# npxr2 = as.character( basins.spdf@data$wtr_stress )[ which(as.character( basins.spdf@data$wtr_stress ) %in% c( 'Stress','High stress','Low stress') ) ]
# cpxr2 = sapply( npxr2, function(nn){ if(nn=='Low stress'){return(c('mediumseagreen'))}else if(nn=='Stress'){return(c('orange'))}else{return(c('darkred'))}} )
# tdf = data.frame( pxr2 = pxr2, bpxr2 = bpxr2, npxr2 = npxr2, cpxr2 = cpxr2 )
# tdf = tdf[ order( tdf[,1], decreasing = TRUE ), ]
# tdft = tdf[ 1:20, ]

# pxc2 = pxc
# bpxc2 = as.character( countries.spdf@data$NAME )
# cpxc2 = 'tan'
# cdf = data.frame( pxc2 = pxc2, bpxc2 = bpxc2, cpxc2 = cpxc2 )
# cdf = cdf[ order( cdf[,1], decreasing = TRUE ), ]

# windows()
# p1 = layout(matrix(c(1,2,3,3),2,2,byrow=TRUE),widths=c(0.45,0.45),heights=c(0.9,0.1))
# par(mar=c(2,8,1,1), oma = c(2,2,2,2))
# barplot(cdft$pxc2,col=as.character(cdft$cpxc2),names.arg=cdft$bpxc2,horiz=TRUE,log='x',xlim=c(1e6,1e9),las=1,cex.names=0.8,cex.axis=0.8,xaxt='n',cex.main=0.8,main='Country')
# axis(side=1,at=c(1e6,1e7,1e8,1e9),labels=c('1e6','1e7','1e8','1e9'))
# abline(v=1e6)
# barplot(tdft$pxr2,col=as.character(tdft$cpxr2),names.arg=tdft$bpxr2,horiz=TRUE,log='x',xlim=c(1e6,1e9),las=1,cex.names=0.8,cex.axis=0.8,xaxt='n',cex.main=0.8,main='Water-stressed Eco-region')
# axis(side=1,at=c(1e6,1e7,1e8,1e9),labels=c('1e6','1e7','1e8','1e9'))
# legend('topright',legend=c('Low\nStress','Medium\nStress','High\nStress'),fill=c('mediumseagreen','orange','darkred'),y.intersp = 1.7,cex=0.8,bty='n')
# abline(v=1e6)
# par(mar=c(0,8,0,2))
# plot.new()
# text(0.5,0.5,c('Difference in number of people with improved water access by 2030\nSDG6 relative to Baseline'),cex=0.9)

# Compare results
# vars = c( 'regional_urban_withdrawal', 'regional_urban_return', 'regional_urban_connection_rate', 'regional_urban_treatment_rate', 'regional_urban_desalination_rate', 'regional_urban_recycling_rate', 'regional_rural_withdrawal', 'regional_rural_return', 'regional_rural_connection_rate', 'regional_rural_treatment_rate', 'regional_irrigation_withdrawal' )
# vnms = data.frame( 	regional_urban_withdrawal = 'Urban Withdrawal [ million cubic meters ]',
# regional_urban_return = 'Urban Return Flow [ million cubic meters ]',
# regional_urban_connection_rate = 'Urban Connection Rate',
# regional_urban_treatment_rate = 'Urban Treatment Rate',
# regional_urban_desalination_rate = 'Urban Desalination Rate',
# regional_urban_recycling_rate = 'Urban Recycling Rate',
# regional_rural_withdrawal = 'Rural Withdrawal [ million cubic meters ]',
# regional_rural_return = 'Rural Return [ million cubic meters ]',
# regional_rural_connection_rate = 'Rural Connection Rate',
# regional_rural_treatment_rate = 'Rural Treatment Rate',
# regional_irrigation_withdrawal = 'Irrigation Withdrawal [ million cubic meters ]')

# ylms = data.frame( 	regional_urban_withdrawal = 250000,
# regional_urban_return = 250000,
# regional_urban_connection_rate = 1,
# regional_urban_treatment_rate = 1,
# regional_urban_desalination_rate = 0.4,
# regional_urban_recycling_rate = 0.6,
# regional_rural_withdrawal = 50000,
# regional_rural_return = 50000,
# regional_rural_connection_rate = 1,
# regional_rural_treatment_rate = 1,
# regional_irrigation_withdrawal = 1e6 )


# cols = data.frame( 	SAS = 'red',
# AFR = 'orange',
# EEU = 'tan',
# MEA = 'gold4',
# LAM = 'blue',
# FSU = 'cyan',
# PAO = 'purple',
# WEU = 'pink',
# PAS = 'black',
# NAM = 'mediumseagreen',
# CPA = 'gray71' )

# res = lapply( vars, function(vv){

# mb = data.frame( read.csv( paste( 'P:/ene.model/data/Water/water_demands/harmonized/ssp',ssp,'_',vv, 'baseline', '.csv', sep = '' ) ) )
# row.names(mb) = mb$X
# mb = mb[,2:ncol(mb)]
# ms =  data.frame( read.csv( paste( 'P:/ene.model/data/Water/water_demands/harmonized/ssp',ssp,'_',vv, 'sdg6_supp', '.csv', sep = '' ) ) )
# row.names(ms) = ms$X
# ms = ms[,2:ncol(ms)]
# ms2 =  data.frame( read.csv( paste( 'P:/ene.model/data/Water/water_demands/harmonized/ssp',ssp,'_',vv, 'sdg6_eff', '.csv', sep = '' ) ) )
# row.names(ms2) = ms2$X
# ms2 = ms2[,2:ncol(ms2)]

# print(vv)

# pdf(paste( 'P:/ene.model/data/Water/water_demands/harmonized/ssp',ssp,'_',vv, '.PDF', sep = '' ),width=10,height=7)
# p1 = layout(matrix(c(1,2,3,4),1,4,byrow=TRUE),widths=c(0.25,0.25,0.25,0.2),heights=c(0.9))
# par(mar=c(5,4,5,0), oma = c(8,2,8,0))
# #matplot( spline(yrs,unlist(mb[,1]))$x, as.matrix( do.call( cbind, lapply( 1:ncol(mb), function(x){ spline(yrs,unlist(mb[,x]))$y } ) ) ), type= 'l', ylab = vnms[vv], xlab = 'Year' , main = 'Baseline' )
# #matplot( spline(yrs,unlist(ms[,1]))$x, as.matrix( do.call( cbind, lapply( 1:ncol(mb), function(x){ spline(yrs,unlist(ms[,x]))$y } ) ) ), type= 'l', ylab = vnms[vv], xlab = 'Year' , main = 'SDG6' )
# matplot( yrs, mb[ as.character( yrs ), ], type= 'l', ylab = as.character( unlist( vnms[vv] )), ylim = c( 0, as.numeric( unlist( ylms[vv] )) ), xlab = 'Year' , main = 'Baseline', lty = 1, col = as.character( unlist( cols[c(unique( names(mb) )) ] ) ) )
# matplot( yrs, ms[ as.character( yrs ), ], type= 'l', ylab = '', ylim = c( 0, as.numeric( unlist( ylms[vv] )) ), xlab = 'Year' , main = 'SDG6-Supply', lty = 1, col = as.character( unlist( cols[c(unique( names(mb) )) ] ) ) )
# matplot( yrs, ms2[ as.character( yrs ), ], type= 'l', ylab = '', ylim = c( 0, as.numeric( unlist( ylms[vv] )) ), xlab = 'Year' , main = 'SDG6-Efficiency', lty = 1, col = as.character( unlist( cols[c(unique( names(mb) )) ] ) ) )
# par(mar=c(0,2,0,0))
# plot.new()
# legend( 'left', legend = c(unique( names(mb) )), lty = c(1), col = as.character( unlist( cols[c(unique( names(mb) )) ] ) ), bty = 'n', cex = 1.1,  seg.len=5, y.intersp=1.6 )
# dev.off()

# } )

# gcoast = gSimplify( coast, tol = 0.2 )
# # Gridded desal and recycling
# pdf(paste('C:/Users/parkinso/Documents/desalination_recycling_2030.pdf',sep=''))
# print( levelplot(stack(advtch__base,advtch__sdgs),zscaleLog=TRUE,margin=FALSE,names.attr=c('Recycling - Baseline','Desalination - Baseline', 'Recycling - SDG6','Desalination - SDG6'))+layer_(sp.polygons(gcoast,col=alpha('grey31',0.3))) )
# dev.off()

# diffs = stack(advtch__sdgs - advtch__base)
# diffs_up = diffs
# diffs_up[[1]][ diffs_up[[1]][] <= 0 ] = NA
# diffs_up[[2]][ diffs_up[[2]][] <= 0 ] = NA

# diffs_down = diffs
# diffs_down[[1]][ diffs_down[[1]][] >= 0 ] = NA
# diffs_down[[2]][ diffs_down[[2]][] >= 0 ] = NA
# diffs_down = abs(diffs_down)

# pdf(paste('C:/Users/parkinso/Documents/downdiff_desalination_recycling_2030.pdf',sep=''))
# print( levelplot(diffs_down,zscaleLog=TRUE,margin=FALSE,names.attr=c('Recycling','Desalination'))+layer_(sp.polygons(gcoast,col=alpha('grey31',0.3))) )
# dev.off()
# pdf(paste('C:/Users/parkinso/Documents/updiff_desalination_recycling_2030.pdf',sep=''))
# print( levelplot(diffs_up,zscaleLog=TRUE,margin=FALSE,names.attr=c('Recycling','Desalination'))+layer_(sp.polygons(gcoast,col=alpha('grey31',0.3))) )
# dev.off()


# windows()
# plot( get( paste( 'd10_cnt', '_base', sep = '_' ) )$x, get( paste( 'd10_cnt', '_base', sep = '_' ) )$y, type = 'l', col = 'black' )
# lines( get( paste( 'd30_cnt', '_base', sep = '_' ) )$x, get( paste( 'd30_cnt', '_base', sep = '_' ) )$y, col = 'brown' )
# lines( get( paste( 'd30_cnt', '_sdgs', sep = '_' ) )$x, get( paste( 'd30_cnt', '_sdgs', sep = '_' ) )$y, col = 'green' )

# windows()
# plot( get( paste( 'd10_trt', '_base', sep = '_' ) )$x, get( paste( 'd10_trt', '_base', sep = '_' ) )$y, type = 'l', col = 'black' )
# lines( get( paste( 'd30_trt', '_base', sep = '_' ) )$x, get( paste( 'd30_trt', '_base', sep = '_' ) )$y, col = 'brown' )
# lines( get( paste( 'd30_trt', '_sdgs', sep = '_' ) )$x, get( paste( 'd30_trt', '_sdgs', sep = '_' ) )$y, col = 'green' )

# SDG achievement plots
# pc = data.frame( base = c(tcnt__base[1],tcnt__base[3],tcnt__base[5],tcnt__base[[length(tcnt__base)]]),sdg = c(tcnt__sdgs[1],tcnt__sdgs[3],tcnt__sdgs[5],tcnt__sdgs[[length(tcnt__sdgs)]]) )
# wc = data.frame( base = c(ttrt__base[1],ttrt__base[3],ttrt__base[5],ttrt__base[[length(ttrt__base)]]),sdg = c(ttrt__sdgs[1],ttrt__sdgs[3],ttrt__sdgs[5],ttrt__sdgs[[length(ttrt__sdgs)]]) )
# row.names(pc) = row.names(wc) = c(2010,2030,2050,2090)

# # Get emissions

# # launch the IX modeling platform using the default central ORCALE database
# source(file.path(Sys.getenv('IXMP_R_PATH'),'ixmp.r'))
# ix_platform = ixPlatform()
# wkd_message_ix = 'C:/Users/parkinso/git/' # !!!!! Set based on local machine
# # Import urban and rural demands from csv
# ssss='SSP2'
# pth = 'sdgs'
# urban_withdrawal.df = 1e-3 * data.frame( read.csv( paste( 'P:/ene.model/data/Water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_urban_withdrawal_', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
# rural_withdrawal.df = 1e-3 * data.frame( read.csv( paste( 'P:/ene.model/data/Water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_rural_withdrawal_', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
# urban_reuse_fraction.df = data.frame( read.csv( paste( 'P:/ene.model/data/Water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_urban_recycling_rate_', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
# glb_scen = paste(ssss,'-Ref-SPA0',sep='' )
# glb_irr.df = data.frame( read.csv( 'P:/ene.model/data/Water/water_demands/cdlinks_globiom_irrigation.csv', stringsAsFactors = FALSE ) )
# irrigation_withdrawal.df = 1e-3 * data.frame( do.call(cbind, lapply( names( urban_reuse_fraction.df ), function(rr){ sapply( c(row.names( urban_reuse_fraction.df ),'2100'), function(yy){ return( glb_irr.df[ which( glb_irr.df$Scenario == glb_scen & glb_irr.df$Region == rr ), paste( 'X', yy, sep = '' ) ] ) } ) } ) ) )
# row.names(irrigation_withdrawal.df) = c(row.names( urban_reuse_fraction.df ),'2100')
# names(irrigation_withdrawal.df) = names( urban_reuse_fraction.df )

# # Import and harmonize historical municipal and manufacturing demands from Floerke et al. 'Domestic and industrial water uses as a mirror for socioeconomic development'
# watergap_mw_hist.df = data.frame( read.csv( 'P:/ene.model/data/Water/water_demands/watergap_historical_water_use_mw.csv', stringsAsFactors = FALSE ) )
# watergap_mw_hist.df$country = countrycode(watergap_mw_hist.df$iso, 'iso3n', 'iso3c')
# watergap_mw_hist.df$region = sapply( watergap_mw_hist.df$country, function(x){ return( country_region_map_key.df$Eleven_region[ which( country_region_map_key.df$Region == x ) ] ) } )
# watergap_mf_hist.df = data.frame( read.csv( 'P:/ene.model/data/Water/water_demands/watergap_historical_water_use_mf.csv', stringsAsFactors = FALSE ) )
# watergap_mf_hist.df$country = countrycode(watergap_mf_hist.df$iso, 'iso3n', 'iso3c')
# watergap_mf_hist.df$region = sapply( watergap_mf_hist.df$country, function(x){ return( country_region_map_key.df$Eleven_region[ which( country_region_map_key.df$Region == x ) ] ) } )

# wkd_GAMS = paste(unlist(strsplit( unlist(strsplit(Sys.getenv('PATH'),';'))[which(grepl('GAMS', unlist(strsplit(Sys.getenv('PATH'),';')) ))[1]], '[\\]' )),collapse='/')
# ixDS1 = ix_platform$datastructure( model = 'MESSAGE-GLOBIOM CD-LINKS R2.1', scen = 'baseline_waterSDG__w0_p0_c0_d0' )
# ixDS2 = ix_platform$datastructure( model = 'MESSAGE-GLOBIOM CD-LINKS R2.1', scen = 'baseline_waterSDG__w0_p0_c1_d0' )
# ixDS3 = ix_platform$datastructure( model = 'MESSAGE-GLOBIOM CD-LINKS R2.1', scen = 'baseline_waterSDG__w1_p0_c0_d0' )
# ixDS2$var('ACT',list(technology = 'CO2t_TCE',year_act = 2030))$level
# em = data.frame( 	base = c( ixDS1$par('historical_activity',list(technology = 'CO2t_TCE',year_act = 2010))$value, ixDS1$var('ACT',list(technology = 'CO2t_TCE',year_act = 2030))$level, ixDS1$var('ACT',list(technology = 'CO2t_TCE',year_act = 2050))$level, ixDS1$var('ACT',list(technology = 'CO2t_TCE',year_act = 2090))$level ),
# sdg = c( ixDS2$par('historical_activity',list(technology = 'CO2t_TCE',year_act = 2010))$value, ixDS2$var('ACT',list(technology = 'CO2t_TCE',year_act = 2030))$level, ixDS2$var('ACT',list(technology = 'CO2t_TCE',year_act = 2050))$level, ixDS2$var('ACT',list(technology = 'CO2t_TCE',year_act = 2090))$level ) )
# row.names(em) = c(2010,2030,2050,2090)
# wt = data.frame( 	base = c( 	c( sum( ixDS1$par('historical_activity',list(technology = 'extract__freshwater_supply',year_act = 2010))$value ) + rowSums( irrigation_withdrawal.df['2010',] ) + rowSums( rural_withdrawal.df['2010',] ) + rowSums( urban_withdrawal.df['2010',] ) + 1e-3 * sum( watergap_mf_hist.df$withdrawal[ which( watergap_mf_hist.df$year == 2010  ) ] ) ),
# sum( ixDS1$var('ACT',list(technology = 'extract__freshwater_supply',year_act = 2030))$level ),
# sum( ixDS1$var('ACT',list(technology = 'extract__freshwater_supply',year_act = 2050))$level ),
# sum( ixDS1$var('ACT',list(technology = 'extract__freshwater_supply',year_act = 2090))$level ) ),
# sdg = c( 	c( sum( ixDS3$par('historical_activity',list(technology = 'extract__freshwater_supply',year_act = 2010))$value ) + rowSums( irrigation_withdrawal.df['2010',] ) + rowSums( rural_withdrawal.df['2010',] ) + rowSums( urban_withdrawal.df['2010',] ) + 1e-3 * sum( watergap_mf_hist.df$withdrawal[ which( watergap_mf_hist.df$year == 2010  ) ] ) ),
# sum( ixDS3$var('ACT',list(technology = 'extract__freshwater_supply',year_act = 2030))$level ),
# sum( ixDS3$var('ACT',list(technology = 'extract__freshwater_supply',year_act = 2050))$level ),
# sum( ixDS3$var('ACT',list(technology = 'extract__freshwater_supply',year_act = 2090))$level ) ) )
# row.names(wt) = c(2010,2030,2050,2090)

# gtst = c(66813,141810,229718,335083,463971)

# cls = c('gray85','black')
# windows()
# par(mar=c(3,5,1,2), oma = c(8,2,8,2))
# barplot( 100*t(pc),beside=TRUE, ylab = c('% Population w/\nclean water'), ylim = c(0,1.2*max( unlist(100*t(pc)) )), col = cls )
# abline(h=0)
# #legend('top',legend=c('Baseline','Mitigation'),fill = cls, ncol = 2, bty = 'n', cex = 0.9 )
# windows()
# par(mar=c(3,5,1,2), oma = c(8,2,8,2))
# barplot( 100*t(wc),beside=TRUE, ylab = c('% Population w/\nwater treatment'), ylim = c(0,1.2*max( unlist(100*t(wc)) )), col = cls )
# abline(h=0)
# #legend('top',legend=c('Baseline','Mitigation'),fill = cls, ncol = 2, bty = 'n', cex = 0.9 )
# windows()
# par(mar=c(3,5,1,2), oma = c(8,2,8,2))
# barplot( t(em),beside=TRUE, ylab = c('Global\ncarbon emissions'),ylim = c(0,1.2*max( unlist(t(em)) )), col = cls )
# #legend('top',legend=c('Baseline','Mitigation'),fill = cls, ncol = 2, bty = 'n', cex = 0.9 )
# abline(h=0)
# windows()
# par(mar=c(3,5,1,2), oma = c(8,2,8,2))
# barplot( t(wt),beside=TRUE, ylab = c('Global\nfreshwater use'),ylim = c(0,1.2*max( unlist(t(wt)) )), col = cls )
# abline(h=0)
# legend('top',legend=c('Baseline','Mitigation'),fill = cls, ncol = 2, bty = 'n', cex = 0.9 )

# gtst = c(66813,141810,229718,463971)
# wt2 = data.frame(	base= c(4000 / gtst), sdgs= c( c(4000,2000,2000,2000) / gtst ) )
# cls = c('gray85','black')
# windows()
# par(mar=c(3,5,1,2), oma = c(8,2,8,2))
# barplot( t(wt2),beside=TRUE, ylab = c('Global\nfreshwater use'),ylim = c(0,1.2*max( unlist(t(wt2)) )), col = cls )
# abline(h=0)
# legend('top',legend=c('Baseline','Mitigation'),fill = cls, ncol = 2, bty = 'n', cex = 0.9 )



# # Desal plot
# wkd_message_ix = 'C:/Users/parkinso/git/'
# modelName = 'MESSAGE-GLOBIOM CD-LINKS R2.1'
# scenarioName = 'baseline_waterSDG'
# source(file.path(Sys.getenv('IXMP_R_PATH'),'ixmp.r'))
# ixDS = ix_platform$datastructure( model = modelName, scen = scenarioName )

# # Model years
# model_years = ixDS$set( 'year' )

# # Model timeslices
# model_time = ixDS$set( 'time' )

# # Define a common mode using the existing DB settings
# mode_common = ixDS$set( 'mode' )[1] # Common mode name from set list
# modes = ixDS$set( 'mode' )

# # Get the name of the regions
# region = as.character( ixDS$set('cat_node')$node )
# rnm = data.frame( AFR = 'Sub-Saharan Africa', CPA = 'Centrally Planned Asia', EEU = 'Eastern EU', FSU = 'Former Soviet Union', LAM = 'Latin America', MEA = 'Middle East & N. Africa', PAO = 'Pacific Oceanic', NAM = 'North America', PAS = 'Pacific Asia', SAS = 'South Asia', WEU = 'Western EU' )

# # Load in the cleaned desal database from Hanasaki et al 2016 and add MESSAGE regions
# reg.spdf = readOGR('P:/ene.general/Water/global_basin_modeling/basin_delineation','REGION_dissolved',verbose=FALSE)
# global_desal.spdf = spTransform( readOGR('P:/ene.general/Water/global_basin_modeling/desalination','global_desalination_plants'), crs(reg.spdf) )
# global_desal.spdf@data$region = over(global_desal.spdf,reg.spdf[,which(names(reg.spdf) == 'REGION')])
# global_desal.spdf = global_desal.spdf[ -1*which( is.na(global_desal.spdf@data$region) ), ]
# global_desal.spdf@data$msg_vintage = sapply( global_desal.spdf@data$online, function(x){ as.numeric( model_years[ which.min( ( as.numeric( model_years ) - x )^2 ) ] ) } )
# global_desal.spdf@data$technology_2 = sapply( global_desal.spdf@data$technology, function(x){ if( grepl('MSF',x)|grepl('MED',x) ){ return('distillation') }else{ return('membrane') }} )

# # From : Chart 1.1 in 'Executive Summary Desalination Technology Markets Global Demand Drivers, Technology Issues, Competitive Landscape, and Market Forecasts'
# # Global desalination capacity in 2010 was approx. 24 km3 / year
# global_desal.spdf@data$m3_per_day = global_desal.spdf@data$m3_per_day  * ( 24  / ( sum( global_desal.spdf@data$m3_per_day ) * 365 / 1e9 ) )

# # Match to message vintaging and regions
# historical_desal_capacity.list = lapply( c('membrane','distillation'), function(tt){
# temp = data.frame( do.call(cbind, lapply( region, function(reg){ sapply( unique(global_desal.spdf@data$msg_vintage)[order(unique(global_desal.spdf@data$msg_vintage))], function(y){ (365/1e9) * max( 0,  sum( global_desal.spdf@data$m3_per_day[ which( global_desal.spdf@data$msg_vintage == y & global_desal.spdf@data$region == unlist(strsplit(reg,'_'))[2] & global_desal.spdf@data$technology_2 == tt ) ] , na.rm=TRUE ), na.rm=TRUE ) } ) } ) ) )
# names(temp) = region
# row.names(temp) = unique(global_desal.spdf@data$msg_vintage)[order(unique(global_desal.spdf@data$msg_vintage))]
# return(temp)
# } )
# names(historical_desal_capacity.list) = c('membrane','distillation')

# # Calculate avg diffusion
# avg_diff = mean( diff(colSums( t(as.matrix(historical_desal_capacity.list[[1]])) ))[6:8] / colSums( t(as.matrix(historical_desal_capacity.list[[1]])) )[6:8] )^(1/5)

# dns = c(NA,NA,35,NA,NA,35,NA,NA,NA,35,NA)
# ang = c(NA,NA,25,NA,NA,25,NA,NA,NA,115,NA)

# windows()
# p1 = layout(matrix(c(3,3,1,2),2,2,byrow=TRUE),widths=c(0.45,0.4),heights=c(0.25,0.75))
# par( mar=c(4,4.5,1,2), oma = c(2,2,2,2) )
# barplot( t(as.matrix(historical_desal_capacity.list[[1]])), col = cols, main = 'Reverse Osmosis', density = dns, angle = ang, ylim = c( 0, max( c( 0, 8 ) ) ), ylab = expression('Installed Capacity'*' [ '*km^3*' per year ]') )
# par(mar=c(4,2,1,2))
# barplot( t(as.matrix(historical_desal_capacity.list[[2]])), col = cols, main = 'Thermal Process', density = dns, angle = ang, ylim = c( 0, max( c( 0, 8 ) ) ) )
# par(mar=c(2,3,1,2))
# plot.new()
# legend( 'center', legend = as.character( unlist( rnm[ sapply( region, function(r){ unlist(strsplit(r,'_'))[2] } ) ] ) ), title = expression( bold('MESSAGE Region')), title.col='black', title.adj = 0, fill = cols, density = dns, angle = ang, ncol = 3, bty = 'n' )

# # withdrawals and return flow
# ssss = 'SSP2'
# pth = 'base'
# urban_withdrawal.df = 1e-3 * data.frame( read.csv( paste( 'P:/ene.model/data/Water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_urban_withdrawal2_', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
# rural_withdrawal.df = 1e-3 * data.frame( read.csv( paste( 'P:/ene.model/data/Water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_rural_withdrawal_', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
# manufacturing_withdrawal.df = 1e-3 * data.frame( read.csv( paste( 'P:/ene.model/data/Water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_manufacturing_withdrawal_', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
# urban_return.df = 1e-3 * data.frame( read.csv( paste( 'P:/ene.model/data/Water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_urban_return2_', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
# rural_return.df = 1e-3 * data.frame( read.csv( paste( 'P:/ene.model/data/Water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_rural_return_', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )
# manufacturing_return.df = 1e-3 * data.frame( read.csv( paste( 'P:/ene.model/data/Water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_manufacturing_return_', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )

# windows()
# p1 = layout(matrix(c(4,4,4,1,2,3),2,3,byrow=TRUE),widths=c(0.35,0.3,0.3),heights=c(0.3,0.7))
# par(mar=c(4,4.5,1,2), oma = c(2,2,2,2))
# barplot( t(as.matrix(urban_withdrawal.df)),las=2, col = cols, main = 'Urban Municipal', density = dns, angle = ang, ylim = c( 0, max( c( 0, 600 ) ) ), ylab = expression('Freshwater Withdrawal'*' [ '*km^3*' per year ]'), cex.names = 0.9 )
# abline(h=0)
# par(mar=c(4,2,1,2))
# barplot( t(as.matrix(rural_withdrawal.df)),las=2, col = cols, main = 'Rural Municipal', density = dns, angle = ang, ylim = c( 0, max( c( 0, 600 ) ) ), cex.names = 0.9 )
# abline(h=0)
# barplot( t(as.matrix(manufacturing_withdrawal.df)),las=2, col = cols, main = 'Manufacturing', density = dns, angle = ang, ylim = c( 0, max( c( 0, 600 ) ) ), cex.names = 0.9 )
# abline(h=0)
# par(mar=c(0,0,1,2))
# plot.new()
# legend( 'center', legend = as.character( unlist( rnm[ names(urban_withdrawal.df) ] ) ), title = expression( bold('MESSAGE Region')), title.col='black', title.adj = 0, fill = cols, density = dns, angle = ang, ncol = 3, bty = 'n', cex=1.1 )

# windows()
# p1 = layout(matrix(c(4,4,4,1,2,3),2,3,byrow=TRUE),widths=c(0.35,0.3,0.3),heights=c(0.3,0.7))
# par(mar=c(4,4.5,1,2), oma = c(2,2,2,2))
# barplot( t(as.matrix(urban_return.df)),las=2, col = cols, main = 'Urban Municipal', density = dns, angle = ang, ylim = c( 0, max( c( 0, 600 ) ) ), ylab = expression('Return-flow'*' [ '*km^3*' per year ]'), cex.names = 0.9 )
# abline(h=0)
# par(mar=c(4,2,1,2))
# barplot( t(as.matrix(rural_return.df)),las=2, col = cols, main = 'Rural Municipal', density = dns, angle = ang, ylim = c( 0, max( c( 0, 600 ) ) ), cex.names = 0.9 )
# abline(h=0)
# barplot( t(as.matrix(manufacturing_return.df)),las=2, col = cols, main = 'Manufacturing', density = dns, angle = ang, ylim = c( 0, max( c( 0, 600 ) ) ), cex.names = 0.9 )
# abline(h=0)
# par(mar=c(0,0,1,2))
# plot.new()
# legend( 'center', legend = as.character( unlist( rnm[ names(urban_withdrawal.df) ] ) ), title = expression( bold('MESSAGE Region')), title.col='black', title.adj = 0, fill = cols, density = dns, angle = ang, ncol = 3, bty = 'n', cex=1.1 )

# # Plot irrigation demands
# # Import urban and rural demands from csv
# pth = 'base'
# ssss = 'SSP2'
# urban_reuse_fraction.df = data.frame( read.csv( paste( 'P:/ene.model/data/Water/water_demands/harmonized/ssp', unlist(strsplit(ssss,'P'))[2], '_regional_urban_recycling_rate_', pth, '.csv', sep = '' ), stringsAsFactors=FALSE, row.names = 1 ) )

# # Import irrigation demands from GLOBIOM and harmonize
# glb_scen = paste(ssss,'-Ref-SPA0',sep='' )
# glb_irr.df = data.frame( read.csv( 'P:/ene.model/data/Water/water_demands/cdlinks_globiom_irrigation.csv', stringsAsFactors = FALSE ) )
# irrigation_withdrawal.df = 1e-3 * data.frame( do.call(cbind, lapply( names( urban_reuse_fraction.df ), function(rr){ sapply( c(row.names( urban_reuse_fraction.df ),'2100'), function(yy){ return( glb_irr.df[ which( glb_irr.df$Scenario == glb_scen & glb_irr.df$Region == rr ), paste( 'X', yy, sep = '' ) ] ) } ) } ) ) )
# row.names(irrigation_withdrawal.df) = c(row.names( urban_reuse_fraction.df ),'2100')
# names(irrigation_withdrawal.df) = names( urban_reuse_fraction.df )

# windows()
# p1 = layout(matrix(c(4,4,4,1,2,3),2,3,byrow=TRUE),widths=c(0.35,0.3,0.3),heights=c(0.3,0.7))
# par(mar=c(4,4.5,1,2), oma = c(2,2,2,2))
# barplot( t(as.matrix(irrigation_withdrawal.df)),las=2, col = cols, main = 'Irrigation', density = dns, angle = ang, ylim = c( 0, max( c( 0, 3800 ) ) ), ylab = expression('Freshwater Withdrawal'*' [ '*km^3*' per year ]'), cex.names = 0.9 )
# abline(h=0)
# par(mar=c(4,2,1,2))
# plot.new()
# plot.new()
# par(mar=c(0,0,1,2))
# plot.new()
# legend( 'center', legend = as.character( unlist( rnm[ names(urban_withdrawal.df) ] ) ), title = expression( bold('MESSAGE Region')), title.col='black', title.adj = 0, fill = cols, density = dns, angle = ang, ncol = 3, bty = 'n', cex=1.1 )




# ### Plot basins and socioeconomic parameters

# windows()
# plot(coast, col = 'black', lwd = 0.3)
# plot( all_bas[ which(as.character( all_bas@data$wtr_stress ) %in% c( 'Low stress' ) ), ], col = 'mediumseagreen', add=TRUE, border = NA )
# plot( all_bas[ which(as.character( all_bas@data$wtr_stress ) %in% c( 'Stress' ) ), ], col = 'orange', add=TRUE, border = NA )
# plot( all_bas[ which(as.character( all_bas@data$wtr_stress ) %in% c( 'High stress' ) ), ], col = 'darkred', add=TRUE, border = NA )
# legend('top', legend = c('Low stress','Stress','High stress'), fill = c('mediumseagreen','orange','darkred'), ncol = 3, bty = 'n', cex = 0.9 )

# bns = all_bas[ which(as.character( all_bas@data$wtr_stress ) %in% c( 'Stress','High stress','Low stress') ), ]
# bns_l = gUnaryUnion( as( bns[which(as.character( bns@data$wtr_stress ) %in% c( 'Low stress' ) ),], 'SpatialPolygons') )
# bns_m = gUnaryUnion( as( bns[which(as.character( bns@data$wtr_stress ) %in% c( 'Stress' ) ),], 'SpatialPolygons') )
# bns_h = gUnaryUnion( as( bns[which(as.character( bns@data$wtr_stress ) %in% c( 'High stress' ) ),], 'SpatialPolygons') )

# # Import socioeconomic raster data from Matt
# vars = c('urban_gdp','urban_income', 'rural_gdp','rural_income')
# for ( i in 1:length(vars) ){ for( s in 1:3 )
# {
# gg = stack(paste('P:/ene.general/downscaling/output/econ_rasters/ssp',s,'_',vars[i],'.tiff',sep=''))
# proj4string(gg) = proj4string(bns)
# assign( paste( vars[i], s, sep = '_' ), gg )
# rm(gg)
# } }

# # Get socioeconomic vars for each year
# res = lapply( c( 'l','m','h'), function(tt){
# rs1 = do.call( cbind, lapply( 1:3, function(s){
# rs2 = do.call( rbind, lapply( 1:7, function(yy){
# print(yy)
# ugdp = unlist( extract( get( paste( 'urban_gdp', s, sep = '_' ) )[[ yy ]], get( paste( 'bns', tt, sep = '_' ) ) ) )
# rgdp = unlist( extract( get( paste( 'rural_gdp', s, sep = '_' ) )[[ yy ]], get( paste( 'bns', tt, sep = '_' ) ) ) )
# ugdpc = unlist( extract( get( paste( 'urban_income', s, sep = '_' ) )[[ yy ]], get( paste( 'bns', tt, sep = '_' ) ) ) )
# rgdpc = unlist( extract( get( paste( 'rural_income', s, sep = '_' ) )[[ yy ]], get( paste( 'bns', tt, sep = '_' ) ) ) )
# upop = ugdp/ugdpc
# upop[is.na(upop)]=0
# rpop = rgdp/rgdpc
# rpop[is.na(rpop)]=0
# rs3 = data.frame( a1 = round( (sum( ugdp ) + sum( rgdp ))/(sum( upop ) + sum( rpop )) ), a2 = round( sum( upop ) + sum( rpop ) ) )
# names(rs3) = c( paste( 'gdpc', paste( 'ssp', s, sep='' ), sep = '_' ), paste( 'pop', paste( 'ssp', s, sep='' ), sep = '_' ) )
# return(rs3)
# } ) )
# row.names(rs2) = seq(2010, 2070, by = 10 )
# return(rs2)
# } ) )
# return( rs1 )
# } )
# names(res) = c( 'Stress','High stress','Low stress')

# yrs = as.numeric( row.names( res[[1]] ) )
# windows()
# p1 = layout(matrix(c(3,3,1,2),2,2,byrow=TRUE),widths=c(0.43,0.43),heights=c(0.25,0.75))
# par(mar=c(4,3.85,0,2), oma = c(2,2,2,2))
# matplot( yrs, 1e-9 * as.matrix( cbind( res[['Low stress']]$pop_ssp2, res[['Stress']]$pop_ssp2, res[['High stress']]$pop_ssp2 ) ), ylim = c(0,2.5), xlab = 'Year', ylab = 'Population [ billions ]', type = 'l', lty = 1, lwd = 1.5, col =  c('mediumseagreen','orange','darkred') )
# matlines( yrs, 1e-9 * as.matrix( cbind( res[['Low stress']]$pop_ssp1, res[['Stress']]$pop_ssp1, res[['High stress']]$pop_ssp1 ) ), lty = 2, lwd = 1.5, col =  c('mediumseagreen','orange','darkred') )
# matlines( yrs, 1e-9 * as.matrix( cbind( res[['Low stress']]$pop_ssp3, res[['Stress']]$pop_ssp3, res[['High stress']]$pop_ssp3 ) ), lty = 4, lwd = 1.5, col =  c('mediumseagreen','orange','darkred') )
# matplot( yrs, 1e-3 * as.matrix( cbind( res[['Low stress']]$gdpc_ssp2, res[['Stress']]$gdpc_ssp2, res[['High stress']]$gdpc_ssp2 ) ), ylim = c(0,60), xlab = 'Year', ylab = 'Per capita GDP [ thousand USD ]', type = 'l', lty = 1, lwd = 1.5, col =  c('mediumseagreen','orange','darkred') )
# matlines( yrs, 1e-3 * as.matrix( cbind( res[['Low stress']]$gdpc_ssp1, res[['Stress']]$gdpc_ssp1, res[['High stress']]$gdpc_ssp1 ) ), lty = 2, lwd = 1.5, col =  c('mediumseagreen','orange','darkred') )
# matlines( yrs, 1e-3 * as.matrix( cbind( res[['Low stress']]$gdpc_ssp3, res[['Stress']]$gdpc_ssp3, res[['High stress']]$gdpc_ssp3 ) ), lty = 4, lwd = 1.5, col =  c('mediumseagreen','orange','darkred') )
# par(mar=c(1,3.5,1,2))
# plot.new()
# legend( 'center', legend = c( c( 'Low stress    ','SSP1','Stress','SSP2', 'High stress', 'SSP3') ), y.intersp=1.2, seg.len=3, lty = c(1,2,1,1,1,4), col =  c('mediumseagreen','black','orange','black','darkred','black'), lwd = 1.5, ncol = 3, bty = 'n', cex = 1 )
