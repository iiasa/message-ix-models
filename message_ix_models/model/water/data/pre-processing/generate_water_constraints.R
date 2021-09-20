# Clear memory and close all windows
rm(list = ls())
graphics.off()

require(reshape)
require(maptools)
require(countrycode)
require(raster)
require(rgeos)
require(rgdal)
require(ncdf4)
require(tidyverse)

memory.limit(size=1e9)

# message-ix-models path
msg_ix_model = Sys.getenv("MESSAGE-IX-MODELS")
## Set path data folder in message_ix working copy
msg_data = paste0('P:/ene.model/NEST')
data_path = path.expand(msg_data)

# Country region mapping key
country_region_map_key.df = data.frame( read.csv( paste( data_path, '/water_demands/country_region_map_key.csv',  sep = '/' ), stringsAsFactors=FALSE) )

# ssp
ssp = 2

# Load municipal water demands and socioeconomic parameters from local drive
dat.df = merge_recurse( lapply( seq(2010,2090,by=10), function(y){

  dat.df = data.frame( readRDS( paste(data_path,'/harmonized_rcp_ssp_data/water_use_ssp2_rcp2_',y,'_data.Rda',sep='') ) )
  dat.df = cbind( dat.df[ ,c('country_id', paste('xloc',y,sep= '.'), paste('yloc', y, sep= '.'), paste('urban_pop', y, sep= '.'), paste('rural_pop', y, sep= '.'), paste('urban_gdp', y, sep= '.'), paste('rural_gdp', y, sep= '.') ) ],
                  rowSums( do.call( cbind, lapply( 1:12, function(m){ dat.df[ ,paste('urban_withdrawal',y,m,sep= '.') ] } ) ) ),
                  rowSums( do.call( cbind, lapply( 1:12, function(m){ dat.df[ ,paste('rural_withdrawal',y,m,sep= '.') ] } ) ) ),
                  rowSums( do.call( cbind, lapply( 1:12, function(m){ dat.df[ ,paste('urban_return',y,m,sep= '.') ] } ) ) ),
                  rowSums( do.call( cbind, lapply( 1:12, function(m){ dat.df[ ,paste('rural_return',y,m,sep= '.') ] } ) ) ),
                  rowSums( do.call( cbind, lapply( 1:12, function(m){ dat.df[ ,paste('urban_withdrawal',y,m,sep= '.') ] } ) ) ) / dat.df[, paste('urban_pop', y, sep= '.') ],
                  rowSums( do.call( cbind, lapply( 1:12, function(m){ dat.df[ ,paste('rural_withdrawal',y,m,sep= '.') ] } ) ) ) / dat.df[, paste('rural_pop', y, sep= '.') ]	)
  names(dat.df) = c('country_id', 'xloc', 'yloc', paste('urban_pop',y,sep= '.'), paste('rural_pop', y, sep= '.'), paste('urban_gdp', y, sep= '.'), paste('rural_gdp', y, sep= '.'), paste('urban_withdrawal', y, sep= '.'), paste('rural_withdrawal', y, sep= '.'), paste('urban_return', y, sep= '.'), paste('rural_return', y, sep= '.'), paste('urban_per_capita_withdrawal', y, sep= '.'), paste('rural_per_capita_withdrawal', y, sep= '.') )

  # Make sure return flows don't exceed withdrawals
  inds = which( unlist( dat.df[, paste('urban_return',y,sep= '.') ] ) > 0.92 * unlist(dat.df[, paste('urban_withdrawal', y, sep= '.') ] ) )
  if( length(inds)>0 ){ dat.df[ inds, paste('urban_return',y,sep= '.') ] = dat.df[inds, paste('urban_withdrawal', y, sep= '.') ] * 0.92 }
  inds = which( unlist( dat.df[, paste('rural_return',y,sep= '.') ] ) > 0.92 * unlist(dat.df[, paste('rural_withdrawal', y, sep= '.') ] ) )
  if( length(inds)>0 ){ dat.df[ inds, paste('rural_return',y,sep= '.') ] = dat.df[inds, paste('rural_withdrawal', y, sep= '.') ] * 0.92 }

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

#### Make spatial####
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

# temp = readOGR('P:/ene.model/data/Water','water_scarcity',verbose=FALSE)
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

# Countries, regions and basins
# Here we can add specification in case of a country model
dat.df$country = sapply( dat.df$country_id, function(cc){ if( cc %in% country_region_map_key.df$UN_Code )
  { return( country_region_map_key.df$ISO3[ which(country_region_map_key.df$UN_Code == cc) ] ) 
  }else{ return(NA) } } )
dat.df$R11 = sapply( dat.df$country_id, function(cc){ if( cc %in% country_region_map_key.df$UN_Code )
  { return( country_region_map_key.df$R11[ which(country_region_map_key.df$UN_Code == cc) ] ) 
  }else{ return(NA) } } )
dat.df$R12 = sapply( dat.df$country_id, function(cc){ if( cc %in% country_region_map_key.df$UN_Code )
  { return( country_region_map_key.df$R12[ which(country_region_map_key.df$UN_Code == cc) ] ) 
  }else{ return(NA) } } )
dat.df = dat.df[which(!is.na(dat.df$R11)),]

#### GOVERNANCE ####
length(unique(dat.df$country))
gov.df = read.csv(paste0(data_path,'/governance/governance_obs_project.csv')) %>%
  filter(!is.na(governance)) %>%
  filter(scenario == paste0('SSP',ssp), year %in% initial_years ) %>%
  dplyr::select(countrycode, year, governance)
names(gov.df)
mean_gov = mean(gov.df$governance)
# missing countries, just add mean_gov
to_add = data.frame(countrycode = c('AFG','AGO', 'ALB', 'ARE', 'MMR', 'PSE', 'QAT', 'TLS','TWN'), governance = mean_gov) %>%
  crossing(year = unique(gov.df$year)) %>%
  dplyr::select(countrycode,year,governance)

gov.df = gov.df %>% bind_rows(to_add) %>%
  dplyr::rename(gov = year, country = countrycode) %>%
  spread(gov,governance, sep = '.')

dat.df = dat.df %>% left_join(gov.df)
#countries that have no governance values
dat.df %>% filter(is.na(gov.2020)) %>%
  distinct(country) # not anymore

#re-add to dat.spdf
dat.spdf = dat.df
coordinates(dat.spdf) = ~ xloc + yloc

#### Existing urban wastewater treatment ####
# WARNING: loading this file or the 'over' function MIGHT TAKE HOURS
# WRI aquedoct. 
# https://www.wri.org/data/aqueduct-global-maps-30-data
# wri_map.spdf = readShapeLines('P:/ene.model/NEST/water_access/Y2019M07D12_Aqueduct30_V01/wri.shp',
#                               delete_null_obj=TRUE)
wri_map.spdf = readOGR('P:/ene.model/NEST/water_access/Y2019M07D12_Aqueduct30_V01','wri',verbose=FALSE)

wri_map.spdf = wri_map.spdf[c("udw_raw","usa_raw")]

dat_simpl.spdf = dat.spdf[c('urban_gdp.2020','rural_gdp.2020')]
dat_simpl.spdf$xcord = dat_simpl.spdf$xloc
dat_simpl.spdf$ycord = dat_simpl.spdf$yloc

# proj4string(dat_simpl.spdf) = proj4string(wri_map.spdf)
new_proj = CRS("+proj=longlat +datum=WGS84 +no_defs")
proj4string(dat_simpl.spdf) = new_proj
proj4string(wri_map.spdf) = new_proj
# dat_proj.spdf = spTransform(dat_simpl.spdf, new_proj)

wri_over = over(dat_simpl.spdf,wri_map.spdf)
identicalCRS(dat_simpl.spdf, wri_map.spdf)

dat.spdf = cbind(dat.spdf , wri_over)

vec = names(dat.spdf)[grepl('urban_gdp|rural_gdp|urban_pop|rural_pop',names(dat.spdf))]

# with new data
ww.df2 = dat.spdf@data %>%  
  select(country_id,all_of(vec), udw_raw, usa_raw) %>% 
  bind_cols(xcord = dat.spdf$xloc,ycord = dat.spdf$yloc) %>% 
  mutate(income.2020 = (urban_gdp.2020 + rural_gdp.2020)/(urban_pop.2020 + rural_pop.2020)) %>%
  # mutate(urb_income.2020 = (urban_gdp.2020)/(urban_pop.2020)) %>% 
  # mutate(rur_income.2020 = (rural_gdp.2020)/(rural_pop.2020)) %>% 
  filter(income.2020 > 0, !is.na(udw_raw), !is.na(usa_raw)) %>% 
  mutate(udw_raw = (1 - udw_raw) * 100,
         usa_raw = (1 - usa_raw) * 100) 

ww.df2_plot = ww.df2 %>% group_by(country_id) %>% 
  summarise(income.2020 = mean(income.2020),
            udw_raw = mean(udw_raw),
            usa_raw = mean(usa_raw)) %>% ungroup() %>% 
  gather(udw_raw,usa_raw, key = 'type',value = 'value')

# new
x = c(33,ww.df2$income.2020)
y = c(0.0001,ww.df2$udw_raw)
r = nls(y ~ SSlogis(x,a,m,s))
cp.a = 1
cp.m = coef(r)[2]
cp.s = coef(r)[3]
# new
y = c(0.0001,ww.df2$usa_raw)
r = nls(y ~ SSlogis(x,a,m,s))
tp.a = 1
tp.m = coef(r)[2]
tp.s = coef(r)[3]

# Plot historical connection levels and model
cpm = data.frame( x = seq( 1, 56000, by = 1000 ), y = SSlogis( seq( 1, 56000, by = 1000 ), 0.99, cp.m, cp.s ) )
tpm = data.frame( x = seq( 1, 56000, by = 1000 ), y = SSlogis( seq( 1, 56000, by = 1000 ), 0.99, tp.m, tp.s ) )

cpm$type = 'udw_raw'
tpm$type = 'usa_raw'
cpm = cpm %>% bind_rows(tpm)

type_names <- list(
  'udw_raw'="Water access",
  'usa_raw'="Sanitation"
)
type_labeller <- function(variable,value){
  return(type_names[value])
}

ggplot()+
  geom_line(data = cpm,aes(x = x, y = 100*y) )+
  geom_point(data = ww.df2_plot, aes(x = income.2020, y= value))+
  ggtitle('Model vs country averages')+
  facet_wrap(~type, labeller = type_labeller) +
  xlab('Per capita Income in 2020 [USD/pc]')+
  ylab('share of population with access [%]')+
  theme_bw()+
  theme(panel.grid.minor = element_blank())

# piped water access / sewerage connection

UR = c('urban','rural')
ww_proj = function(ww.df2,UR,yy){ 
  
  t1 = rlang::sym(paste0(UR,'_gdp.',yy))
  t2 = rlang::sym(paste0(UR,'_pop.',yy) )
  inc_f = rlang::sym(paste0(UR,'_income.',yy) )
  varc = rlang::sym(paste0(UR,'_connection_rate.',yy) )
  vart = rlang::sym(paste0(UR,'_treatment_rate.',yy) )
  decay = max( 0, 1 / length(yrs) * log( 1 / 0.01 ), na.rm=TRUE )
  
  ww.df2 %>% 
    mutate("{UR}_income.{yy}" := pmax(0, !!t1 / !!t2 ,na.rm = T) ,
           year = yy) %>% 
    mutate(c0 = udw_raw / 100,
           t0 = usa_raw / 100) %>% 
    mutate(c_mod = c( SSlogis( !!inc_f, cp.a, cp.m, cp.s ) ),
           t_mod = c( SSlogis( !!inc_f, tp.a, tp.m, tp.s ) )) %>% 
    # 2020 is base year, 2010 need smaller values, 2010 need to changed at the end or not calculated
    mutate(!!varc := pmin(0.999, if_else(year == 2010, 0.95*c0,
                                         ( 1 +  ( c0 / c_mod - 1 ) * 
                                             exp( -1 * decay * ( which(yrs == yy) - 2 ) ) ) * c_mod ) ),
           !!vart := pmin(0.999, if_else(year == 2010, 0.95*t0, #assumption in 2010
                                         ( 1 +  ( t0 / t_mod - 1 ) * 
                                             exp( -1 * decay * ( which(yrs == yy) - 2 ) ) ) * t_mod ) ) ) %>% 
    mutate(!!varc := if_else(!!inc_f == 0, 0, !!varc),
           !!vart := if_else(!!inc_f == 0, 0, !!vart) )  %>% 
    select(-year, -c0, -t0, -c_mod,-t_mod)
}

add_recycl = function(df,yy){
  varr = rlang::sym(paste0('urban_treatment_rate.',yy) )
  df %>%
    mutate("recycling_rate.{yy}" := pmin(0.8, 0.8 * !!varr ))
}

#adding all estimation for future years
ww_proj.df = ww.df2
for (yy in yrs) {
  for (uu in UR) {
    ww_proj.df = ww_proj.df %>% left_join(
      ww_proj(ww.df2,uu,yy) )
  }
  ww_proj.df = ww_proj.df %>% left_join(
    add_recycl(ww_proj.df,yy) )
}

dat.df = dat.spdf@data %>% 
  mutate(xcord = dat_simpl.spdf$xloc,
         ycord = dat_simpl.spdf$yloc) %>% 
  left_join(ww_proj.df %>% 
              select(-all_of(vec),-country_id,-udw_raw, -usa_raw))

dat.df[is.na(dat.df)] = 0

#sanitation curve more on the right, but also urban wrt rural, not clear
# for sanitation it makes a difference if I take country means of single point
# for water access the difference is minimal

#Plotting # of people without access: Urban and rural in 2020
#re-add to dat.spdf
dat.spdf = dat.df
coordinates(dat.spdf) = ~ xcord + ycord

ww_plot = dat.spdf[c("BCU_name",'urban_pop.2020','rural_pop.2020',"urban_connection_rate.2020",
                     "rural_connection_rate.2020","urban_treatment_rate.2020",
                     "rural_treatment_rate.2020")]
ww_plot@data$urban_no_wa.2020 = ww_plot$urban_pop.2020 * (1 - ww_plot$urban_connection_rate.2020) *1e-6
ww_plot@data$rural_no_wa.2020 = ww_plot$rural_pop.2020 * (1 - ww_plot$rural_connection_rate.2020) *1e-6
ww_plot@data$urban_no_san.2020 = ww_plot$urban_pop.2020 * (1 - ww_plot$urban_treatment_rate.2020) *1e-6
ww_plot@data$rural_no_san.2020 = ww_plot$rural_pop.2020 * (1 - ww_plot$rural_treatment_rate.2020) *1e-6

totals_ww = ww_plot@data %>% summarise(urban_no_wa.2020 = sum(urban_no_wa.2020),
                                       rural_no_wa.2020 = sum(rural_no_wa.2020),
                                       urban_no_san.2020 = sum(urban_no_san.2020),
                                       rural_no_san.2020 = sum(rural_no_san.2020))

extent(ww_plot)
proj4string(ww_plot)
empty.rs = extent(-180, 180, -90, 90)
empty.rs <- raster(empty.rs)
res(empty.rs)<- 1
projection(empty.rs)<-CRS(proj4string(ww_plot))

rw.rs = rasterize(ww_plot,empty.rs,'rural_no_wa.2020',fun = sum)
names(rw.rs) = 'Rural pop. w/o clean water access.2020 [millions]'
uw.rs = rasterize(ww_plot,empty.rs,'urban_no_wa.2020',fun = sum)
names(uw.rs) = 'Urban pop. w/o clean water access.2020 [millions]'
rs.rs = rasterize(ww_plot,empty.rs,'rural_no_san.2020',fun = sum)
names(rs.rs) = 'Rural pop. w/o clean sanitation [millions]'
us.rs = rasterize(ww_plot,empty.rs,'urban_no_san.2020',fun = sum)
names(us.rs) = 'Urban pop. w/o clean sanitation.2020 [millions]'

ww.br = brick(rw.rs,uw.rs,rs.rs,us.rs)
plot(ww.br)
# OLD ####
# Income vs connection from Baum et al 2010 - model fit logistic
# income_vs_connection.df = data.frame(level = c('low','middle','upper','high'), max_income = c(1045/2,(4125-1045)/2+1045,(12375-4125)/2+4125,(30000-12375)/2+12375), connection = c(3.6,12.7,53.6,86.8)/100, treatment=c(0.02,2,13.8,78.9)/100)
# y = c(0.0001,income_vs_connection.df$connection,0.99)
# x = c(100,income_vs_connection.df$max_income,60000)
# r = nls(y ~ SSlogis(x,a,m,s))
# cp.a = 1
# cp.m = coef(r)[2]
# cp.s = coef(r)[3]
# y = c(0.0001,income_vs_connection.df$treatment,0.99)
# r = nls(y ~ SSlogis(x,a,m,s))
# tp.a = 1
# tp.m = coef(r)[2]
# tp.s = coef(r)[3]
# 
# # Get the historical data from Baum et al.
# ww.df = data.frame(read.csv( paste( data_path, '/wastewater_Baum_2013/sewerage_connection_and_treatment.csv', sep = '' ), header=TRUE, sep=',', stringsAsFactors=F, as.is=T))
# 
# ww.df$country[ which( ww.df$country == '' ) ] = 'Venezuela'
# ww.df$country_id = as.character(countrycode(ww.df$country, 'country.name', 'iso3c'))
# ww.df$income.2010 = sapply( ww.df$country_id, function( ccc ){
#   inds = which( dat.df$country == ccc )
#   return( sum( dat.df$urban_gdp.2010[ inds ] + dat.df$rural_gdp.2010[ inds ] ) / sum( dat.df$urban_pop.2010[ inds ] + dat.df$rural_pop.2010[ inds ] ) )
# } )
# ww.df2 = ww.df[ which( ww.df$income.2010 > 0 ), ]
# 
# # Plot historical connection levels and model
# cpm = data.frame( x = seq( 1, 56000, by = 1000 ), y = SSlogis( seq( 1, 56000, by = 1000 ), 0.99, cp.m, cp.s ) )
# tpm = data.frame( x = seq( 1, 56000, by = 1000 ), y = SSlogis( seq( 1, 56000, by = 1000 ), 0.99, tp.m, tp.s ) )
# #pdf(paste(getwd(),'/baum_model.pdf',sep=''))
# p1 = layout(matrix(c(1,2),1,2,byrow=TRUE),widths=c(0.45,0.45),heights=c(0.9,0.1))
# par(mar=c(5,4,5,2), oma = c(2,2,2,2))
# plot(cpm$x, 100*cpm$y, col = 'red', type = 'l', lwd = 2, xlab = 'Per Capita Income [ USD2010 ]', ylab = 'Population with Piped Water Access [ % ]', ylim = c(0,100) )
# points( ww.df2$income.2010, ww.df2$connected.2010 )
# text( 1e3, 99, 'a', cex= 1.2, font = 2 )
# text( 3.5e4, 7, expression( y == frac( 1, 1 + ~ "exp[ " ~ frac(  7516.48 - x , 2360.56 ) ~ " ]" ) ), cex = 0.75 )
# plot(tpm$x, 100*tpm$y, col = 'red', type = 'l', lwd = 2, xlab = 'Per Capita Income [ USD2010 ]', ylab = 'Population with Wastewater Treatment [ % ]', ylim = c(0,100))
# points( ww.df2$income.2010, ww.df2$treated.2010 )
# text( 1e3, 99, 'b', cex= 1.2, font = 2 )
# text( 3.5e4, 7, expression( y == frac( 1, 1 + ~ "exp[ " ~ frac(  15769.69 - x , 3871.52 ) ~ " ]" ) ), cex = 0.75 )
# dev.off()

# store for later
bbbb = dat.df

#rst = lapply( 1:2, function( add_SDG_constrain ){
#for( add_SDG_constrain in 1:3 )
for( scn in c('baseline') ) # 'sdg6'
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
      mfpc = unlist( sapply( cnt, function(cc){ return( mf_withdrawal.df[ which( as.character( mf_withdrawal.df$Scenario ) == fsc & as.character( mf_withdrawal.df$Country_Code ) == cc ), paste('X',yy,sep='') ] / sum(dat.df[ which( dat.df$country == cc ), paste('urban_pop',yy,sep= '.') ], na.rm = TRUE ) ) } ) )
      tmp.df = data.frame( country = dat.df$country )
      tmp.df$res = NA
      for( x in cnt ){ tmp.df$res[ as.character( tmp.df$country ) == x ] = dat.df[ as.character( dat.df$country ) == x, paste('urban_pop',yy,sep= '.') ] * mfpc[x ] }
      res = data.frame( tmp.df$res )
      names(res) = paste('mf_withdrawal',yy,sep= '.')
      row.names(res) = row.names(dat.df)
      return(res) } ) ) )
    dat.df = cbind( dat.df, do.call( cbind, lapply( yrs, function(yy){
      cnt = unique( as.character( dat.df$country ) )[ which( unique( as.character( dat.df$country ) ) %in% unique( as.character( mf_return.df$Country_Code ) ) ) ]
      mfpc = unlist( sapply( cnt, function(cc){ return( mf_return.df[ which( as.character( mf_return.df$Scenario ) == fsc & as.character( mf_return.df$Country_Code ) == cc ), paste('X',yy,sep='') ] / sum(dat.df[ which( dat.df$country == cc ), paste('urban_pop',yy,sep= '.') ], na.rm = TRUE ) ) } ) )
      tmp.df = data.frame( country = dat.df$country )
      tmp.df$res = NA
      for( x in cnt ){ tmp.df$res[ as.character( tmp.df$country ) == x ] = dat.df[ as.character( dat.df$country ) == x, paste('urban_pop',yy,sep= '.') ] * mfpc[x ] }
      res = data.frame( tmp.df$res )
      names(res) = paste('mf_return',yy,sep= '.')
      row.names(res) = row.names(dat.df)
      return(res) } ) ) )

    ch = scn

    # Adjust demands in urban grid cells with less than 100 L per day and for rural cells with less than 50 L per day - only for post-2030
    urb_min_sdg = 100 / 0.8
    rur_min_sdg = 50 / 0.8
    for( yyy in yrs[ yrs >= 2030 ] )
    {

      upo = dat.df[,paste('urban_pop',yyy,sep= '.')]
      rpo = dat.df[,paste('rural_pop',yyy,sep= '.')]

      upc = dat.df[,paste('urban_per_capita_withdrawal',yyy,sep= '.')] * 1e9 / 365 # convert from mcm per year to liters per day
      rpc = dat.df[,paste('rural_per_capita_withdrawal',yyy,sep= '.')] * 1e9 / 365 # convert from mcm per year to liters per day
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
      dat.df[,paste('urban_per_capita_withdrawal',yyy,sep= '.')] = upc * 1e-9 * 365
      dat.df[,paste('rural_per_capita_withdrawal',yyy,sep= '.')] = rpc * 1e-9 * 365

      dat.df[,paste('urban_withdrawal',yyy,sep= '.')] = dat.df[, paste('urban_per_capita_withdrawal', yyy, sep= '.')] * dat.df[, paste('urban_pop', yyy, sep= '.')]
      dat.df[,paste('rural_withdrawal',yyy,sep= '.')] = dat.df[, paste('rural_per_capita_withdrawal', yyy, sep= '.')] * dat.df[, paste('rural_pop', yyy, sep= '.')]

        # previously sdg6_eff
        # Further end-use conservation assumed combining a 10% reduction in withdrawals due to
        # behavioral changes and 10% recycling
        fct = 0.9 * 0.9

        dat.df[,paste('urban_withdrawal',yyy,sep= '.')] = fct * dat.df[, paste('urban_withdrawal', yyy, sep= '.')]
        dat.df[,paste('urban_return',yyy,sep= '.')] = fct * dat.df[, paste('urban_return', yyy, sep= '.')]
        dat.df[,paste('rural_withdrawal',yyy,sep= '.')] = fct * dat.df[, paste('rural_withdrawal', yyy, sep= '.')]
        dat.df[,paste('rural_return',yyy,sep= '.')] = fct * dat.df[, paste('rural_return', yyy, sep= '.')]
        dat.df[,paste('mf_withdrawal',yyy,sep= '.')] = fct * dat.df[, paste('mf_withdrawal', yyy, sep= '.')]
        dat.df[,paste('mf_return',yyy,sep= '.')] = fct * dat.df[, paste('mf_return', yyy, sep= '.')]


      inds = which( unlist( dat.df[, paste('urban_return',yyy,sep= '.') ] ) > 0.92 * unlist(dat.df[, paste('urban_withdrawal', yyy, sep= '.') ] ) )
      if( length(inds)>0 ){ dat.df[ inds, paste('urban_return',yyy,sep= '.') ] = dat.df[inds, paste('urban_withdrawal', yyy, sep= '.') ] * 0.92 }
      inds = which( unlist( dat.df[, paste('rural_return',yyy,sep= '.') ] ) > 0.92 * unlist(dat.df[, paste('rural_withdrawal', yyy, sep= '.') ] ) )
      if( length(inds)>0 ){ dat.df[ inds, paste('rural_return',yyy,sep= '.') ] = dat.df[inds, paste('rural_withdrawal', yyy, sep= '.') ] * 0.92 }
      inds = which( unlist( dat.df[, paste('mf_return',yyy,sep= '.') ] ) > 0.92 * unlist(dat.df[, paste('mf_withdrawal', yyy, sep= '.') ] ) )
      if( length(inds)>0 ){ dat.df[ inds, paste('mf_return',yyy,sep= '.') ] = dat.df[inds, paste('mf_withdrawal', yyy, sep= '.') ] * 0.92 }

    }


  }else{ # baseline


    # Add manufacturing demands - simple approach where national values generated previously are downscaled to urban areas based on population
    mf_withdrawal.df = data.frame( read.csv( paste0(data_path,'/manufacturing_water_demand_results/national/IIASA_water_withdrawal_manufacturing_Static.csv'), stringsAsFactors = FALSE ) )
    mf_return.df = data.frame( read.csv( paste0(data_path,'/manufacturing_water_demand_results/national/IIASA_water_return_manufacturing_Static.csv'), stringsAsFactors = FALSE ) )
    mf_withdrawal.df = mf_withdrawal.df[which( as.character(mf_withdrawal.df$Scenario) == 'SSP2' & !is.na(mf_withdrawal.df$UN_Code)),]
    mf_return.df = mf_return.df[which( as.character(mf_return.df$Scenario) == 'SSP2' & !is.na(mf_return.df$UN_Code)),]

    dat.df = cbind( dat.df, do.call( cbind, lapply( yrs, function(yy){
      cnt = unique( as.character( dat.df$country ) )[ which( unique( as.character( dat.df$country ) ) %in% unique( as.character( mf_withdrawal.df$Country_Code ) ) ) ]
      mfpc = unlist( sapply( cnt, function(cc){ return( mf_withdrawal.df[ which( as.character( mf_withdrawal.df$Scenario ) == 'SSP2' & as.character( mf_withdrawal.df$Country_Code ) == cc ), paste('X',yy,sep='') ] / sum(dat.df[ which( dat.df$country == cc ), paste('urban_pop',yy,sep= '.') ], na.rm = TRUE ) ) } ) )
      tmp.df = data.frame( country = dat.df$country )
      tmp.df$res = NA
      for( x in cnt ){ tmp.df$res[ as.character( tmp.df$country ) == x ] = dat.df[ as.character( dat.df$country ) == x, paste('urban_pop',yy,sep= '.') ] * mfpc[x ] }
      res = data.frame( tmp.df$res )
      names(res) = paste('mf_withdrawal',yy,sep= '.')
      row.names(res) = row.names(dat.df)
      return(res) } ) ) )
    dat.df = cbind( dat.df, do.call( cbind, lapply( yrs, function(yy){
      cnt = unique( as.character( dat.df$country ) )[ which( unique( as.character( dat.df$country ) ) %in% unique( as.character( mf_return.df$Country_Code ) ) ) ]
      mfpc = unlist( sapply( cnt, function(cc){ return( mf_return.df[ which( as.character( mf_return.df$Scenario ) == 'SSP2' & as.character( mf_return.df$Country_Code ) == cc ), paste('X',yy,sep='') ] / sum(dat.df[ which( dat.df$country == cc ), paste('urban_pop',yy,sep= '.') ], na.rm = TRUE ) ) } ) )
      tmp.df = data.frame( country = dat.df$country )
      tmp.df$res = NA
      for( x in cnt ){ tmp.df$res[ as.character( tmp.df$country ) == x ] = dat.df[ as.character( dat.df$country ) == x, paste('urban_pop',yy,sep= '.') ] * mfpc[x ] }
      res = data.frame( tmp.df$res )
      names(res) = paste('mf_return',yy,sep= '.')
      row.names(res) = row.names(dat.df)
      return(res) } ) ) )

    ch = scn

  } # end baseline
  # CHECK TILL HERE
  
  #### ATTENTION HERE IT SEEMS IT WORK WIT 10 Y TIME STEPS, see below
  # national_connection_rate.df = do.call( rbind, lapply( yrs , function(yy){
  # # MAYBE REPLACE cc with ISO3????
  #   ret1 = data.frame( do.call( rbind, lapply( unique( dat.df$country_id )[ which( unique( dat.df$country_id ) %in% as.numeric( country_region_map_key.df$UN_Code ) ) ], function(cc){
  # 
  #     ret2 = do.call( rbind, lapply( c('urban','rural'), function(tt){
  # 
  # 
  #       # Use the gridded income data and logistics model to  estimate connection levels.
  #       # Assume convergence to the model along exponential path
  #       inc_o = max( 0, sum( dat.df[ which(  dat.df$country_id == cc ) , c(  paste( tt, '_gdp.', 2010, sep='' ) ) ], na.rm=TRUE ) / sum( dat.df[ which(  dat.df$country_id == cc ) , c(  paste( tt, '_pop.', 2010, sep='' ) ) ], na.rm=TRUE ) , na.rm=TRUE )
  #       inc_f = max( 0, sum( dat.df[ which(  dat.df$country_id == cc ) , c(  paste( tt, '_gdp.', yy, sep='' ) ) ], na.rm=TRUE ) / sum( dat.df[ which(  dat.df$country_id == cc ) , c(  paste( tt, '_pop.', yy, sep='' ) ) ], na.rm=TRUE  ) , na.rm=TRUE )
  #       ind = which( ww.df$country_id == as.character( country_region_map_key.df$Region[ which( as.numeric( country_region_map_key.df$UN_Code ) == cc ) ] ) )
  #       if( length( ind ) > 0 )
  #       {
  #         c0 = ww.df$connected.2010[ ind ] / 100
  #         t0 = ww.df$treated.2010[ ind ] / 100
  #       }else
  #       {
  #         if(inc_f > 0)
  #         {
  #           c0 = c( SSlogis(inc_o,cp.a,cp.m,cp.s) )
  #           t0 = c( SSlogis(inc_o,tp.a,tp.m,tp.s) )
  #         }else
  #         {
  #           c0 = 0
  #           t0 = 0
  #         }
  #       }
  #       #### ATTENTION HERE IT SEEMS IT WORK WIT 10 Y TIME STEPS
  #       decay = max( 0, 1 / length(yrs) * log( 1 / 0.01 ), na.rm=TRUE )
  #       if( yy > 2010 )
  #       {
  #         c_mod = c( SSlogis( inc_f, cp.a, cp.m, cp.s ) )
  #         cp = ( 1 +  ( c0 / c_mod - 1 ) * exp( -1 * decay * ( which(yrs == yy) - 1 ) ) ) * c_mod
  #         if( scn %in% c('sdg6') & yy >= 2030 & cp < 0.99 ){ cp = 0.99 }
  # 
  #         t_mod = c( SSlogis( inc_f, tp.a, tp.m, tp.s ) )
  #         tp = ( 1 +  ( t0 / t_mod - 1 ) * exp( -1 * decay * ( which(yrs == yy) - 1 ) ) ) * t_mod
  #         if( scn %in% c('sdg6') & yy >= 2030 & tp < 0.5 ){ tp = 0.5 }
  # 
  #       }else
  #       {
  #         cp = c0
  #         tp = t0
  #       }
  # 
  #       ret3  = data.frame( cp = cp, tp = tp )
  #       names(ret3) = paste(tt,names(ret3),sep= '.')
  # 
  #       return( as.matrix(c(cp,tp)) )
  # 
  #     } ) )
  # 
  #     ret2 = data.frame(t(ret2))
  #     names(ret2) = c( 'urban.cp', 'urban.tp', 'rural.cp', 'rural.tp' )
  #     row.names(ret2) = paste(country_region_map_key.df$Region[ which( as.numeric( country_region_map_key.df$UN_Code ) == cc ) ],yy,sep= '.')
  #     return(ret2)
  # 
  #   } ) ) )
  # 
  #   return(ret1)
  # 
  # } ) )
  # 
  # assign( paste( 'national_connection_rate', ch, sep='_' ), national_connection_rate.df )

  # TO CHECK, NOT WORKING, but also not used
  # Limit diffusion in low-income regions according to logistic model fit saturating at per capita income for Israel
  # national_advtech_rate.df = do.call( cbind, lapply( yrs , function(yy){
  #   ret1 = data.frame( do.call( rbind, lapply( unique( dat.df$country_id ), function(cc){
  #     inc_f = max( 0, sum( dat.df[ which(  dat.df$country_id == cc ) , 
  #                                  c(  paste( 'urban', '_gdp.', yy, sep='' ) ) ], na.rm=TRUE ) / 
  #                    sum( dat.df[ which(  dat.df$country_id == cc ) , 
  #                                                      c(  paste( 'urban', '_pop.', yy, sep='' ) ) ],  na.rm=TRUE  ) 
  #                  , na.rm=TRUE )
  #     return( c( round( SSlogis( inc_f, 1, 15000, 1000 ), digits = 2 ) ) )
  #   } ) ) )
  #   names(ret1) = yy
  #   row.names(ret1) = sapply( unique( dat.df$country_id ), function(cc){ 
  #     as.character( country_region_map_key.df$Region[ which( as.numeric( country_region_map_key.df$UN_Code ) == cc ) ] ) } )
  #   return(ret1)
  # } ) )
  # 
  # assign( paste( 'national_advtech_rate', ch, sep='_' ), national_advtech_rate.df )

  # Downscale
  # dat.df = cbind( dat.df, do.call(cbind, lapply(yrs, function(yy){
  #   ret = data.frame( res = sapply( dat.df$country, function(cc){ national_connection_rate.df[paste( cc, yy, sep = '.'), 'urban.cp' ] } ) )
  #   ret$res[ which( !( unlist( dat.df[ , paste( 'urban_pop', yy, sep = '.') ] ) > 0 ) ) ] = 0
  #   names(ret) = paste( 'urban_connection_rate', yy, sep = '.')
  #   return(ret)
  # } ) ) )
  # dat.df = cbind( dat.df, do.call(cbind, lapply(yrs, function(yy){
  #   ret = data.frame( res = sapply( dat.df$country, function(cc){ national_connection_rate.df[paste( cc, yy, sep = '.'), 'urban.tp' ] } ) )
  #   ret$res[ which( !( unlist( dat.df[ , paste( 'urban_pop', yy, sep = '.') ] ) > 0 ) ) ] = 0
  #   names(ret) = paste( 'urban_treated_rate', yy, sep = '.')
  #   return(ret)
  # } ) ) )
  # dat.df = cbind( dat.df, do.call(cbind, lapply(yrs, function(yy){
  #   ret = data.frame( res = sapply( dat.df$country, function(cc){ national_connection_rate.df[paste( cc, yy, sep = '.'), 'rural.cp' ] } ) )
  #   ret$res[ which( !( unlist( dat.df[ , paste( 'rural_pop', yy, sep = '.') ] ) > 0 ) ) ] = 0
  #   names(ret) = paste( 'rural_connection_rate', yy, sep = '.')
  #   return(ret)
  # } ) ) )
  # dat.df = cbind( dat.df, do.call(cbind, lapply(yrs, function(yy){
  #   ret = data.frame( res = sapply( dat.df$country, function(cc){ national_connection_rate.df[paste( cc, yy, sep = '.'), 'rural.tp' ] } ) )
  #   ret$res[ which( !( unlist( dat.df[ , paste( 'rural_pop', yy, sep = '.') ] ) > 0 ) ) ] = 0
  #   names(ret) = paste( 'rural_treated_rate', yy, sep = '.')
  #   return(ret)
  # } ) ) )
  # 
  # dat.df[ is.na(dat.df) ] = 0

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
  cccc = dat.df
  
  for(reg in c('R11','R12')){
    #### Make spatial####
    dat.spdf = cccc
    coordinates(dat.spdf) = ~ xcord + ycord
    #load shapefile
    basin_by_region.spdf = readOGR('P:/ene.model/NEST/delineation/data/delineated_basins_new',
                                  paste0('basins_by_region_simpl_',reg), verbose=FALSE)
    basin_by_region.spdf = gBuffer(basin_by_region.spdf, byid=TRUE, width=0.2)
    
    proj4string(dat.spdf) = basin_by_region.spdf@proj4string
    
    empty_points = SpatialPoints(coords = coordinates(dat.spdf))
    proj4string(empty_points) = proj4string(dat.spdf)
    
    inters = over(empty_points,basin_by_region.spdf)
    
    inters_cols = inters %>% select(NAME,REGION,BCU_name)
    #two alternatives
    dat.spdf@data = dat.spdf@data %>% bind_cols(inters_cols)
    print(paste0(sum(is.na(inters$BCU_name)),' points have no basin infomration, are removed'))
    #
    dat.df = dat.spdf@data
    dat.df = dat.df[!is.na(dat.df$BCU_name),]
    dat.df$region = dat.df$BCU_name
    
    ysr_full = c(yrs)
    
    #including manufacturing
    regional_urban_withdrawal.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum(unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_withdrawal', yy, sep= '.') ) ] ) , na.rm=TRUE ) + sum(unlist(dat.df[which(dat.df$region == rr ) , c(paste('mf_withdrawal', yy, sep= '.') ) ] ) , na.rm=TRUE ) } ) } ) ) )
    names(regional_urban_withdrawal.df) = unique( dat.df$region )
    row.names(regional_urban_withdrawal.df) = ysr_full
    regional_urban_withdrawal.df = round( regional_urban_withdrawal.df, digits = 3 )
    
    #initialize var_list
    var_list = list('regional_urban_withdrawal' = regional_urban_withdrawal.df)
  
    # urban return flows
    regional_urban_return.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum(unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_return', yy, sep= '.') ) ] ) , na.rm=TRUE ) + sum(unlist(dat.df[which(dat.df$region == rr ) , c(paste('mf_return', yy, sep= '.') ) ] ) , na.rm=TRUE ) } ) } ) ) )
    names(regional_urban_return.df) = unique( dat.df$region )
    row.names(regional_urban_return.df) = ysr_full
    regional_urban_return.df = round( regional_urban_return.df, digits = 3 )
    var_list = append(var_list,list('regional_urban_return' = regional_urban_return.df) )
  
    # without manufacturing
    regional_urban_withdrawal2.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum(unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_withdrawal', yy, sep= '.') ) ] ) , na.rm=TRUE ) } ) } ) ) )
    names(regional_urban_withdrawal2.df) = unique( dat.df$region )
    row.names(regional_urban_withdrawal2.df) = ysr_full
    regional_urban_withdrawal2.df = round( regional_urban_withdrawal2.df, digits = 3 )
    var_list = append(var_list,list('regional_urban_withdrawal2' = regional_urban_withdrawal2.df))
  
    regional_urban_return2.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum(unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_return', yy, sep= '.') ) ] ) , na.rm=TRUE ) } ) } ) ) )
    names(regional_urban_return2.df) = unique( dat.df$region )
    row.names(regional_urban_return2.df) = ysr_full
    regional_urban_return2.df = round( regional_urban_return2.df, digits = 3 )
    var_list = append(var_list,list('regional_urban_return2' = regional_urban_return2.df))
  
    # manufacturing withdrawals
    regional_manufacturing_withdrawal.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum(unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'mf_withdrawal', yy, sep= '.') ) ] ) , na.rm=TRUE ) } ) } ) ) )
    names(regional_manufacturing_withdrawal.df) = unique( dat.df$region )
    row.names(regional_manufacturing_withdrawal.df) = ysr_full
    regional_manufacturing_withdrawal.df = round( regional_manufacturing_withdrawal.df, digits = 3 )
    var_list = append(var_list,list('regional_manufacturing_withdrawal' = regional_manufacturing_withdrawal.df))
  
    # manufacturing return flows
    regional_manufacturing_return.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){ sum(unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'mf_return', yy, sep= '.') ) ] ) , na.rm=TRUE ) } ) } ) ) )
    names(regional_manufacturing_return.df) = unique( dat.df$region )
    row.names(regional_manufacturing_return.df) = ysr_full
    regional_manufacturing_return.df = round( regional_manufacturing_return.df, digits = 3 )
    var_list = append(var_list,list('regional_manufacturing_return' = regional_manufacturing_return.df))
  
    # urban connection rate
    regional_urban_connection_rate.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){ sum(c( unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_pop', yy, sep= '.') ) ] )  * unlist(dat.df[which(dat.df$region == rr ) , c(paste('urban_connection_rate', yy, sep= '.') ) ] ) ) , na.rm=TRUE ) / sum(c(unlist(dat.df[which(dat.df$region == rr ) , c(paste('urban_pop', yy, sep= '.') ) ] ) ) , na.rm = TRUE ) } ) } ) ) )
    names(regional_urban_connection_rate.df) = unique( dat.df$region )
    row.names(regional_urban_connection_rate.df) = ysr_full
    regional_urban_connection_rate.df = round( regional_urban_connection_rate.df, digits = 3 )
    var_list = append(var_list,list('regional_urban_connection_rate' = regional_urban_connection_rate.df))
  
    #urban treatment rate
    regional_urban_treatment_rate.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum( c( unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_pop', yy, sep='.' ) ) ] )  * unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_treatment_rate', yy, sep='.' ) ) ] ) ) , na.rm=TRUE ) / sum( c( unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_pop', yy, sep='.' ) ) ] ) ) , na.rm = TRUE ) } ) } ) ) )
    names(regional_urban_treatment_rate.df ) = unique( dat.df$region )
    row.names(regional_urban_treatment_rate.df) = ysr_full
    regional_urban_treatment_rate.df = round( regional_urban_treatment_rate.df, digits = 3 )
    var_list = append(var_list,list('regional_urban_treatment_rate' = regional_urban_treatment_rate.df))
    
    ## COMMENTED FOR NOW
    # regional_urban_desalination_rate.df =  data.frame( round(  do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){ sum( unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'desalinated', yy, sep='.' ) ) ] ) , na.rm=TRUE ) / sum( unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'urban_withdrawal', yy, sep='.' ) ) ] ) + unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'mf_withdrawal', yy, sep='.' ) ) ] ) , na.rm=TRUE ) } ) } ) ), digits = 3 ) )
    # names(regional_urban_desalination_rate.df) = unique( dat.df$region )
    # row.names(regional_urban_desalination_rate.df) = ysr_full
    # regional_urban_desalination_rate.df = round( regional_urban_desalination_rate.df, digits = 3 )
    # regional_urban_desalination_rate.df[ regional_urban_desalination_rate.df < 0.005 ] = 0.005
    # var_list = append(var_list,list('regional_urban_desalination_rate' = regional_urban_desalination_rate.df))
    # # END COMMENTED PART
  
    # urban recycling rate
    regional_urban_recycling_rate.df = data.frame( round(do.call( cbind, lapply( unique( dat.df$region ), function(rr){ 
      sapply( ysr_full, function(yy){ 
        sum( (unlist(dat.df[which(dat.df$region == rr ) , c(paste('urban_withdrawal', yy, sep= '.') ) ] ) + 
                unlist(dat.df[which(dat.df$region == rr ) , c(paste('mf_withdrawal', yy, sep= '.') ) ] )) * 
               unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'recycling_rate', yy, sep= '.') ) ] ) , na.rm=TRUE ) / 
          sum(unlist(dat.df[which(dat.df$region == rr ) , c(paste('urban_withdrawal', yy, sep= '.') ) ] ) + 
                unlist(dat.df[which(dat.df$region == rr ) , c(paste('mf_withdrawal', yy, sep= '.') ) ] ) , na.rm=TRUE )
        } ) } ) ), digits = 3 ) )
    names(regional_urban_recycling_rate.df) = unique( dat.df$region )
    row.names(regional_urban_recycling_rate.df) = ysr_full
    regional_urban_recycling_rate.df = round( regional_urban_recycling_rate.df, digits = 3 )
    regional_urban_recycling_rate.df[ regional_urban_recycling_rate.df < 0.005 ] = 0.005
    var_list = append(var_list,list( 'regional_urban_recycling_rate' = regional_urban_recycling_rate.df))
  
    # rural connection rate
    regional_rural_connection_rate.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum(c( unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'rural_pop', yy, sep= '.') ) ] )  * unlist(dat.df[which(dat.df$region == rr ) , c(paste('rural_connection_rate', yy, sep= '.') ) ] ) ) , na.rm=TRUE ) / sum(c(unlist(dat.df[which(dat.df$region == rr ) , c(paste('rural_pop', yy, sep= '.') ) ] ) ) , na.rm = TRUE ) } ) } ) ) )
    names(regional_rural_connection_rate.df) = unique( dat.df$region )
    row.names(regional_rural_connection_rate.df) = ysr_full
    regional_rural_connection_rate.df = round( regional_rural_connection_rate.df, digits = 3 )
    var_list = append(var_list,list('regional_rural_connection_rate' = regional_rural_connection_rate.df))
  
    # rural treatment rates
    regional_rural_treatment_rate.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum(c( unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'rural_pop', yy, sep= '.') ) ] )  * unlist(dat.df[which(dat.df$region == rr ) , c(paste('rural_treatment_rate', yy, sep= '.') ) ] ) ) , na.rm=TRUE ) / sum(c(unlist(dat.df[which(dat.df$region == rr ) , c(paste('rural_pop', yy, sep= '.') ) ] ) ) , na.rm = TRUE ) } ) } ) ) )
    names(regional_rural_treatment_rate.df) = unique( dat.df$region )
    row.names(regional_rural_treatment_rate.df) = ysr_full
    regional_rural_treatment_rate.df = round( regional_rural_treatment_rate.df, digits = 3 )
    var_list = append(var_list,list('regional_rural_treatment_rate' = regional_rural_treatment_rate.df))
  
    # rural withdrawals
    regional_rural_withdrawal.df = data.frame( do.call( cbind, lapply( unique( dat.df$region ), function(rr){ sapply( ysr_full, function(yy){  sum(unlist( dat.df[ which( dat.df$region == rr ) , c( paste( 'rural_withdrawal', yy, sep= '.') ) ] ) , na.rm=TRUE ) } ) } ) ) )
    names(regional_rural_withdrawal.df) = unique( dat.df$region )
    row.names(regional_rural_withdrawal.df) = ysr_full
    regional_rural_withdrawal.df = round( regional_rural_withdrawal.df, digits = 3 )
    var_list = append(var_list,list('regional_rural_withdrawal' = regional_rural_withdrawal.df))
  
    # return flows, only SDG6
    regional_rural_return.df = data.frame(
      do.call( cbind, lapply( unique( dat.df$region ), function(rr){
        sapply( ysr_full, function(yy){
          sum(unlist( dat.df[ which( dat.df$region == rr ) ,
                               c( paste( 'rural_return', yy, sep= '.') ) ] ) , na.rm=TRUE ) } ) } ) ) )
    names(regional_rural_return.df) = unique( dat.df$region )
    row.names(regional_rural_return.df) = ysr_full
    regional_rural_return.df = round( regional_rural_return.df, digits = 3 )
    var_list = append(var_list,list('regional_rural_return' = regional_rural_return.df))
  
    # ADDING FINAL TIME STEPS AND SAVE csv #
    rates = names(var_list)[grepl('_rate',names(var_list))]
    for (v in names(var_list)){
      df = as.data.frame(var_list[v][[1]])
      df[is.na(df)] = 0
      n=ncol(df)+1
      
      for_lm = df %>%
        mutate(year = row.names(.)) %>%
        select(year,everything()) %>%
        filter(year >= 2060) %>%
        gather(key = 'region',value = 'value',2:n) %>%
        group_by(region) %>%
        mutate(int = lm(value ~ year)$coefficients[1],
               coef = lm(value ~ year)$coefficients[2]) %>%
        ungroup()
      # add 2100, 2110
      new_vals = for_lm %>% select(-year,-value) %>%
        distinct() %>%
        mutate(`2100` := int + (2100-2060)/10 * coef ,
               `2110` := int + (2110-2060)/10 * coef  ) %>%
        mutate(`2100` =  pmax(0,`2100` ),
               `2110` = pmax(0,`2110`) ) 
      
      if (v %in% rates) {
        new_vals = new_vals %>% 
          mutate(`2100` =  pmin(0.99,`2100` ),
                 `2110` = pmin(0.99,`2110`)) %>% 
          gather(key = year, value = 'value',`2100`,`2110`)
      }else{ new_vals = new_vals %>% 
        gather(key = year, value = 'value',`2100`,`2110`)}
      
      # add back to df
      final_df = df %>% bind_rows(
        new_vals %>% select(year,region,value) %>%
          spread(region,value) %>%
          tibble::column_to_rownames(var = 'year')
      )
    # Write in forlders according to region definition
      write.csv( final_df,
                 paste0( data_path, '/water_demands/harmonized/',reg,'/ssp',ssp,'_',
                        v,'_', ch, '.csv' ), row.names = TRUE )
  
    }

  } # for R11,R12
} # for baseline vs sdg6, obsolete
