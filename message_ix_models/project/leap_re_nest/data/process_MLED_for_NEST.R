# read energy demand file from MLED
rm(list = ls())
library(sf)
library(tidyverse)
library(zoo)

country = "Zambia"
iso3_c = "ZMB"
country_lc = tolower(country)

# scen_name = "baseline"
scenarios = c("baseline","improved_access","ambitious_development")
years = c(2020,2030,2040,2050,2060)
months = c(seq(1,12,1))

dem_all_scen = data.frame()
for (scen_name in scenarios){
  print(scen_name)
  data_nest = st_read(paste0("M-LED/",country_lc,"_nest_clusters_with_mled_loads_UR_",scen_name,"_tot_lat_d.gpkg"))
  # data_ons = st_read(paste0("M-LED/",country_lc,"_onsset_clusters_with_mled_loads_ssp2_rcp60_0.998_0.248_0.1_tot_lat_d.gpkg"))
  
  dat_tab = as.data.frame(data_nest)
  var_w_m = names(data_nest)[grepl("monthly",names(dat_tab))]
  
  # data from Onsset
  ons_data = read_csv(paste0( getwd(),"/OnSSET/onsset_mled_scenario_results_files_full/",scen_name,"_zm-2-0_0_0_0_0_0.csv"))
  # aggregate electrification rate at the BCU level
  
  ons_data.df = ons_data %>% select(X_deg,Y_deg,BCU,tot_dem_2020,tot_dem_2030,tot_dem_2060) %>% 
    gather(key = year, value = demand, 4:6) %>% 
    mutate(year = as.numeric(gsub("tot_dem_","",year))) %>%  
    # add isurban
    bind_cols(ons_data %>% select(X_deg,Y_deg,isurban_2020,isurban_future_2030,isurban_future_2060) %>% 
                gather(key = year_ur, value = isurban,3:5) %>% 
                mutate(year_ur = as.numeric(gsub("isurban_|isurban_future_","",year_ur))) %>% 
                rename(lat = X_deg , long = Y_deg)) %>% 
    # add electrification rate
    bind_cols(ons_data %>% select(X_deg,Y_deg,ElecStart,ElecStatusIn2030,ElecStatusIn2060) %>% 
                gather(key = year_el, value = elec,3:5) %>% 
                mutate(year_el = if_else(year_el == "ElecStart", 2020,
                                         as.numeric(gsub("ElecStatusIn","",year_el))) )  %>% 
                rename(lat2 = X_deg , long2 = Y_deg))
  
  # check if the years are matching, we can also remove the variable and just use year
  check_y = ons_data.df %>% mutate(checj_y = if_else(year == year_ur, 0,
                                           if_else(year_ur == year_el,1,2)))
  # also all the coordinates match
  tot_dem_bcu = ons_data.df %>% group_by(BCU,year,isurban) %>% 
    summarise(tot_demand = sum(demand)) %>% ungroup()
  
  act_dem = ons_data.df %>% group_by(BCU,year,isurban) %>% 
    filter(elec == 1) %>% 
    summarise(act_demand = sum(demand)) %>% ungroup()
    
  share_dem_bcu = act_dem %>% left_join(tot_dem_bcu) %>% 
    mutate(share = round(act_demand/tot_demand, digits = 2)) %>% 
    select(-act_demand,-tot_demand)
  
  empty_df = crossing(BCU = unique(share_dem_bcu$BCU),
                      year = c(2040,2050),
                      isurban = c(0,1)) %>% 
    mutate(share = NA)
  
  share_dem_int = share_dem_bcu %>% bind_rows(as.data.frame(empty_df) ) %>% 
    group_by(BCU,isurban) %>% 
    mutate(share = na.approx(share,x = year,maxgap = 4, rule = 2)) %>% ungroup()
  
  
  ggplot(share_dem_int)+
    geom_line(aes(x = year , y = share, color = as.factor(isurban) ))+
    facet_wrap(~BCU)+
    theme_bw()
    
  #because of name mismatch
  # var_wp = names(data)[grepl("water_pumping",names(dat_tab))]
  # var_wp = var_wp[!grepl("tt",var_wp)]
  dat_only_m = dat_tab %>% select(BCU,isurban,all_of(var_w_m)) %>%  # id or PID?
    mutate(id = row_number())
  
  dat_only_y = dat_tab %>% select(-all_of(var_w_m)) %>% 
    mutate(id = row_number())
  
  plot(st_geometry(data_nest))
  plot(data_nest["residential_tt_monthly_6_2030"])
  
  # questions, unit, ID, sectors
  
  dat_long_m = data.frame()
  dat_long_y = data.frame()
  for (y in years){
    var_w_y = names(dat_only_m)[grepl(y,names(dat_only_m))]
    dat_y4m = dat_only_m %>% select(BCU, isurban, all_of(var_w_y) ) %>% 
      mutate(year = y,
             unit = 'kWh/mth')
    #make a test dataframe with yearly values
    var_w_y2 = names(dat_only_y)[grepl(y,names(dat_only_y))]
    dat_y4y = dat_only_y %>% select(BCU, isurban, all_of(var_w_y2) ) %>% 
      mutate(year = y,
             unit = 'kWh/yr')
    var_y = names(dat_y4y[grepl(paste0('_tt_'), names(dat_y4y))])
    dat_y = dat_y4y %>%  
      select(BCU,isurban, year, unit, all_of(var_y))
    new_names = gsub(paste0('_tt_',y),'',names(dat_y))
    names(dat_y) = new_names
    dat_long_y = dat_long_y %>% bind_rows(dat_y)
    for (m in months){
      var_m = names(dat_y4m[grepl(paste0('_',m,"_"), names(dat_y4m))])
      dat_m = dat_y4m %>% mutate(month = m) %>% 
        select(BCU, isurban, year,month, unit, all_of(var_m))
      new_names = gsub(paste0('_',m,"_",y),'',names(dat_m))
      names(dat_m) = new_names
      dat_long_m = dat_long_m %>% bind_rows(dat_m)
    }
  }
  
  dat_df = dat_long_m %>%  
    gather(key = 'sector', value = 'value', 6:14) %>% 
    mutate(value = value * 1e-6,
           unit = "GWh/mth",
           sector = gsub('_monthly','',sector)) %>% 
    left_join(share_dem_int) %>% 
    mutate(share = if_else(is.na(share), 0, share)) %>% 
    mutate(value = share * value) #substituting value
  
  ggplot(dat_df %>% group_by(year, unit, sector) %>% 
           summarise(value = sum(value)) %>% ungroup())+
    geom_line(aes(x = year, y = value, color = sector), size = 1)+
    theme_light()+ylab("GWh/yr")+
    ggtitle(paste0("Sectoral demand: ", country, ", ", scen_name))
  
  # load message demand from country model, GWa/yr
  # dem = hist_act IEA * multiplier in Parameters_ZA.xls * growth rates for projections
  dem_mess = read.csv(paste0('P:/ene.model/NEST/',iso3_c,'/M-LED/demand_message.csv'),
           check.names = F) %>% gather('year',"value",3:8) %>% 
    mutate(year = as.numeric(year),
           value = value * 8760,
           unit = "GWh/yr")
  
  # aggregate and plot all electrciity demands, from both source
  dat_df_agg = dat_df %>% group_by(year,unit) %>% 
    summarize(value = sum(value)) %>% ungroup()
  
  #test that yearly values are the same
  dat_df_agg_y = dat_df %>% group_by(year,unit) %>% 
    summarize(value = sum(value)) %>% ungroup()
  
  dem_mess_agg = dem_mess %>% filter(sector %in% c("i_spec","rc_spec","non-comm","transport")) %>% 
    mutate(value = if_else(sector == "transport", value * 0.05,
                           if_else(sector == "non-comm", value * 0.5, value))) %>% 
    group_by(year,unit) %>% 
    summarize(value = sum(value)) %>% ungroup()
  
  dem_agg_plot = dat_df_agg %>% mutate(model = "MLED") %>% 
    bind_rows(dem_mess_agg %>% mutate(model = 'MESSAGE-IEA')) %>% 
    bind_rows(dat_df_agg_y %>% mutate(model = 'MLEDy-test'))
  
  ggplot(dem_agg_plot) +
    geom_line(aes(x = year,y = value,color = model),size = 1)+
    ylab("GWh/yr")+
    theme_light()
  
  
  # aggregate to define new MESSAGE demands
  
  #mapping of aggregation
  map_sect = data.frame(sector = unique(dat_df$sector),
                        mess_sect = c("res_com","ind_man","res_com","res_com",
                                      "agri","agri","crop","ind_man","res_com"))
  
  dem_agg4mess_UR = dat_df %>% left_join(map_sect) %>% 
    group_by(BCU,isurban,year,month,unit,mess_sect) %>% 
    summarise(value = sum(value)) %>% ungroup()
  
  dem_agg4mess = dat_df %>% left_join(map_sect) %>% 
    group_by(BCU,year,month,unit,mess_sect) %>% 
    summarise(value = sum(value)) %>% ungroup()
  
  # plot monthly trends
  ggplot(dem_agg4mess %>% filter(year == 2020))+
    geom_line(aes(x = month, y = value, color = mess_sect) )+
    facet_wrap(~BCU)+
    theme_light()
  
  dem_agg_y = dem_agg4mess %>% group_by(year,mess_sect) %>% 
    summarise(value = sum(value)) %>% ungroup()
  
  dem_all_scen = dem_all_scen %>% bind_rows(dem_agg_y %>% mutate(scenario = scen_name))
  
  # calculate growth rates that can be used for electricity and other demand
  dem_agg_y_inc = dem_agg_y %>% group_by(mess_sect) %>% 
    mutate(hist = first(value),
           inc = (value - hist ) / hist*100 )
  clipr::write_clip(dem_agg_y_inc)
  #save
  save_name = gsub("_.*","",scen_name)
  write.csv(dem_agg4mess_UR,paste0("M-LED/electricity_demand_MLED_NEST_GWh_mth_",save_name,".csv"),row.names = F)
  print(paste0(scen_name, " processed and saved!") )
  #check
  a = dem_agg4mess %>% filter(year == 2020, mess_sect != 'agri')
  sum(a$value)
}

# plot annual trends
pl = ggplot(dem_all_scen %>% mutate(scenario = as.factor(scenario)) %>% 
              filter(year != 2060,
            mess_sect != "agri") )+
  geom_line(aes(x = year,y = value,color = mess_sect,
                linetype = scenario),size = 1)+
  theme_classic()+ylab("GWh/year")+
  facet_wrap(~mess_sect,scales = "free_y",ncol = 1)+
  ggtitle(paste0("Sectoral demand: ", country))

png(paste0("out_figures/message_sectoral_demand_",country,"_all_scen.png"),
    height = 10,width = 14,units = "cm",res = 300)
# pdf(file = paste0('Plots/pl_sdg',n,".pdf"), useDingbats=FALSE,
#     height = 4,width = 5.5)
print(pl)
dev.off()
