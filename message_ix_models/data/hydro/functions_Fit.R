
FitStepFunc <- function(data.sprd, nstep, nregion = 11, region, s=s.idx, eps = 1e-6) {
  final_cost_df0 = c()
  rsq.val <- c()
  cp_record <- data.frame(matrix(ncol = nregion+1, nrow = 0))
  Dn_record <- data.frame(matrix(ncol = nregion+1, nrow = 0))
  rho_record <- data.frame(matrix(ncol = nregion+1, nrow = 0))
  names(cp_record) = region
  names(Dn_record) = region
  names(rho_record) = region
  cp_record[1,] = 0.01
  Dn_record[1,] = 1
  rho_record[1,] = 0.001
  
  for (c in unique(data.sprd$code)) {
    for (j in seq(1, nregion, 1)) {
      z=1
      minsplit_z = 2
      fit = 1 # was able to fit? = 1
      repeat{  
        cp_i=0.01
        i=1
        rho = 0.001
        repeat{
          tree_cost <- rpart(get(paste(region[j])) ~ x, 
                             data = data.sprd %>% filter(code==c), 
                             control=rpart.control (minsplit = minsplit_z, cp = cp_i))
          nsplit <- max(as.data.frame(tree_cost$cptable)$nsplit)
          if (nsplit+1 == nstep) {break} else {
            i = i+1
            Dn_record[i,j] = nsplit+1-nstep
            cp_i = max(0, cp_i*(1+ rho * Dn_record[i,j]) )
            cp_record[i,j] = cp_i
            if (Dn_record[c(i-1),j]*Dn_record[i,j]>0) {rho = 1.5*rho} else{rho = 0.5* rho}
          }
          rho_record[i,j] = paste0(minsplit_z, "_", rho )
          if ((i>5000) | (abs(cp_record[i,j]-cp_record[c(i-1),j]+eps/100) < eps ) ) {
            print(paste0("In region ", region[j], " increase minsplit to ",minsplit_z+1))
            break}
        }
        if (nsplit+1 == nstep) {break} else {
          minsplit_z <- minsplit_z+1
          #      Dn_record[,j] = 0
          #      Dn_record[1,j] = 1
        }
        if (minsplit_z > 100/nstep) {
          print(paste0("The curve in region ", region[j], " can't be fitted with nstep= ", nstep))
          fit = 0
          break}
      }
      # print(tree_cost$cptable)
      rsq.val <- bind_rows(rsq.val, data.frame(code = c, msg_reg = region[j], rsq =  (1-printcp(tree_cost)[, c(3)]), 
                                               n_split = seq(1, dim(tree_cost$cptable)[1], 1) ) )
      # n_split = seq(1, nstep, 1) ) )
      frame_cost0 <- as.data.frame(predict(tree_cost, data.frame(x=s))) %>% mutate(x = s)
      if (fit==0) {
        frame_cost0 <- data.sprd %>% select(LF_agg, x) # Overwrite with original if not fittable. (whene more steps than the number of data)
      }
      names(frame_cost0)[1] = "yval"
      frame_cost_j <- frame_cost0 %>% group_by(yval) %>%
        mutate(xval = tail(x,1)) %>%
        select(-x) %>%
        distinct() %>% 
        ungroup() %>% 
        mutate(code = c, msg_reg = region[j])
      
      final_cost_df0 <- rbind(final_cost_df0, frame_cost_j)
      if (nsplit+1 == nstep) print(paste0("Region ",region[j]," completed"))
    }
  }
  
  return(list(final_cost_df0, rsq.val))
}

ReadIMAGEData <- function(res, # National or Regional
                                 focus.res # Names of regions/nations
                                 ) {
  if (res=="Nat") {
    cost_curves.sprd <- read_excel("HYDRO_cost_country_Gernaat et al. - JM.xlsx", 
                                   sheet = "CAP_COST")
    max_pot <- read_excel("HYDRO_cost_country_Gernaat et al. - JM.xlsx", 
                          sheet = "MAX_POTENTIAL")
    load_fact <- read_excel("HYDRO_cost_country_Gernaat et al. - JM.xlsx", 
                            sheet = "LOAD_FACTOR")
    
    # Remove countries with 0 curve
    cost_curves.sprd <- cost_curves.sprd[, -which(colSums(cost_curves.sprd)==0)]
    load_fact <- load_fact[, -which(colSums(load_fact)==0)]
    max_pot <- max_pot[, -which(colSums(max_pot)==0)]
    
    region <- countrycode(names(cost_curves.sprd)[-1], 'country.name', 'iso3c')
    
    ## cost_curves
    names(cost_curves.sprd) = c("x", region)
    names(load_fact) = c("x", region) 
    names(max_pot) = region 
    
    # Remove x=0 row
    load_fact <- load_fact %>% slice(-1)
    cost_curves.sprd <- cost_curves.sprd %>% slice(-1)
    
    # Reorder based on cost curves (JMin)
    cost_curves <- cost_curves.sprd %>% gather(msg_reg, cost_agg, -x)
    load_fact.long <- load_fact %>% gather(msg_reg, LF_agg, -x)
    data.comb <- cost_curves %>% left_join(load_fact.long) %>% arrange(msg_reg, cost_agg) %>% group_by(msg_reg) %>% mutate(x=seq(0.01, 1, 0.01))
    
    cost_curves.sprd <- data.comb %>% select(-LF_agg) %>% spread(msg_reg, cost_agg) # Adriano keeps only this as wide form.
  } else if (res=="R12" | res=="R11") {
    fname = paste0("HYDRO_cost_MESSAGE_", res, "_update.ordered.xlsx")
    cost_curves <- read_excel(fname, sheet = "CAP_COST")
    load_fact <- read_excel(fname,sheet = "LOAD_FACTOR")
    max_pot <- read_excel(fname, sheet = "MAX_POTENTIAL")
    # a <- data.frame(t(max_pot[,2]))
    # names(a) <- t(max_pot[,1])
    # max_pot <- a
    
    # Reorder cost curves (JM)
    data.comb <- cost_curves %>% left_join(load_fact) %>% 
      # arrange(msg_reg, cost_agg) %>% # Already arranged in the xlsx file
      group_by(code, msg_reg) %>% # %>% mutate(x=seq(0,1,.01))
      filter(!is.na(msg_reg))
    cost_curves.sprd <- data.comb %>% select(-LF_agg) %>%
      # spread(msg_reg,cost_agg)
      pivot_wider( names_from = msg_reg, values_from = cost_agg)
    
    region <- tail(names(cost_curves.sprd), -2)
  }
  
  max_pot <- max_pot %>%
    mutate(pot_agg = pot_agg* 0.000277777778 / 365 / 24)   # From GJ to GWa

  return(list(data.comb, cost_curves.sprd, max_pot, region))
}


GenerateInvCostCurve <- function(focus.res, # Names of regions/nations
                                 nstep, # # of steps in the curve
                                 cost_curves
) {  
  ### USE REGRESSION TREE TO FIT THE CURVES WITH STEP FUNCTION ###
  #----
  # define the number of steps desired
  s.idx = cost_curves.sprd$x
  # to make the first step for plotting
  a= data.frame(msg_reg = as.character(head(focus.res,-1)), xval = 0)
  a$msg_reg = as.character(a$msg_reg)
  eps = 1e-6
  
  # list[final_cost_df0, rsq.val] <- FitStepFunc(cost_curves.sprd, nstep, nregion = nregion, region)
  list[final_cost_df0, rsq.val] <- FitStepFunc(cost_curves.sprd %>% select(code, x, !!focus.res), 
                                               nstep, nregion = length(focus.res), region = focus.res, s.idx)
  
  
  ### Check R-sq for the fit
  
  # plot r squared
  rsq.plot <- rsq.val %>% 
    filter(n_split > 1 )
  # ggplot(rsq.plot)+
  #   geom_boxplot(aes(msg_reg,rsq))+ggtitle(paste0("R-squared with ",nstep," steps") )
  
  rsq.ord <- rsq.val %>% filter(n_split==nstep)
  
  return(final_cost_df0)
}



GenerateCapacityFactorCurve <- function(res, # National or Regional
                                 focus.res, # Names of regions/nations
                                 n.substep, # # of substeps in each step
                                 data.comb, # main data read from above
                                 max_pot
                                 ) {
  load_fact <- data.comb %>% select(-cost_agg) %>% filter(msg_reg %in% focus.res)
  # max_pot <- data.frame(t(max_pot)) %>% rownames_to_column(var="msg_reg")
  
  annual_avg_lf1 <- left_join(load_fact, final_cost_df0 %>% rename(x=xval),by = c("code", "x", "msg_reg")) %>% 
    mutate(xval = yval/yval*x) %>% 
    group_by(code, msg_reg) %>% 
    mutate(xval = na.locf(xval,na.rm = F, fromLast = T)) %>% 
    select(-yval,-x) %>% 
    ungroup()
  
  # Reorder LF within each interval
  annual_avg_lf1.ord <- annual_avg_lf1 %>% arrange(code, msg_reg, xval, -LF_agg) %>% group_by(code, msg_reg) %>%
    mutate(x=seq(0.01, 1, 0.01)) %>% left_join(max_pot) %>%
    # mutate(x = x*(max_pot %>% select(!!focus.res) %>% as.numeric()))
    mutate(x = x*pot_agg) 
  
  sub_LF.all.c <- list() # for each code
  for (c in unique(data.comb$code)) {
    # steps <- unique(annual_avg_lf1$xval)
    annual_avg_lf1.ord.c = annual_avg_lf1.ord %>%
      filter(code == c) 
    
    steps.all <- annual_avg_lf1.ord.c %>% 
      count(xval)
    
    # Empty list with names of focus.res
    sub_LF.all <- vector("list", length(focus.res)) 
    names(sub_LF.all) <- focus.res
    # org_LF.all <- vector("list", length(focus.res)) 
    # names(org_LF.all) <- focus.res
    
    for (j in focus.res) {
      sub_LF <- list()
      steps <- (steps.all %>% filter(msg_reg == j) %>% ungroup())$xval
      for (i in 1:length(steps)) {
        temp <- annual_avg_lf1.ord.c %>% filter(msg_reg == j) %>% filter(xval==steps[i]) %>% ungroup()
        
        list[sub_LF[[i]], rsq.sub] <- FitStepFunc(temp %>% select(code, x, LF_agg), 
                                                  n.substep, nregion = 1, region = "LF_agg", s=temp$x)
      }
      sub_LF.all[[j]] <- bind_rows(sub_LF) %>% rename(avgLF=yval) %>% mutate(code = c, msg_reg=j)
      sub_LF.all[[j]] <- sub_LF.all[[j]] %>% 
        slice(1) %>% 
        mutate(xval=0) %>% rbind(sub_LF.all[[j]]) 
      
      # org_LF.all[[j]] <- annual_avg_lf1.ord
    }
    # Combine all regions/steps of a code
    sub_LF.all.c[[c]] = bind_rows(sub_LF.all)
  }
  
  return(list(bind_rows(sub_LF.all.c), annual_avg_lf1.ord))
}