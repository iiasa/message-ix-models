data.path = "P:/ene.model/NEST/energy_potentials_Gernaat/MESSAGE/Hydro/"

fnames = list.files(path = data.path)
fnames_cost = fnames[sapply(fnames, function(x) grepl("CostCurve", x))]   # $/kWh
fnames_maxp = fnames[sapply(fnames, function(x) grepl("MaxProd", x))]     # kWh/y

for (i in 1:length(fnames_cost)) {
  df.cost = read.delim(paste0(data.path, fnames_cost[[i]]), sep=";", header=TRUE) %>% select(-1, -length(.)) 
  a = strsplit(names(df.cost), split='[.]')
  names(df.cost) = sapply(a, "[[", 2)
  df.maxp = read.delim(paste0(data.path, fnames_maxp[[i]]), sep=";", header=TRUE) %>% select(-1) 
  names(df.maxp) = names(df.cost) 
  
  
}