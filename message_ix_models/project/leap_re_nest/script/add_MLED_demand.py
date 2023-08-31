# add MLED demand and create distribution line technologies
# also using OnSSeT data
from message_ix_models.util import (broadcast,private_data_path)
import numpy as np
import pandas as pd

def dem_from_csv(sc,csv_file):
    
    strings = ["B"+ str(x) for x in csv_file['BCU']]
    csv_file['BCU'] = strings
    
    nodes = list(sc.set('node'))
    nodes_df = pd.DataFrame({
        "node" : nodes,
        "BCU" : [x.split("|")[0] for x in nodes]
        })
    
    dem = csv_file.merge(nodes_df,how = 'left')
    dem = dem[dem['mess_sect'] != "agri"]
    dem_names = sc.idx_names('demand')
    dem_ur = dem[dem["isurban"] == 1]
    dem_ur['mess_sect'] = dem_ur['mess_sect']+ "_urb"
    dem_ur = dem_ur[dem_ur['mess_sect'] != "crop_urb"]
    dem_rur = dem[dem["isurban"] == 0]
    dem_rur['mess_sect'] = dem_rur['mess_sect']+ "_rur"
    
    dem = pd.concat([dem_ur,dem_rur]) 
    dem['value'] = dem['value'] / 8760 # from GWh/month to GWa/m
    dem.drop(columns=["isurban"],inplace = True)
    dem.columns = ['id', 'year', 'time', 'unit', 'commodity', 'value', 'node']
    dem['level'] = "useful"
    dem['unit'] = 'GWa/a' # actual unit is GWa/m
    dem = dem[[*dem_names,'unit','value']]
    
    return dem


def main(sc,ss):
    """ This script removes old electricity demand from MESSAGE and
    replaces it with estimated demand from MLED, with sub-annual timestep 
    (by adding the technologies...)
    """

    #load previous total demand in firstmodelyear
    firstmy = sc.firstmodelyear
    dem_el_old = sc.par("demand",{"year" : firstmy,
                                  "commodity" : ["i_spec","rc_spec"]})
    tot_dem_el_old = sum(dem_el_old.value) # GWa/a
    
    dem_remove = sc.par("demand", {"commodity" : ["i_spec","rc_spec"]})
    sc.check_out()
    sc.remove_par("demand",dem_remove)
    print("Removed old electricity demand: " + str(tot_dem_el_old) + 
          " GWa/a")
    
    file = "electricity_demand_MLED_NEST_GWh_mth_" + ss + ".csv"
    path_csv = private_data_path('projects','leap_re_nest',file)
    
    dem_csv = pd.read_csv(path_csv)
    dem = dem_from_csv(sc,dem_csv)
    tot_dem_el_new = sum(dem[dem["year"] == firstmy]["value"])
    
    #add demand
    sc.add_set('commodity',["ind_man_urb","ind_man_rur",
                            "res_com_urb","res_com_rur","crop_rur"])
    sc.add_par('demand',dem)
    sc.commit("")
    print('Electricity demand from MLED added ' + str(tot_dem_el_new) + 
          " GWa/a")
    


if __name__ == '__main__':
    import sys
    # parse sys.argv[1:] using optparse or argparse or what have you
    main('test')
    
    
    
    
    
    
    
