# utils for NEST model
import pandas as pd
from message_ix import make_df
from message_ix_models.util import (private_data_path,broadcast)

def map_basin(sc):
    """Return specification for mapping basins to regions

    The basins are spatially consolidated from HydroSHEDS basins delineation
    database.This delineation is then intersected with MESSAGE regions to form new
    water sector regions for the nexus module.
    The nomenclature for basin names is <basin_id>|<MESSAGEregion> such as R1|AFR
    """


    # define an empty dictionary
    results = {}
    # read csv file for basin names and region mapping
    # reading basin_delineation
    FILE = f"basins_by_region_simpl_ZMB.csv"
    PATH = private_data_path("projects","leap_re_nest", "delineation", FILE)

    df = pd.read_csv(PATH)
    # Assigning proper nomenclature
    df["node"] = "B" + df["BCU_name"].astype(str)
    df["mode"] = "M" + df["BCU_name"].astype(str)
    df["region"] = df["REGION"].astype(str)
    results["node"] = df["node"]
    results["mode"] = df["mode"]
    # map nodes as per dimensions
    df1 = pd.DataFrame({"node_parent": df["region"], "node": df["node"]})
    df2 = pd.DataFrame({"node_parent": df["node"], "node": df["node"]})
    frame = [df1, df2]
    df_node = pd.concat(frame)
    nodes = df_node.values.tolist()

    results["map_node"] = nodes

    # context.all_nodes = df["node"]

    for set_name, config in results.items():
        
        print("Adding set",set_name)
        # print("config",config)
        # Sets  to add
        sc.add_set(set_name,config)
        
    print("sets for nodes updated")
    
def add_cap_bound_fossil(sc):
    """Return bound_new_capacity_up in MESSAGE for all regionas 
    and all fossil fuel technologies
    """
    fy = sc.firstmodelyear
    fossil_tec = ["foil_ppl", "loil_cc", "loil_ppl", 
                 "gas_cc", "gas_cc_ccs", "gas_ct", 
                  "gas_ppl", "coal_adv", "coal_adv_ccs",
                  "coal_ppl", "coal_ppl_u", "igcc", "igcc_ccs"]
    all_y = sc.set("year")
    reg = sc.set("node")[1] # the zero should always be World, 1 the country
    yrs = list(all_y[all_y > fy])
    bncu = (make_df(
                "bound_new_capacity_up",
                node_loc=reg,
                technology=fossil_tec,
                value=0,
                unit="GWa",
            )
        .pipe(broadcast, year_vtg=yrs)
        )
    act_growth = sc.par("growth_activity_lo")
    to_remove = ["coal","gas","oil","loil","foil","LNG"]
    act_g_rem = (act_growth[act_growth["technology"]
                            .str.contains("|".join(to_remove))]
                 )
    # also need to remobe
    sc.check_out()
    sc.add_par("bound_new_capacity_up", bncu)
    #remove parts of the bound_act_lo
    sc.remove_par("growth_activity_lo", act_g_rem)
    sc.commit("No future fossil fuel capcity constraint is now set up.")
    print("No future fossil fuel capcity constraint is now set up.")
    