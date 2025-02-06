import ixmp
import message_ix
import numpy as np
import pandas as pd
import yaml

from collections.abc import Mapping
from itertools import repeat
from message_ix.models import MESSAGE_ITEMS
from message_ix.utils import make_df

from message_ix_models.tools.add_dac import add_tech
from message_ix_models.tools.costs.config import Config
from message_ix_models.tools.costs.projections import create_cost_projections

from typing import Literal

%matplotlib inline

# CO2 storage potential from Matt and Sidd
R12_potential = {
    'R12_FSU': 83310.661,
    'R12_LAM': 55868.988,
    'R12_WEU': 11793.358,
    'R12_EEU':  1808.370,
    'R12_AFR': 61399.222,
    'R12_MEA': 57369.171,
    'R12_NAM': 58983.981,
    'R12_SAS':  4765.219,
    'R12_PAS': 21108.578,
    'R12_PAO': 34233.825,
    'R12_CHN': 15712.475,
    'R12_RCPA': 3754.285,
}

# max rate in MtC per year
max_rate = np.round(15000/3.667,0)

# technology modes
modes = ["M1","M2","M3"]

# length for each periods
len_periods = {2025: 5, 2030: 5, 
               2035: 5, 2040: 5, 
               2045: 5, 2050: 5, 
               2055: 5, 2060: 5, 
               2070:10, 2080:10, 
               2090:10, 2100:10, 
               2110:10}

# nodes
nodes = ['R12_AFR', 'R12_EEU', 'R12_LAM', 
         'R12_MEA', 'R12_NAM', 'R12_SAS', 
         'R12_WEU', 'R12_FSU', 'R12_PAO', 
         'R12_PAS', 'R12_CHN', 'R12_RCPA', 'R12_GLB']

# years
years = [year for year in list(len_periods.keys()) 
         if year > 2025]


# SSPs to run
ssps = ["LED","SSP1","SSP2","SSP3","SSP4","SSP5"]
ssps = ["SSP2"]

# SSPs CCS parameters
ccs_ssp_pars = {
    "LED":{
        "co2storage": 0.25,
        "co2rate"   : 4000/3.667,
    },
    "SSP1":{
        "co2storage": 0.25,
        "co2rate"   : 4000/3.667,
    },
    "SSP2":{
        "co2storage": 0.50,
        "co2rate"   : 9000/3.667,
    },
    "SSP3":{
        "co2storage": 1.00,
        "co2rate"   : 15000/3.667,
    },
    "SSP4":{
        "co2storage": 1.00,
        "co2rate"   : 15000/3.667,
    },
    "SSP5":{
        "co2storage": 1.00,
        "co2rate"   : 15000/3.667,
    },
}


# scenario parameters list for edit
co2_pipes = ['co2_tr_dis', 'bco2_tr_dis']
co2_pipes_par = [
    'inv_cost',
    'fix_cost',
    'input',
    'capacity_factor',
    'technical_lifetime',
    'construction_time',
    'abs_cost_activity_soft_up',
    'growth_activity_lo',
    'level_cost_activity_soft_lo',
    'level_cost_activity_soft_up',
    'relation_activity',
    'var_cost',
    'output',
    'emission_factor',
    'soft_activity_lo',
    'soft_activity_up',
    'growth_activity_up',
    'initial_activity_up',
]

ccs_techs = [
    # BECCS
    'bio_istig_ccs',
    'biomass_NH3_ccs',
    'bio_ppl_co2scr',
    'eth_bio_ccs',
    'meth_bio_ccs',
    'h2_bio_ccs',
    'liq_bio_ccs',
    # Fossil and Industrial CCS
    'bf_ccs_steel',
    'c_ppl_co2scr',
    'clinker_dry_ccs_cement',
    'clinker_wet_ccs_cement',
    'coal_adv_ccs',
    'coal_NH3_ccs',
    'dri_gas_ccs_steel',
    'fueloil_NH3_ccs',
    'g_ppl_co2scr',
    'gas_cc_ccs', 
    'gas_NH3_ccs',
    'h2_coal_ccs',
    'h2_smr_ccs',
    'igcc_ccs',
    'meth_coal_ccs',
    'meth_ng_ccs',
    'syn_liq_ccs',
    # DACCS
    "dac_lt",
    "dac_hte",
    "dac_htg",
]

dac_techs = [
    "dac_lt", 
    "dac_hte", 
    "dac_htg"
]

def gen_te_projections(
    scen: message_ix.Scenario,
    ssp: Literal["all", "LED", "SSP1", "SSP2", "SSP3", "SSP4", "SSP5"] = "SSP2",
    method: Literal["constant", "convergence", "gdp"] = "convergence",
    ref_reg: str = "R12_NAM",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calls message_ix_models.tools.costs with config for MESSAGEix-Materials
    and return inv_cost and fix_cost projections for energy and materials
    technologies

    Parameters
    ----------
    scen: message_ix.Scenario
        Scenario instance is required to get technology set
    ssp: str
        SSP to use for projection assumptions
    method: str
        method to use for cost convergence over time
    ref_reg: str
        reference region to use for regional cost differentiation

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        tuple with "inv_cost" and "fix_cost" DataFrames
    """
    model_tec_set = dac_techs
    cfg = Config(
        module="dac",
        ref_region=ref_reg,
        method=method,
        format="message",
        scenario=ssp,
    )
    out_materials = create_cost_projections(cfg)
    fix_cost = (
        out_materials["fix_cost"]
        .drop_duplicates()
        .drop(["scenario_version", "scenario"], axis=1)
    )
    fix_cost = fix_cost[fix_cost["technology"].isin(model_tec_set)]
    inv_cost = (
        out_materials["inv_cost"]
        .drop_duplicates()
        .drop(["scenario_version", "scenario"], axis=1)
    )
    inv_cost = inv_cost[inv_cost["technology"].isin(model_tec_set)]
    return inv_cost, fix_cost

for ssp in ssps:
    mp = ixmp.Platform()
    
    # calling base scenario
    model_name = f'SSP_dev_{ssp}_v1.0_testco2v3'
    scen_name  = "baseline"
    
    base_scen = message_ix.Scenario(
        mp, 
        model = model_name, 
        scenario = scen_name
    )
    
    # clone scenario to add CCS infrastructure and DAC
    scen = base_scen.clone(
        f"SSP_dev_{ssp}_v1.0_testco2v3_split",
        "baseline",
        "scenario with CCS infrastructure and DAC",
        keep_solution=False, 
    )
    scen.check_out()

    
    # ==============================================
    # Remove old setup ================================
    ## rem. relations
    rels = ['co2_trans_disp',
            'bco2_trans_disp',
            'CO2_Emission_Global_Total', 
            'CO2_Emission','CO2_PtX_trans_disp_split']

    scen.remove_par(
        "relation_activity", 
        scen.par("relation_activity",
                 {'technology':co2_pipes + ccs_techs,
                  'relation':rels})
    )
    
    ## rem. 'CO2_PtX_trans_disp_split' fro relation set
    scen.remove_set('relation','CO2_PtX_trans_disp_split')
    
    ## rem. pipelines
    for par in co2_pipes_par:
        scen.remove_par(
            par, scen.par(par,
                          {'technology':co2_pipes})
        )

    ## rem. co2 pipeline sets    
    scen.remove_set('technology',co2_pipes)
    scen.remove_set(
        'relation', 
        ['co2_trans_disp', 'bco2_trans_disp']
    )
    
    
    # ==============================================
    # Add new setup ================================
    ## setup pipelines, storage, and non-dac ccs technologies
    filepath = r'C:\Users\pratama\Documents\GitHub\MESSAGEix\message-ix-models\message_ix_models\data\ccs-dac'
    add_tech(scen,filepath=filepath+f'\co2infrastructure_data_{ssp}dev.yaml')
    
    ## setup dac technologies
    filepath = r'C:\Users\pratama\Documents\GitHub\MESSAGEix\message-ix-models\message_ix_models\data\ccs-dac'
    add_tech(scen,filepath=filepath+f'\daccs_setup_data_{ssp}dev.yaml')
    
    ## add dac costs using meas's tool
    ##> making the projection
    inv_cost_dac, fix_cost_dac  = gen_te_projections(scen, ssp, 'gdp')

    inv_cost_dac = inv_cost_dac[inv_cost_dac['technology'].isin(dac_techs)]
    fix_cost_dac = fix_cost_dac[fix_cost_dac['technology'].isin(dac_techs)]
    
    ##> adding the costs to the model
    scen.add_par('inv_cost', inv_cost_dac)
    scen.add_par('fix_cost', fix_cost_dac)
    
    ## removing excess year_act
    pars2remove = ['capacity_factor','fix_cost','input','output']
    ##> use 2030 R12_NAM as basis to get technology lifetime
    lt = scen.par("technical_lifetime", 
                    {"technology":ccs_techs, 
                     "node_loc":"R12_NAM", 
                     "year_vtg":2030})
    for par in pars2remove:
        df2remove = []
        df = scen.par(par,{'technology':ccs_techs})
        rem_techs = ccs_techs if par == 'output' else dac_techs
        for tech in rem_techs:
            lt_tech = np.int32(lt[lt["technology"]==tech]["value"].iloc[0])
            df2remove_tech = df[df['year_act'] > df['year_vtg'].add(lt_tech)]
            df2remove_tech = df2remove_tech[df2remove_tech['technology'] == tech]
            df2remove += [df2remove_tech]
        df2remove = pd.concat(df2remove)
        scen.remove_par(par, df2remove)
    
    ## make pipelines and storage a single period technology
    newpipesnstors = ['co2_stor','co2_trans1', 'co2_trans2']
    pars2remove = ['var_cost','input','output','emission_factor','capacity_factor']
    for par in pars2remove:
        df = scen.par(par,{'technology':newpipesnstors})
        df= df.loc[df['year_vtg'] != df['year_act']]
        scen.remove_par(par, df)

        
    # ==============================================
    # Setup technology and relations to track cumulative storage
    ## adding new set and technologies
    scen.add_set("technology","co2_storcumulative")
    
    ## each storage mode is represented by one relation
    for mode in modes:
        scen.add_set("relation", f"co2_storcum_{mode}")

    ## create relation activity
    list_relation = []
    for node in nodes:
        for mode in modes:
            for yr in years:
                ya = [y for y in years if y <= yr]
                relact_co2stor = make_df("relation_activity",
                            relation=f"co2_storcum_{mode}",
                            node_rel=node,
                            year_rel=yr,
                            node_loc=node,
                            technology="co2_stor",
                            year_act=ya,
                            mode=mode,
                            value= [-1 * len_periods[y] for y in ya],
                            unit = "-"
                       )

                relact_co2storcumulative = make_df("relation_activity",
                            relation=f"co2_storcum_{mode}",
                            node_rel=node,
                            year_rel=yr,
                            node_loc=node,
                            technology="co2_storcumulative",
                            year_act=yr,
                            mode=mode,
                            value=1,
                            unit = "-"
                       )
                list_relation += [relact_co2stor, relact_co2storcumulative]
    df_relation = pd.concat(list_relation)

    ## create relation bounds
    list_rel_eq = []
    for node in nodes:
        for mode in modes:
            rel_eq = make_df("relation_upper",
                    relation = f"co2_storcum_{mode}",
                    node_rel = node,
                    year_rel = years,
                    value = 0,
                    unit = "-"
                   )
            list_rel_eq += [rel_eq]
    df_rel_eq = pd.concat(list_rel_eq)

    ## adding parameters
    scen.add_par("relation_activity", df_relation)
    scen.add_par("relation_upper", df_rel_eq)
    scen.add_par("relation_lower", df_rel_eq)
    
    
    # adding set up for limiting CO2 storage availabilities
    nodes = [node for node in nodes if node not in ["R12_GLB","World"]]
    df_list = []
    for node in nodes:
        for year in years:
            df = make_df("bound_activity_up",
                         node_loc=node,
                         technology="co2_storcumulative",
                         year_act=year,
                         mode="all",
                         time="year",
                         value = (R12_potential[node] 
                                  * ccs_ssp_pars[ssp]["co2storage"]),
                         unit = "???",
                        )
            df_list += [df]
    df_stor = pd.concat(df_list)
    
    scen.add_par("bound_activity_up", df_stor)

    
    # ==============================================
    # Setup tech and relations to limit global CO2 injection
    
    ## adding new set and technologies
    scen.add_set("technology","co2_stor_glb")
    for mode in modes:
        scen.add_set("relation",f"co2_storglobal_{mode}")

    ## create relation activity
    list_relation = []
    for mode in modes:
        for yr in years:
            for node in nodes:
                relact_co2stor = make_df("relation_activity",
                            relation=f"co2_storglobal_{mode}",
                            node_rel="R12_GLB",
                            year_rel=yr,
                            node_loc=node,
                            technology="co2_stor",
                            year_act=yr,
                            mode=mode,
                            value= -1,
                            unit = "-"
                       )
                list_relation += [relact_co2stor]
            relact_co2stor_glb = make_df("relation_activity",
                        relation=f"co2_storglobal_{mode}",
                        node_rel="R12_GLB",
                        year_rel=yr,
                        node_loc="R12_GLB",
                        technology="co2_stor_glb",
                        year_act=yr,
                        mode=mode,
                        value=1,
                        unit = "-"
                   )
            list_relation += [relact_co2stor_glb]
    df_relation = pd.concat(list_relation)

    ## create relation bounds
    list_rel_eq = []
    for mode in modes:
        rel_eq = make_df("relation_upper",
                relation = f"co2_storglobal_{mode}",
                node_rel = "R12_GLB",
                year_rel = years,
                value = 0,
                unit = "-"
               )
        list_rel_eq += [rel_eq]
    df_rel_eq = pd.concat(list_rel_eq)

    ## adding parameters
    scen.add_par("relation_activity", df_relation)
    scen.add_par("relation_upper", df_rel_eq)
    scen.add_par("relation_lower", df_rel_eq)
    
    df_list = []
    for year in years:
        df = make_df("bound_activity_up",
                     node_loc="R12_GLB",
                     technology="co2_stor_glb",
                     year_act=year,
                     mode="all",
                     time="year",
                     value = ccs_ssp_pars[ssp]["co2rate"],
                     unit = "???",
                    )
        df_list += [df]
    df_co2ratelim = pd.concat(df_list)
    
    ## adding parameters
    scen.add_par("bound_activity_up", df_co2ratelim)
    
    
    # ==============================================
    ## Setup relation_upper and _lower for DAC market penetration limit
    rels = ["DAC_mpen_c"]
    df_list = []
    for rel in rels:
        for node in nodes:
            df = make_df("relation_upper",
                         relation=rel,
                         node_rel=node,
                         year_rel=years,
                         unit = "-",
                         value = 0
                        )
            df_list = df_list + [df]
    dfpar2add = pd.concat(df_list)
    scen.add_par("relation_upper", dfpar2add)
    scen.add_par("relation_lower", dfpar2add)
    
    scen.commit(comment=f"{ssp} baseline with dac and CCS infrastructure")
    
    with scen.transact(""):
        scen.add_set("balance_equality", ["bic_co2", "secondary"])
        scen.add_set("balance_equality", ["fic_co2", "secondary"])
        scen.add_set("balance_equality", ["dac_co2", "secondary"])
        scen.add_set("balance_equality", ["methanol", "final_material"])
        scen.add_set("balance_equality", ["HVC", "demand"])
        scen.add_set("balance_equality", ["HVC", "export"])
        scen.add_set("balance_equality", ["HVC", "import"])
        scen.add_set("balance_equality", ["ethylene", "final_material"])
        scen.add_set("balance_equality", ["propylene", "final_material"])
        scen.add_set("balance_equality", ["BTX", "final_material"])

    scen.set_as_default()
    
    scen.solve(solve_options={'scaind':'-1'})
    
    # close connection with the database
    mp.close_db()
    