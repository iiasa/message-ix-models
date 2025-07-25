"""Functions used in :mod:`.project.ssp.script.scenarios`.

Originally added via :pull:`235`, cherry-picked and merged in :pull:`340`.

.. todo::
   - Move to other locations as appropriate.
   - Add tests.
   - Add documentation.
"""

from typing import Literal

import message_ix
import numpy as np
import pandas as pd
from message_ix.utils import make_df

from message_ix_models.tools.add_dac import add_tech
from message_ix_models.tools.costs.config import Config
from message_ix_models.tools.costs.projections import create_cost_projections
from message_ix_models.util import broadcast, load_package_data


def modify_rc_bounds(s_original, s_target, mod_years):
    # get ACT for biomass_rc in SAS
    act_bio = s_original.var(
        "ACT", filters={"technology": "biomass_rc", "node_loc": "R12_SAS"}
    )
    del act_bio  # NB Currently unused

    # get ACT for loil_rc in SAS
    act_loil_sas = s_original.var(
        "ACT", filters={"technology": "loil_rc", "node_loc": "R12_SAS"}
    )

    val_bound_loil_sas = (
        act_loil_sas.loc[act_loil_sas["year_act"] == 2055]
        .reset_index()
        ._get_value(0, "lvl")
        .round(2)
    )

    # get ACT for loil_rc in MEA
    act_loil_mea = s_original.var(
        "ACT", filters={"technology": "loil_rc", "node_loc": "R12_MEA"}
    )

    val_bound_loil_mea = (
        act_loil_mea.loc[act_loil_mea["year_act"] == 2055]
        .reset_index()
        ._get_value(0, "lvl")
        .round(2)
    )

    # get bound_activity_up for biomass_rc in SAS
    orig_bound_bio = s_original.par(
        "bound_activity_up", filters={"technology": "biomass_rc", "node_loc": "R12_SAS"}
    )

    # get bound_activity_up for loil_rc in SAS
    orig_bound_loil_sas = s_original.par(
        "bound_activity_up", filters={"technology": "loil_rc", "node_loc": "R12_SAS"}
    )

    # get bound_activity_up for loil_rc in MEA
    orig_bound_loil_mea = s_original.par(
        "bound_activity_up", filters={"technology": "loil_rc", "node_loc": "R12_MEA"}
    )

    # function to get first row of bound and create new rows for set_years
    # and replace value with specified value
    def get_new_bound(df, set_years, value):
        new_rows = []
        for year in set_years:
            new_row = df.iloc[0].copy()
            new_row["year_act"] = year
            new_row["value"] = value
            new_rows.append(new_row)
        df_new = pd.DataFrame(new_rows).reset_index(drop=True)
        return df_new

    new_bound_bio = get_new_bound(orig_bound_bio, mod_years, 0)
    new_bound_loil_sas = get_new_bound(
        orig_bound_loil_sas, mod_years, val_bound_loil_sas
    )
    new_bound_loil_mea = get_new_bound(
        orig_bound_loil_mea, mod_years, val_bound_loil_mea
    )

    # check out and add each new bound_activity_up
    s_target.check_out()
    s_target.add_par("bound_activity_up", new_bound_bio)
    s_target.add_par("bound_activity_up", new_bound_loil_sas)
    s_target.add_par("bound_activity_up", new_bound_loil_mea)

    # commit the changes
    s_target.commit("New bounds added to biomass_rc and loil_rc")


def modify_tax_emission(s_original, s_target, scalar_val):
    # get PRICE_EMISSION variable
    price_emission = s_original.var(
        "PRICE_EMISSION", filters={"type_emission": "TCE_non-CO2"}
    )

    # get tax_emission parameter
    tax_emission_old = s_original.par(
        "tax_emission", filters={"type_emission": "TCE_non-CO2"}
    )

    # for PRICE_EMISSION after 2080, multiply by mult_price
    price_emission_mod = (
        price_emission.copy()
        .assign(lvl=lambda x: x.lvl * scalar_val)[["year", "lvl"]]
        .rename(columns={"year": "type_year"})
    )

    # merge with tax_emission
    tax_emission_new = (
        tax_emission_old.copy()
        .assign(type_year=lambda x: x.type_year.astype("int64"))
        .merge(price_emission_mod, on="type_year")
        .assign(value=lambda x: np.where(x.type_year >= 2080, x.lvl, x.value))
        .assign(type_emission="TCE")
        .drop(columns="lvl")
    )

    # check out and add new tax_emission
    s_target.check_out()
    s_target.remove_par("tax_emission", tax_emission_old)
    s_target.add_par("tax_emission", tax_emission_new)
    s_target.commit("New tax_emission added")


def remove_bof_steel_lower(s, rem_years):
    remove_growth_activity_lo = s.par(
        "growth_activity_lo",
        filters={"technology": ["bof_steel"], "year_act": rem_years},
    )
    remove_initial_activity_lo = s.par(
        "initial_activity_lo",
        filters={"technology": ["bof_steel"], "year_act": rem_years},
    )
    s.check_out()
    s.remove_par("growth_activity_lo", remove_growth_activity_lo)
    s.remove_par("initial_activity_lo", remove_initial_activity_lo)
    s.commit("bof_steel bounds removed")


def modify_steel_growth(s, techs, rem_years, growth_val):
    # get old values
    old_growth_activity_up = s.par(
        "growth_activity_up",
        filters={"technology": techs, "year_act": rem_years},
    )

    # modify values
    new_growth_activity_up = old_growth_activity_up.copy().assign(value=growth_val)

    # check out; remove old bounds and add new bounds
    s.check_out()
    s.remove_par("growth_activity_up", old_growth_activity_up)
    s.add_par("growth_activity_up", new_growth_activity_up)
    s.commit("Modified bounds for steel alternatives")


def modify_steel_initial(s, techs, rem_years, initial_val):
    # get old values
    old_initial_activity_up = s.par(
        "initial_activity_up",
        filters={"technology": techs, "year_act": rem_years},
    )

    # modify values
    new_initial_activity_up = old_initial_activity_up.copy().assign(value=initial_val)

    # check out; remove old bounds and add new bounds
    s.check_out()
    s.remove_par("initial_activity_up", old_initial_activity_up)
    s.add_par("initial_activity_up", new_initial_activity_up)
    s.commit("Modified bounds for steel alternatives")


def add_steel_sector_nze(s, steel_target_array):
    co2_ind = s.par(
        "relation_activity",
        filters={
            "relation": "CO2_ind",
            "technology": ["DUMMY_coal_supply", "DUMMY_gas_supply"],
        },
    )

    co2_emi = s.par(
        "output",
        filters={
            "commodity": "fic_co2",
            "technology": ["dri_gas_ccs_steel", "bf_ccs_steel"],
        },
    )

    co2_emi["relation"] = "CO2_Emission"
    co2_emi.rename(columns={"node_dest": "node_rel"}, inplace=True)
    co2_emi["year_rel"] = co2_emi["year_act"]
    co2_emi.drop(
        ["year_vtg", "commodity", "level", "time", "time_dest"], axis=1, inplace=True
    )

    co2_emi["value"] *= -1

    rel_new = pd.concat([co2_ind, co2_emi], ignore_index=True)

    rel_new = rel_new[rel_new["year_rel"] >= 2070]

    rel_new["node_rel"] = "R12_GLB"
    rel_new["relation"] = "steel_sector_target"

    rel_new = rel_new.drop_duplicates()

    s.check_out()
    s.add_set("relation", "steel_sector_target")

    relation_upper_df = pd.DataFrame(
        {
            "relation": "steel_sector_target",
            "node_rel": "R12_GLB",
            "year_rel": [2070, 2080, 2090, 2100],
            "value": steel_target_array,  # Slack values from Gamze, added manually
            "unit": "???",
        }
    )

    relation_lower_df = pd.DataFrame(
        {
            "relation": "steel_sector_target",
            "node_rel": "R12_GLB",
            "year_rel": [2070, 2080, 2090, 2100],
            "value": 0,
            "unit": "???",
        }
    )

    s.add_par("relation_activity", rel_new)
    s.add_par("relation_upper", relation_upper_df)
    s.add_par("relation_lower", relation_lower_df)

    s.commit("Steel sector target added.")


def add_balance_equality(scen):
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
    dac_techs = ["dac_lt", "dac_hte", "dac_htg"]
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


def _add_new_meth_h2_modes(scenario: message_ix.Scenario):
    """
    Add new modes for meth_h2 technology parametrization from the scenario.

    meth_h2 uses CO2 to produce methanol.
    fuel and feedstock mode are split into 3 modes each
    depending on the CO2 source that is used:
    biogenic (=bic), direct air capture (=dac), fossil (=fic).

    Parameters
    ----------
    scenario: message_ix.Scenario
        scenario, where parameters for new modes should be added
    """
    par_dict = {}
    for par in [x for x in scenario.par_list() if "mode" in scenario.idx_sets(x)]:
        df = scenario.par(par, filters={"technology": "meth_h2"})
        if len(df.index):
            par_dict[par] = df.copy(deep=True)

    par_dict_new = {k: pd.DataFrame() for k in par_dict.keys()}
    for par, df in par_dict.items():
        for mode in ["feedstock", "fuel"]:
            df_tmp = df[df["mode"].values == mode].copy(deep=True)
            df_tmp["mode"] = None
            df_tmp = df_tmp.pipe(
                broadcast, mode=[f"{mode}_{suffix}" for suffix in ["bic", "dac", "fic"]]
            )
            par_dict_new[par] = pd.concat([par_dict_new[par], df_tmp])

    for par, df in par_dict_new.items():
        scenario.add_par(par, df)


def _remove_old_meth_h2_modes(scenario: message_ix.Scenario):
    """
    Remove old modes for meth_h2 technology from the scenario.

    Parameters
    ----------
    scenario: message_ix.Scenario
        scenario, where parameters for new modes should be added
    """
    par_dict = {}
    for par in [x for x in scenario.par_list() if "mode" in scenario.idx_sets(x)]:
        df = scenario.par(
            par, filters={"technology": "meth_h2", "mode": ["feedstock", "fuel"]}
        )
        if len(df.index):
            par_dict[par] = df.copy(deep=True)

    for par, df in par_dict.items():
        scenario.remove_par(par, df)


def _register_new_modes(scenario):
    """
    Register new modes required for meth_h2 technology parametrization

    Parameters
    ----------
    scenario: message_ix.Scenario
        scenario, where meth_h2 modes should be registered
    """
    modes = ["bic", "dac", "fic"]
    for mode in ["feedstock", "fuel"]:
        scenario.add_set("mode", [f"{mode}_{suffix}" for suffix in modes])


def update_meth_h2_modes(scenario: message_ix.Scenario):
    """Add new meth_h2 modes to set and update meth_h2 parametrization accordingly.

    Parameters
    ----------
    scenario: message_ix.Scenario
        scenario, where meth_h2 mode update should be applied
    """
    _register_new_modes(scenario)
    _add_new_meth_h2_modes(scenario)
    _remove_old_meth_h2_modes(scenario)


def add_ccs_setup(scen: message_ix.Scenario, ssp="SSP2"):
    with scen.transact(""):
        # CO2 storage potential from Matt and Sidd
        R12_potential = {
            "R12_FSU": 83310.661,
            "R12_LAM": 55868.988,
            "R12_WEU": 11793.358,
            "R12_EEU": 1808.370,
            "R12_AFR": 61399.222,
            "R12_MEA": 57369.171,
            "R12_NAM": 58983.981,
            "R12_SAS": 4765.219,
            "R12_PAS": 21108.578,
            "R12_PAO": 34233.825,
            "R12_CHN": 15712.475,
            "R12_RCPA": 3754.285,
        }

        # max rate in MtC per year
        max_rate = np.round(15000 / 3.667, 0)

        # technology modes
        modes = ["M1", "M2", "M3"]

        # length for each period
        len_periods = {
            2025: 5,
            2030: 5,
            2035: 5,
            2040: 5,
            2045: 5,
            2050: 5,
            2055: 5,
            2060: 5,
            2070: 10,
            2080: 10,
            2090: 10,
            2100: 10,
            2110: 10,
        }

        # nodes
        nodes = [
            "R12_AFR",
            "R12_EEU",
            "R12_LAM",
            "R12_MEA",
            "R12_NAM",
            "R12_SAS",
            "R12_WEU",
            "R12_FSU",
            "R12_PAO",
            "R12_PAS",
            "R12_CHN",
            "R12_RCPA",
            "R12_GLB",
        ]

        # years
        years = [year for year in list(len_periods.keys()) if year > 2025]

        # SSPs to run
        ssps = ["LED", "SSP1", "SSP2", "SSP3", "SSP4", "SSP5"]
        ssps = ["SSP2"]

        # SSPs CCS parameters
        ccs_ssp_pars = {
            "LED": {
                "co2storage": 0.25,
                "co2rate": 6000 / 3.667,
            },
            "SSP1": {
                "co2storage": 0.25,
                "co2rate": 6000 / 3.667,
            },
            "SSP2": {
                "co2storage": 0.50,
                "co2rate": 16500 / 3.667,
            },
            "SSP3": {
                "co2storage": 1.00,
                "co2rate": 18750 / 3.667,
            },
            "SSP4": {
                "co2storage": 1.00,
                "co2rate": 35000 / 3.667,
            },
            "SSP5": {
                "co2storage": 1.00,
                "co2rate": 35000 / 3.667,
            },
        }

        # scenario parameters list for edit
        co2_pipes = ["co2_tr_dis", "bco2_tr_dis"]
        co2_pipes_par = [
            "inv_cost",
            "fix_cost",
            "input",
            "capacity_factor",
            "technical_lifetime",
            "construction_time",
            "abs_cost_activity_soft_up",
            "growth_activity_lo",
            "level_cost_activity_soft_lo",
            "level_cost_activity_soft_up",
            "relation_activity",
            "var_cost",
            "output",
            "emission_factor",
            "soft_activity_lo",
            "soft_activity_up",
            "growth_activity_up",
            "initial_activity_up",
        ]

        ccs_techs = [
            # BECCS
            "bio_istig_ccs",
            "biomass_NH3_ccs",
            "bio_ppl_co2scr",
            "eth_bio_ccs",
            "meth_bio_ccs",
            "h2_bio_ccs",
            "liq_bio_ccs",
            # Fossil and Industrial CCS
            "bf_ccs_steel",
            "c_ppl_co2scr",
            "clinker_dry_ccs_cement",
            "clinker_wet_ccs_cement",
            "coal_adv_ccs",
            "coal_NH3_ccs",
            "dri_gas_ccs_steel",
            "fueloil_NH3_ccs",
            "g_ppl_co2scr",
            "gas_cc_ccs",
            "gas_NH3_ccs",
            "h2_coal_ccs",
            "h2_smr_ccs",
            "igcc_ccs",
            "meth_coal_ccs",
            "meth_ng_ccs",
            "syn_liq_ccs",
            # DACCS
            "dac_lt",
            "dac_hte",
            "dac_htg",
        ]

        dac_techs = ["dac_lt", "dac_hte", "dac_htg"]

        update_meth_h2_modes(scen)

        # mp = ixmp.Platform()

        # calling base scenario
        # model_name = f"SSP_dev_{ssp}_v1.0_testco2v3"
        # scen_name = "baseline"

        # base_scen = message_ix.Scenario(mp, model=model_name, scenario=scen_name)

        # clone scenario to add CCS infrastructure and DAC
        # scen = base_scen.clone(
        #    f"SSP_dev_{ssp}_v1.0_testco2v3_split",
        #    "baseline",
        #    "scenario with CCS infrastructure and DAC",
        #    keep_solution=False,
        # )
        # scen.check_out()

        # ==============================================
        # Remove old setup ================================
        ## rem. relations
        rels = [
            "co2_trans_disp",
            "bco2_trans_disp",
            "CO2_Emission_Global_Total",
            "CO2_Emission",
            "CO2_PtX_trans_disp_split",
        ]

        scen.remove_par(
            "relation_activity",
            scen.par(
                "relation_activity",
                {"technology": co2_pipes + ccs_techs, "relation": rels},
            ),
        )

        ## rem. 'CO2_PtX_trans_disp_split' fro relation set
        scen.remove_set("relation", "CO2_PtX_trans_disp_split")

        ## rem. pipelines
        for par in co2_pipes_par:
            scen.remove_par(par, scen.par(par, {"technology": co2_pipes}))

        ## rem. co2 pipeline sets
        scen.remove_set("technology", co2_pipes)
        scen.remove_set("relation", ["co2_trans_disp", "bco2_trans_disp"])

        # ==============================================
        # Add new setup ================================
        ## setup pipelines, storage, and non-dac ccs technologies
        add_tech(
            scen, load_package_data("ccs-dac", f"co2infrastructure_data_{ssp.lower()}dev.yaml")
        )

        ## setup dac technologies
        add_tech(scen, load_package_data("ccs-dac", f"daccs_setup_data_{ssp.lower()}dev.yaml"))

        ## add dac costs using meas's tool
        ##> making the projection
        inv_cost_dac, fix_cost_dac = gen_te_projections(scen, ssp, "gdp")

        inv_cost_dac = inv_cost_dac[inv_cost_dac["technology"].isin(dac_techs)]
        fix_cost_dac = fix_cost_dac[fix_cost_dac["technology"].isin(dac_techs)]

        ##> adding the costs to the model
        scen.add_par("inv_cost", inv_cost_dac)
        scen.add_par("fix_cost", fix_cost_dac)

        ## removing excess year_act
        pars2remove = ["capacity_factor", "fix_cost", "input", "output"]
        ##> use 2030 R12_NAM as basis to get technology lifetime
        lt = scen.par(
            "technical_lifetime",
            {"technology": ccs_techs, "node_loc": "R12_NAM", "year_vtg": 2030},
        )
        for par in pars2remove:
            df2remove = []
            df = scen.par(par, {"technology": ccs_techs})
            rem_techs = ccs_techs if par == "output" else dac_techs
            for tech in rem_techs:
                lt_tech = np.int32(lt[lt["technology"] == tech]["value"].iloc[0])
                df2remove_tech = df[df["year_act"] > df["year_vtg"].add(lt_tech)]
                df2remove_tech = df2remove_tech[df2remove_tech["technology"] == tech]
                df2remove += [df2remove_tech]
            df2remove = pd.concat(df2remove)
            scen.remove_par(par, df2remove)

        ## make pipelines and storage a single period technology
        newpipesnstors = ["co2_stor", "co2_trans1", "co2_trans2"]
        pars2remove = [
            "var_cost",
            "input",
            "output",
            "emission_factor",
            "capacity_factor",
        ]
        for par in pars2remove:
            df = scen.par(par, {"technology": newpipesnstors})
            df = df.loc[df["year_vtg"] != df["year_act"]]
            scen.remove_par(par, df)

        # ==============================================
        # Setup technology and relations to track cumulative storage
        ## adding new set and technologies
        scen.add_set("technology", "co2_storcumulative")

        ## each storage mode is represented by one relation
        for mode in modes:
            scen.add_set("relation", f"co2_storcum_{mode}")

        ## create relation activity
        list_relation = []
        for node in nodes:
            for mode in modes:
                for yr in years:
                    ya = [y for y in years if y <= yr]
                    relact_co2stor = make_df(
                        "relation_activity",
                        relation=f"co2_storcum_{mode}",
                        node_rel=node,
                        year_rel=yr,
                        node_loc=node,
                        technology="co2_stor",
                        year_act=ya,
                        mode=mode,
                        value=[-1 * len_periods[y] for y in ya],
                        unit="-",
                    )

                    relact_co2storcumulative = make_df(
                        "relation_activity",
                        relation=f"co2_storcum_{mode}",
                        node_rel=node,
                        year_rel=yr,
                        node_loc=node,
                        technology="co2_storcumulative",
                        year_act=yr,
                        mode=mode,
                        value=1,
                        unit="-",
                    )
                    list_relation += [relact_co2stor, relact_co2storcumulative]
        df_relation = pd.concat(list_relation)

        ## create relation bounds
        list_rel_eq = []
        for node in nodes:
            for mode in modes:
                rel_eq = make_df(
                    "relation_upper",
                    relation=f"co2_storcum_{mode}",
                    node_rel=node,
                    year_rel=years,
                    value=0,
                    unit="-",
                )
                list_rel_eq += [rel_eq]
        df_rel_eq = pd.concat(list_rel_eq)

        ## adding parameters
        scen.add_par("relation_activity", df_relation)
        scen.add_par("relation_upper", df_rel_eq)
        scen.add_par("relation_lower", df_rel_eq)

        # adding set up for limiting CO2 storage availabilities
        nodes = [node for node in nodes if node not in ["R12_GLB", "World"]]
        df_list = []
        for node in nodes:
            for year in years:
                df = make_df(
                    "bound_activity_up",
                    node_loc=node,
                    technology="co2_storcumulative",
                    year_act=year,
                    mode="all",
                    time="year",
                    value=(R12_potential[node] * ccs_ssp_pars[ssp]["co2storage"]),
                    unit="???",
                )
                df_list += [df]
        df_stor = pd.concat(df_list)

        scen.add_par("bound_activity_up", df_stor)

        # ==============================================
        # Setup tech and relations to limit global CO2 injection

        ## adding new set and technologies
        scen.add_set("technology", "co2_stor_glb")
        for mode in modes:
            scen.add_set("relation", f"co2_storglobal_{mode}")

        ## create relation activity
        list_relation = []
        for mode in modes:
            for yr in years:
                for node in nodes:
                    relact_co2stor = make_df(
                        "relation_activity",
                        relation=f"co2_storglobal_{mode}",
                        node_rel="R12_GLB",
                        year_rel=yr,
                        node_loc=node,
                        technology="co2_stor",
                        year_act=yr,
                        mode=mode,
                        value=-1,
                        unit="-",
                    )
                    list_relation += [relact_co2stor]
                relact_co2stor_glb = make_df(
                    "relation_activity",
                    relation=f"co2_storglobal_{mode}",
                    node_rel="R12_GLB",
                    year_rel=yr,
                    node_loc="R12_GLB",
                    technology="co2_stor_glb",
                    year_act=yr,
                    mode=mode,
                    value=1,
                    unit="-",
                )
                list_relation += [relact_co2stor_glb]
        df_relation = pd.concat(list_relation)

        ## create relation bounds
        list_rel_eq = []
        for mode in modes:
            rel_eq = make_df(
                "relation_upper",
                relation=f"co2_storglobal_{mode}",
                node_rel="R12_GLB",
                year_rel=years,
                value=0,
                unit="-",
            )
            list_rel_eq += [rel_eq]
        df_rel_eq = pd.concat(list_rel_eq)

        ## adding parameters
        scen.add_par("relation_activity", df_relation)
        scen.add_par("relation_upper", df_rel_eq)
        scen.add_par("relation_lower", df_rel_eq)

        df_list = []
        for year in years:
            df = make_df(
                "bound_activity_up",
                node_loc="R12_GLB",
                technology="co2_stor_glb",
                year_act=year,
                mode="all",
                time="year",
                value=ccs_ssp_pars[ssp]["co2rate"],
                unit="???",
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
                df = make_df(
                    "relation_upper",
                    relation=rel,
                    node_rel=node,
                    year_rel=years,
                    unit="-",
                    value=0,
                )
                df_list = df_list + [df]
        dfpar2add = pd.concat(df_list)
        scen.add_par("relation_upper", dfpar2add)
        scen.add_par("relation_lower", dfpar2add)

        # ==============================================
        ## Adjust dac_htg CO2_cc as it burns gas as input
        cc2rem = scen.par(
            "relation_activity", {"relation": "CO2_cc", "technology": "dac_htg"}
        )
        cc2add = cc2rem.copy()
        cc2add["value"] = cc2add["value"].sub(1)

        scen.remove_par("relation_activity", cc2rem)
        scen.add_par("relation_activity", cc2add)
