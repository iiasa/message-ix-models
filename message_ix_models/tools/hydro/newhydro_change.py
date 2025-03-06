# -*- coding: utf-8 -*-
"""
Created on Jan 2019, updated Jan 2022

@author: Jihoon Min, Adriano Vinca

This script removed existing hydro_lc and hydro_hc from the MESSAGE model and
replaces it with 8 new technologies representing a potential-cost curve and LF bind.
It is possible to consider:
    1. to select regions to update (country or region)
    2. to check which parameters exist in the scenario and react differently
        (what to modify/what to define)
    3. to select different climate assumption (hist, rcp6p0, rcp 2p6)

    Data source:
    Gernaat et al. Climate change impacts on renewable energy supply. 
    Nat. Clim. Chang. 11, 119–125 (2021). https://doi.org/10.1038/s41558-020-00949-9

"""

import time
from copy import deepcopy
from pathlib import Path

import numpy as np
import pandas as pd

# import ixmp as ix
# import message_ix
from message_ix_models import ScenarioInfo
from message_ix_models.util import private_data_path


def UpdateNewHydro(scenario, code="ensemble_2p6", reg="R11", startyear=2020):
    """Update new hydro technologies to an existing scenrio
    That already had the new hydro implementation, this allows
    switching to other climate assumptions

    scenario : :class:`message_ix.Scenario`
        scenario for which the update of hydro should happen
    pol:  can be used to add a simple emissions constraint
    code: an identifier made of model name/RCP
    reg: regional definition (e.g. R11, R12, ISO)

    NOTE: Here it is assumed the scenario is not checked out.
    """

    data_path = private_data_path("hydro", "output_MESSAGE_aggregation", reg)

    # List of parameters which 'may' have old hydro tecs
    # Derived from set(par_tecHydro) in the original script
    param_tec_hydro = {"fix_cost", "inv_cost", "capacity_factor"}
    param_com_hydro = {"renewable_potential", "renewable_capacity_factor"}

    # This decides which policy scenario we work with.
    scenario_code = code.split("_")[1]

    s_info = ScenarioInfo(scenario)
    nodes = s_info.N
    years = s_info.yv_ya.year_vtg.unique()
    fmy = s_info.y0
    vtg_years = years[years < fmy].tolist()
    msg_reg = [x for x in nodes if "GLB" not in x and "World" not in x]

    scenario.check_out()

    # Read in the LF and CC values generated from R - for the new technologies
    numstep = 8
    newtechnames = ["hydro_" + str(x) for x in range(1, numstep + 1)]
    newcommnames = ["hydro_c" + str(x) for x in range(1, numstep + 1)]
    allhydrocomm = newcommnames

    # save renewable_capaicty_factor before change
    dfrcf_pc = scenario.par("renewable_capacity_factor", {"grade": ["c1"],},)

    # remove previous parameter content for hydro_# tecs
    print("removing paramenters to be substituted")

    for i in param_com_hydro:
        for h in newcommnames:
            df = scenario.par(i, {"commodity": h})
            scenario.remove_par(i, df)
    print("Paramters removed")

    # %% Param inputs (Part 1 of 3).

    # For those parameters which mostly need simple replacement of technology names
    # But some of these will still be modified additionally below.

    # # Parameters that we don't need to copy from scenario:

    # Looping for all regions
    for focus_region in msg_reg:

        print("Parametrizing each MESSAGE region: " + focus_region)
        start_time = time.time()
        LF = pd.read_csv(
            Path(
                data_path,
                "LoadFac_" + focus_region.partition("_")[2] + "_8main_4subs_update.csv",
            )
        )

        CC = pd.read_csv(
            Path(
                data_path,
                "CapCost_" + focus_region.partition("_")[2] + "_8main_4subs_update.csv",
            )
        )

        # Account for two different parameter sets for climate scenarios
        CC_hist = (
            CC.loc[CC.code.str.contains(code)]
            .iloc[1 : numstep + 1, :]
            .drop("code", axis=1)
        )
        CC_hist["technology"] = newtechnames

        # For scaling 'initial_new_capacity_up'
        CCsum_hist = sum(1 / CC["avgIC"][1 : (numstep - 1)])
        if scenario_code == "hist":  # baseline scenario
            CC = (
                CC.loc[CC.code.str.contains(code)]
                .iloc[1 : numstep + 1, :]
                .drop("code", axis=1)
            )

            # For scaling 'initial_new_capacity_up'
            CCsum = sum(1 / CC["avgIC"][1 : (numstep - 1)])

            LF = LF.loc[LF.code.str.contains(code)].iloc[1:, :].drop("code", axis=1)
            LF = LF.reset_index(drop=True)
        else:
            code1 = code + "_2030-2070"
            code2 = code + "_2071-2100"

            CC1 = (
                CC.loc[CC.code == code1]
                .iloc[1 : numstep + 1, :]
                .drop("code", axis=1)
                .reset_index(drop=True)
            )  # First half of the century
            CC2 = (
                CC.loc[CC.code == code2]
                .iloc[1 : numstep + 1, :]
                .drop("code", axis=1)
                .reset_index(drop=True)
            )  # Second half of the century

            CC = pd.concat(
                [
                    CC1.rename(columns={"xval": "xval_1st", "avgIC": "avgIC"}),
                    CC2.rename(columns={"xval": "xval_2nd", "avgIC": "avgIC_2nd"}).drop(
                        columns="msg_reg"
                    ),
                ],
                axis=1,
            )

            # For scaling 'initial_new_capacity_up'
            CCsum = sum(1 / CC["avgIC"][1 : (numstep - 1)])

            LF1 = (
                LF.loc[LF.code == code1]
                .iloc[1:, :]
                .drop("code", axis=1)
                .reset_index(drop=True)
            )
            LF2 = (
                LF.loc[LF.code == code2]
                .iloc[1:, :]
                .drop("code", axis=1)
                .reset_index(drop=True)
            )

            LF = pd.concat(
                [
                    LF1.rename(
                        columns={
                            "xval": "xval_1st",
                            "avgLF": "avgLF_1st",
                            "x.interval": "x.interval_1st",
                        }
                    ),
                    LF2.rename(
                        columns={
                            "xval": "xval_2nd",
                            "avgLF": "avgLF_2nd",
                            "x.interval": "x.interval_2nd",
                        }
                    ).drop(columns=["msg_reg", "commodity"]),
                ],
                axis=1,
            )

        CC["technology"] = newtechnames

        # For most parameters, Simply reuse the values in the ref scenario and change the tec name
        for t in ["initial_new_capacity_up"]:
            print("Parameter: " + t)

            nodename = [
                x
                for x in scenario.idx_names(t)
                if x in ["node", "node_loc", "node_rel"]
            ]  # There are different node indices.

            if len(nodename) != 0:
                node_column = nodename[0]

            for i in newtechnames:
                df = scenario.par(t, {"technology": i, node_column: [focus_region]})
                dfOrg = deepcopy(df)
                # print("Technology: " + i)
                if df.empty:
                    break
                if t == "initial_new_capacity_up":
                    df["value"] = (
                        dfOrg["value"]
                        / CCsum
                        * CCsum_hist
                        / float(CC["avgIC"].loc[CC["technology"] == i])
                        * float(CC_hist["avgIC"].loc[CC_hist["technology"] == i])
                    )
                # Scale the original hydro_hc value (arbitrary)
                scenario.add_par(t, df)

        #%% Param inputs (Part 2 of 3).

        # Parameters which need to be customized: inv_cost, fix_cost, & other renewable params
        # Input the new LF and CF values to the corresponding parameters: inv_cost & renewable_capacity_factor
        for i in newtechnames:
            df = scenario.par("inv_cost", {"technology": i, "node_loc": [focus_region]})
            scenario.remove_par("inv_cost", df)
            df_fx = scenario.par(
                "fix_cost", {"technology": i, "node_loc": [focus_region]}
            )
            scenario.remove_par("fix_cost", df_fx)

            # Account for two different parameter sets for climate scenarios
            if scenario_code == "hist":  # baseline scenario
                invcost = float(CC.loc[CC.technology == i, "avgIC"])
                df["value"] = invcost
                # fixed cost = 2.2% of capital cost
                df_fx["value"] = invcost * 0.022
            else:  # climate scenarios
                invcost1 = float(CC.loc[CC.technology == i, "avgIC"])
                invcost2 = float(CC.loc[CC.technology == i, "avgIC_2nd"])
                df.loc[df.year_vtg <= 2070, "value"] = invcost1
                df.loc[df.year_vtg > 2070, "value"] = invcost2
                # import pdb; pdb.set_trace()
                df_fx.loc[df_fx.year_vtg <= 2070, "value"] = (
                    invcost1 * 0.022
                )  # fixed cost = 2.2% of capital cost
                df_fx.loc[df_fx.year_vtg > 2070, "value"] = (
                    invcost2 * 0.022
                )  # fixed cost = 2.2% of capital cost

            df_fx["technology"] = i
            scenario.add_par("inv_cost", df)
            scenario.add_par("fix_cost", df_fx)

        # %% Param inputs (Part 2 of 3).

        # These parameters are empty in global model.

        # So create them and do the following:
        #       1. take care of commodity (in input/output)
        #       2. renewable params with only commodity (not tec) like renewable_potential etc.

        # LF only for the new techs. LF for hist tech is in capacity_factor instead of renewable_capacity_factor

        model_years = scenario.set("year")[8:].reset_index(drop="TRUE")  # 1990 and on

        for i in newcommnames:

            if scenario_code == "hist":
                lfsub = LF.loc[LF.commodity == i].reset_index(drop=True)
                InputLoadFactor(scenario, focus_region, lfsub, comm=i)
            else:
                lf1sub = LF1.loc[LF1.commodity == i].reset_index(drop=True)
                InputLoadFactor(scenario, focus_region, lf1sub, comm=i, case="pol_1st")
                lf2sub = LF2.loc[LF2.commodity == i].reset_index(drop=True)
                InputLoadFactor(scenario, focus_region, lf2sub, comm=i, case="pol_2nd")

        #%% edit renewable_potential and reneweable_capacity_factor for historical conditions
        # rewnewable potential, add max hist_act to the first grade potential

        # First retreive historical activity, this time of hydro_1 and hydro_2
        # # NEW_ED all historic act to hydro_1
        df = scenario.par(
            "historical_activity",
            {"technology": ["hydro_1"], "node_loc": [focus_region]},
        )
        max_hist_act1 = float(df[df.year_act == 2015].value)

        # same for hydro_hc and hydro_2
        # NEW_ED commented
        df = scenario.par(
            "historical_activity",
            {"technology": ["hydro_2"], "node_loc": [focus_region]},
        )
        max_hist_act2 = float(df[df.year_act == 2015].value)

        # no edit the renewable_potential
        df = scenario.par(
            "renewable_potential",
            {
                "commodity": ["hydro_c1", "hydro_c2"],
                "node": [focus_region],
                "grade": ["c1"],
            },
        )
        indexer = df[df.commodity == "hydro_c1"].index
        df.loc[indexer, "value"] = df.loc[indexer, "value"] + max_hist_act1
        indexer = df[df.commodity == "hydro_c2"].index
        df.loc[indexer, "value"] = df.loc[indexer, "value"] + max_hist_act2

        scenario.add_par("renewable_potential", df)

        # renewable capacity factor, add a lower capacity factor for vintage capacity
        # CF(hydro_1,vtg) = CF(hydro_lh,vtg)/renCF(hydro1_vtg)
        # FOR NOW DON'T MODIFY IT, THE CHANGES WILL BE MINIMAL, in theory should be 0
        hydro_old_new = pd.DataFrame(
            [["hydro_1", "hydro_c1", "hydro_lc"], ["hydro_2", "hydro_c2", "hydro_hc"]],
            columns=["new_tec", "new_comm", "old_tec"],
        )
        for index, row in hydro_old_new.iterrows():
            print(row["new_tec"])
            print(row["old_tec"])
            dfrcf_pre_ch = dfrcf_pc[
                (dfrcf_pc["commodity"] == row["new_comm"])
                & (dfrcf_pc["node"] == focus_region)
            ]
            dfrcf = scenario.par(
                "renewable_capacity_factor",
                {
                    "commodity": [row["new_comm"]],
                    "node": [focus_region],
                    "grade": ["c1"],
                },
            )
            # take value, before 2070 all th same
            short_term_rCF_pre_ch = float(
                dfrcf_pre_ch[dfrcf_pre_ch.year == startyear].value
            )
            short_term_rCF = float(dfrcf[dfrcf.year == startyear].value)
            df = scenario.par(
                "capacity_factor",
                {
                    "technology": [row["new_tec"]],
                    "year_vtg": vtg_years,
                    "node_loc": [focus_region],
                },
            )

            # divide by the ren CF because it willbe multiply by it again in the model
            df["value"] = df["value"] * short_term_rCF_pre_ch / short_term_rCF

            scenario.add_par("capacity_factor", df)

    scenario.commit("Finished updating hydro parameters (based on IMAGE data)!")
    scenario.set_as_default()
    print("The scenario with the new hydro implementation is committed.")

    return scenario


def InputLoadFactor(scenario, focus_region, lfsub, comm, case="all"):
    """
    Take care of param inputs for different periods for the load factor info

    Parameters
    ----------
    lf.df : pandas DF
        DESCRIPTION.
    case : string, optional
        DESCRIPTION. The default is "all". "pol_1st" or "pol_2nd", notating
        which time period to tackle

    Returns
    -------
    pot : TYPE
        DESCRIPTION.
    cfac : TYPE
        DESCRIPTION.

    """
    model_years = scenario.set("year")[8:].reset_index(drop="TRUE")  # 1990 and on

    if case == "all":
        yrs = model_years
    elif case == "pol_1st":
        yrs = model_years[:15]
    elif case == "pol_2nd":
        yrs = model_years[15:]

    base_hydro_input = {
        "node": focus_region,
        "level": "renewable",
        "year": yrs,  # 1990 and on
    }

    for g in lfsub.index:
        # print("InputLoadFactor:", focus_region, ", comm:", comm, ", grade:", "c" + str(g + 1), lfsub)
        pot = pd.DataFrame(
            dict(
                commodity=comm,
                # value=lfsub.at[g, "x.interval"],
                grade="c" + str(g + 1),
                unit="GWa",
                **base_hydro_input
            )
        )
        cfac = pd.DataFrame(
            dict(
                commodity=comm,
                # value=lfsub.at[g, "avgLF"],
                grade="c" + str(g + 1),
                unit="%",
                **base_hydro_input
            )
        )

        pot["value"] = lfsub.at[g, "x.interval"]
        cfac["value"] = lfsub.at[g, "avgLF"]

        scenario.add_par("renewable_potential", pot)
        scenario.add_par("renewable_capacity_factor", cfac)
