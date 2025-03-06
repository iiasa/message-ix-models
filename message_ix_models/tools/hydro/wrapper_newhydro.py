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
from message_ix_models import ScenarioInfo
from message_ix_models.util import private_data_path


def IncorporateNewHydro(scenario, code="ensemble_2p6", reg="R11", startyear=2020):
    """Add new hydro technologies to an existing scenrio

    scenario : :class:`message_ix.Scenario`
        scenario for which the update of hydro should happen
    code: an identifier made of model name/RCP
    reg: regional definition (e.g. R11, R12, ISO)

    NOTE: Here it is assumed the scenario is not checked out.
    """
    # check that the new hydro technologies are not existing in the scenrio
    if "hydro_1" in list(scenario.set("technology")):
        print(
            "new 'hydro_1' technology is already existing, please use the 'UpdateNewHydro' function instead"
        )
        return

    data_path = private_data_path("hydro", "output_MESSAGE_aggregation", reg)

    # List of parameters which 'may' have old hydro tecs
    # Derived from set(par_tecHydro) in the original script
    param_set_hydro = {
        "capacity_factor",
        "construction_time",
        "fix_cost",
        "historical_activity",
        "historical_new_capacity",
        # 'input_cap_new',
        # 'input_cap_ret',
        "inv_cost",
        "level_cost_activity_soft_lo",
        "level_cost_activity_soft_up",
        "output",
        # 'output_cap_ret',
        "relation_activity",
        "relation_total_capacity",
        "soft_activity_lo",
        "soft_activity_up",
        "technical_lifetime",
        "var_cost",
        "bound_new_capacity_up",
        "bound_new_capacity_lo",
        "bound_activity_up",
        "bound_activity_lo",
        "initial_new_capacity_up",
        "growth_new_capacity_up",
        "initial_new_capacity_lo",
        "growth_new_capacity_lo",
        # 'initial_activity_up',
        "growth_activity_up",
        # 'initial_activity_lo',
        "growth_activity_lo",
        "relation_activity",
        "ref_new_capacity",
        "ref_activity",
    }

    rel_set_hydro = {
        "relation_upper",
        "relation_lower",
        "relation_activity",
        "ref_relation",
    }

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
    # histtechname = ["hydro_hist"]
    newaggname = ["hydro_mpen"]
    allhydrotech = newtechnames + newaggname
    newcommnames = ["hydro_c" + str(x) for x in range(1, numstep + 1)]
    # histcommname = ["hydro_c_hist"]
    allhydrocomm = newcommnames

    # # Find parameters relevent to hydro
    # print("Collecting parameters containing old hydro technologies")
    # tecList = [x for x in scenario.par_list()
    #            if "technology" in scenario.idx_sets(x)]
    # par_tecHydro = [
    #     x for x in tecList
    #     if "hydro_hc" in set(scenario.par(x)["technology"].tolist())
    # ]
    # par_tecHydrol = [
    #     x for x in tecList
    #     if "hydro_lc" in set(scenario.par(x)["technology"].tolist())
    # ]

    # print("Collecting relations containing old hydro technologies")
    # relList = [x for x in scenario.par_list() if "relation" in scenario.idx_sets(x)]
    # par_relHydroPot = [
    #     x for x in relList
    #     if "hydro_pot" in set(scenario.par(x)["relation"].tolist())
    # ]
    # par_relHydroMin = [
    #     x for x in relList
    #     if "hydro_min" in set(scenario.par(x)["relation"].tolist())
    # ]

    # %% Modify set inputs
    print("Adding new sets")
    scenario.add_set("technology", allhydrotech)
    scenario.add_set("commodity", allhydrocomm)
    scenario.add_set("level", "renewable")
    scenario.add_set("level_renewable", "renewable")
    # scenario.add_set("grade", ["c1", "c2", "c3", "c4"])
    scenario.add_set("grade", ["c" + str(x) for x in range(1, 15)])
    scenario.add_set("rating", ["0_rel", "30_rel", "60_rel", "90_rel", "full_rel"])
    relation_act_hydro = ["hydro_mpen_act"]
    relation_cap_hydro = ["hydro_mpen_cap"]
    scenario.add_set("relation", relation_act_hydro)
    scenario.add_set("relation", relation_cap_hydro)

    cat_tec = pd.DataFrame(
        {
            "type_tec": "powerplant_low-carbon",  # 'powerplant',
            "technology": allhydrotech,
        }
    )

    scenario.add_set("cat_tec", cat_tec)

    # %% Param inputs (Part 1 of 3).

    # For those parameters which mostly need simple replacement of technology names
    # But some of these will still be modified additionally below.

    # # Parameters that we don't need to copy from scenario:

    print("Identifying which params have old hydro tecs")
    par_hydro = [
        x for x in param_set_hydro if "hydro_hc" in set(scenario.par(x)["technology"])
    ]

    par_skip_new = (
        [x for x in par_hydro if "bound" in x]
        + [x for x in par_hydro if "ref_" in x]
        # + [x for x in par_hydro if "initial_" in x]
        + [x for x in par_hydro if "growth_" in x]
        + ["historical_activity", "historical_new_capacity"]
    )

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
        for t in set(par_hydro) - set(par_skip_new):
            print("Parameter: " + t)

            nodename = [
                x
                for x in scenario.idx_names(t)
                if x in ["node", "node_loc", "node_rel"]
            ]  # There are different node indices.

            if len(nodename) != 0:
                node_column = nodename[0]

            df = scenario.par(
                t, {"technology": ["hydro_hc"], node_column: [focus_region]}
            )
            dfOrg = deepcopy(df)

            for i in newtechnames:
                # print("Technology: " + i)
                if df.empty:
                    break
                df["technology"] = i
                if t == "capacity_factor":
                    df["value"] = 1  # capacity_factor = 1 for new hydro
                # input part moved below
                elif t == "initial_new_capacity_up":
                    df["value"] = (
                        dfOrg["value"]
                        / CCsum
                        / float(CC["avgIC"].loc[CC["technology"] == i])
                    )
                    df = df.loc[df.year_vtg <= fmy]
                    # Scale the original hydro_hc value (arbitrary)
                elif t == "relation_activity":
                    dfnew = deepcopy(df.set_index("relation"))
                    dfnew = dfnew.drop(
                        "hydro_pot"
                    ).reset_index()  # Remove hydro_pot relation
                    scenario.add_par(t, dfnew)
                    continue
                elif t == "inv_cost":
                    continue  # taken care of later for new tecs
                scenario.add_par(t, df)

        #%% Param inputs (Part 2 of 3).

        # Parameters which need to be customized: input, inv_cost, fix_cost, & other renewable params
        # newtechnames allhydrotech
        for i in newtechnames:
            df = scenario.par(
                "output", {"technology": ["hydro_hc"], "node_loc": [focus_region]}
            )
            dfin = scenario.par(
                "input", {"technology": ["gas_ppl"], "node_loc": [focus_region]}
            )
            df.columns = dfin.columns
            dfWater = deepcopy(df)
            dfWater["technology"] = i
            dfWater["value"] = 1
            comname = i.split("_")
            dfWater["commodity"] = comname[0] + "_c" + comname[1]
            dfWater["level"] = "renewable"
            scenario.add_par("input", dfWater)

            # Input the new LF and CF values to the corresponding parameters: inv_cost & renewable_capacity_factor
        df = scenario.par(
            "inv_cost", {"technology": ["hydro_lc"], "node_loc": [focus_region]}
        )
        df_fx = scenario.par(
            "fix_cost", {"technology": ["hydro_lc"], "node_loc": [focus_region]}
        )
        for i in newtechnames:
            df["technology"] = i

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

        # %% Historical activity & capacity

        # based on historical_activity of older model in 2010
        # hydro_1 gets from hydro_lc, hydro_2 from hydro_hc
        # # NEW_ED all historic act to hydro_1
        df = scenario.par(
            "historical_activity",
            {"technology": ["hydro_lc"], "node_loc": [focus_region]},
        )
        reg_hist_activity = deepcopy(df.groupby("year_act").agg("sum")).reset_index()

        dfsub = (
            deepcopy(df.loc[df.technology == "hydro_lc"])
            .sort_values(by=["year_act"])
            .reset_index(drop="TRUE")
        )
        dfsub.technology = "hydro_1"
        # Warning occurs because dfsub is derived from another df. (!?) https://stackoverflow.com/questions/44723183/set-value-to-an-entire-column-of-a-pandas-dataframe
        dfsub["value"] = reg_hist_activity.value
        scenario.add_par("historical_activity", dfsub)
        max_hist_act1 = float(dfsub[dfsub.year_act == 2015].value)

        # same for hydro_hc and hydro_2
        # NEW_ED commented
        df = scenario.par(
            "historical_activity",
            {"technology": ["hydro_hc"], "node_loc": [focus_region]},
        )
        reg_hist_activity = deepcopy(df.groupby("year_act").agg("sum")).reset_index()

        dfsub = (
            deepcopy(df.loc[df.technology == "hydro_hc"])
            .sort_values(by=["year_act"])
            .reset_index(drop="TRUE")
        )
        dfsub.technology = "hydro_2"
        # Warning occurs because dfsub is derived from another df. (!?) https://stackoverflow.com/questions/44723183/set-value-to-an-entire-column-of-a-pandas-dataframe
        dfsub["value"] = reg_hist_activity.value
        scenario.add_par("historical_activity", dfsub)
        max_hist_act2 = float(dfsub[dfsub.year_act == 2015].value)

        # historical_new_capacity = also the sum of hydro_lc & hydro_hc
        # # NEW_ED all to hydro_1
        df = scenario.par(
            "historical_new_capacity",
            {"technology": ["hydro_lc"], "node_loc": [focus_region]},
        )
        reg_hcap = df.groupby(["year_vtg"]).sum().sort_values(by=["year_vtg"])
        reg_hcap = reg_hcap.reset_index(drop="TRUE")

        dfsub = (
            deepcopy(df.loc[df.technology == "hydro_lc"])
            .sort_values(by=["year_vtg"])
            .reset_index(drop="TRUE")
        )
        dfsub["technology"] = "hydro_1"
        dfsub["value"] = reg_hcap.value  # Need to be modified
        scenario.add_par("historical_new_capacity", dfsub)
        # same for hydro_hc and hydro_2
        # NEW_ED commented
        df = scenario.par(
            "historical_new_capacity",
            {"technology": ["hydro_hc"], "node_loc": [focus_region]},
        )
        reg_hcap = df.groupby(["year_vtg"]).sum().sort_values(by=["year_vtg"])
        reg_hcap = reg_hcap.reset_index(drop="TRUE")

        dfsub = (
            deepcopy(df.loc[df.technology == "hydro_hc"])
            .sort_values(by=["year_vtg"])
            .reset_index(drop="TRUE")
        )
        dfsub["technology"] = "hydro_2"
        dfsub["value"] = reg_hcap.value  # Need to be modified
        scenario.add_par("historical_new_capacity", dfsub)

        # %% Dynamic constraints on hydrotec adoption
        # Set relations: hydro_new_const
        df = scenario.par(
            "relation_activity",
            {
                "relation": ["hydro_pot"],
                "technology": ["hydro_lc"],
                "node_rel": [focus_region],
            },
        )
        df = df.loc[df.year_act > 2010]
        for i in newtechnames + newaggname:
            for j in relation_act_hydro:  # + relation_cap_hydro:
                df["relation"] = j
                df["technology"] = i
                df["value"] = 1 if (i in newtechnames) else -1
                scenario.add_par("relation_activity", df)

        for i in ["growth_activity_up"]:
            df = scenario.par(
                i, {"technology": ["hydro_lc"], "node_loc": [focus_region]}
            )
            df = df.loc[df.year_act > 2010]
            df["technology"] = "hydro_1"
            # hydro_2
            scenario.add_par(i, df)
            df = scenario.par(
                i, {"technology": ["hydro_hc"], "node_loc": [focus_region]}
            )
            df = df.loc[df.year_act > 2010]
            df["technology"] = "hydro_2"
            scenario.add_par(i, df)

        for i in ["growth_new_capacity_up"]:
            df = scenario.par(
                i, {"technology": ["coal_ppl"], "node_loc": [focus_region]}
            )
            df = df.loc[df.year_vtg > 2010]
            df["technology"] = "hydro_1"
            df["value"] = 0.1
            # hydro_2
            scenario.add_par(i, df)
            df["technology"] = "hydro_2"
            scenario.add_par(i, df)
        # bound act_up
        for i in ["bound_activity_up"]:
            df = scenario.par(
                i, {"technology": ["hydro_lc"], "node_loc": [focus_region]}
            )
            df = df.loc[df.year_act <= fmy]
            df["technology"] = "hydro_1"
            scenario.add_par(i, df)
            df = scenario.par(
                i, {"technology": ["hydro_hc"], "node_loc": [focus_region]}
            )
            df = df.loc[df.year_act <= fmy]
            df["technology"] = "hydro_2"
            scenario.add_par(i, df)

        # %% Tweaking initial_activity_up

        # Get the highest new capacity built in a year from the history
        # NEW_ED removed hydro_2
        df = scenario.par(
            "historical_new_capacity",
            {"technology": ["hydro_1", "hydro_2"], "node_loc": [focus_region]},
        )
        # since capacity factor is defined just after 1990, look at that range
        df = df[df.year_vtg >= 1990]
        reg_hcap = df.groupby(["year_vtg"]).sum().sort_values(by=["year_vtg"])
        reg_hcap = reg_hcap.reset_index(drop="TRUE")

        dfsub = (
            deepcopy(df.loc[df.technology == "hydro_1"])
            .sort_values(by=["year_vtg"])
            .reset_index(drop="TRUE")
        )
        dfsub["value"] = reg_hcap.value  # Need to be modified
        newcap_max = dfsub.value.max()
        year_max = dfsub.iloc[
            np.where(dfsub.value == dfsub.value.max())[0][0], :
        ].year_vtg

        # Get the highest capacity factor from that vintage year
        dfcf = scenario.par(
            "capacity_factor",
            {
                "technology": ["hydro_hc"],
                "node_loc": [focus_region],
                "year_vtg": [year_max],
            },
        )
        cfac_max = dfcf.value.max()

        # These give the highest startup activity per year.
        # init_act_up = newcap_max * cfac_max

        # %% Param inputs (Part 2 of 3).

        # These parameters are empty in global model.

        # So create them and do the following:
        #       1. take care of commodity (in input/output)
        #       2. renewable params with only commodity (not tec) like renewable_potential etc.

        # LF only for the new techs. LF for hist tech is in capacity_factor instead of renewable_capacity_factor

        model_years = scenario.set("year")[8:].reset_index(drop="TRUE")  # 1990 and on

        base_hydro_rating = {
            "node": focus_region,
            "year_act": model_years,  # 1990 and on
            "commodity": "electr",
            "level": "secondary",
            "time": "year",
            "rating": "full_rel",
        }

        for i in newcommnames:
            # if scenario_code == "hist":
            #     lfsub = LF.loc[LF.commodity == i].reset_index(drop=True)
            #     lfidx = lfsub.index
            # else:
            #     lf1sub = LF1.loc[LF1.commodity == i].reset_index(drop=True)
            #     lf2sub = LF2.loc[LF2.commodity == i].reset_index(drop=True)

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
        # NEW_ED remove hydro_2 & commented
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
        # NEW_ED commented hydro_2
        hydro_old_new = pd.DataFrame(
            [["hydro_1", "hydro_c1", "hydro_lc"], ["hydro_2", "hydro_c2", "hydro_hc"]],
            columns=["new_tec", "new_comm", "old_tec"],
        )
        for index, row in hydro_old_new.iterrows():
            print(row["new_tec"])
            print(row["old_tec"])

            dfrcf = scenario.par(
                "renewable_capacity_factor",
                {
                    "commodity": [row["new_comm"]],
                    "node": [focus_region],
                    "grade": ["c1"],
                },
            )
            # take value, before 2070 all th same
            short_term_rCF = float(dfrcf[dfrcf.year == startyear].value)
            df = scenario.par(
                "capacity_factor",
                {
                    "technology": [row["old_tec"]],
                    "year_vtg": vtg_years,
                    "node_loc": [focus_region],
                },
            )
            df["technology"] = row["new_tec"]
            # divide by the ren CF because it willbe multiply by it again in the model
            df["value"] = df["value"] / short_term_rCF

            scenario.add_par("capacity_factor", df)

        #%%

        for i in allhydrotech:  # not sure is hist tec should have it after 2020
            df = pd.DataFrame(
                dict(technology=i, value=1, unit="%", **base_hydro_rating)
            )
            scenario.add_par("rating_bin", df)
            scenario.add_par("reliability_factor", df)

        # renew_par = ["renewable_capacity_factor", "renewable_potential"]

        # %% Remove the old tec (This is only for deleting them for all regions.)

        par_rm = par_hydro.copy()
        par_rm.remove(
            "technical_lifetime"
        )  # Leaving 'technical_lifetime' because it sticks in map_tec and gives an error

        for t in par_rm:
            nodename = [
                x
                for x in scenario.idx_names(t)
                if x in ["node", "node_loc", "node_rel"]
            ]  # There are different node indices.

            if len(nodename) != 0:
                node_column = nodename[0]

            df = scenario.par(
                t, {"technology": ["hydro_hc", "hydro_lc"], node_column: [focus_region]}
            )
            scenario.remove_par(t, df)

        # Remove hydro_pot from relations (For other regions hydro_min may be considered too.)
        par_relHydroPot = [
            x for x in rel_set_hydro if "hydro_pot" in set(scenario.par(x)["relation"])
        ]
        par_relHydroMin = [
            x for x in rel_set_hydro if "hydro_min" in set(scenario.par(x)["relation"])
        ]

        for t in par_relHydroPot:
            df = scenario.par(
                t, {"relation": ["hydro_pot"], "node_rel": [focus_region]}
            )
            scenario.remove_par(t, df)

        for t in par_relHydroMin:  # This was not necessary for FSU.
            df = scenario.par(
                t, {"relation": ["hydro_min"], "node_rel": [focus_region]}
            )
            scenario.remove_par(t, df)

        print(
            focus_region.partition("_")[2]
            + " region: finished filling in parameters for new hydro in %.6s sec."
            % (time.time() - start_time)
        )

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
