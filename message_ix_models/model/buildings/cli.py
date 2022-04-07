"""MESSAGEix-Buildings model."""
import os
import gc
from pathlib import Path
from time import time

import click
import message_ix
import message_ix_models.util as mutil
import numpy as np
import pandas as pd

# from message_data.projects.ngfs.util import add_macro_COVID  # Unused

# Workaround if rpy2 not installed
# (to avoid potential segfaults)
try:
    # Check Python and R environments (for debugging)
    import rpy2.situation

    rpy2_installed = 1

except ImportError:
    rpy2_installed = 0


@click.command("buildings")
@click.pass_obj
def cli(context):
    """MESSAGEix-Buildings model."""
    if rpy2_installed:
        # load rpy2 modules
        import rpy2.robjects as ro
        from rpy2.robjects import pandas2ri
        from rpy2.robjects.conversion import localconverter

        for row in rpy2.situation.iter_info():
            print(row)

        # paths to r code and lca data
        rcode_path = os.getcwd() + "/STURM_model/"
        data_path = os.getcwd() + "/STURM_data/"
        rout_path = os.getcwd() + "/STURM_output/"

    # TODO(PNK) this expects the MESSAGE_Buildings code in the same directory; instead
    # read the location from the context
    from E_USE_Model import Simulation_ACCESS_E_USE
    from utils import rc_afofi, add_globiom

    # Load database
    mp = context.get_platform()

    # Add floorspace unit
    mp.add_unit("Mm2/y", "mil. square meters by year")

    # Specify SSP and Climate scenario
    ssp_scen = "SSP2"
    clim_scen = "BL"
    # clim_scen = "2C"

    # Specify whether to make a new copy from a baseline
    # scenario or load an existing scenario
    clone = 1

    # Specify whether to solve MESSSGE (0) or MESSAGE-MACRO (1)
    solve_macro = 0

    # Specify the scenario to be cloned
    # NOTE: this scenario has the updated GLOBIOM matrix

    # # M -> BM baseline
    # mod_orig = "MESSAGEix-GLOBIOM 1.1-M-R12-NGFS"
    # scen_orig = "baseline"

    # mod_new = "MESSAGEix-GLOBIOM 1.1-BM-R12-NGFS"
    # scen_new = "baseline"

    # BM NPi (after "run_cdlinks_setup" for NPi)
    # This has MACRO but here run MESSAGE only.
    mod_orig = "MESSAGEix-GLOBIOM 1.1-BM-R12-NGFS"
    scen_orig = "NPi2020-con-prim-dir-ncr"

    mod_new = "MESSAGEix-GLOBIOM 1.1-BM-R12-NGFS"
    scen_new = "NPi2020-con-prim-dir-ncr-building"

    if clone:
        scen_to_clone = message_ix.Scenario(mp, mod_orig, scen_orig)
        scenario = scen_to_clone.clone(model=mod_new, scenario=scen_new)
    else:
        scen_to_clone = message_ix.Scenario(mp, mod_orig, scen_orig)
        scenario = message_ix.Scenario(mp, mod_new, scen_new)

    # Open reference climate scenario if needed
    if clim_scen == "2C":
        mod_mitig = "ENGAGE_SSP2_v4.1.8"
        scen_mitig = "EN_NPi2020_1000f"
        scen_mitig_prices = message_ix.Scenario(mp, mod_mitig, scen_mitig)

    ######################
    #  MESSAGE ITERATION #
    ######################

    done = 0
    start_time = time()
    iterations = 0

    old_diff = -1
    oscilation = 0

    # Commodities for the buildings sector
    build_commodities = [
        "resid_floor_construction",  # floorspace to be constructed
        "resid_floor_demolition",  # floorspace to be demolished
        "comm_floor_construction",  # floorspace to be constructed
        "comm_floor_demolition",  # floorspace to be demolished
        # TODO: Need to harmonize on the commodity names (remove the material name)
    ]

    # Technologies for the buildings sector
    build_techs = [
        # technology providing residential floorspace activity
        "construction_resid_build",
        "demolition_resid_build",
        # technology providing commercial floorspace activity
        "construction_comm_build",
        "demolition_comm_build",
    ]

    # Commodity names to be converted for use in MESSAGEix-Material
    build_comm_convert = [
        "resid_mat_int_scrap_steel",
        "resid_mat_int_scrap_aluminum",
        "resid_mat_int_scrap_cement",
        "resid_mat_int_demand_steel",
        "resid_mat_int_demand_aluminum",
        "resid_mat_int_demand_cement",
        "comm_mat_int_scrap_steel",
        "comm_mat_int_scrap_aluminum",
        "comm_mat_int_scrap_cement",
        "comm_mat_int_demand_steel",
        "comm_mat_int_demand_aluminum",
        "comm_mat_int_demand_cement",
    ]

    # Types of materials
    materials = ["steel", "cement", "aluminum"]

    # Non Model and Model Years
    years_not_mod = [
        year
        for year in scenario.par("demand").year.unique()
        if year < scenario.firstmodelyear
    ]
    years_not_mod.sort()
    years_model = [
        year
        for year in scenario.par("demand").year.unique()
        if year >= scenario.firstmodelyear
    ]
    years_model.sort()

    nodes = scenario.set("node")

    while done < 1:
        # Get prices from MESSAGE
        # On the first iteration, from the parent scenario; onwards, from the current
        # scenario
        price_source = scen_to_clone if iterations == 0 else scenario
        prices = price_source.var(
            "PRICE_COMMODITY",
            filters={
                "level": "final",
                "commodity": ["biomass", "coal", "lightoil", "gas", "electr", "d_heat"],
            },
        )

        suffix = prices.node.str.split("_", expand=True)[0][0]
        prices = prices.loc[prices["node"] != suffix + "_GLB"]

        # Save demand from previous iteration for
        # comparison
        if iterations == 0:
            demand_old = pd.DataFrame()
            diff_dd = 1e6
        else:
            demand_old = demand.copy(True)

        # Run Models

        # ACCESS-E-USE
        e_use_scenarios = Simulation_ACCESS_E_USE.run_E_USE(
            scenario=ssp_scen, prices=prices
        )

        # Scale results to match historical activity
        # NOTE: ignore biomass, data was always imputed here
        # so we are dealing with guesses over guesses
        e_use_2010 = (
            e_use_scenarios.loc[e_use_scenarios["year"] == 2010]
            .loc[
                e_use_scenarios.commodity.isin(
                    com
                    for com in e_use_scenarios["commodity"]
                    if "bio" not in com and "non-comm" not in com
                )
            ]
            .groupby("node", as_index=False)
            .sum()
        )
        rc_act_2010 = scen_to_clone.par(
            "historical_activity",
            filters={
                "year_act": 2010,
                "technology": [
                    tec
                    for tec in scenario.set("technology")
                    if "rc" in tec and "bio" not in tec
                ],
            },
        )
        rc_act_2010 = rc_act_2010.rename(columns={"node_loc": "node"})
        rc_act_2010 = (
            rc_act_2010[["node", "value"]].groupby("node", as_index=False).sum()
        )

        adj_fact = rc_act_2010.copy(True)
        adj_fact["value"] = adj_fact["value"] / e_use_2010["value"]
        adj_fact = adj_fact.rename(columns={"value": "adj_fact"})

        e_use_scenarios = e_use_scenarios.merge(adj_fact, on=["node"])
        e_use_scenarios["value"] = (
            e_use_scenarios["value"] * e_use_scenarios["adj_fact"]
        )
        e_use_scenarios = e_use_scenarios.drop("adj_fact", axis=1)
        e_use_scenarios = e_use_scenarios.loc[e_use_scenarios["year"] > 2010]

        # STURM
        if rpy2_installed:

            print("rpy2 found")
            # Source R code
            r = ro.r
            r.source(str(Path(rcode_path + "/F10_scenario_runs_MESSAGE_2100.R")))

            with localconverter(ro.default_converter + pandas2ri.converter):
                # Residential
                sturm_scenarios = r.run_scenario(
                    run=ssp_scen,
                    scenario_name=ssp_scen + "_" + clim_scen,
                    prices=prices,
                    path_in=str(data_path),
                    path_rcode=str(rcode_path),
                    path_out=str(rout_path),
                    geo_level_report="R12",
                    sector="resid",
                )
                # Commercial
                # NOTE: run only on the first iteration!
                if iterations == 0:
                    comm_sturm_scenarios = r.run_scenario(
                        run=ssp_scen,
                        scenario_name=ssp_scen + "_" + clim_scen,
                        prices=prices,
                        path_in=str(data_path),
                        path_rcode=str(rcode_path),
                        path_out=str(rout_path),
                        geo_level_report="R12",
                        sector="comm",
                    )

            del r
            gc.collect()
        else:
            print("rpy2 NOT found")
            # Prepare input files
            if not os.path.exists(str(Path(os.getcwd() + "/temp"))):
                os.mkdir(str(Path(os.getcwd() + "/temp")))
            prices.to_csv("./temp/prices.csv")

            # Residential
            with open("run_STURM.R", "r") as file:
                mm = file.readlines()

            mm[8] = 'ssp_scen <- "' + ssp_scen + '"\n'
            mm[9] = 'clim_scen <- "' + clim_scen + '"\n'
            mm[10] = 'sect <- "resid"\n'

            with open("run_STURM.R", "w") as file:
                file.writelines(mm)

            os.system("Rscript run_STURM.R")
            sturm_scenarios = pd.read_csv("./temp/resid_sturm.csv")
            os.remove("./temp/resid_sturm.csv")

            # Commercial
            if iterations == 0:
                with open("run_STURM.R", "r") as file:
                    mm = file.readlines()

                mm[8] = 'ssp_scen <- "' + ssp_scen + '"\n'
                mm[9] = 'clim_scen <- "' + clim_scen + '"\n'
                mm[10] = 'sect <- "comm"\n'

                with open("run_STURM.R", "w") as file:
                    file.writelines(mm)

                os.system("Rscript run_STURM.R")
                comm_sturm_scenarios = pd.read_csv("./temp/comm_sturm.csv")
                os.remove("./temp/comm_sturm.csv")

            os.remove("./temp/prices.csv")
            os.rmdir(str(Path(os.getcwd() + "/temp")))

        # TEMP: remove commodity "comm_heat_v_no_heat"
        if iterations == 0:
            comm_sturm_scenarios = comm_sturm_scenarios.loc[
                (comm_sturm_scenarios.commodity != "comm_heat_v_no_heat")
                & (comm_sturm_scenarios.commodity != "comm_hotwater_v_no_heat")
            ]

        # TEMP: remove commodity "resid_heat_v_no_heat"
        sturm_scenarios = sturm_scenarios.loc[
            (sturm_scenarios.commodity != "resid_heat_v_no_heat")
            & (sturm_scenarios.commodity != "resid_hotwater_v_no_heat")
        ]

        # Subset desired energy demands
        demand = e_use_scenarios.loc[
            e_use_scenarios["commodity"].isin(
                [
                    com
                    for com in e_use_scenarios["commodity"].unique()
                    if "therm" not in com
                ]
            )
        ]
        demand = pd.concat(
            [
                demand,
                sturm_scenarios.loc[
                    sturm_scenarios["commodity"].isin(
                        [
                            com
                            for com in sturm_scenarios["commodity"].unique()
                            if ("hotwater" in com) | ("cool" in com) | ("heat" in com)
                        ]
                    )
                ],
            ]
        )
        # Add commercial demand in first iteration
        if iterations == 0:
            demand = pd.concat(
                [
                    demand,
                    comm_sturm_scenarios.loc[
                        comm_sturm_scenarios["commodity"].isin(
                            [
                                com
                                for com in comm_sturm_scenarios["commodity"].unique()
                                if ("hotwater" in com)
                                | ("cool" in com)
                                | ("heat" in com)
                            ]
                        )
                    ],
                ]
            )

        # Set energy demand level to useful (although it is final)
        # to be in line with 1 to 1 technologies btw final and useful
        demand["level"] = "useful"

        # Append floorspace demand from sturm_scenarios
        # TODO: Need to harmonize on the commodity names
        # (remove the material names)
        demand_resid_build = sturm_scenarios[
            sturm_scenarios["commodity"].isin(
                ["resid_floor_construction", "resid_floor_demolition"]
            )
        ]
        demand = pd.concat([demand, demand_resid_build])

        # Add commercial demand in first iteration
        if iterations == 0:
            demand_comm_build = comm_sturm_scenarios[
                comm_sturm_scenarios["commodity"].isin(
                    ["comm_floor_construction", "comm_floor_demolition"]
                )
            ]
            demand = pd.concat([demand, demand_comm_build])

        # Fill with zeros if nan
        demand = demand.fillna(0)

        # Update demands in the scenario
        if scenario.has_solution():
            scenario.remove_solution()

        scenario.check_out()

        # Check if not buildings scenario
        if build_commodities[0] not in scenario.par("demand")["commodity"].unique():

            # Add new commodities and technologies
            scenario.add_set("commodity", build_commodities)
            scenario.add_set("technology", build_techs)

            # Find emissions in relation activity
            emiss_rel = [
                rel
                for rel in scenario.par("relation_activity").relation.unique()
                if "Emission" in rel
            ]

            # Create new demands and techs for AFOFI
            # based on percentages between 2010 and 2015
            # (see rc_afofi.py in utils)
            dd_replace = scenario.par(
                "demand",
                filters={"commodity": ["rc_spec", "rc_therm"], "year": years_model},
            )
            [perc_afofi_therm, perc_afofi_spec] = rc_afofi.return_PERC_AFOFI()
            afofi_dd = dd_replace.copy(True)
            for reg in perc_afofi_therm.index:
                afofi_dd.loc[
                    (afofi_dd["node"] == "R12_" + reg)
                    & (afofi_dd["commodity"] == "rc_therm"),
                    "value",
                ] = (
                    afofi_dd.loc[
                        (afofi_dd["node"] == "R12_" + reg)
                        & (afofi_dd["commodity"] == "rc_therm"),
                        "value",
                    ]
                    * perc_afofi_therm.loc[reg][0]
                )
                afofi_dd.loc[
                    (afofi_dd["node"] == "R12_" + reg)
                    & (afofi_dd["commodity"] == "rc_spec"),
                    "value",
                ] = (
                    afofi_dd.loc[
                        (afofi_dd["node"] == "R12_" + reg)
                        & (afofi_dd["commodity"] == "rc_spec"),
                        "value",
                    ]
                    * perc_afofi_spec.loc[reg][0]
                )

            afofi_dd["commodity"] = afofi_dd["commodity"].str.replace("rc", "afofi")
            scenario.add_set("commodity", afofi_dd["commodity"].unique())
            scenario.add_par("demand", afofi_dd)

            rc_techs = scenario.par(
                "output", filters={"commodity": ["rc_therm", "rc_spec"]}
            )["technology"].unique()

            for tech_orig in rc_techs:
                tech_new = tech_orig.replace("rc", "afofi")
                if "RC" in tech_orig:
                    tech_new = tech_orig.replace("RC", "AFOFI")

                afofi_in = scenario.par("input", filters={"technology": tech_orig})
                afofi_in["technology"] = tech_new

                afofi_out = scenario.par("output", filters={"technology": tech_orig})
                afofi_out["technology"] = tech_new
                afofi_out["commodity"] = afofi_out["commodity"].str.replace(
                    "rc", "afofi"
                )

                afofi_cf = scenario.par(
                    "capacity_factor", filters={"technology": tech_orig}
                )
                afofi_cf["technology"] = tech_new

                afofi_ef = scenario.par(
                    "emission_factor", filters={"technology": tech_orig}
                )
                afofi_ef["technology"] = tech_new

                afofi_rel = scenario.par(
                    "relation_activity",
                    filters={"technology": tech_orig, "relation": emiss_rel},
                )
                afofi_rel["technology"] = tech_new

                scenario.add_set("technology", tech_new)
                scenario.add_par("input", afofi_in)
                scenario.add_par("output", afofi_out)
                scenario.add_par("capacity_factor", afofi_cf)
                scenario.add_par("emission_factor", afofi_ef)
                scenario.add_par("relation_activity", afofi_rel)

            # Set model demands for rc_therm and rc_spec to zero
            dd_replace["value"] = 0
            scenario.add_par("demand", dd_replace)

            # Create new input/output for building material intensities
            for c in build_comm_convert:
                comm = c.split("_")[-1]
                typ = c.split("_")[-2]
                rc = c.split("_")[-5]  # "resid" or "comm"

                # Exclude World and GLB regions
                for n in nodes.drop(index=[0, 5]):  # TODO: Avoid hard-coded index
                    if rc == "resid":
                        df_mat = sturm_scenarios.loc[
                            (sturm_scenarios["commodity"] == c)
                            & (sturm_scenarios["node"] == n)
                        ]
                    elif rc == "comm" and iterations == 0:
                        df_mat = comm_sturm_scenarios.loc[
                            (comm_sturm_scenarios["commodity"] == c)
                            & (comm_sturm_scenarios["node"] == n)
                        ]

                    common = dict(
                        time="year",
                        time_origin="year",
                        time_dest="year",
                        mode="M1",
                        node_loc=n,
                        node_origin=n,
                        node_dest=n,
                        year_vtg=years_model,
                        year_act=years_model,
                    )

                    if typ == "demand":
                        tec = "construction_" + rc + "_build"
                        # Need to take care of 2110 by appending the last value
                        df_demand = mutil.make_io(
                            (comm, "demand", "t"),
                            (rc + "_floor_construction", "demand", "t"),
                            efficiency=pd.concat([df_mat.value, df_mat.value.tail(1)]),
                            technology=tec,
                            **common
                        )
                        scenario.add_par("input", df_demand["input"])
                        scenario.add_par("output", df_demand["output"])
                    elif typ == "scrap":
                        tec = "demolition_" + rc + "_build"
                        # Need to take care of 2110 by appending the last value
                        df_scrap = mutil.make_io(
                            (comm, "end_of_life", "t"),  # will be flipped to output
                            (rc + "_floor_demolition", "demand", "t"),
                            efficiency=pd.concat([df_mat.value, df_mat.value.tail(1)]),
                            technology=tec,
                            **common
                        )
                        # Flip input to output (no input for demolition)
                        df_temp = df_scrap["input"].rename(
                            columns={
                                "node_origin": "node_dest",
                                "time_origin": "time_dest",
                            }
                        )
                        scenario.add_par("output", df_temp)
                        scenario.add_par("output", df_scrap["output"])

            # Subtract building material demand from existing demands in scenario
            for rc in ["resid", "comm"]:
                if not (rc == "comm" and iterations > 0):
                    df_out = (
                        sturm_scenarios.copy(True)
                        if rc == "resid"
                        else comm_sturm_scenarios.copy(True)
                    )
                    df = df_out[
                        df_out["commodity"].isin(
                            [
                                rc + "_mat_demand_cement",
                                rc + "_mat_demand_steel",
                                rc + "_mat_demand_aluminum",
                            ]
                        )
                    ]  # .copy(True)
                    df["commodity"] = df.apply(
                        lambda x: x.commodity.split("_")[-1], axis=1
                    )
                    df = df.rename(columns={"value": "demand_" + rc + "_const"}).drop(
                        columns=["level", "time", "unit"]
                    )
                    # df = df.stack()
                    mat_demand = (
                        scenario.par("demand", {"level": "demand"})
                        .join(
                            df.set_index(["node", "year", "commodity"]),
                            on=["node", "year", "commodity"],
                            how="left",
                        )
                        .dropna()
                    )
                    mat_demand["value"] = np.maximum(
                        mat_demand["value"] - mat_demand["demand_" + rc + "_const"], 0
                    )
                    scenario.add_par(
                        "demand", mat_demand.drop(columns="demand_" + rc + "_const")
                    )

            # Create new technologies for building energy
            rc_tech_fuel = pd.DataFrame(
                {
                    "fuel": ["biomass", "coal", "lightoil", "gas", "electr", "d_heat"],
                    "technology": [
                        "biomass_rc",
                        "coal_rc",
                        "loil_rc",
                        "gas_rc",
                        "elec_rc",
                        "heat_rc",
                    ],
                }
            )

            # Add for fuels above
            for fuel in prices["commodity"].unique():

                # Find the original rc technology for the fuel
                tech_orig = rc_tech_fuel.loc[
                    rc_tech_fuel["fuel"] == fuel, "technology"
                ].values[0]

                # Remove lower bound in activity for older, now unused
                # rc techs to allow them to reach zero
                act_lo = scenario.par(
                    "bound_activity_lo",
                    filters={"technology": tech_orig, "year_act": years_model},
                )
                act_lo["value"] = 0.0
                scenario.add_par("bound_activity_lo", act_lo)

                growth_act = scenario.par(
                    "growth_activity_lo",
                    filters={"technology": tech_orig, "year_act": years_model},
                )
                growth_act["value"] = -1.0
                scenario.add_par("growth_activity_lo", growth_act)

                soft_act = scenario.par(
                    "soft_activity_lo",
                    filters={"technology": tech_orig, "year_act": years_model},
                )
                soft_act["value"] = 0.0
                scenario.add_par("soft_activity_lo", soft_act)

                # Create the technologies for the new commodities
                for commodity in [
                    com
                    for com in demand["commodity"].unique()
                    if ("_" + fuel in com) or ("-" + fuel in com)
                ]:

                    # Fix for lightoil gas included
                    if "lightoil-gas" in commodity:
                        tech_new = (
                            fuel + "_lg_" + commodity.replace("_lightoil-gas", "")
                        )
                    else:
                        tech_new = fuel + "_" + commodity.replace("_" + fuel, "")

                    build_in = scenario.par("input", filters={"technology": tech_orig})
                    build_in["technology"] = tech_new
                    build_in["value"] = 1.0

                    build_out = scenario.par(
                        "output", filters={"technology": tech_orig}
                    )
                    build_out["technology"] = tech_new
                    build_out["commodity"] = commodity
                    build_out["value"] = 1.0

                    build_cf = scenario.par(
                        "capacity_factor", filters={"technology": tech_orig}
                    )
                    build_cf["technology"] = tech_new

                    build_ef = scenario.par(
                        "emission_factor", filters={"technology": tech_orig}
                    )
                    build_ef["technology"] = tech_new

                    build_rel = scenario.par(
                        "relation_activity",
                        filters={"technology": tech_orig, "relation": emiss_rel},
                    )
                    build_rel["technology"] = tech_new

                    scenario.add_set("commodity", commodity)
                    scenario.add_set("technology", tech_new)
                    scenario.add_par("input", build_in)
                    scenario.add_par("output", build_out)
                    scenario.add_par("capacity_factor", build_cf)
                    scenario.add_par("emission_factor", build_ef)
                    scenario.add_par("relation_activity", build_rel)

        # Rename non-comm
        demand.loc[
            demand["commodity"] == "resid_cook_non-comm", "commodity"
        ] = "non-comm"

        # Fix years (they appear as float)
        demand["year"] = demand["year"].astype(int)

        # Fill missing years...
        # ...with zeroes before model starts
        fill_dd = demand.loc[demand["year"] == scenario.firstmodelyear].copy(True)
        fill_dd["value"] = 0
        for year in years_not_mod:
            fill_dd["year"] = year
            demand = pd.concat([demand, fill_dd])
        # and with the same growth as between 2090 and 2100 for 2110
        dd_2110 = demand.loc[demand["year"] >= 2090].copy(True)
        dd_2110 = dd_2110.pivot(
            index=["node", "commodity", "level", "time", "unit"],
            columns="year",
            values="value",
        ).reset_index()
        dd_2110["value"] = dd_2110[2100] * dd_2110[2100] / dd_2110[2090]
        # unless the demand from 2090 is zero, which creates div by zero
        # in which case take the average (i.e. value for 2100 div by 2)
        # NOTE: no particular reason, just my choice!
        dd_2110.loc[dd_2110[2090] == 0, "value"] = (
            dd_2110.loc[dd_2110[2090] == 0, 2100] / 2
        )
        # or if the demand grows too much indicating a
        # relatively too low value for 2090
        dd_2110.loc[dd_2110["value"] > 3 * dd_2110[2100], "value"] = (
            dd_2110.loc[dd_2110["value"] > 3 * dd_2110[2100], 2100] / 2
        )
        # or simply if there is an NA
        dd_2110.loc[dd_2110["value"].isna(), "value"] = (
            dd_2110.loc[dd_2110["value"].isna(), 2100] / 2
        )

        dd_2110["year"] = 2110
        demand = pd.concat(
            [
                demand,
                dd_2110[
                    ["node", "commodity", "level", "year", "time", "value", "unit"]
                ],
            ],
            ignore_index=True,
        )

        # Update demand in scenario
        demand = demand.sort_values(by=["node", "commodity", "year"])
        scenario.add_par("demand", demand)

        # Add tax emissions from mitigation scenario if running a
        # climate scenario and if they are not already there
        if (scenario.par("tax_emission").size == 0) and (clim_scen != "BL"):
            tax_emission_new = scen_mitig_prices.var("PRICE_EMISSION")
            tax_emission_new.columns = scenario.par("tax_emission").columns
            tax_emission_new["unit"] = "USD/tCO2"
            scenario.add_par("tax_emission", tax_emission_new)

        if "time_relative" not in scenario.set_list():
            scenario.init_set("time_relative")

        # Run MESSAGE
        scenario.commit("buildings test")

        # Add bio backstop
        add_globiom.add_bio_backstop(scenario)

        if solve_macro:
            mod = "MESSAGE-MACRO"
        else:
            mod = "MESSAGE"

        try:  # Try with barrier, faster
            message_ix.models.DEFAULT_CPLEX_OPTIONS = {
                "advind": 0,
                "lpmethod": 4,
                "threads": 4,
                "epopt": 1e-06,
            }
            scenario.solve(model=mod)
        # If barrier doesn't work, try dual simplex
        except Exception:
            message_ix.models.DEFAULT_CPLEX_OPTIONS = {
                "advind": 0,
                "lpmethod": 2,
                "threads": 4,
                "epopt": 1e-06,
            }
            scenario.solve(model=mod)

        # Compare prices and see if they converge
        prices_new = scenario.var(
            "PRICE_COMMODITY",
            filters={
                "level": "final",
                "commodity": ["biomass", "coal", "lightoil", "gas", "electr", "d_heat"],
            },
        )
        prices_new = prices_new.loc[prices_new["node"] != suffix + "_GLB"]

        # Create the DataFrames to keep track of demands and prices
        if iterations == 0:
            price_sav = prices_new.copy(True).drop("lvl", axis=1)
            demand_sav = demand.copy(True).drop("value", axis=1)

        # Compare differences in mean percentage deviation
        diff = prices_new.merge(prices, on=["node", "commodity", "year"])
        diff = diff.loc[diff["year"] != 2110]
        diff["diff"] = (diff["lvl_x"] - diff["lvl_y"]) / (
            0.5 * ((diff["lvl_x"] + diff["lvl_y"]))
        )
        diff = np.mean(abs(diff["diff"]))

        print("Iteration:", iterations)
        print("Mean Percentage Deviation in Prices:", diff)

        # Compare differences in demand after the first iteration
        if iterations > 0:
            diff_dd = demand.merge(demand_old, on=["node", "commodity", "year"])
            diff_dd = diff_dd.loc[diff_dd["year"] != 2110]
            diff_dd["diff_dd"] = (diff_dd["value_x"] - diff_dd["value_y"]) / (
                0.5 * ((diff_dd["value_x"] + diff_dd["value_y"]))
            )
            diff_dd = np.mean(abs(diff_dd["diff_dd"]))
            print("Mean Percentage Deviation in Demand:", diff_dd)

        # Uncomment this on for testing
        # diff = 0.0

        if (diff < 5e-3) or ((iterations > 0) & (diff_dd < 5e-3)):
            done = 1
            print("Converged in ", iterations, " iterations")
            print("Total time:", (time() - start_time) / 3600)
            # scenario.set_as_default()

        if iterations > 10:
            done = 2
            print("Not Converged after 10 iterations!")
            print("Averaging last two demands and running MESSAGE one more time")
            price_sav["lvl" + str(iterations)] = prices_new["lvl"]
            demand_sav = demand_sav.merge(
                demand,
                on=["node", "commodity", "level", "year", "time", "unit"],
                how="left",
            )
            demand_sav = demand_sav.rename(columns={"value": "value" + str(iterations)})
            demand_sav.columns.isin(demand.columns)
            dd_avg = demand_sav[
                [
                    "node",
                    "commodity",
                    "level",
                    "year",
                    "time",
                    "unit",
                    "value" + str(iterations - 1),
                    "value" + str(iterations),
                ]
            ].copy(True)
            dd_avg["value_avg"] = (
                dd_avg["value" + str(iterations - 1)]
                + dd_avg["value" + str(iterations)]
            ) / 2
            dd_avg = dd_avg.loc[~dd_avg["value_avg"].isna()]

            demand = demand.merge(
                dd_avg[
                    ["node", "commodity", "level", "year", "time", "unit", "value_avg"]
                ],
                on=["node", "commodity", "level", "year", "time", "unit"],
                how="left",
            )
            demand.loc[~demand["value_avg"].isna(), "value"] = demand.loc[
                ~demand["value_avg"].isna(), "value_avg"
            ]
            demand = demand.drop(columns="value_avg")

            scenario.remove_solution()
            scenario.check_out()
            scenario.add_par("demand", demand)
            scenario.commit("buildings test")
            scenario.solve(model=mod)
            iterations = iterations + 0.5
            prices_new = scenario.var(
                "PRICE_COMMODITY",
                filters={
                    "level": "final",
                    "commodity": [
                        "biomass",
                        "coal",
                        "lightoil",
                        "gas",
                        "electr",
                        "d_heat",
                    ],
                },
            )
            prices_new = prices_new.loc[prices_new["node"] != suffix + "_GLB"]
            print("Final solution after Averaging last two demands")
            print("Total time:", (time() - start_time) / 3600)

        if abs(old_diff - diff) < 1e-5:
            oscilation = 1

        # Keep track of results
        demand_sav = demand_sav.merge(
            demand,
            on=["node", "commodity", "level", "year", "time", "unit"],
            how="left",
        )
        demand_sav = demand_sav.rename(columns={"value": "value" + str(iterations)})
        price_sav["lvl" + str(iterations)] = prices_new["lvl"]
        price_sav.to_csv("price_track.csv")
        demand_sav.to_csv("demand_track.csv")

        iterations = iterations + 1
        old_diff = diff

    # Calibrate MACRO with the outcome of MESSAGE baseline iterations
    # if done and solve_macro==0 and clim_scen=="BL":
    #     sc_macro = add_macro_COVID(scenario, reg="R12", check_converge=False)
    #     sc_macro = sc_macro.clone(scenario = "baseline_DEFAULT")
    #     sc_macro.set_as_default()

    mp.close_db()
