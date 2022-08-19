"""Reporting for MESSAGEix-Buildings."""
import logging
from pathlib import Path

import message_ix
import pandas as pd
from message_ix_models import Context, ScenarioInfo
from message_ix_models.util import MESSAGE_DATA_PATH

log = logging.getLogger(__name__)

# Mappings for .replace()
SECTOR_NAME_MAP = {"comm": "Commercial", "resid": "Residential"}
FUEL_NAME_MAP = {
    "biomass": "Solids|Biomass",
    "biomass_nc": "Solids|Biomass|Traditional",
    "coal": "Solids|Coal",
    "d_heat": "Heat",
    "lightoil": "Liquids|Oil",
    "gas": "Gases",
    "electr": "Electricity",
}
NAME_MAP = dict(fuel=FUEL_NAME_MAP, sector=SECTOR_NAME_MAP)

# Common list of columns for several operations
COLS = ["node", "variable", "unit", "year", "value"]


def callback(rep: message_ix.Reporter, context: Context) -> None:
    """:meth:`.prepare_reporter` callback for MESSAGE-Buildings."""
    # Guess location of MESSAGE_Buildings code
    if "buildings" not in context:
        buildings_code_dir = MESSAGE_DATA_PATH.parent.joinpath("buildings")
        assert buildings_code_dir.exists()
        context.setdefault("buildings", dict(code_dir=buildings_code_dir))

    # Path where STURM output files are found
    rep.add(
        "sturm output path", context["buildings"]["code_dir"].joinpath("STURM_output")
    )

    # Temporary; disable storing time series
    rep.graph["config"]["buildings"] = dict(store_ts=False)

    # Add a key which invokes the monolithic reporting function
    rep.add("buildings all", report, "scenario", "config", "sturm output path")


def report(
    scenario: message_ix.Scenario, config: dict, sturm_output_path: Path
) -> None:
    """Report `scenario`.

    STURM output data are loaded from CSV files and merged with computed values stored
    as timeseries on `scenario`.

    Originally transcribed from :file:`reporting_EFC.py` in the buildings repository.

    .. todo:: decompose further by making use of genno features.
    """
    info = ScenarioInfo(scenario)

    build_ene_tecs = [
        tec
        for tec in info.set["technology"]
        if (
            (("resid" in tec) or ("comm" in tec))
            and (
                ("apps" in tec)
                or ("cook" in tec)
                or ("heat" in tec)
                or ("hotwater" in tec)
                or ("cool" in tec)
            )
        )
    ] + ["biomass_nc"]

    filters = dict(technology=build_ene_tecs, year_act=info.Y)

    # Final Energy Demand

    act = scenario.var("ACT", filters=filters)

    FE_rep = act.copy(True)

    # Fix for non commercial biomass to be consistent with MESSAGE's original numbers
    # which go directly from primary to useful. So, we are "de-usefulizing" here using
    # our conversion factor
    FE_rep.loc[FE_rep["technology"] == "biomass_nc", "lvl"] = (
        FE_rep.loc[FE_rep["technology"] == "biomass_nc", "lvl"] / 0.15
    )
    FE_bio_to_add = FE_rep.loc[FE_rep["technology"] == "biomass_nc"].copy(True)
    FE_bio_to_add.loc[
        FE_bio_to_add["technology"] == "biomass_nc", "technology"
    ] = "biomass_resid_cook"
    FE_rep = FE_rep.append(FE_bio_to_add)
    FE_rep.loc[
        FE_rep["technology"] == "biomass_nc", "technology"
    ] = "biomass_nc_resid_cook"

    FE_rep["commodity"] = FE_rep["technology"].str.rsplit("_", 1, expand=True)[0]
    FE_rep = FE_rep.rename(
        columns={"year_act": "year", "node_loc": "node", "lvl": "value"}
    )

    # Calculate totals by (commodity, node, year)
    FE_rep = (
        FE_rep[["node", "commodity", "year", "value"]]
        .groupby(["node", "commodity", "year"])
        .sum()
        .reset_index()
    )

    FE_rep[["fuel", "sector"]] = FE_rep["commodity"].str.rsplit("_", 1, expand=True)

    # Adjust sector and fuel names
    FE_rep.replace(NAME_MAP, inplace=True)

    FE_rep["variable"] = "Final Energy|" + FE_rep["sector"] + "|" + FE_rep["fuel"]

    # Convert from internal ACT GWa to EJ
    # FIXME(PNK) don't hard-code conversion factors; check input units
    FE_rep["unit"] = "EJ/yr"
    FE_rep["value"] = FE_rep["value"] * 31.536 / 1e3

    # Sum commercial and residential by fuel type
    FE_rep_tot = FE_rep.groupby(["node", "fuel", "unit", "year"]).sum().reset_index()
    FE_rep_tot["variable"] = (
        "Final Energy|" + "Residential and Commercial|" + FE_rep_tot["fuel"]
    )

    FE_rep = FE_rep[COLS].append(FE_rep_tot[COLS], ignore_index=True)

    # Compute a global total
    glob_rep = FE_rep.groupby(["variable", "unit", "year"]).sum().reset_index()
    glob_rep["node"] = "R12_GLB"

    FE_rep = FE_rep.append(glob_rep, ignore_index=True)

    FE_rep = FE_rep.sort_values(["node", "variable", "year"]).reset_index(drop=True)

    # Emissions from Demand
    emiss = scenario.par("relation_activity", filters=filters)

    emiss_rels = [rel for rel in emiss["relation"].unique() if "Emission" in rel]

    emiss = emiss.loc[emiss["relation"].isin(emiss_rels)]
    emiss = emiss.merge(act)
    emiss["value"] = emiss["value"] * emiss["lvl"]  # ?

    # Some fixes
    emiss.loc[
        emiss["technology"] == "biomass_nc", "technology"
    ] = "biomass_nc_resid_cook"
    emiss["commodity"] = emiss["technology"].str.rsplit("_", 1, expand=True)[0]
    emiss["emission"] = emiss["relation"].str.rsplit("_", 1, expand=True)[0]
    emiss["unit"] = "Mt " + emiss["emission"] + "/yr"

    emiss = emiss[["node_loc", "year_act", "commodity", "emission", "unit", "value"]]
    emiss = (
        emiss.groupby(["node_loc", "year_act", "commodity", "emission", "unit"])
        .sum()
        .reset_index()
    )

    emiss = emiss.rename(columns={"year_act": "year", "node_loc": "node"})

    emiss[["fuel", "sector"]] = emiss["commodity"].str.rsplit("_", 1, expand=True)

    # Adjust sector and fuel names
    emiss.replace(NAME_MAP, inplace=True)

    emiss["variable"] = (
        "Emissions|"
        + emiss["emission"]
        + "|Energy|Demand|"
        + emiss["sector"]
        + "|"
        + emiss["fuel"]
    )

    emiss_tot = (
        emiss.groupby(["node", "emission", "fuel", "unit", "year"]).sum().reset_index()
    )
    emiss_tot["variable"] = (
        "Emissions|"
        + emiss_tot["emission"]
        + "|Energy|Demand|Residential and Commercial|"
        + emiss_tot["fuel"]
    )

    emiss = emiss[COLS].append(emiss_tot[COLS], ignore_index=True)

    # Global total
    glob_emiss = emiss.groupby(["variable", "unit", "year"]).sum().reset_index()
    glob_emiss["node"] = "R12_GLB"
    emiss_rep = emiss.append(glob_emiss, ignore_index=True)

    emiss_rep = emiss_rep.sort_values(["node", "variable", "year"]).reset_index(
        drop=True
    )

    # Add STURM reporting
    if "baseline" in scenario.scenario:
        res_filename = "report_NGFS_SSP2_BL_resid_R12.csv"
        com_filename = "report_NGFS_SSP2_BL_comm_R12.csv"
    else:
        res_filename = "report_IRP_SSP2_2C_resid_R12.csv"
        com_filename = "report_IRP_SSP2_2C_comm_R12.csv"

    sturm_res = pd.read_csv(sturm_output_path / res_filename)
    sturm_com = pd.read_csv(sturm_output_path / com_filename)

    sturm_rep = sturm_res.append(sturm_com).melt(
        id_vars=["Model", "Scenario", "Region", "Variable", "Unit"], var_name="year"
    )
    sturm_rep["node"] = "R12_" + sturm_rep["Region"]
    sturm_rep = sturm_rep.rename(columns={"Variable": "variable", "Unit": "unit"})
    sturm_rep = sturm_rep[FE_rep.columns]

    # glob_sturm = sturm_rep.groupby(["variable", "unit", "year"]).sum().reset_index()
    # glob_sturm["node"] = "R12_GLB"
    #
    # sturm_rep = sturm_rep.append(glob_sturm, ignore_index=True)
    # sturm_rep = sturm_rep.sort_values(["node", "variable", "year"]).reset_index(
    #     drop=True
    # )

    # ------------------------------------------------------------------------------
    # The part below is added for futher data wrangling on top of the orginal one
    # Start from here
    # change the variable name and do sum for sturm_rep

    test = sturm_rep.copy(True)  # temp variable name used for trying

    prefix = "Energy Service|Residential and Commercial|"
    var_list = [
        f"{prefix}|Commercial",
        f"{prefix}|Residential|Multi-family|Floor space",
        f"{prefix}|Residential|Single-family|Floor space",
        f"{prefix}|Residential|Slum|Floor space",
    ]

    # for dealing with the final energy related variable names

    # Final energy related variables
    test_2 = test[~test["variable"].isin(var_list)].reset_index(drop=True)

    test_2[["variable_head", "sector", "sub_sector", "variable_rest"]] = test_2[
        "variable"
    ].str.split("|", 3, expand=True)

    test_2["new_var_name"] = test_2["variable_head"] + "|" + test_2["variable_rest"]

    test_2 = test_2.drop(
        columns=["sector", "variable_rest", "variable_head", "variable"]
    )

    # Sum of residential and commercial
    test_2 = (
        test_2.groupby(["node", "unit", "year", "new_var_name"]).sum().reset_index()
    )

    test_2[["new_var_head", "new_var_rest"]] = test_2["new_var_name"].str.split(
        "|", 1, expand=True
    )

    test_2["sector"] = "|Residential and Commercial|"

    test_2["variable"] = (
        test_2["new_var_head"] + test_2["sector"] + test_2["new_var_rest"]
    )

    test_2 = test_2.drop(
        columns=["new_var_name", "new_var_head", "new_var_rest", "sector"]
    )

    # Order the columns
    test_2 = test_2[COLS]

    test_full = sturm_rep.append(test_2)

    # Global total
    glob_sturm = test_full.groupby(["variable", "unit", "year"]).sum().reset_index()
    glob_sturm["node"] = "R12_GLB"
    test_full = test_full.append(glob_sturm, ignore_index=True)

    test_full = test_full.sort_values(["node", "variable", "year"]).reset_index(
        drop=True
    )

    # ----------------------------------------------
    # sum of the building related Final Energy by fuel types to get the variable
    # "Final Energy|Residential and Commercial",
    # "Final Energy|Residential", and "Final Energy|Commercial"
    # for FE_rep

    FE_rep["fuel_type"] = FE_rep["variable"].str.split("|", 2, expand=True)[1]

    var_list_drop = [
        "Final Energy|Residential and Commercial|Solids|Biomass|Traditional",
        "Final Energy|Residential|Solids|Biomass|Traditional",
        "Final Energy|Commercial|Solids|Biomass",
    ]

    # Sum of fuel types for different building sub-setors (R, C and R+C)
    FE_rep_tot_fuel = (
        FE_rep[~FE_rep["variable"].isin(var_list_drop)]
        .groupby(["node", "unit", "year", "fuel_type"])
        .sum()
        .reset_index()
    )

    FE_rep_tot_fuel["variable"] = "Final Energy|" + FE_rep_tot_fuel["fuel_type"]

    FE_rep_tot_fuel = FE_rep_tot_fuel.drop(columns=["fuel_type"])

    FE_rep = FE_rep.drop(columns=["fuel_type"])

    FE_rep = FE_rep[COLS].append(FE_rep_tot_fuel[COLS], ignore_index=True)

    # Store time series data on `scenario`
    # TODO use store_ts from ixmp
    if config["buildings"]["store_ts"]:
        scenario.check_out(timeseries_only=True)

        scenario.add_timeseries(FE_rep)
        scenario.add_timeseries(emiss_rep)
        # scenario.add_timeseries(sturm_rep)
        scenario.add_timeseries(test_full)  # temp use

        scenario.commit("MESSAGEix-Buildings reporting")
    else:
        log.info("Skip storing data")

    # Write time series data to files
    # TODO use write_report from genno
    FE_rep.to_csv(config["output_path"] / "buildings-FE.csv")
    emiss_rep.to_csv(config["output_path"] / "buildings-emiss.csv")
    sturm_rep.to_csv(config["output_path"] / "sturm.csv")
    test_full.to_csv(config["output_path"] / "sturm-name-change.csv")
