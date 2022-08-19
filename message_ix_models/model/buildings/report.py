"""Reporting for MESSAGEix-Buildings."""
import logging
import re
from pathlib import Path
from typing import Dict, List

import message_ix
import pandas as pd
from iam_units import registry
from message_ix_models import Context
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
        assert MESSAGE_DATA_PATH is not None
        buildings_code_dir = MESSAGE_DATA_PATH.parent.joinpath("buildings")
        assert buildings_code_dir.exists()
        context.setdefault("buildings", dict(code_dir=buildings_code_dir))

    # Path where STURM output files are found
    rep.add(
        "sturm output path", context["buildings"]["code_dir"].joinpath("STURM_output")
    )

    # Filters for retrieving data. Use the keys "t" and "y::model" that are
    # automatically populated by message_ix and message_data, respectively.
    rep.add("buildings filters", buildings_filters, "t", "y::model")

    # Lists of keys for use later
    store_keys = []
    file_keys = []

    # Iterate over each of the "tables"
    for i, (func, args, store_enabled, base) in {
        # index: (function, inputs to the function, whether to store_ts, file basename)
        0: (report0, ["buildings filters"], True, "buildings-FE"),
        1: (report1, ["buildings filters"], True, "buildings-emiss"),
        2: (report2, ["sturm output path"], False, "sturm"),
        3: (report3, ["buildings 2"], True, "sturm-name-change"),
    }.items():
        # Short string to identify this table
        k1 = f"buildings {i}"

        # Add a key to run the function, returning a pd.DataFrame
        rep.add(k1, func, "scenario", *args)

        # Maybe add to the list of data to be stored on the scenario
        if store_enabled:
            store_keys.append(k1)

        # Make a path for file output
        k2 = rep.add("make_output_path", f"{k1} path", "config", f"{base}.csv")

        # Write the data frame to this path
        # FIXME(PNK) upstream genno.computations.write_report handles only Quantity, not
        #            pd.DataFrame. Add that feature, then remove the lambda function
        k3 = rep.add(f"{k1} file", lambda df, path: df.to_csv(path), k1, k2)

        # Add to the list of files to be stored
        file_keys.append(k3)

    # Add keys that collect others:
    # 1. Store all data on the scenario.
    # 2. Write all the data to respective files.
    # 3. Do both 1 and 2.
    rep.add("store_ts", "buildings iamc store", "scenario", *store_keys)
    rep.add("buildings iamc file", file_keys)
    rep.add("buildings all", ["buildings iamc store", "buildings iamc file"])

    # Temporary: disable storing time series data by replacing with a no-op
    rep.add("buildings iamc store", [])  # Does nothing


def buildings_filters(all_techs: List[str], years: List) -> Dict:
    """Return filters for buildings reporting."""
    # Regular expression to match technology IDs relevant for buildings reporting
    tech_re = re.compile("(resid|comm).*(apps|cool|cook|heat|hotwater)")
    return dict(
        technology=list(
            filter(lambda t: tech_re.search(t) or t == "biomass_nc", all_techs)
        ),
        year_act=years,
    )


def report0(scenario: message_ix.Scenario, filters: dict) -> pd.DataFrame:
    """Report `scenario`.

    STURM output data are loaded from CSV files and merged with computed values stored
    as timeseries on `scenario`.

    Originally transcribed from :file:`reporting_EFC.py` in the buildings repository.

    .. todo:: decompose further by making use of genno features.
    """
    # Final Energy Demand

    # - Retrieve ACT data using `filters`
    # - Rename dimensions.
    FE_rep = scenario.var("ACT", filters=filters).rename(
        columns={"year_act": "year", "node_loc": "node", "lvl": "value"}
    )

    # Fix for non commercial biomass to be consistent with MESSAGE's original numbers
    # which go directly from primary to useful. So, we are "de-usefulizing" here using
    # our conversion factor
    FE_rep.loc[FE_rep["technology"] == "biomass_nc", "value"] /= 0.15

    # - Rename "biomass_nc" to "biomass_nc_resid_cook"
    # - Duplicate data as "biomass_resid_cook"
    # - Extract commodity from technology labels
    FE_rep = pd.concat(
        [
            FE_rep.replace(dict(technology={"biomass_nc": "biomass_nc_resid_cook"})),
            FE_rep.query("technology == 'biomass_nc'").assign(
                technology="biomass_resid_cook"
            ),
        ]
    ).assign(commodity=lambda df: df["technology"].str.rsplit("_", 1, expand=True)[0])

    # - Calculate totals by (commodity, node, year)
    # NB(PNK) genno will do this automatically if FE_rep is described with sums=True
    FE_rep = (
        FE_rep[["node", "commodity", "year", "value"]]
        .groupby(["node", "commodity", "year"])
        .sum()
        .reset_index()
    )

    # Extract fuel and sector from commodity labels
    FE_rep[["fuel", "sector"]] = FE_rep["commodity"].str.rsplit("_", 1, expand=True)

    # Adjust sector and fuel names
    FE_rep.replace(NAME_MAP, inplace=True)

    # Construct a variable label
    FE_rep["variable"] = "Final Energy|" + FE_rep["sector"] + "|" + FE_rep["fuel"]

    # Convert from internal ACT GWa to EJ
    units_to = "EJ/yr"
    converted = registry.Quantity(FE_rep["value"].values, "GWa/year").to("EJ/yr")
    FE_rep = FE_rep.assign(value=converted.magnitude, unit=units_to)

    # Sum commercial and residential by fuel type
    FE_rep_tot = (
        FE_rep.groupby(["node", "fuel", "unit", "year"])
        .sum()
        .reset_index()
        .assign(
            variable=lambda df: "Final Energy|Residential and Commercial|" + df["fuel"]
        )
    )

    FE_rep = pd.concat([FE_rep[COLS], FE_rep_tot[COLS]], ignore_index=True)

    # Compute a global total
    glob_rep = (
        FE_rep.groupby(["variable", "unit", "year"])
        .sum()
        .reset_index()
        .assign(node="R12_GLB")
    )

    FE_rep = (
        pd.concat([FE_rep, glob_rep], ignore_index=True)
        .sort_values(["node", "variable", "year"])
        .reset_index(drop=True)
    )

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
    ).assign(variable=lambda df: "Final Energy|" + df["fuel_type"])

    FE_rep = pd.concat([FE_rep[COLS], FE_rep_tot_fuel[COLS]], ignore_index=True)

    return FE_rep


def report1(scenario: message_ix.Scenario, filters: dict) -> pd.DataFrame:
    # Emissions from Demand
    act = scenario.var("ACT", filters=filters)
    emiss = scenario.par("relation_activity", filters=filters)

    # Subset of emissions data where the relation name contains "Emission"
    emiss = emiss.loc[emiss["relation"].str.contains("Emission")]

    emiss = emiss.merge(act).rename(columns={"year_act": "year", "node_loc": "node"})
    emiss["value"] = emiss["value"] * emiss["lvl"]  # ?

    # Some fixes
    emiss = emiss.assign(
        technology=emiss["technology"].replace("biomass_nc", "biomass_nc_resid_cook"),
        commodity=emiss["technology"].str.rsplit("_", 1, expand=True)[0],
        emission=emiss["relation"].str.rsplit("_", 1, expand=True)[0],
        unit=lambda df: "Mt " + df["emission"] + "/yr",
    )[["node", "year", "commodity", "emission", "unit", "value"]]

    # NB(PNK) genno will do this automatically if emiss is described with sums=True
    emiss = (
        emiss.groupby(["node", "year", "commodity", "emission", "unit"])
        .sum()
        .reset_index()
    )

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

    # Compute a total
    emiss_tot = (
        emiss.groupby(["node", "emission", "fuel", "unit", "year"]).sum().reset_index()
    ).assign(
        variable=lambda df: "Emissions|"
        + df["emission"]
        + "|Energy|Demand|Residential and Commercial|"
        + df["fuel"]
    )

    emiss = pd.concat([emiss[COLS], emiss_tot[COLS]], ignore_index=True)

    # Global total
    glob_emiss = (
        emiss.groupby(["variable", "unit", "year"])
        .sum()
        .reset_index()
        .assign(node="R12_GLB")
    )
    emiss_rep = (
        pd.concat([emiss, glob_emiss], ignore_index=True)
        .sort_values(["node", "variable", "year"])
        .reset_index(drop=True)
    )

    return emiss_rep


def report2(scenario: message_ix.Scenario, sturm_output_path: Path) -> pd.DataFrame:
    # Add STURM reporting
    if "baseline" in scenario.scenario:
        filename = "report_NGFS_SSP2_BL_{}_R12.csv"
    else:
        filename = "report_IRP_SSP2_2C_{}_R12.csv"

    # - Read 2 files and concatenate.
    # - Melt into long format.
    # - Rename columns to lower case.
    # - Construct the region name by adding an R12_ prefix.
    # - Drop others.
    sturm_rep = (
        pd.concat(
            [
                pd.read_csv(sturm_output_path / filename.format("resid")),
                pd.read_csv(sturm_output_path / filename.format("comm")),
            ]
        )
        .melt(
            id_vars=["Model", "Scenario", "Region", "Variable", "Unit"], var_name="year"
        )
        .rename(columns=lambda c: c.lower())
        .assign(node=lambda df: "R12_" + df["region"])[COLS]
    )

    return sturm_rep


def report3(scenario: message_ix.Scenario, sturm_rep: pd.DataFrame) -> pd.DataFrame:
    """Manipulate variable names for `sturm_rep` and compute additional sums."""

    # Variables to exclude
    prefix = "Energy Service|Residential and Commercial|"
    exclude = [
        f"{prefix}|Commercial",
        f"{prefix}|Residential|Multi-family|Floor space",
        f"{prefix}|Residential|Single-family|Floor space",
        f"{prefix}|Residential|Slum|Floor space",
    ]

    # - Exclude certain variables.
    # - Convert a name like A|B|C|D to A|Residential and Commercial|D.
    data = (
        sturm_rep[~sturm_rep["variable"].isin(exclude)]
        .reset_index(drop=True)
        .assign(
            variable=lambda df: df["variable"].str.replace(
                r"([^\|]*\|)([^\|]*\|)([^\|]*\|)(.*)",
                r"\g<1>Residential and Commercial|\g<4>",
            )
        )
    )

    # Sum of residential and commercial, i.e. omitting the discarded "B|C" dimensions
    sum_1 = data.groupby(["node", "variable", "unit", "year"]).sum().reset_index()
    data = pd.concat([data, sum_1], ignore_index=True)

    # Global total, i.e. omitting "node"
    sum_2 = (
        data.groupby(["variable", "unit", "year"])
        .sum()
        .reset_index()
        .assign(node="R12_GLB")
    )

    data = (
        pd.concat([data, sum_2], ignore_index=True)
        .sort_values(["node", "variable", "year"])
        .reset_index(drop=True)
    )

    return data
