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


# Helper functions


def add_global_total(df: pd.DataFrame) -> pd.DataFrame:
    """Add a global total (across the "node" dimension) to `df`."""
    assert set(df.columns) == set(COLS)

    total = (
        df.groupby(["variable", "unit", "year"])
        .sum()
        .reset_index()
        .assign(node="R12_GLB")
    )
    return (
        pd.concat([df, total], ignore_index=True)
        .sort_values(["node", "variable", "year"])
        .reset_index(drop=True)
    )


def fuel_sector_from_commodity(df: pd.DataFrame) -> pd.DataFrame:
    """Extract "fuel" and "sector" from "commodity" in `df`; apply `NAME_MAP`."""
    f_s = df["commodity"].str.rsplit("_", 1, expand=True)
    return df.assign(fuel=f_s[0], sector=f_s[1]).replace(NAME_MAP)


def sum_on(df: pd.DataFrame, *columns) -> pd.DataFrame:
    """Compute a sum on `df`, grouped by `columns`."""
    return df.groupby(list(columns)).sum().reset_index()


def var_name(df: pd.DataFrame, expr: str) -> pd.DataFrame:
    """Format the "variable" column of `df` given `expr`.

    `expr` should be like "Some text {other_col} text {different_col}", referencing
    existing columns of `df`.
    """
    # Prepend 0. to replacement groups in `expr` so it is suitable for use with apply()
    return df.assign(variable=df.apply(expr.replace("{", "{0.").format, axis=1))


# Reporting computations/atomic steps


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
    mask = FE_rep["technology"] == "biomass_nc"
    FE_rep.loc[mask, "value"] /= 0.15

    # - Rename "biomass_nc" to "biomass_nc_resid_cook"
    # - Duplicate data as "biomass_resid_cook"
    # - Extract commodity from technology labels
    # - Select some columns.
    # - Calculate totals by (commodity, node, year)
    #   NB(PNK) genno will do this automatically if FE_rep is described with sums=True
    # - Extract fuel and sector from commodity labels.
    # - Adjust sector and fuel names.
    # - Construct a variable label.
    FE_rep = (
        pd.concat(
            [
                FE_rep.replace(
                    dict(technology={"biomass_nc": "biomass_nc_resid_cook"})
                ),
                FE_rep[mask].assign(technology="biomass_resid_cook"),
            ]
        )
        .assign(
            commodity=lambda df: df["technology"].str.rsplit("_", 1, expand=True)[0]
        )[["node", "commodity", "year", "value"]]
        .pipe(sum_on, "node", "commodity", "year")
        .pipe(fuel_sector_from_commodity)
        .pipe(var_name, "Final Energy|{sector}|{fuel}")
    )

    # Convert from internal ACT GWa to EJ
    # TODO(PNK) provide a common function similar to
    #           message_ix_models.util.convert_units() for this kind of operation
    units_to = "EJ/yr"
    converted = registry.Quantity(FE_rep["value"].values, "GWa/year").to("EJ/yr")
    FE_rep = FE_rep.assign(value=converted.magnitude, unit=units_to)

    # Sum commercial and residential by fuel type
    FE_rep_tot = FE_rep.pipe(sum_on, "node", "fuel", "unit", "year").pipe(
        var_name, "Final Energy|Residential and Commercial|{fuel}"
    )

    FE_rep = (
        pd.concat([FE_rep[COLS], FE_rep_tot[COLS]], ignore_index=True)
        .pipe(add_global_total)
        .assign(fuel_type=lambda df: df["variable"].str.split("|", 2, expand=True)[1])
    )

    # sum of the building related Final Energy by fuel types to get the variable
    # "Final Energy|Residential and Commercial",
    # "Final Energy|Residential", and "Final Energy|Commercial"
    # for FE_rep

    exclude = [
        "Final Energy|Residential and Commercial|Solids|Biomass|Traditional",
        "Final Energy|Residential|Solids|Biomass|Traditional",
        "Final Energy|Commercial|Solids|Biomass",
    ]

    # Sum of fuel types for different building sub-setors (R, C and R+C)
    FE_rep_tot_fuel = (
        FE_rep[~FE_rep["variable"].isin(exclude)]
        .pipe(sum_on, "node", "unit", "year", "fuel_type")
        .pipe(var_name, "Final Energy|{fuel_type}")
    )

    FE_rep = pd.concat([FE_rep[COLS], FE_rep_tot_fuel[COLS]], ignore_index=True)

    return FE_rep


def report1(scenario: message_ix.Scenario, filters: dict) -> pd.DataFrame:
    """Report buildings emissions using the ``relation_activity`` approach."""

    # Retrieve data
    act = scenario.var("ACT", filters=filters)
    emiss = scenario.par("relation_activity", filters=filters)

    # - Subset of emissions data where the relation name contains "Emission"
    # - Merge ACT data
    # - Rename columns.
    # - Product of the "value" (from relation_activity) and "lvl" (from ACT)
    #   TODO provide this product (relation_activity * lvl) from message_ix.reporting
    # - Adjust technology, commodity, emission, and unit labels.
    # - Select some columns.
    # - Compute sums.
    # - Extract fuel and sector from commodity label.
    # - Adjust sector and fuel names.
    # - Assemble variable names.
    emiss = (
        emiss[emiss["relation"].str.contains("Emission")]
        .merge(act)
        .rename(columns={"year_act": "year", "node_loc": "node"})
        .assign(
            value=lambda df: df["value"] * df["lvl"],
            technology=lambda df: df["technology"].replace(
                "biomass_nc", "biomass_nc_resid_cook"
            ),
            commodity=lambda df: df["technology"].str.rsplit("_", 1, expand=True)[0],
            emission=lambda df: df["relation"].str.rsplit("_", 1, expand=True)[0],
            unit=lambda df: "Mt " + df["emission"] + "/yr",
        )[["node", "year", "commodity", "emission", "unit", "value"]]
        .pipe(sum_on, "node", "year", "commodity", "emission", "unit")
        .pipe(fuel_sector_from_commodity)
        .pipe(var_name, "Emissions|{emission}|Energy|Demand|{sector}|{fuel}")
    )

    # - Compute a total across sector.
    # - Construct variable names.
    emiss_tot = emiss.pipe(sum_on, "node", "emission", "fuel", "unit", "year").pipe(
        var_name, "Emissions|{emission}|Energy|Demand|Residential and Commercial|{fuel}"
    )

    emiss = pd.concat([emiss[COLS], emiss_tot[COLS]], ignore_index=True).pipe(
        add_global_total
    )

    return emiss


def report2(scenario: message_ix.Scenario, sturm_output_path: Path) -> pd.DataFrame:
    """Load STURM reporting outputs from file and return.

    This function does not do any numerical manipulations. The only changes applied are:

    - Data is transformed from wide to long format.
    - The "node" dimension labels have "R12_" prepended.
    """
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

    # Variables to exclude from the manipulations
    prefix = "Energy Service|Residential and Commercial"
    exclude = [
        f"{prefix}|Commercial",
        f"{prefix}|Residential|Multi-family|Floor space",
        f"{prefix}|Residential|Single-family|Floor space",
        f"{prefix}|Residential|Slum|Floor space",
    ]

    # - Exclude certain variables.
    # - Convert a name like A|B|C|D to A|Residential and Commercial|D.
    # - Sum, e.g. over the discarded B|C dimensions.
    data = (
        sturm_rep[~sturm_rep["variable"].isin(exclude)]
        .reset_index(drop=True)
        .assign(
            variable=lambda df: df["variable"].str.replace(
                r"([^\|]*\|)([^\|]*\|)([^\|]*\|)(.*)",
                r"\g<1>Residential and Commercial|\g<4>",
                regex=True,
            )
        )
        .pipe(sum_on, "node", "variable", "unit", "year")
    )

    # Reassemble; compute global total
    data = pd.concat([sturm_rep, data], ignore_index=True).pipe(add_global_total)

    return data
