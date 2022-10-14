"""Reporting for MESSAGEix-Buildings.

STURM output data are loaded from CSV files, manipulated, and stored as timeseries on a
scenario.

Originally transcribed from :file:`reporting_EFC.py` in the buildings repository.
"""
import logging
import re
from functools import lru_cache, partial
from typing import Dict, List

import message_ix
import pandas as pd
from iam_units import registry
from message_ix_models import Context

from .build import get_spec, get_techs
from .sturm import scenario_name

log = logging.getLogger(__name__)

#: Mappings for .replace().
#:
#: .. todo:: combine and use with those defined in .reporting.util.
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
    """:meth:`.prepare_reporter` callback for MESSAGE-Buildings.

    Adds the keys:

    - "buildings iamc file": write IAMC-formatted reporting output to file.
    - "buildings iamc store": store IAMC-formatted reporting on the scenario.
    - "buildings all": both of the above.
    """

    # Path where STURM output files are found
    rep.graph["config"].setdefault(
        "sturm output path", context.get_local_path("buildings")
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
        # commented: 2022-09-09 temporary
        # Disabled
        # 0: (report0, ["buildings filters"], True, "buildings-FE"),
        # 1: (report1, ["buildings filters"], True, "buildings-emiss"),
        2: (report2, ["config"], False, "sturm-raw"),
        3: (report3, ["buildings 2"], True, "buildings"),
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


def configure_legacy_reporting(config: dict) -> None:
    """Callback to configure the legacy reporting."""
    context = Context.get_instance()

    # FIXME don't hard-code this
    context.setdefault("regions", "R12")

    spec = get_spec(context)

    # Generate some lists
    for c in "back biomass coal d_heat elec eth foil gas h2 loil meth solar".split():
        _c = {"elec": "electr", "heat": "d_heat", "loil": "lightoil"}.get(c, c)
        config[f"rc {c}"] = get_techs(spec, commodity=_c)

    # Extend some groups
    # TODO group automatically based on attributes
    config["rc elec"].extend(get_techs(spec, commodity="hp_el"))
    config["rc gas"].extend(get_techs(spec, commodity="hp_gas"))


# Helper functions


def add_global_total(df: pd.DataFrame) -> pd.DataFrame:
    """Add a global total (across the "node" dimension) to `df`."""
    assert set(df.columns) == set(COLS)

    total = (
        df.groupby(["variable", "unit", "year"])
        .sum(numeric_only=True)
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
    """Report buildings final energy."""
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


def report2(scenario: message_ix.Scenario, config: dict) -> pd.DataFrame:
    """Load STURM reporting outputs from file and return.

    The files are located with names like::

       report_NAVIGATE_{scenario}_[comm|resid]_{regions}.csv

    This function does not do any numerical manipulations. The only changes applied are:

    - Data is transformed from wide to long format.
    - The "node" dimension labels have "R12_" prepended.
    """
    # Directory containing STURM output files
    base = config["sturm output path"]
    # File name template, using the STURM name corresponding to the MESSAGE name
    fn = f"report_NAVIGATE_{scenario_name(scenario.scenario)}_{{}}_R12.csv"

    @lru_cache()
    def _add_R12_prefix(value :str) -> str:
        return value if value.startswith("R12_") else f"R12_{value}"

    # - Read 2 files and concatenate.
    # - Melt into long format.
    # - Rename columns to lower case.
    # - Construct the region name by adding an R12_ prefix.
    # - Drop others.
    sturm_rep = (
        pd.concat(
            [pd.read_csv(base / fn.format(rc), comment="#") for rc in ("resid", "comm")]
        )
        .rename(columns=lambda c: c.lower())
        .assign(node=lambda df: df["region"].apply(_add_R12_prefix))
        .drop(["model", "scenario", "region"], axis=1)
        .melt(id_vars=COLS[:-2], var_name="year")
    )

    return sturm_rep


MAPS = (
    {
        "Energy Service|Residential|Floor space": [
            "Energy Service|Residential|Multi-family|Floor space",
            "Energy Service|Residential|Single-family|Floor space",
            "Energy Service|Residential|Slum|Floor space",
        ],
        "Final Energy|Commercial": [
            "Final Energy|Commercial|Electricity",
            "Final Energy|Commercial|Gases",
            "Final Energy|Commercial|Heat",
            "Final Energy|Commercial|Liquids",
            "Final Energy|Commercial|Solids|Biomass",
            "Final Energy|Commercial|Solids|Coal",
        ],
        "Final Energy|Residential": [
            "Final Energy|Residential|Electricity",
            "Final Energy|Residential|Gases",
            "Final Energy|Residential|Heat",
            "Final Energy|Residential|Liquids",
            "Final Energy|Residential|Solids|Biomass",
            "Final Energy|Residential|Solids|Coal",
        ],
    },
    {
        "Energy Service|Residential and Commercial|Floor space": [
            "Energy Service|Commercial",
            "Energy Service|Residential|Floor space",
        ],
    },
)


@lru_cache(maxsize=len(MAPS))
def _groups(map_index: int) -> dict:
    """Return a reversed mapping for element `map_index` of :data:`MAPS`."""
    # Reverse the mapping
    result = dict()
    for k, names in MAPS[map_index].items():
        result.update({v: k for v in names})
    return result


@lru_cache()
def grouper(value: tuple, idx: int, map_index: int) -> tuple:
    # Map the variable name
    mapped = _groups(map_index).get(value[idx])

    if mapped is None:
        # Not to be aggregated → catch-all group
        return (None, None, None, None)
    else:
        return value[:idx] + (mapped,) + value[idx + 1 :]


def add_aggregates(df: pd.DataFrame, map_index: int) -> pd.DataFrame:
    """Add aggregates to `df` using element `map_index` from :data:`MAPS`.

    Uses pandas' groupby features for performance.
    """
    columns = COLS[:-1]

    # Function to group `df` by `columns`
    _grouper = partial(grouper, idx=columns.index("variable"), map_index=map_index)

    # Compute grouped sum
    sums = df.set_index(columns).groupby(_grouper).sum()

    # - Restore index to columns (pandas doesn't seem to do this automatically)
    # - Drop the catch-all group.
    # - Drop the index generated by groupby().
    result = (
        pd.concat(
            [sums, sums.index.to_series().apply(pd.Series, index=columns)], axis=1
        )
        .dropna(subset=COLS[:-1], how="all")
        .reset_index(drop=True)
    )

    return pd.concat([df, result], ignore_index=True)


def _rename(df: pd.DataFrame) -> pd.DataFrame:
    """Convert variable names like "A|Residential and Commercial|B|C…" to "A|B|C…"."""
    return df.assign(
        variable=df["variable"].str.replace(
            r"([^\|]*)\|Residential and Commercial\|(.*)",
            r"\g<1>|\g<2>",
            regex=True,
        )
    )


def report3(scenario: message_ix.Scenario, sturm_rep: pd.DataFrame) -> pd.DataFrame:
    """Manipulate variable names for `sturm_rep` and compute additional sums."""
    # - Munge names.
    # - Compute global totals.
    # - Add aggregates in 2 stages.
    # - Sort.
    return (
        sturm_rep.pipe(_rename)
        .pipe(add_global_total)
        .pipe(add_aggregates, 0)
        .pipe(add_aggregates, 1)
        .sort_values(COLS)
    )
