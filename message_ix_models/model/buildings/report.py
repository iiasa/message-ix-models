"""Reporting for MESSAGEix-Buildings.

STURM output data are loaded from CSV files, manipulated, and stored as timeseries on a
scenario.

Originally transcribed from :file:`reporting_EFC.py` in the buildings repository.
"""

import logging
import re
from functools import lru_cache, partial
from itertools import product

import message_ix
import pandas as pd
from genno import Key, operator
from iam_units import registry

from message_ix_models import Context, Spec
from message_ix_models.report import iamc as add_iamc

# TODO Remove type exclusion after release of message-ix-models >2025.1.10
from message_ix_models.report.operator import (  # type: ignore [attr-defined]
    nodes_world_agg,
)
from message_ix_models.report.util import add_replacements

from . import Config
from .build import get_spec, get_tech_groups
from .sturm import scenario_name

log = logging.getLogger(__name__)

# Common list of columns for several operations
COLS = ["node", "variable", "unit", "year", "value"]


def callback(rep: message_ix.Reporter, context: Context) -> None:
    """:meth:`.prepare_reporter` callback for MESSAGE-Buildings.

    Adds the keys:

    - "buildings iamc file": write IAMC-formatted reporting output to file.
    - "buildings iamc store": store IAMC-formatted reporting on the scenario.
    - "buildings all": both of the above.
    """
    from message_ix_models.report.util import REPLACE_DIMS

    # Path where STURM output files are found
    rep.graph["config"].setdefault(
        "sturm output path", context.get_local_path("buildings")
    )
    # FIXME don't hard-code this
    rep.graph["config"].setdefault("regions", "R12")

    context.setdefault(
        "buildings",
        Config(sturm_scenario=scenario_name(rep.graph["scenario"].scenario)),
    )

    # Store a Spec in the graph for use by e.g. buildings_agg0()
    spec = get_spec(context)
    rep.add("buildings spec", spec)

    # Configure message_ix_models.report.util.collapse to map commodity and technology
    # IDs
    add_replacements("t", spec.add.set["buildings_sector"])
    for s, e in product(spec.add.set["buildings_sector"], spec.add.set["enduse"]):
        # Append "$" so the expressions only match the full/end of string
        REPLACE_DIMS["t"][f"{s.id.title()} {e.id.title()}$"] = (
            f"{s.eval_annotation('report')}|{e.eval_annotation('report')}"
        )

    log.info(f"Will replace:\n{REPLACE_DIMS!r}")

    # Filters for retrieving data. Use the keys "t" and "y::model" that are
    # automatically populated by message_ix and message_data, respectively.
    rep.add("buildings filters 0", buildings_filters0, "t", "y::model")
    rep.add("buildings filters 1", buildings_filters1, "y::model")

    # Mapping for aggregation
    rep.add("buildings agg", buildings_agg0, "buildings spec", "config")
    # Aggregate
    rep.add(
        "in:nl-t-ya-c-l:buildings",
        operator.aggregate,
        "in:nl-t-ya-c-l",
        "buildings agg",
        False,
    )
    # Select for final energy
    rep.add(
        "select",
        "buildings fe:nl-t-ya-c-l:0",
        "in:nl-t-ya-c-l:buildings",
        "buildings filters 1",
    )
    # Assign missing units, then convert to EJ / a
    buildings_fe = Key("buildings fe:nl-t-ya-c-l")
    buildings_fe_2 = buildings_fe + "2"
    rep.add("assign_units", buildings_fe + "1", buildings_fe + "0", "GWa/year")
    rep.add("convert_units", buildings_fe_2, buildings_fe + "1", "EJ / a", sums=True)

    # Convert to IAMC structure
    # - Ensure the unit string is "EJ/yr", nor "EJ / a".
    # - Include partial sums over commodities.
    add_iamc(
        rep,
        dict(
            base=buildings_fe_2 / "l",
            variable="buildings fe",
            var=["Final Energy", "t", "c"],
            sums=["c"],
            unit="EJ/yr",
        ),
    )

    # Lists of keys for use later
    store_keys = []
    file_keys = []

    # Iterate over each of the "tables"
    for i, (func, args, store_enabled, base) in {
        # index: (function, inputs to the function, whether to store_ts, file basename)
        #
        # commented: 2022-09-09 temporarily disabled
        # 0: (report0, ["buildings filters 0"], False, "debug-report0"),
        # 1: (report1, ["buildings filters 0"], False, "debug-report1"),
        #
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
        k_path = f"{k1} path"
        rep.add(k_path, "make_output_path", "config", f"{base}.csv")

        # Write the data frame to this path
        k3 = rep.add(f"{k1} file", "write_report", k1, k_path)

        # Add to the list of files to be stored
        file_keys.append(k3)

    # Same for final energy
    k1 = "buildings fe::iamc"
    store_keys.append(k1)
    k_path = "buildings fe path"
    rep.add(k_path, "make_output_path", "config", "final-energy-new.csv")
    k3 = rep.add("buildings fe file", lambda df, path: df.to_csv(path), k1, k_path)
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
    # NB the legacy reporting doesn't pass a context object to the hook that calls this
    #    function, so get an instance directly
    context = Context.get_instance(-1)

    # FIXME don't hard-code this
    context.setdefault("regions", "R12")

    spec = get_spec(context)

    # Update using tech groups
    config.update(get_tech_groups(spec, "commodity", legacy=True))


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
    raise NotImplementedError("NAME_MAP no longer defined")

    NAME_MAP = dict()

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


# Reporting operators/atomic steps


def buildings_filters0(all_techs: list[str], years: list) -> dict:
    """Return filters for buildings reporting."""
    # Regular expression to match technology IDs relevant for buildings reporting
    tech_re = re.compile("(resid|comm).*(apps|cool|cook|heat|hotwater)")
    return dict(
        technology=list(
            filter(lambda t: tech_re.search(t) or t == "biomass_nc", all_techs)
        ),
        year_act=years,
    )


def buildings_filters1(years: list) -> dict:
    """Return filters for buildings reporting."""
    return dict(l=["final"], ya=years)


def buildings_agg0(spec: Spec, config: dict) -> dict:
    """Return mapping for buildings aggregation."""
    result = dict(nodes_world_agg(config))
    result["t"] = get_tech_groups(spec, include="enduse")  # type: ignore [assignment]

    log.info(f"Will aggregate:\n{result!r}")

    return result


def report0(scenario: message_ix.Scenario, filters: dict) -> pd.DataFrame:
    """Report buildings final energy.

    This function descends from logic in :file:`reporting_EFC.py` in the
    MESSAGE_Buildings repository. It is suspected the values could be incorrect, because
    ``ACT`` is not multiplied by ``input``. Per :func:`callback`; the values returned
    are not currently stored as time series data, or used further.
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

    # Sum of fuel types for different building sub-sectors (R, C and R+C)
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
    #   TODO use `rel` (this same product) as provided by message_ix.report
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
    - The `node` dimension labels have `R12_` prepended.
    """
    # Directory containing STURM output files
    base = config["sturm output path"]
    # File name template, using the STURM name corresponding to the MESSAGE name
    fn = f"report_NAVIGATE_{scenario_name(scenario.scenario)}_{{}}_R12.csv"

    @lru_cache()
    def _add_R12_prefix(value: str) -> str:
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


def _drop_unused(df: pd.DataFrame) -> pd.DataFrame:
    """Drop unused values from STURM reporting.

    - All "Final Energy…" variable names.
    """
    mask = df["variable"].str.match("^Final Energy")

    return df[~mask]


def report3(scenario: message_ix.Scenario, sturm_rep: pd.DataFrame) -> pd.DataFrame:
    """Manipulate variable names for `sturm_rep` and compute additional sums."""
    # - Munge names.
    # - Compute global totals.
    # - Add aggregates in 2 stages.
    # - Sort.
    return (
        sturm_rep.pipe(_rename)
        .pipe(_drop_unused)
        .pipe(add_global_total)
        .pipe(add_aggregates, 0)
        .pipe(add_aggregates, 1)
        .sort_values(COLS)
    )
