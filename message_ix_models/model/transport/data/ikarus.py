"""Prepare non-LDV data from the IKARUS model via :file:`GEAM_TRP_techinput.xlsx`."""
import logging
from collections import defaultdict

import pandas as pd
from iam_units import registry
from message_ix import make_df
from message_ix_models.util import (
    broadcast,
    cached,
    convert_units,
    private_data_path,
    same_node,
    series_of_pint_quantity,
)
from openpyxl import load_workbook

from message_data.model.transport.utils import input_commodity_level

log = logging.getLogger(__name__)

#: Name of the input file.
#:
#: The input file uses the old, MESSAGE V names for parameters:
#:
#: - inv_cost = inv
#: - fix_cost = fom
#: - technical_lifetime = pll
#: - input (efficiency) = minp
#: - output (efficiency) = moutp
#: - capacity_factor = plf
FILE = "GEAM_TRP_techinput.xlsx"

#: Mapping from parameters to 3-tuples of units:
#:
#: 1. Factor for units appearing in the input file.
#: 2. Units appearing in the input file.
#: 3. Target units for MESSAGEix-GLOBIOM.
UNITS = dict(
    # Appearing in input file
    inv_cost=(1.0e6, "EUR_2000 / vehicle", "MUSD_2005 / vehicle"),
    fix_cost=(1000.0, "EUR_2000 / vehicle / year", "MUSD_2005 / vehicle / year"),
    var_cost=(0.01, "EUR_2000 / kilometer", None),
    technical_lifetime=(1.0, "year", None),
    availability=(100, "kilometer / vehicle / year", None),
    input=(0.01, "GJ / kilometer", None),
    output=(1.0, "", None),
    # Created below
    capacity_factor=(1.0, None, "gigapassenger kilometre / vehicle / year"),
)

#: Rows appearing in each cell range.
ROWS = [
    "inv_cost",
    "fix_cost",
    "var_cost",
    "technical_lifetime",
    "availability",
    "input",
    "output",
]

#: Starting and final cells delimiting tables in sheet.
CELL_RANGE = {
    "rail_pub": ["C103", "I109"],
    "dMspeed_rai": ["C125", "I131"],
    "Mspeed_rai": ["C147", "I153"],
    "Hspeed_rai": ["C169", "I175"],
    "con_ar": ["C179", "I185"],
    # Same parametrization as 'con_ar' (per cell references in spreadsheet):
    "conm_ar": ["C179", "I185"],
    "conE_ar": ["C179", "I185"],
    "conh_ar": ["C179", "I185"],
    "ICE_M_bus": ["C197", "I203"],
    "ICE_H_bus": ["C205", "I211"],
    "ICG_bus": ["C213", "I219"],
    # Same parametrization as 'ICG_bus'. Conversion factors will be applied.
    "ICAe_bus": ["C213", "I219"],
    "ICH_bus": ["C213", "I219"],
    "PHEV_bus": ["C213", "I219"],
    "FC_bus": ["C213", "I219"],
    # Both equivalent to 'FC_bus'
    "FCg_bus": ["C213", "I219"],
    "FCm_bus": ["C213", "I219"],
    "Trolley_bus": ["C229", "I235"],
}

#: Years appearing in the input file.
COLUMNS = [2000, 2005, 2010, 2015, 2020, 2025, 2030]


@cached
def read_ikarus_data(occupancy, k_output, k_inv_cost):
    """Read the IKARUS data from :data:`FILE`.

    No transformation is performed.

    **NB** this function takes only simple arguments so that :func:`.cached` computes
    the same key every time to avoid the slow step of opening/reading the spreadsheet.
    :func:`get_ikarus_data` then conforms the data to particular context settings.
    """
    # Open the input file using openpyxl
    wb = load_workbook(
        private_data_path("transport", FILE), read_only=True, data_only=True
    )
    # Open the 'updateTRPdata' sheet
    sheet = wb["updateTRPdata"]

    # 'technology name' -> pd.DataFrame
    dfs = {}
    for tec, cell_range in CELL_RANGE.items():
        # - Read values from table for one technology, e.g. "regional train electric
        #   efficient" = rail_pub.
        # - Extract the value from each openpyxl cell object.
        # - Set all non numeric values to NaN.
        # - Transpose so that each variable is in one column.
        # - Convert from input units to desired units.
        df = (
            pd.DataFrame(list(sheet[slice(*cell_range)]), index=ROWS, columns=COLUMNS)
            .applymap(lambda c: c.value)
            .apply(pd.to_numeric, errors="coerce")
            .transpose()
            .apply(convert_units, unit_info=UNITS, store="quantity")
        )

        # Conversion of IKARUS data to MESSAGEix-scheme parameters.

        # Read output efficiency (occupancy) from config and apply units
        output_value = occupancy[tec] * registry("pkm / km")

        # Convert to a Series so operations are element-wise
        output = series_of_pint_quantity([output_value] * len(df.index), index=df.index)

        # Compute output efficiency
        df["output"] = output / df["input"] * k_output.get(tec, 1.0)

        # Compute capacity factor = availability × output
        df["capacity_factor"] = df["availability"] * output

        # Check units: (km / vehicle / year) × (passenger km / km) [=] passenger km /
        # vehicle / year
        assert df["capacity_factor"].values[0].units == registry(
            "passenger km / vehicle / year"
        )

        df["inv_cost"] *= k_inv_cost.get(tec, 1.0)

        # Include variable cost * availability in fix_cost
        df["fix_cost"] += df["availability"] * df["var_cost"]

        # Store
        dfs[tec] = df.drop(columns=["availability", "var_cost"])

    # Finished reading IKARUS data from spreadsheet
    wb.close()

    # - Concatenate to pd.DataFrame with technology and param as columns.
    # - Reformat as a pd.Series with a 3-level index: year, technology, param
    return (
        pd.concat(dfs, axis=1, names=["technology", "param"])
        .rename_axis(index="year")
        .stack(["technology", "param"])
    )


def get_ikarus_data(context):
    """Prepare non-LDV data from :cite:`Martinsen2006`.

    The data is read from from ``GEAM_TRP_techinput.xlsx``, and the processed data is
    exported into ``non_LDV_techs_wrapped.csv``.

    Parameters
    ----------
    context : .Context

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'fix_cost'.
        Values are data frames ready for :meth:`~.Scenario.add_par`.
        Years in the data include the model horizon indicated by
        ``context["transport build info"]``, plus the additional year 2010.
    """
    # Reference to the transport configuration
    config = context["transport config"]
    tech_info = context["transport set"]["technology"]["add"]
    info = context["transport build info"]

    # Retrieve the data from the spreadsheet. Use Additional output efficiency and
    # investment cost factors for some bus technologies
    data = read_ikarus_data(
        occupancy=config["non-ldv output"],
        k_output=config["efficiency"]["bus output"],
        k_inv_cost=config["cost"]["bus inv"],
    )

    # Create data frames to add imported params to MESSAGEix

    # Vintage and active years from scenario info
    # Prepend years between 2010 and *firstmodelyear* so that values are saved
    missing_years = [x for x in info.set["year"] if (x >= 2010) and (x < info.y0)]
    vtg_years = missing_years + info.yv_ya["year_vtg"].tolist()
    act_years = missing_years + info.yv_ya["year_act"].tolist()

    # Default values to be used as args in make_df()
    defaults = dict(
        mode="all",
        year_act=act_years,
        year_vtg=vtg_years,
        time="year",
        time_origin="year",
        time_dest="year",
    )

    # Dict of ('parameter name' -> [list of data frames])
    result = defaultdict(list)

    # Iterate over each parameter and technology
    for (par, tec), group_data in data.groupby(["param", "technology"]):
        # Dict including the default values to be used as args in make_df()
        args = defaults.copy()
        args["technology"] = tec

        # Parameter-specific arguments/processing
        if par == "input":
            pass  # Handled by input_commodity_level(), below
        elif par == "output":
            # Get the mode for a technology
            mode = tech_info[tech_info.index(tec)].parent.id
            args.update(dict(commodity=f"transport pax {mode.lower()}", level="useful"))
        elif par == "capacity_factor":
            # Convert to preferred units
            group_data = group_data.apply(lambda v: v.to(UNITS[par][2]))

        # Units, as an abbreviated string
        units = group_data.apply(lambda x: x.units).unique()
        assert len(units) == 1, "Units must be unique per (tec, par)"
        args["unit"] = f"{units[0]:~}"

        # Create data frame with values from *args*
        df = make_df(par, **args)

        # Assign input commodity and level according to the technology
        if par == "input":
            df = input_commodity_level(df, default_level="secondary")

        # Copy data into the 'value' column, by vintage year
        for (year, *_), value in group_data.items():
            df.loc[df["year_vtg"] == year, "value"] = value.magnitude

        # Drop duplicates. For parameters with 'year_vtg' but no 'year_act' dimension,
        # the same year_vtg appears multiple times because of the contents of *defaults*
        df.drop_duplicates(inplace=True)

        # Fill remaining values for the rest of vintage years with the last value
        # registered, in this case for 2030.
        df["value"] = df["value"].fillna(method="ffill")

        # Round up **technical_lifetime** values due to incompatibility in handling
        # non-integer values in the GAMS code
        if par == "technical_lifetime":
            df["value"] = df["value"].round()

        # Broadcast across all nodes
        result[par].append(df.pipe(broadcast, node_loc=info.N[1:]).pipe(same_node))

    # Directory for debug output (if any)
    debug_dir = context.get_local_path("debug")
    # Ensure the directory
    debug_dir.mkdir(parents=True, exist_ok=True)

    # Concatenate data frames for each model parameter
    for par, list_of_df in result.items():
        result[par] = pd.concat(list_of_df)

        if context.get("debug", False):
            target = debug_dir.joinpath(f"ikarus-{par}.csv")
            log.info(f"Dump data to {target}")
            result[par].to_csv(target, index=False)

    return result
