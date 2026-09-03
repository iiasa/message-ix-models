"""Handle data from Global Energy Monitor (GEM).

The Global Integrated Power Tracker (GIPT) lists power-generating units worldwide with
their capacity, status, and start and retirement years. GEM provides it as a workbook
through a data request form at https://globalenergymonitor.org, under CC BY 4.0, so
there is no service to retrieve from: :func:`read_units` reads a local copy. Unit size
thresholds exclude small plant, so capacity totals derived from the tracker are a lower
bound.
"""

import logging
from collections.abc import Mapping
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

#: Sheet of the GIPT workbook that lists units.
SHEET = "Power facilities"

#: Columns read from :data:`SHEET`, and the names used for them in the result of
#: :func:`read_units`.
COLUMNS: Mapping[str, str] = {
    "Type": "gem_type",
    "Plant / Project name": "plant",
    "Unit / Phase name": "unit_name",
    "Capacity (MW)": "capacity_mw",
    "Status": "status",
    "Start year": "start_year",
    "Retired year": "retired_year",
    "Technology": "technology",
    "Fuel classification (oil/gas only)": "fuel_classification",
    "GEM unit/phase ID": "gem_unit_id",
}

#: Names in the "Country/area" column of the tracker for codes that :mod:`pycountry`
#: does not have. "XKX" is the user-assigned code for Kosovo used by the World Bank.
COUNTRY_NAME: Mapping[str, tuple[str, ...]] = {"XKX": ("Kosovo",)}


def country_names(country: str) -> tuple[str, ...]:
    """Return the names under which `country` may appear in the tracker.

    These are the name, common name, and official name of the ISO 3166-1 alpha-3 code
    in :mod:`pycountry`—for instance "Moldova, Republic of", "Moldova", and "Republic of
    Moldova"—or the entry in :data:`COUNTRY_NAME`.

    Raises
    ------
    ValueError
        if `country` is in neither.
    """
    from pycountry import countries

    c = countries.get(alpha_3=country)
    if c is not None:
        names = (
            c.name,
            getattr(c, "common_name", None),
            getattr(c, "official_name", None),
        )
        return tuple(n for n in names if n)
    elif country in COUNTRY_NAME:
        return COUNTRY_NAME[country]
    else:
        raise ValueError(f"country={country!r} is not an ISO 3166-1 alpha-3 code")


def read_units(path: Path, country: str) -> pd.DataFrame:
    """Return the power units of `country` from a GIPT workbook at `path`.

    Units of every status are returned: which of them count toward capacity in a given
    year is a decision for the caller. The tracker marks unknown years with strings such
    as "not found"; these become :any:`NaN` in the "start_year" and "retired_year"
    columns.

    Parameters
    ----------
    country
        ISO 3166-1 alpha-3 code. Units are selected by the names given by
        :func:`country_names` in the tracker's "Country/area" column.

    Returns
    -------
    pandas.DataFrame
        with the columns given by the values of :data:`COLUMNS`.

    Raises
    ------
    ValueError
        if `country` is not a known code, or the tracker has no unit under any of its
        names. The message lists the names tried, so that a spelling the tracker uses
        can be added to :data:`COUNTRY_NAME`.
    """
    names = country_names(country)

    df = pd.read_excel(path, sheet_name=SHEET)
    result = (
        df[df["Country/area"].isin(names)][list(COLUMNS)]
        .rename(columns=COLUMNS)
        .reset_index(drop=True)
    )
    if result.empty:
        raise ValueError(f"No unit in {path} has Country/area in {names!r}")

    for column in ("start_year", "retired_year"):
        result[column] = pd.to_numeric(result[column], errors="coerce")

    log.info(f"{len(result)} units in {names[0]}")
    return result


def technology(unit: pd.Series) -> str | None:
    """Return the MESSAGEix-GLOBIOM technology for a GEM power `unit`.

    `unit` is a row of the frame returned by :func:`read_units`. Units of type "oil/gas"
    are distinguished by the tracker's "technology" and "fuel_classification" columns:
    combined-cycle units map to ``gas_cc``; other units follow the explicit fuel
    classification, with multi-fuel units counted as gas-fired. Every "hydropower" unit
    maps to ``hydro_ppl``, including pumped storage, which the tracker does not separate
    from generation in its type column.

    Returns :any:`None` for types with no counterpart here, including "nuclear",
    "bioenergy", and "geothermal".
    """
    match unit["gem_type"]:
        case "coal":
            return "coal_ppl"
        case "hydropower":
            return "hydro_ppl"
        case "wind":
            return "wind_ppl"
        case "utility-scale solar":
            return "solar_pv_ppl"
        case "oil/gas":
            if unit["technology"] == "combined cycle":
                return "gas_cc"
            return "oil_ppl" if unit["fuel_classification"] == "Oil" else "gas_ppl"
        case _:
            return None
