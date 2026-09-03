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
    "CCS": "ccs",
    "GEM unit/phase ID": "gem_unit_id",
}

#: Values of the tracker's "Status" column in which a unit exists or has existed, and
#: so belongs to the inherited stock rather than to a build still to come.
EXISTING_STATUS: frozenset[str] = frozenset({"operating", "retired", "mothballed"})

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

    `unit` is a row of the frame returned by :func:`read_units`. The name returned is
    the one the R12 baselines put the unit's class under, which for an existing plant
    is often a historical-stock name—one the baselines carry as inherited capacity but
    do not offer as a future build. :any:`None` means the baselines have no technology
    whose capacity is commensurable with the unit, not that the unit is unimportant.

    The tracker's discriminating columns are "Type", "Technology", "Fuel classification
    (oil/gas only)", "CCS" and "Status". Where they do not settle the name, these rules
    do:

    1. Pulverised coal. The baselines' ``coal_ppl`` and ``coal_ppl_u`` share the
       region's calibrated efficiency and split on eligibility for a CO2-scrubber
       retrofit, which the tracker does not record; ``coal_ppl_u`` additionally has no
       vintage after 2020. So the split is made on status instead: a unit that exists
       or has existed—operating, retired or mothballed—goes to the inherited-stock name
       ``coal_ppl_u``, and a unit not yet built to ``coal_ppl``. Ultra-supercritical
       units go to ``coal_adv`` whatever their status; that is an inference from
       ``coal_adv``'s 0.47 new-build efficiency being the closest in the baselines to a
       modern ultra-supercritical plant, and the repository's own ``tech_map.csv``
       disputes it by costing ``coal_adv`` as supercritical. IGCC goes to ``igcc``. Any
       coal unit marked ``CCS = yes`` goes to ``coal_adv_ccs``, or ``igcc_ccs`` for
       IGCC, the only capturing coal plants the baselines have.
    2. Fleets the baselines split and the tracker does not. Hydro and nuclear are each
       split into a low- and a high-cost class and solar thermal into two solar
       multiples, and the tracker publishes neither a cost nor a solar-multiple field.
       Each fleet therefore goes to whichever class carries ``historical_new_capacity``
       in the R12 baselines: ``hydro_hc``, whose largest historical addition exceeds
       ``hydro_lc``'s in all twelve regions of all three SSPs, by factors from five to
       thirty-seven; ``nuc_lc``, which leads ``nuc_hc`` in seven regions against four
       (one region has neither), so it is the majority class rather than the unanimous
       one; and ``csp_sm1_ppl``, since ``csp_sm3_ppl`` carries no historical addition
       in any region of any SSP.
    3. Renewables. Onshore wind goes to ``wind_ppl`` and offshore to ``wind_ppf``,
       including the tracker's unknown-mount offshore class; wind of unknown type goes
       to ``wind_ppl``. Utility PV goes to ``solar_pv_ppl`` and solar thermal to
       ``csp_sm1_ppl``. These are capacity accounts of graded fleets, and writing
       capacity to them alone produces no generation: the matching cohort rows are the
       caller's to add.
    4. Capture on oil and gas. The baselines capture CO2 on gas in exactly two ways—the
       purpose-built combined cycle ``gas_cc_ccs``, and the ``g_ppl_co2scr`` retrofit,
       whose relation admits ``gas_cc`` and ``gas_ppl`` and not ``gas_ct``—and no
       oil-fired plant captures at all. So a gas combined cycle marked ``CCS = yes``
       goes to ``gas_cc_ccs`` and a gas steam turbine to ``gas_ppl``, whose capture
       route is the retrofit device the caller adds beside it. Every other unit marked
       ``CCS = yes`` returns :any:`None`: an open-cycle turbine can be neither built
       with capture nor retrofitted, an oil-fired plant has no capturing counterpart,
       and where the tracker does not state the prime mover the two routes cannot be
       told apart.
    5. No counterpart. Pumped storage returns :any:`None`—the baselines' only storage
       technology, ``stor_ppl``, sends its output to a sink and acts through the
       operating-reserve and curtailment relations, so no equivalence with a
       pumped-storage nameplate is established here. So do gas, LNG and multi-fuel
       reciprocating engines, fuel cells, and IGCC on oil or gas, none of which the
       baselines hold in any form.

    Two further defaults cover classes the tracker leaves open. A unit of unknown prime
    mover goes to ``gas_cc`` on gas and ``foil_ppl`` on oil. Bioenergy goes to
    ``bio_ppl``, the combustion plant, since the tracker publishes no gasification
    class that would select ``bio_istig``. LNG-only and multi-fuel units count as
    gas-fired.
    """
    match unit["gem_type"]:
        case "bioenergy":
            return "bio_ppl"
        case "coal":
            return _coal(unit)
        case "geothermal":
            return "geo_ppl"
        case "hydropower":
            return None if _text(unit, "technology") == "pumped storage" else "hydro_hc"
        case "nuclear":
            return "nuc_lc"
        case "oil/gas":
            return _oil_gas(unit)
        case "utility-scale solar":
            solar_thermal = _text(unit, "technology") == "solar thermal"
            return "csp_sm1_ppl" if solar_thermal else "solar_pv_ppl"
        case "wind":
            offshore = "offshore" in _text(unit, "technology")
            return "wind_ppf" if offshore else "wind_ppl"
        case _:
            return None


def _text(unit: pd.Series, column: str) -> str:
    """Return `column` of `unit` in lower case, with a missing value as ``""``."""
    value = unit[column]
    return "" if pd.isna(value) else str(value).strip().lower()


def _coal(unit: pd.Series) -> str:
    """Return the technology for a coal `unit`. See rule 1 of :func:`technology`."""
    # The tracker states capture both in its "CCS" column and as a suffix of the steam
    # class, for instance "ultra-supercritical/CCS"
    steam_class = _text(unit, "technology")
    capture = _text(unit, "ccs") == "yes" or steam_class.endswith("/ccs")

    if steam_class.startswith("igcc"):
        return "igcc_ccs" if capture else "igcc"
    elif capture:
        return "coal_adv_ccs"
    elif steam_class.startswith("ultra-supercritical"):
        return "coal_adv"
    elif _text(unit, "status") in EXISTING_STATUS:
        return "coal_ppl_u"
    else:
        return "coal_ppl"


def _oil_gas(unit: pd.Series) -> str | None:
    """Return the technology for an "oil/gas" `unit`.

    See rules 4 and 5 of :func:`technology`.
    """
    prime_mover = _text(unit, "technology")
    oil = _text(unit, "fuel_classification") == "oil"
    capture = _text(unit, "ccs") == "yes"

    # "ICCC" and "ISCC" are the tracker's integrated coal-gasification and integrated
    # solar combined cycles; the baselines have only the plain combined cycle
    if prime_mover in ("combined cycle", "iccc", "iscc"):
        if oil:
            return None if capture else "loil_cc"
        return "gas_cc_ccs" if capture else "gas_cc"
    elif prime_mover == "gas turbine":  # Open cycle, on either fuel
        if capture:
            return None
        return "loil_ppl" if oil else "gas_ct"
    elif prime_mover == "steam turbine":
        if oil:
            return None if capture else "foil_ppl"
        return "gas_ppl"
    elif prime_mover == "internal combustion":
        # The baselines have no engine; a light-oil unit is closest to loil_ppl
        return "loil_ppl" if oil and not capture else None
    elif prime_mover in ("afc", "igcc"):  # Alkaline fuel cell; gasification on oil/gas
        return None
    elif capture:  # Prime mover not stated, so neither capture route is verified
        return None
    else:
        return "foil_ppl" if oil else "gas_cc"
