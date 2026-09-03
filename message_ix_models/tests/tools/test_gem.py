import pandas as pd
import pytest

from message_ix_models.tools.gem import (
    COLUMNS,
    SHEET,
    country_names,
    read_units,
    technology,
)


@pytest.fixture(scope="module")
def workbook(tmp_path_factory) -> str:
    """A workbook like the GIPT with 6 units in 4 countries."""
    rows = [
        # Country, type, plant, unit, MW, status, start, retired, technology, fuel, CCS
        (
            "Kazakhstan",
            "coal",
            "A",
            "1",
            500.0,
            "operating",
            1980,
            None,
            "subcritical",
            None,
            "no",
        ),
        (
            "Kazakhstan",
            "oil/gas",
            "B",
            "1",
            200.0,
            "operating",
            2010,
            None,
            "combined cycle",
            "Gas",
            "no",
        ),
        (
            "Kazakhstan",
            "wind",
            "C",
            "--",
            50.0,
            "announced",
            "not found",
            None,
            "onshore",
            None,
            None,
        ),
        (
            "Serbia",
            "coal",
            "D",
            "1",
            300.0,
            "retired",
            1970,
            2015,
            "subcritical",
            None,
            "no",
        ),
        # Names the tracker uses that are not the pycountry name, or not in pycountry
        (
            "Moldova",
            "hydropower",
            "E",
            "1",
            16.0,
            "operating",
            1964,
            None,
            "unknown",
            None,
            None,
        ),
        (
            "Kosovo",
            "coal",
            "F",
            "A3",
            200.0,
            "operating",
            1970,
            None,
            "subcritical",
            None,
            "no",
        ),
    ]
    columns = ["Country/area"] + [c for c in COLUMNS if c != "GEM unit/phase ID"]
    df = pd.DataFrame(rows, columns=columns).assign(
        **{"GEM unit/phase ID": [f"G{i}" for i in range(len(rows))], "Region": "x"}
    )

    path = tmp_path_factory.mktemp("gem") / "gipt.xlsx"
    df.to_excel(path, sheet_name=SHEET, index=False)
    return str(path)


def test_read_units(workbook) -> None:
    result = read_units(workbook, "KAZ")

    # Only the country's units, with the renamed columns
    assert 3 == len(result)
    assert list(COLUMNS.values()) == list(result.columns)
    # Unknown years become NaN
    assert result["start_year"].isna().tolist() == [False, False, True]
    assert 1980.0 == result["start_year"][0]


@pytest.mark.parametrize("country, name", [("MDA", "Moldova"), ("XKX", "Kosovo")])
def test_read_units_names(workbook, country, name) -> None:
    """Units are found under a common name or a name pycountry lacks."""
    result = read_units(workbook, country)

    assert 1 == len(result)
    assert name in country_names(country)


def test_read_units_unknown(workbook) -> None:
    with pytest.raises(ValueError, match="alpha-3"):
        read_units(workbook, "XXX")
    # The names tried appear in the message
    with pytest.raises(ValueError, match="'Austria', 'Republic of Austria'"):
        read_units(workbook, "AUT")


#: Every unit class of the March 2026 tracker with more than 500 MW operating, and the
#: name the SSP1/2/3 v6.6 R12 baselines put it under. The classes and their target
#: names are transcribed from the comparison of the tracker against those baselines;
#: the resolution rules in the docstring of :func:`.gem.technology` decide the classes
#: the comparison leaves as a candidate set. Status is "operating" throughout, since
#: these are the classes' operating capacity; :data:`RULES` varies it.
CLASSES = (
    # gem_type, technology, fuel_classification, CCS, expected
    ("bioenergy", None, None, None, "bio_ppl"),
    ("coal", "IGCC", None, "no", "igcc"),
    ("coal", "subcritical", None, None, "coal_ppl_u"),
    ("coal", "subcritical", None, "no", "coal_ppl_u"),
    ("coal", "subcritical", None, "not found", "coal_ppl_u"),
    ("coal", "subcritical/CCS", None, "yes", "coal_adv_ccs"),
    ("coal", "supercritical", None, None, "coal_ppl_u"),
    ("coal", "supercritical", None, "no", "coal_ppl_u"),
    ("coal", "supercritical", None, "not found", "coal_ppl_u"),
    ("coal", "supercritical/CCS", None, "yes", "coal_adv_ccs"),
    ("coal", "ultra-supercritical", None, None, "coal_adv"),
    ("coal", "ultra-supercritical", None, "no", "coal_adv"),
    ("coal", "ultra-supercritical", None, "not found", "coal_adv"),
    ("coal", "ultra-supercritical/CCS", None, "yes", "coal_adv_ccs"),
    ("coal", "unknown", None, None, "coal_ppl_u"),
    ("coal", "unknown", None, "no", "coal_ppl_u"),
    ("geothermal", "binary cycle", None, None, "geo_ppl"),
    ("geothermal", "dry steam", None, None, "geo_ppl"),
    ("geothermal", "flash steam - double", None, None, "geo_ppl"),
    ("geothermal", "flash steam - single", None, None, "geo_ppl"),
    ("geothermal", "unknown type", None, None, "geo_ppl"),
    ("hydropower", "conventional and pumped storage", None, None, "hydro_hc"),
    ("hydropower", "conventional and run-of-river", None, None, "hydro_hc"),
    ("hydropower", "conventional storage", None, None, "hydro_hc"),
    ("hydropower", "pumped storage", None, None, None),
    ("hydropower", "run-of-river", None, None, "hydro_hc"),
    ("hydropower", "unknown", None, None, "hydro_hc"),
    ("nuclear", "boiling water reactor", None, None, "nuc_lc"),
    ("nuclear", "fast breeder reactor", None, None, "nuc_lc"),
    ("nuclear", "gas-cooled reactor", None, None, "nuc_lc"),
    ("nuclear", "light water graphite reactor", None, None, "nuc_lc"),
    ("nuclear", "pressurized heavy water reactor", None, None, "nuc_lc"),
    ("nuclear", "pressurized water reactor", None, None, "nuc_lc"),
    ("oil/gas", "ICCC", "Gas", None, "gas_cc"),
    ("oil/gas", "ICCC", "Gas", "not found", "gas_cc"),
    ("oil/gas", "ICCC", "Multi fuel", None, "gas_cc"),
    ("oil/gas", "ICCC", "Multi fuel", "not found", "gas_cc"),
    ("oil/gas", "ICCC", "Oil", None, "loil_cc"),
    ("oil/gas", "ICCC", "Oil", "not found", "loil_cc"),
    ("oil/gas", "ISCC", "Multi fuel", None, "gas_cc"),
    ("oil/gas", "ISCC", "Multi fuel", "not found", "gas_cc"),
    ("oil/gas", "combined cycle", "Gas", None, "gas_cc"),
    ("oil/gas", "combined cycle", "Gas", "no", "gas_cc"),
    ("oil/gas", "combined cycle", "Gas", "not found", "gas_cc"),
    ("oil/gas", "combined cycle", "Gas", "yes", "gas_cc_ccs"),
    ("oil/gas", "combined cycle", "LNG only", None, "gas_cc"),
    ("oil/gas", "combined cycle", "LNG only", "not found", "gas_cc"),
    ("oil/gas", "combined cycle", "LNG only", "yes", "gas_cc_ccs"),
    ("oil/gas", "combined cycle", "Multi fuel", None, "gas_cc"),
    ("oil/gas", "combined cycle", "Multi fuel", "not found", "gas_cc"),
    ("oil/gas", "combined cycle", "Multi fuel", "yes", "gas_cc_ccs"),
    ("oil/gas", "combined cycle", "Oil", None, "loil_cc"),
    ("oil/gas", "combined cycle", "Oil", "no", "loil_cc"),
    ("oil/gas", "combined cycle", "Oil", "not found", "loil_cc"),
    ("oil/gas", "gas turbine", "Gas", None, "gas_ct"),
    ("oil/gas", "gas turbine", "Gas", "no", "gas_ct"),
    ("oil/gas", "gas turbine", "Gas", "not found", "gas_ct"),
    ("oil/gas", "gas turbine", "Gas", "yes", None),
    ("oil/gas", "gas turbine", "LNG only", "not found", "gas_ct"),
    ("oil/gas", "gas turbine", "Multi fuel", None, "gas_ct"),
    ("oil/gas", "gas turbine", "Multi fuel", "no", "gas_ct"),
    ("oil/gas", "gas turbine", "Multi fuel", "not found", "gas_ct"),
    ("oil/gas", "gas turbine", "Oil", None, "loil_ppl"),
    ("oil/gas", "gas turbine", "Oil", "no", "loil_ppl"),
    ("oil/gas", "gas turbine", "Oil", "not found", "loil_ppl"),
    ("oil/gas", "internal combustion", "Gas", None, None),
    ("oil/gas", "internal combustion", "Gas", "no", None),
    ("oil/gas", "internal combustion", "Gas", "not found", None),
    ("oil/gas", "internal combustion", "LNG only", "not found", None),
    ("oil/gas", "internal combustion", "Multi fuel", None, None),
    ("oil/gas", "internal combustion", "Multi fuel", "not found", None),
    ("oil/gas", "internal combustion", "Oil", None, "loil_ppl"),
    ("oil/gas", "internal combustion", "Oil", "not found", "loil_ppl"),
    ("oil/gas", "steam turbine", "Gas", None, "gas_ppl"),
    ("oil/gas", "steam turbine", "Gas", "no", "gas_ppl"),
    ("oil/gas", "steam turbine", "Gas", "not found", "gas_ppl"),
    ("oil/gas", "steam turbine", "LNG only", None, "gas_ppl"),
    ("oil/gas", "steam turbine", "Multi fuel", None, "gas_ppl"),
    ("oil/gas", "steam turbine", "Multi fuel", "no", "gas_ppl"),
    ("oil/gas", "steam turbine", "Multi fuel", "not found", "gas_ppl"),
    ("oil/gas", "steam turbine", "Oil", None, "foil_ppl"),
    ("oil/gas", "steam turbine", "Oil", "no", "foil_ppl"),
    ("oil/gas", "steam turbine", "Oil", "not found", "foil_ppl"),
    ("oil/gas", "unknown", "Gas", None, "gas_cc"),
    ("oil/gas", "unknown", "Gas", "not found", "gas_cc"),
    ("oil/gas", "unknown", "LNG only", None, "gas_cc"),
    ("oil/gas", "unknown", "LNG only", "not found", "gas_cc"),
    ("oil/gas", "unknown", "Multi fuel", "not found", "gas_cc"),
    ("oil/gas", "unknown", "Oil", None, "foil_ppl"),
    ("oil/gas", "unknown", "Oil", "not found", "foil_ppl"),
    ("utility-scale solar", "Assumed PV", None, None, "solar_pv_ppl"),
    ("utility-scale solar", "PV", None, None, "solar_pv_ppl"),
    ("utility-scale solar", "Solar Thermal", None, None, "csp_sm1_ppl"),
    ("wind", "Offshore hard mount", None, None, "wind_ppf"),
    ("wind", "Offshore mount unknown", None, None, "wind_ppf"),
    ("wind", "Onshore", None, None, "wind_ppl"),
    ("wind", "Unknown", None, None, "wind_ppl"),
)

#: Classes that :data:`CLASSES` does not exercise: the status split of rule 1, the
#: capture rules of rule 4 on classes below 500 MW operating, and the types with no
#: counterpart at all.
RULES = (
    # gem_type, technology, fuel_classification, CCS, status, expected
    # Rule 1: existing pulverised coal takes the inherited-stock name, a unit still to
    # be built the future-build name; the efficiency inference is status-independent
    ("coal", "subcritical", None, "no", "retired", "coal_ppl_u"),
    ("coal", "subcritical", None, "no", "mothballed", "coal_ppl_u"),
    ("coal", "subcritical", None, "no", "construction", "coal_ppl"),
    ("coal", "subcritical", None, "no", "pre-construction", "coal_ppl"),
    ("coal", "subcritical", None, "no", "announced", "coal_ppl"),
    ("coal", "supercritical", None, "no", "construction", "coal_ppl"),
    ("coal", "ultra-supercritical", None, "no", "announced", "coal_adv"),
    ("coal", "IGCC", None, "yes", "operating", "igcc_ccs"),
    ("coal", "unknown/CCS", None, "yes", "operating", "coal_adv_ccs"),
    # Rule 4: only a gas combined cycle and a gas steam plant can carry capture
    ("oil/gas", "steam turbine", "Gas", "yes", "operating", "gas_ppl"),
    ("oil/gas", "ICCC", "Gas", "yes", "operating", "gas_cc_ccs"),
    ("oil/gas", "steam turbine", "Oil", "yes", "operating", None),
    ("oil/gas", "combined cycle", "Oil", "yes", "operating", None),
    ("oil/gas", "ICCC", "Oil", "yes", "operating", None),
    ("oil/gas", "gas turbine", "Oil", "yes", "operating", None),
    ("oil/gas", "internal combustion", "Gas", "yes", "operating", None),
    ("oil/gas", "unknown", "Gas", "yes", "operating", None),
    ("oil/gas", "unknown", "Multi fuel", "yes", "operating", None),
    # Rule 5: no counterpart in the baselines
    ("oil/gas", "AFC", "Gas", None, "operating", None),
    ("oil/gas", "AFC", "Gas", "yes", "operating", None),
    ("oil/gas", "IGCC", "Multi fuel", "no", "operating", None),
    ("hydropower", "pumped storage", None, None, "construction", None),
    # A type the tracker may add
    ("marine", None, None, None, "operating", None),
)


def unit(gem_type, technology_, fuel_classification, ccs, status) -> pd.Series:
    """One row of the frame :func:`.gem.read_units` returns."""
    return pd.Series(
        dict(
            gem_type=gem_type,
            technology=technology_,
            fuel_classification=fuel_classification,
            ccs=ccs,
            status=status,
        )
    )


@pytest.mark.parametrize(
    "gem_type, technology_, fuel_classification, ccs, expected", CLASSES
)
def test_technology(gem_type, technology_, fuel_classification, ccs, expected) -> None:
    """Every tracker class above 500 MW operating reaches the name it belongs under."""
    assert expected == technology(
        unit(gem_type, technology_, fuel_classification, ccs, "operating")
    )


@pytest.mark.parametrize(
    "gem_type, technology_, fuel_classification, ccs, status, expected", RULES
)
def test_technology_rules(
    gem_type, technology_, fuel_classification, ccs, status, expected
) -> None:
    assert expected == technology(
        unit(gem_type, technology_, fuel_classification, ccs, status)
    )
