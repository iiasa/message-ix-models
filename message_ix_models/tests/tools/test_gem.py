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
        # Country, type, plant, unit, MW, status, start, retired, technology, fuel
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
        ),
        ("Serbia", "coal", "D", "1", 300.0, "retired", 1970, 2015, "subcritical", None),
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


@pytest.mark.parametrize(
    "gem_type, technology_, fuel_classification, expected",
    [
        ("coal", "subcritical", None, "coal_ppl"),
        ("hydropower", "unknown", None, "hydro_ppl"),
        ("wind", "onshore", None, "wind_ppl"),
        ("utility-scale solar", "PV", None, "solar_pv_ppl"),
        ("oil/gas", "combined cycle", "Gas", "gas_cc"),
        ("oil/gas", "steam turbine", "Gas", "gas_ppl"),
        ("oil/gas", "steam turbine", "Multi fuel", "gas_ppl"),
        ("oil/gas", "internal combustion", "Oil", "oil_ppl"),
        ("nuclear", "unknown", None, None),
    ],
)
def test_technology(gem_type, technology_, fuel_classification, expected) -> None:
    unit = pd.Series(
        dict(
            gem_type=gem_type,
            technology=technology_,
            fuel_classification=fuel_classification,
        )
    )
    assert expected == technology(unit)
