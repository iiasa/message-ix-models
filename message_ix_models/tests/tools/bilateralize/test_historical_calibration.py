import os

import pandas as pd
import pytest

from message_ix_models.tools.bilateralize import (
    historical_calibration as historical_calibration_module,
)
from message_ix_models.tools.bilateralize.historical_calibration import (
    build_hist_new_capacity_flow,
    build_hist_new_capacity_trade,
    build_historical_activity,
    build_historical_price,
    check_iea_balances,
    convert_trade,
    generate_cfdict,
    import_iea_balances,
    import_iea_gas,
    import_uncomtrade,
    reformat_to_parameter,
    setup_datapath,
)

MARK = pytest.mark.xfail(
    raises=FileNotFoundError,
    reason="Input data files not available for testing.",
)


@pytest.fixture
def message_regions() -> str:
    """`message_regions` parameter to some functions."""
    return "R12"


@MARK  # IMO/GISIS/* Tankers.csv
@pytest.mark.parametrize(
    "infile, ship_type",
    # Values appearing where the function is used in bare_to_scenario()
    [
        ("Crude Tankers.csv", "crudeoil_tanker_loil"),
        ("LH2 Tankers.csv", "lh2_tanker_loil"),
        ("LNG Tankers.csv", "LNG_Tanker_loil"),
        ("LNG Tankers.csv", "LNG_Tanker_LNG"),
        ("Oil Tankers.csv", "oil_tanker_eth"),
        ("Oil Tankers.csv", "oil_tanker_foil"),
        ("Oil Tankers.csv", "oil_tanker_loil"),
    ],
)
def test_build_hist_new_capacity_flow(infile: str, ship_type: str) -> None:
    build_hist_new_capacity_flow(infile, ship_type)


@MARK  # UN Comtrade/BACI/shortenedBACI.csv
def test_build_hist_new_capacity_trade() -> None:
    build_hist_new_capacity_trade()


@MARK  # UN Comtrade/BACI/shortenedBACI.csv
def test_build_historical_activity() -> None:
    build_historical_activity()


def test_build_historical_activity_diagnosis_only(monkeypatch, tmp_path) -> None:
    """`diagnosis_only=True` returns early; otherwise output rows are deduplicated."""
    monkeypatch.setattr(
        historical_calibration_module,
        "convert_trade",
        lambda **kw: pd.DataFrame(
            {
                "YEAR": [2020],
                "EXPORTER": ["USA"],
                "IMPORTER": ["FRA"],
                "HS": ["2709"],
                "MESSAGE COMMODITY": ["crudeoil"],
                "ENERGY (TJ)": [100.0],
            }
        ),
    )
    monkeypatch.setattr(
        historical_calibration_module,
        "import_iea_gas",
        lambda **kw: pd.DataFrame(
            {
                "YEAR": [2020],
                "EXPORTER": ["USA"],
                "IMPORTER": ["DEU"],
                "MESSAGE COMMODITY": ["gas_piped"],
                "ENERGY (TJ)": [20.0],
            }
        ),
    )
    monkeypatch.setattr(
        historical_calibration_module,
        "setup_datapath",
        lambda **kw: {"iea_web": str(tmp_path)},
    )

    def fake_read_csv(path, **kw):
        if "country_iso3" in str(path):
            return pd.DataFrame({"REGION": ["USA_REGION"], "ISO3": ["USA"]})
        return pd.DataFrame(
            {
                "REGION": ["USA_REGION"],
                "FLOW": ["EXPORTS"],
                "IEA-WEB VALUE": [20.0],
                "IEA-WEB COMMODITY": ["NATURAL_GAS"],
                "YEAR": [2020],
            }
        )

    monkeypatch.setattr(pd, "read_csv", fake_read_csv)
    # check_iea_balances is tested in isolation elsewhere
    monkeypatch.setattr(
        historical_calibration_module,
        "check_iea_balances",
        lambda **kw: kw["indf"],
    )

    # diagnosis_only=True: return before reformat_to_parameter is called
    result = build_historical_activity(diagnosis_only=True)
    assert "ENERGY (GWa)" in result.columns

    # diagnosis_only=False (default): duplicate output rows are summed, not kept
    monkeypatch.setattr(
        historical_calibration_module,
        "reformat_to_parameter",
        lambda **kw: pd.DataFrame(
            {
                "node_loc": ["R12_NAM", "R12_NAM"],
                "technology": ["x_exp_weu", "x_exp_weu"],
                "year_act": [2020, 2020],
                "mode": ["M1", "M1"],
                "time": ["year", "year"],
                "value": [10.0, 5.0],
            }
        ),
    )
    result = build_historical_activity()
    assert len(result) == 1
    assert result["value"].iloc[0] == 15.0


@MARK  # UN Comtrade/BACI/shortenedBACI.csv
def test_build_historical_price() -> None:
    build_historical_price()


def test_build_historical_price_adds_gas_piped(monkeypatch) -> None:
    """Piped gas gets an LNG-derived price, added as an extra commodity row."""
    monkeypatch.setattr(
        historical_calibration_module,
        "convert_trade",
        lambda **kw: pd.DataFrame(
            {
                "YEAR": [2021],
                "EXPORTER": ["USA"],
                "IMPORTER": ["FRA"],
                "MESSAGE COMMODITY": ["lng"],
                "ENERGY (TJ)": [10.0],
                "VALUE (1000USD)": [1000.0],
            }
        ),
    )
    captured = {}

    def fake_reformat_to_parameter(indf, **kw):
        captured["indf"] = indf
        return pd.DataFrame({"value": []})

    monkeypatch.setattr(
        historical_calibration_module,
        "reformat_to_parameter",
        fake_reformat_to_parameter,
    )

    build_historical_price()

    assert set(captured["indf"]["MESSAGE COMMODITY"]) == {"LNG_shipped", "gas_piped"}


# @MARK  # IEA/WEB2025/WEB_TRADEFLOWS.txt
# def test_check_iea_balances() -> None:
#    indf = pd.DataFrame()
#    check_iea_balances(indf)


def test_check_iea_balances(monkeypatch, tmp_path) -> None:
    """Trade is calibrated to IEA balances by export multiplier.

    `gas_piped`/`LNG_shipped` rows bypass calibration and are added back
    unchanged.
    """
    monkeypatch.setattr(
        historical_calibration_module,
        "setup_datapath",
        lambda **kwargs: {"iea_web": str(tmp_path), "iea_diag": str(tmp_path)},
    )

    def fake_read_csv(path, **kwargs):
        if "country_iso3" in str(path):
            return pd.DataFrame({"REGION": ["USA_REGION"], "ISO3": ["USA"]})
        return pd.DataFrame(
            {
                "REGION": ["USA_REGION"],
                "FLOW": ["EXPORTS"],
                "IEA-WEB VALUE": [100.0],
                "IEA-WEB COMMODITY": ["CRUDE_OIL"],
                "YEAR": [2020],
                "IEA-WEB UNIT": ["TJ"],
            }
        )

    monkeypatch.setattr(pd, "read_csv", fake_read_csv)

    indf = pd.DataFrame(
        {
            "YEAR": [2020, 2020],
            "EXPORTER": ["USA", "USA"],
            "IMPORTER": ["FRA", "DEU"],
            "MESSAGE COMMODITY": ["crudeoil_shipped", "gas_piped"],
            "ENERGY (TJ)": [100.0, 50.0],
        }
    )

    result = check_iea_balances(indf)

    # gas_piped bypasses calibration and is added back unchanged
    gas_row = result[result["MESSAGE COMMODITY"] == "gas_piped"]
    assert gas_row["ENERGY (TJ)"].iloc[0] == 50.0

    # crudeoil_shipped is calibrated: IEA export value (-100, after the
    # EXPORTS sign flip) / ENERGY (TJ) (100) gives a multiplier of -1
    oil_row = result[result["MESSAGE COMMODITY"] == "crudeoil_shipped"]
    assert oil_row["ENERGY (TJ)"].iloc[0] == -100.0


@MARK  # UN Comtrade/BACI/shortenedBACI.csv
def test_convert_trade(message_regions: str) -> None:
    convert_trade(message_regions)


@MARK  # IEA/WEB2025/CONV.txt
def test_generate_cfdict(message_regions: str) -> None:
    generate_cfdict(message_regions)


@MARK  # IEA/WEB2025/EARLYBIG1.txt
def test_import_iea_balances() -> None:
    import_iea_balances()


def test_import_iea_balances_concatenates_files(monkeypatch, tmp_path) -> None:
    """`import_iea_balances` reads and concatenates all three input files."""
    monkeypatch.setattr(
        historical_calibration_module,
        "setup_datapath",
        lambda **kwargs: {"iea_web": str(tmp_path)},
    )

    # One EXPORTS and one IMPORTS row, each returned for every one of the
    # three input files that get concatenated
    monkeypatch.setattr(
        pd,
        "read_csv",
        lambda *args, **kwargs: pd.DataFrame(
            [
                ["FRA", "NATURAL_GAS", 2020, "EXPORTS", "TJ", 100.0, 0.0],
                ["FRA", "NATURAL_GAS", 2020, "IMPORTS", "TJ", 50.0, 0.0],
            ]
        ),
    )

    written: dict[str, pd.DataFrame] = {}
    monkeypatch.setattr(
        pd.DataFrame,
        "to_csv",
        lambda self, path, **kw: written.__setitem__(os.path.basename(path), self),
    )

    import_iea_balances()

    result = written["WEB_TRADEFLOWS.csv"]
    # 2 rows/file x 3 files concatenated
    assert len(result) == 6
    assert set(result["FLOW"]) == {"EXPORTS", "IMPORTS"}


@MARK  # IEA/NATGAS/WIMPDAT.txt
def test_import_iea_gas() -> None:
    import_iea_gas()


@MARK  # UN Comtrade/BACI/BACI_HS92_Y2005_V202501.csv
def test_import_uncomtrade() -> None:
    import_uncomtrade()


@pytest.mark.xfail(raises=UnboundLocalError, reason="Test input data is empty.")
def test_reformat_to_parameter(message_regions: str) -> None:
    # Column names that must be present on `indf`
    # TODO Mention these in the function docstring
    indf = pd.DataFrame(columns=["IMPORTER", "EXPORTER", "MESSAGE COMMODITY", "YEAR"])
    parameter_name = "foo"
    reformat_to_parameter(indf, message_regions, parameter_name)


def test_setup_datapath() -> None:
    setup_datapath()
    # TODO Extend with assertions
