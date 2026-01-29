import pandas as pd
import pytest

from message_ix_models.tools.bilateralize.historical_calibration import (
    build_hist_new_capacity_flow,
    build_hist_new_capacity_trade,
    build_historical_activity,
    build_historical_price,
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


@MARK  # UN Comtrade/BACI/shortenedBACI.csv
def test_build_historical_price() -> None:
    build_historical_price()


# @MARK  # IEA/WEB2025/WEB_TRADEFLOWS.txt
# def test_check_iea_balances() -> None:
#    indf = pd.DataFrame()
#    check_iea_balances(indf)


@MARK  # UN Comtrade/BACI/shortenedBACI.csv
def test_convert_trade(message_regions: str) -> None:
    convert_trade(message_regions)


@MARK  # IEA/WEB2025/CONV.txt
def test_generate_cfdict(message_regions: str) -> None:
    generate_cfdict(message_regions)


@MARK  # IEA/WEB2025/EARLYBIG1.txt
def test_import_iea_balances() -> None:
    import_iea_balances()


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
