import pytest

from message_ix_models.tools.bilateralize.bare_to_scenario import (
    build_parameter_sheets,
    calibrate_historical_shipping,
)
from message_ix_models.tools.bilateralize.utils import get_logger, load_config

MARK = pytest.mark.xfail(
    raises=FileNotFoundError,
    reason="Input data files not available for testing.",
)

@MARK #P:/ene.model/MESSAGE_trade/IMO/GISIS/Crude Tankers.csv
def test_calibrate_historical_shipping(
    project_name: str | None = None, config_name: str | None = None
) -> None:
    config, config_path, tec_config = load_config(
        project_name=project_name, config_name=config_name, load_tec_config=True
    )

    covered_tec = config["covered_trade_technologies"]

    # Get logger
    log = get_logger(__name__)

    # Read and inflate sheets based on model horizon
    trade_dict = build_parameter_sheets(
        log=log, project_name=project_name, config_name=config_name
    )
    calibrate_historical_shipping(
        config=config,
        trade_dict=trade_dict,
        covered_tec=covered_tec,
        project_name=project_name,
        config_name=config_name,
    )
    assert True
