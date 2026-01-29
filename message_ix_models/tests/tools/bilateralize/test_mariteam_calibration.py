import pytest

from message_ix_models.tools.bilateralize.mariteam_calibration import (
    calibrate_mariteam,
)
from message_ix_models.tools.bilateralize.utils import load_config

MARK = pytest.mark.xfail(
    raises=FileNotFoundError,
    reason="Input data files not available for testing.",
)


@MARK  # P:/ene.model/MESSAGE_trade/MariTEAM/MariTEAM_output_2025-07-21.csv
def test_calibrate_mariteam(
    project_name: str | None = None,
    config_name: str | None = None,
) -> None:
    config, config_path, tec_config = load_config(
        project_name=project_name, config_name=config_name, load_tec_config=True
    )

    covered_tec = config["covered_trade_technologies"]

    calibrate_mariteam(
        covered_tec=covered_tec,
        message_regions="R12",
        project_name=project_name,
        config_name=config_name,
    )

    assert True
