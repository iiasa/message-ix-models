"""Tests for :mod:`message_ix_models.model.bmt.config`."""

import pytest

from message_ix_models.model.bmt.config import apply_bmt_config
from message_ix_models.model.buildings.config import METHOD
from message_ix_models.model.transport.config import Config as TransportConfig
from message_ix_models.util import package_data_path


@pytest.fixture(autouse=True)
def buildings_code_dir(tmp_path, monkeypatch):
    """Avoid ixmp/package lookup when BuildingsConfig initializes code_dir."""
    monkeypatch.setattr("importlib.util.find_spec", lambda name: None)
    monkeypatch.setattr(
        "ixmp.config.get",
        lambda key, default=None: str(tmp_path)
        if key == "message buildings dir"
        else default,
    )


def test_apply_bmt_config_custom_path(test_context, tmp_path):
    """apply_bmt_config loads YAML into context attributes."""
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text(
        """
model_name: "TEST-MODEL"
buildings:
  code: "R"
  with_materials: true
macro: "custom_macro.xlsx"
transport:
  code: "SSP2"
""",
        encoding="utf-8",
    )

    apply_bmt_config(test_context, path=yaml_path)

    assert test_context.bmt["model_name"] == "TEST-MODEL"
    assert test_context.macro == "custom_macro.xlsx"
    assert test_context.buildings.code == "R"
    assert test_context.buildings.with_materials is True
    assert test_context.buildings.method is METHOD.B
    assert test_context.buildings.sturm_scenario == "NONE"
    assert isinstance(test_context.transport, TransportConfig)
    assert test_context.transport.code == "SSP2"


# TODO: need to update accordingly if the input changes
def test_apply_bmt_config_default_path(test_context):
    """apply_bmt_config loads the packaged BMT config when path is omitted."""
    apply_bmt_config(test_context)

    assert test_context.bmt["model_name"] == "MESSAGEix-GLOBIOM-GAINS 2.1-BMT-R12"
    assert test_context.macro == "macro_calibration_input_SSP2_bmt.xlsx"
    assert test_context.buildings.code == "R"
    assert test_context.transport.code == "SSP2"
    assert package_data_path("bmt", "config.yaml").is_file()
