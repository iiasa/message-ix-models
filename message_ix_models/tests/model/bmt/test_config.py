"""Tests for :mod:`message_ix_models.model.bmt.config`."""

from collections.abc import Iterator
from pathlib import Path

import pytest

from message_ix_models import Context
from message_ix_models.model.bmt.config import apply_bmt_config
from message_ix_models.model.buildings.config import METHOD
from message_ix_models.model.transport.config import Config as TransportConfig


@pytest.fixture
def custom_config_path(tmp_path: Path) -> Iterator[Path]:
    """Path to a temporary file containing custom BMT configuration."""
    path = tmp_path / "config.yaml"
    path.write_text(
        """model_name: "TEST-MODEL"

buildings:
  code: "R"
  with_materials: true

macro: "custom_macro.xlsx"

transport:
  code: "SSP2"
"""
    )
    yield path


def test_apply_bmt_config_custom_path(
    test_context: Context, custom_config_path: Path
) -> None:
    """:func:`apply_bmt_config` loads YAML into Context attributes."""
    apply_bmt_config(test_context, path=custom_config_path)

    assert test_context.bmt["model_name"] == "TEST-MODEL"
    assert test_context.macro == "custom_macro.xlsx"
    assert test_context.buildings.code == "R"
    assert test_context.buildings.with_materials is True
    assert test_context.buildings.method is METHOD.B
    assert test_context.buildings.sturm_scenario == "NONE"
    assert isinstance(test_context.transport, TransportConfig)
    assert test_context.transport.code == "SSP2"


def test_apply_bmt_config_default_path(test_context: Context) -> None:
    """:func:`apply_bmt_config` loads the packaged BMT config when path is omitted.

    .. note:: Update these assertions if the contents of the default file change.
    """
    apply_bmt_config(test_context)

    assert test_context.bmt["model_name"] == "MESSAGEix-GLOBIOM-GAINS 2.1-BMT-R12"
    assert test_context.macro == "macro_calibration_input_SSP2_bmt.xlsx"
    assert test_context.buildings.code == "R"
    assert test_context.transport.code == "SSP2"
