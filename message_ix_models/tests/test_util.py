import logging
from pathlib import Path

import pytest

from message_ix_models.util import (
    MESSAGE_DATA_PATH,
    MESSAGE_MODELS_PATH,
    as_codes,
    load_package_data,
    load_private_data,
    package_data_path,
    private_data_path,
)

_actual_package_data = Path(__file__).parents[1].joinpath("data")


def test_as_codes():
    """Forward reference to a child is silently dropped."""
    data = dict(
        foo=dict(child=["bar"]),
        bar=dict(name="Bar!"),
    )
    result = as_codes(data)
    assert result[1] not in result[0].child


@pytest.mark.parametrize(
    "data",
    (
        set(),
        # dict() with a value that is not a str or a further dict()
        dict(foo="foo", bar=[1, 2, 3]),
    ),
)
def test_as_codes_invalid(data):
    """as_codes() rejects invalid data."""
    with pytest.raises(TypeError):
        as_codes(data)


@pytest.mark.parametrize("path", _actual_package_data.rglob("*.yaml"))
def test_load_package_data(path):
    """Existing package data can be loaded."""
    load_package_data(*path.relative_to(_actual_package_data).parts)


def test_load_package_data_twice(caplog):
    """Loading the same data twice logs a message."""
    caplog.set_level(logging.DEBUG)
    load_package_data("node", "R11")
    load_package_data("node", "R11")
    assert "'node R11' already loaded; skip" in caplog.messages


def test_load_package_data_invalid():
    """load_package_data() raises an exception for an unsupported file type."""
    with pytest.raises(ValueError):
        load_package_data("test.xml")


@pytest.mark.xfail(
    condition=MESSAGE_DATA_PATH is None, reason="Requires message_data to be installed."
)
def test_load_private_data(*parts, suffix=None):
    load_private_data("sources.yaml")


def test_package_data_path(*parts, suffix=None):
    assert MESSAGE_MODELS_PATH.joinpath("data", "foo", "bar") == package_data_path(
        "foo", "bar"
    )


@pytest.mark.xfail(
    condition=MESSAGE_DATA_PATH is None, reason="Requires message_data to be installed."
)
def test_private_data_path(*parts, suffix=None):
    assert MESSAGE_DATA_PATH.joinpath("data", "foo", "bar") == private_data_path(
        "foo", "bar"
    )
