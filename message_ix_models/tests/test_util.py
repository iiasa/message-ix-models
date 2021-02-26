import logging
from pathlib import Path

import pytest

from message_ix_models.util import as_codes, load_package_data

package_data_path = Path(__file__).parents[1].joinpath("data")


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


@pytest.mark.parametrize("path", package_data_path.rglob("*.yaml"))
def test_load_package_data(path):
    """Existing package data can be loaded."""
    load_package_data(*path.relative_to(package_data_path).parts)


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
