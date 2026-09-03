import logging
from typing import TYPE_CHECKING

import pandas as pd
import pytest

from message_ix_models.testing.check import HasCoords, NoDuplicates

if TYPE_CHECKING:
    from message_ix_models.types import ParameterData


class TestNoDuplicates:
    @pytest.fixture
    def data(self) -> "ParameterData":
        """Test data with duplicated rows."""
        duplicated = pd.DataFrame(
            [
                ["x1", "y1", 1.0],
                ["x1", "y1", 2.0],
            ],
            columns=["x", "y", "value"],
        )
        return dict(input=pd.DataFrame(), output=duplicated)

    def test_run(self, caplog: pytest.LogCaptureFixture, data: "ParameterData") -> None:
        instance = NoDuplicates()

        with caplog.at_level(logging.DEBUG, NoDuplicates.__module__):
            result = instance.run(data)

        # Return values from the check are as expected
        assert False is result[0]
        assert """No duplicate indices in parameter data in 1/2 parameters

FAIL: 1 parameters
'output':
1 duplicated keys:""" == result[1]

        # Log messages contain further details
        assert "    x   y  value\n1  x1  y1    2.0" == caplog.messages[0]


def test_has_coords_subset():
    """Subset mode accepts only labels from the declared coordinate set."""
    check = HasCoords({"node": ["World", "KAZ"]}, subset=True)

    assert check.run(pd.DataFrame({"node": ["KAZ"], "value": [1.0]}))[0]
    result = check.run(pd.DataFrame({"node": ["XXX"], "value": [1.0]}))
    assert not result[0]
    assert "unexpected coords {'XXX'}" in result[1]

    with pytest.raises(ValueError, match="contradictory"):
        HasCoords({"node": []}, inverse=True, subset=True)


def test_has_coords_as_str():
    """Integer year labels match declared strings only when compared as strings."""
    coords = {"year": ["2020", "2030"]}
    df = pd.DataFrame({"year": [2020], "value": [1.0]})

    assert not HasCoords(coords, subset=True).run(df)[0]
    assert HasCoords(coords, subset=True, as_str=True).run(df)[0]
