import logging
from typing import TYPE_CHECKING

import pandas as pd
import pytest

from message_ix_models.testing.check import NoDuplicates

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
