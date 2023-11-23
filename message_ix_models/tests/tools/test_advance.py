from shutil import copyfile

import pytest

from message_ix_models.tools.advance import (
    DIMS,
    LOCATION,
    advance_data,
    get_advance_data,
)
from message_ix_models.util import package_data_path


@pytest.fixture(scope="module")
def advance_test_data(session_context):
    # Copy test data from the package directory into the local data directory for
    # `test_context`. get_advance_data() only uses this file if :mod:`message_data` is
    # NOT installed.
    target = session_context.get_local_path(*LOCATION)
    target.parent.mkdir(parents=True)
    copyfile(package_data_path("test", *LOCATION), target)


pytestmark = pytest.mark.usefixtures("advance_test_data")


def test_get_advance_data(session_context):
    """Test :func:`.get_advance_data`."""
    # Returns a pd.Series with the expected index levels
    result = get_advance_data()
    assert DIMS == result.index.names

    # Returns a genno.Quantity with the expected units
    result = advance_data("Transport|Service demand|Road|Freight")
    assert {"[length]": 1, "[mass]": 1, "[time]": -1} == result.units.dimensionality
