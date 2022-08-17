from shutil import copyfile

import pytest

from message_ix_models.tools.advance import (
    DIMS,
    LOCATION,
    _fuzz_data,
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


def test_fuzz_data(session_context):
    """Test that :func:`_fuzz_data` runs successfully.

    NB this only produces a file in the :mod:`pytest` temporary directory. To update
    the test specimen in the package directory that is used by
    :func:`test_get_advance_data`, see the body of _fuzz_data.
    """
    # size argument should be a fraction (~= 0.1) of the size of the test specimen
    _fuzz_data(
        size=50, include=[("Transport|Service demand|Road|Freight", "billion tkm/yr")]
    )
