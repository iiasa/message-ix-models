"""Tests of :mod:`.tools`.

.. todo:: As the module is expanded, split these tests to multiple files in a directory.
"""
from shutil import copyfile

import pandas as pd

from message_ix_models.tools import iea_web
from message_ix_models.util import package_data_path


def test_iea_web(test_context, tmp_path):
    # Store in the temporary directory for this test
    test_context.cache_path = tmp_path.joinpath("cache")

    result = iea_web.load_data()

    # Result has the correct type
    assert isinstance(result, pd.DataFrame)

    # print(result.head(1).transpose())  # DEBUG

    # Check the structure of the returned data
    assert {"flow", "commodity", "node", "year", "value", "unit", "flag"} == set(
        result.columns
    )


