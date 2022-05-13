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


def test_generate_code_lists(test_context, tmp_path):
    # Copy the data file to a temporary directory
    copyfile(package_data_path("iea", iea_web.FILE), tmp_path.joinpath(iea_web.FILE))

    # generate_code_lists() runs
    iea_web.generate_code_lists(tmp_path)
