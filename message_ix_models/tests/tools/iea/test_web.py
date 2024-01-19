"""Tests of :mod:`.tools`."""
from shutil import copyfile

import pandas as pd

from message_ix_models.tools.iea.web import (
    FILE,
    fuzz_data,
    generate_code_lists,
    load_data,
)
from message_ix_models.util import package_data_path


def test_iea_web(test_context, tmp_path):
    # Store in the temporary directory for this test
    test_context.cache_path = tmp_path.joinpath("cache")

    result = load_data()

    # Result has the correct type
    assert isinstance(result, pd.DataFrame)

    # print(result.head(1).transpose())  # DEBUG

    # Check the structure of the returned data
    assert {"flow", "commodity", "node", "year", "value", "unit", "flag"} == set(
        result.columns
    )


def test_generate_code_lists(test_context, tmp_path):
    # Copy the data file to a temporary directory
    copyfile(package_data_path("iea", FILE), tmp_path.joinpath(FILE))

    # generate_code_lists() runs
    generate_code_lists(tmp_path)


def test_fuzz_data(test_context, tmp_path):
    # fuzz_data() runs
    fuzz_data(target_path=tmp_path)
