"""Tests of :mod:`.tools`."""
from shutil import copyfile

import pandas as pd
import pytest

from message_ix_models.tools.iea.web import (
    DIMS,
    FILES,
    fuzz_data,
    generate_code_lists,
    load_data,
)
from message_ix_models.util import package_data_path


@pytest.mark.parametrize("provider, edition", FILES.keys())
def test_load_data(test_context, tmp_path, provider, edition):
    # # Store in the temporary directory for this test
    # test_context.cache_path = tmp_path.joinpath("cache")

    result = load_data(provider, edition)

    # Result has the correct type
    assert isinstance(result, pd.DataFrame)

    # print(result.head().to_string())  # DEBUG
    # print(result.head(1).transpose())  # DEBUG

    # Data have the expected dimensions
    assert (set(DIMS) & {"Value"}) < set(result.columns)


@pytest.mark.skip(reason="Refactoring")
def test_generate_code_lists(test_context, tmp_path):
    # Copy the data file to a temporary directory
    copyfile(package_data_path("iea", FILES), tmp_path.joinpath(FILES))

    # generate_code_lists() runs
    generate_code_lists(tmp_path)


@pytest.mark.skip(reason="Refactoring")
def test_fuzz_data(test_context, tmp_path):
    # fuzz_data() runs
    fuzz_data(target_path=tmp_path)
