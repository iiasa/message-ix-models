"""Tests of :mod:`.tools`."""
from shutil import copyfile

import pandas as pd
import pytest
from genno import Computer

from message_ix_models.tools.exo_data import prepare_computer
from message_ix_models.tools.iea.web import (
    DIMS,
    FILES,
    fuzz_data,
    generate_code_lists,
    load_data,
)
from message_ix_models.util import package_data_path


class TestIEA_EWEB:
    @pytest.mark.parametrize("source", ("IEA_EWEB",))
    @pytest.mark.parametrize(
        "source_kw",
        (
            dict(
                provider="OECD", edition="2021", product=["CHARCOAL"], flow=["RESIDENT"]
            ),
            # All flows related to transport
            dict(
                provider="OECD",
                edition="2022",
                flow=[
                    "DOMESAIR",
                    "DOMESNAV",
                    "PIPELINE",
                    "RAIL",
                    "ROAD",
                    "TOTTRANS",
                    "TRNONSPE",
                    "WORLDAV",
                    "WORLDMAR",
                ],
            ),
        ),
    )
    def test_prepare_computer(self, test_context, source, source_kw):
        # FIXME The following should be redundant, but appears mutable on GHA linux and
        #       Windows runners.
        test_context.model.regions = "R14"

        c = Computer()

        keys = prepare_computer(test_context, c, source, source_kw)

        # Preparation of data runs successfully
        result = c.get(keys[0])
        # print(result.to_string())

        # Data has the expected dimensions
        assert {"n", "y", "product", "flow"} == set(result.dims)

        # Data is complete
        assert 14 == len(result.coords["n"])
        assert {1980, 2020} < set(result.coords["y"].data)


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
