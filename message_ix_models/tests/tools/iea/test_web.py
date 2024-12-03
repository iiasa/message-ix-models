"""Tests of :mod:`.tools`."""

from importlib.metadata import version

import pandas as pd
import pytest
from genno import Computer
from packaging.version import parse

from message_ix_models.testing import GHA
from message_ix_models.tools.exo_data import prepare_computer
from message_ix_models.tools.iea.web import (
    DIMS,
    generate_code_lists,
    get_mapping,
    load_data,
)
from message_ix_models.util import HAS_MESSAGE_DATA

# Dask < 2024.4.1 is incompatible with Python >= 3.11.9, but we pin dask in this range
# for tests of message_ix < 3.7.0. Skip these tests:
MARK_DASK_PYTHON = pytest.mark.skipif(
    condition=parse(version("message_ix")) < parse("3.7.0"),
    reason="Pinned dask version and certain Python versions are incompatible",
)


class TestIEA_EWEB:
    @MARK_DASK_PYTHON
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
            pytest.param(
                dict(provider="IEA", edition="2023", extra_kw="FOO"),
                marks=pytest.mark.xfail(raises=ValueError),
            ),
            dict(provider="IEA", edition="2024", flow=["AVBUNK"]),
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

        # Data contain expected coordinates
        # NB cannot test total counts here because the fuzzed test data does not
        #    necessarily include ≥1 data point from each COUNTRY and TIME
        assert {"R14_AFR", "R14_WEU"} < set(result.coords["n"].data)
        assert {1980, 2018} < set(result.coords["y"].data)


# NB once there is a fuzzed version of the (IEA, 2023) data available, usage of this
#    variable can be replaced with list(FILES.keys())
PROVIDER_EDITION = (
    pytest.param(
        "IEA",
        "2023",
        marks=pytest.mark.xfail(
            GHA or not HAS_MESSAGE_DATA, reason="No fuzzed version of this data"
        ),
    ),
    ("IEA", "2024"),
    ("OECD", "2021"),
    ("OECD", "2022"),
    ("OECD", "2023"),
)


@MARK_DASK_PYTHON
@pytest.mark.parametrize("provider, edition", PROVIDER_EDITION)
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


@MARK_DASK_PYTHON
@pytest.mark.parametrize("provider, edition", PROVIDER_EDITION)
def test_generate_code_lists(tmp_path, provider, edition):
    # generate_code_lists() runs
    generate_code_lists(provider, edition, tmp_path)


@pytest.mark.parametrize("provider, edition", PROVIDER_EDITION)
def test_get_mapping(provider, edition) -> None:
    # MappingAdapter can be generated
    result = get_mapping(provider, edition)

    # Only "COUNTRY" labels are mapped
    assert {"n"} == set(result.maps)

    # Expected number of values are mapped
    assert {
        ("IEA", "2023"): 191,
        ("IEA", "2024"): 191,
        ("OECD", "2021"): 190,
        ("OECD", "2022"): 185,
        ("OECD", "2023"): 191,
    }[(provider, edition)] == len(result.maps["n"])


@pytest.mark.parametrize(
    "urn, N",
    (
        ("IEA:COUNTRY_IEA(2023)", 191),
        ("IEA:COUNTRY_IEA(2024)", 191),
        ("IEA:COUNTRY_OECD(2021)", 190),
        ("IEA:COUNTRY_OECD(2022)", 185),
        ("IEA:COUNTRY_OECD(2023)", 191),
        ("IEA:FLOW_IEA(2023)", 108),
        ("IEA:FLOW_IEA(2024)", 108),
        ("IEA:FLOW_OECD(2021)", 108),
        ("IEA:FLOW_OECD(2022)", 108),
        ("IEA:FLOW_OECD(2023)", 108),
        ("IEA:PRODUCT_IEA(2023)", 68),
        ("IEA:PRODUCT_IEA(2024)", 68),
        ("IEA:PRODUCT_OECD(2021)", 68),
        ("IEA:PRODUCT_OECD(2022)", 68),
        ("IEA:PRODUCT_OECD(2023)", 68),
    ),
)
def test_load_codelists(urn, N):
    from message_ix_models.util.sdmx import read

    # Code list can be read using its URN
    cl = read(urn)

    # Code list has the expected number of codes
    assert N == len(cl)
