"""Tests of :mod:`.tools`."""

import logging
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from genno import Computer

from message_ix_models.testing import GHA, MARK
from message_ix_models.tools.iea.web import (
    DIMS,
    IEA_EWEB,
    TRANSFORM,
    generate_code_lists,
    get_mapping,
    load_data,
)
from message_ix_models.util import HAS_MESSAGE_DATA

if TYPE_CHECKING:
    from message_ix_models import Context

log = logging.getLogger(__name__)


_FLOW = [
    "DOMESAIR",
    "DOMESNAV",
    "PIPELINE",
    "RAIL",
    "ROAD",
    "TOTTRANS",
    "TRNONSPE",
    "WORLDAV",
    "WORLDMAR",
]


@IEA_EWEB.transform.minimum_version
class TestIEA_EWEB:
    @pytest.mark.usefixtures("iea_eweb_test_data")
    @pytest.mark.parametrize(
        "source_kw",
        (
            dict(
                provider="OECD", edition="2021", product=["CHARCOAL"], flow=["RESIDENT"]
            ),
            # All flows related to transport
            dict(provider="OECD", edition="2022", flow=_FLOW),
            pytest.param(
                dict(provider="IEA", edition="2023", extra_kw="FOO"),
                marks=pytest.mark.xfail(raises=TypeError),
            ),
            dict(provider="IEA", edition="2024", flow=["AVBUNK"]),
            pytest.param(
                dict(provider="IEA", edition="2024", transform="B"),
                marks=pytest.mark.xfail(
                    raises=ValueError, reason="Missing regions= kwarg"
                ),
            ),
            dict(provider="IEA", edition="2024", transform="B", regions="R12"),
            dict(
                provider="IEA",
                edition="2024",
                flow=["AVBUNK"] + _FLOW,
                transform=TRANSFORM.B | TRANSFORM.C,
                regions="R12",
            ),
        ),
    )
    def test_add_tasks(self, test_context: "Context", source_kw: dict) -> None:
        # FIXME The following should be redundant, but appears mutable on GHA linux and
        #       Windows runners.
        test_context.model.regions = "R14"

        c = Computer()

        keys = IEA_EWEB.add_tasks(c, context=test_context, **source_kw)

        # Preparation of data runs successfully
        result = c.get(keys[0])
        # print(result.to_string())

        # Data has the expected dimensions
        assert {"n", "y", "product", "flow"} == set(result.dims)

        # Data contain expected coordinates
        # NB cannot test total counts here because the fuzzed test data does not
        #    necessarily include ≥1 data point from each (n, y)
        n = source_kw.get("regions", "R14")
        assert {f"{n}_AFR", f"{n}_WEU"} < set(result.coords["n"].data)
        assert {1980, 2018} < set(result.coords["y"].data)


class TestTRANSFORM:
    def test_from_value(self) -> None:
        assert TRANSFORM.from_value() is TRANSFORM.DEFAULT
        assert TRANSFORM.from_value(None) is TRANSFORM.DEFAULT
        assert TRANSFORM.from_value(TRANSFORM.DEFAULT) is TRANSFORM.DEFAULT
        assert TRANSFORM.from_value("B") is TRANSFORM.B

        with pytest.raises(KeyError):
            TRANSFORM.from_value("D")

    def test_is_valid(self, caplog) -> None:
        with pytest.raises(ValueError):
            (TRANSFORM["A"] | TRANSFORM["B"]).is_valid()

        assert (TRANSFORM["A"] | TRANSFORM["B"]).is_valid(fail="log") is False
        assert caplog.messages[-1].endswith("mutually exclusive")

        assert (TRANSFORM["A"] | TRANSFORM["C"]).is_valid() is True
        assert (TRANSFORM["B"] | TRANSFORM["C"]).is_valid() is True

        assert TRANSFORM["C"].is_valid(fail="log") is False
        assert " at least one of " in caplog.messages[-1]

    def test_values(self) -> None:
        """Test ordinary behaviours of enum.Flag."""
        TRANSFORM["A"] == TRANSFORM.A == 1
        TRANSFORM["B"] == TRANSFORM.B == 2
        TRANSFORM["C"] == TRANSFORM.C == 4

        v1 = TRANSFORM.A | TRANSFORM.C
        assert TRANSFORM.A & v1
        assert not TRANSFORM.B & v1
        assert TRANSFORM.C & v1

        v2 = TRANSFORM.C
        assert not TRANSFORM.A & v2
        assert not TRANSFORM.B & v2
        assert TRANSFORM.C & v2


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


@MARK["#375"]
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


@pytest.mark.usefixtures("iea_eweb_test_data")
@pytest.mark.parametrize("provider, edition", PROVIDER_EDITION)
def test_generate_code_lists(tmp_path, provider, edition):
    """:func:`.generate_code_lists` runs."""
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
