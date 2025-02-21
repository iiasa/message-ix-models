import logging
import re
from typing import TYPE_CHECKING

import pytest
import sdmx
from sdmx.model.common import Annotation, Code

from message_ix_models.util.sdmx import eval_anno, make_dataflow, make_enum, read

if TYPE_CHECKING:
    from message_ix_models.types import MaintainableArtefactArgs

log = logging.getLogger(__name__)


def test_eval_anno(caplog, recwarn):
    c = Code()

    with pytest.warns(DeprecationWarning):
        assert None is eval_anno(c, "foo")

    c.annotations.append(Annotation(id="foo", text="bar baz"))

    with (
        caplog.at_level(logging.DEBUG, logger="message_ix_models"),
        pytest.warns(DeprecationWarning),
    ):
        assert "bar baz" == eval_anno(c, "foo")

    assert re.fullmatch(
        r"Could not eval\('bar baz'\): .* \(<string>, line 1\)", caplog.messages[0]
    )

    c.annotations.append(Annotation(id="qux", text="3 + 4"))

    with pytest.warns(DeprecationWarning):
        assert 7 == eval_anno(c, id="qux")


@pytest.mark.parametrize(
    "id_, dims, name",
    (
        ("TEST", "t-c-e", None),
        ("GDP", "n-y", None),
        ("POPULATION", "n-y", None),
        ("TRANSPORT_ACTIVITY", "n-y-t", None),
        ("FE_TRANSPORT", "n-t-c", "Final energy use in transport"),
    ),
)
def test_make_dataflow(tmp_path, test_context, id_, dims, name) -> None:
    ma_kwargs: "MaintainableArtefactArgs" = dict()

    dims_tuple = tuple(dims.split("-"))
    sm = make_dataflow(id_, dims_tuple, name, ma_kwargs, test_context)

    # Message contains the expected items
    assert len(dims_tuple) == len(sm.codelist)  # One codelist per item
    assert {"CS_MESSAGE_IX_MODELS"} == set(sm.concept_scheme)
    assert {f"DF_{id_}"} == set(sm.dataflow)
    assert {f"DS_{id_}"} == set(sm.structure)

    path_out = tmp_path.joinpath("output.xml")
    path_out.write_bytes(sdmx.to_xml(sm, pretty_print=True))

    log.debug(path_out)


def test_make_enum0():
    """:func:`.make_enum` works with :class:`~enum.Flag` and subclasses."""
    from enum import Flag, IntFlag

    E = make_enum("ICONICS:SSP(2017)", base=Flag)

    # Values are bitwise flags
    assert not isinstance(E["1"], int)

    # Expected length
    assert 2 ** (len(E) - 1) == list(E)[-1].value

    # Flags can be combined
    flags = E["1"] | E["2"]
    assert E["1"] & flags
    assert E["2"] & flags
    assert not (E["3"] & flags)

    # Similar, with IntFlag
    E = make_enum("IIASA_ECE:AGENCIES(0.1)", base=IntFlag)

    # Values are ints
    assert isinstance(E["IIASA_ECE"], int)

    # Expected length
    assert 2 ** (len(E) - 1) == list(E)[-1].value


_urn_prefix = "urn:sdmx:org.sdmx.infomodel.codelist"


@pytest.mark.parametrize(
    "urn, expected",
    (
        ("ICONICS:SSP(2017)", f"{_urn_prefix}.Code=ICONICS:SSP(2017).1"),
        ("ICONICS:SSP(2024)", f"{_urn_prefix}.Code=ICONICS:SSP(2024).1"),
        ("SSP(2017)", f"{_urn_prefix}.Code=ICONICS:SSP(2017).1"),
        ("SSP(2024)", f"{_urn_prefix}.Code=ICONICS:SSP(2024).1"),
        ("SSP", f"{_urn_prefix}.Code=ICONICS:SSP(2017).1"),
        pytest.param(
            "AGENCIES",
            f"{_urn_prefix}.Agency=IIASA_ECE:AGENCIES(0.1).IEA",
            marks=pytest.mark.xfail(raises=KeyError, reason="XML needs update"),
        ),
    ),
)
def test_make_enum1(urn, expected):
    # make_enum() runs
    E = make_enum(urn)

    # A known URN retrieves an enumeration member
    E.by_urn(expected)


@pytest.mark.parametrize(
    "urn, expected",
    (
        ("ICONICS:SSP(2017)", "Codelist=ICONICS:SSP(2017)"),
        ("ICONICS:SSP(2024)", "Codelist=ICONICS:SSP(2024)"),
        ("SSP(2017)", "Codelist=ICONICS:SSP(2017)"),
        ("SSP(2024)", "Codelist=ICONICS:SSP(2024)"),
        ("SSP", "Codelist=ICONICS:SSP(2017)"),
        ("AGENCIES", "AgencyScheme=IIASA_ECE:AGENCIES(0.1)"),
    ),
)
def test_read0(urn, expected):
    obj = read(urn)
    assert expected in obj.urn


def test_read1():
    SSPS = read("ssp")

    # Identify an SSP by matching strings in its name
    code0 = next(filter(lambda c: "2" in repr(c), iter(SSPS)))
    code1 = next(filter(lambda c: "SSP2" in repr(c), iter(SSPS)))
    code2 = next(filter(lambda c: "middle of the road" in repr(c).lower(), iter(SSPS)))

    assert code0 is code1 is code2

    with pytest.raises(FileNotFoundError):
        read("foo")
