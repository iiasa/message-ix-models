import logging
import re

import pytest
from sdmx.model.v21 import Annotation, Code

from message_ix_models.util.sdmx import eval_anno, read


def test_eval_anno(caplog):
    c = Code()

    assert None is eval_anno(c, "foo")

    c.annotations.append(Annotation(id="foo", text="bar baz"))

    with caplog.at_level(logging.DEBUG, logger="message_ix_models"):
        assert "bar baz" == eval_anno(c, "foo")

    assert re.fullmatch(
        r"Could not eval\('bar baz'\): .* \(<string>, line 1\)", caplog.messages[0]
    )

    c.annotations.append(Annotation(id="qux", text="3 + 4"))

    assert 7 == eval_anno(c, id="qux")


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
