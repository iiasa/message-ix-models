import logging
import re

import genno
import pytest
from genno import Computer, Key
from sdmx.model.common import Code
from sdmx.model.v21 import Annotation

from message_ix_models.model.transport import (
    build,
    # Ensure .util.sdmx.DATAFLOW is populated. This seems needed only for Python ≤ 3.9
    # TODO Remove once Python 3.9 is no longer supported
    data,  # noqa: F401
    testing,
)
from message_ix_models.util.sdmx import DATAFLOW, Dataflow, eval_anno, make_enum, read

log = logging.getLogger(__name__)


class TestDataflow:
    """Test :class:`.Dataflow."""

    @pytest.fixture
    def any_df(self):
        yield next(iter(DATAFLOW.values()))

    # TODO Use a broader-scoped context to allow (scope="class")
    @pytest.fixture
    def build_computer(self, test_context):
        """A :class:`.Computer` from :func:`.configure_build`.

        This in turn invokes :func:`.transport.build.add_exogenous_data`, which adds
        each of :data:`.FILES` to a Computer.
        """
        c, _ = testing.configure_build(test_context, regions="R12", years="B")
        yield c

    def test_init(self, caplog) -> None:
        # Message is logged for invalid units
        Dataflow(module=__name__, name="test_init_0", path="test-init-0", units="foo")
        assert "'foo' is not defined in the unit registry" in caplog.records[0].message

        # Exception is raised for duplicate definition
        with pytest.raises(
            RuntimeError, match="Definition of .*DF_TEST_INIT_0.*duplicates"
        ):
            Dataflow(
                module=__name__, name="test_init_0", path="test-init-0", units="foo"
            )

    def test_add_tasks(self, caplog, test_context) -> None:
        c = Computer()

        n, p = "test_add_tasks", "test-add-tasks"

        # FileNotFoundError is raised when adding to Computer with no file
        df0 = Dataflow(module=__name__, name=f"{n}0", path=f"{p}0", units="")
        with pytest.raises(FileNotFoundError):
            c.add("", df0, context=test_context)

        # With required=False, no exception, but also no keys added
        df1 = Dataflow(
            module=__name__, name=f"{n}1", path=f"{p}1", units="", required=False
        )
        result = c.add("", df1, context=test_context)
        assert () == result

    @build.get_computer.minimum_version
    @pytest.mark.parametrize(
        "file",
        [f for f in DATAFLOW.values() if f.intent & Dataflow.FLAG.IN],
        ids=lambda f: "-".join(f.path.parts),
    )
    def test_configure_build(
        self, build_computer: "Computer", file: "Dataflow"
    ) -> None:
        """Input data can be read and has the expected dimensions."""
        c = build_computer

        # Task runs
        result = c.get(file.key)

        # Quantity is loaded
        assert isinstance(result, genno.Quantity)

        # Dimensions are as expected
        assert set(Key(result).dims) == set(file.key.dims)

    def test_generate_csv_template(self, any_df: "Dataflow") -> None:
        with pytest.raises(NotImplementedError):
            any_df.generate_csv_template()

    def test_repr(self, any_df: "Dataflow") -> None:
        urn = (
            "urn:sdmx:org.sdmx.infomodel.datastructure.DataflowDefinition=IIASA_ECE:"
            "DF_FREIGHT_ACTIVITY(2025.3.11)"
        )
        assert (
            "<Dataflow wrapping "
            "'DataflowDefinition=IIASA_ECE:DF_FREIGHT_ACTIVITY(2025.3.11)'>"
            == repr(DATAFLOW[urn])
        )

    def test_required(self, any_df: "Dataflow") -> None:
        """The :`ExogenousDataFiles.required` property has a :class:`bool` value."""
        assert isinstance(any_df.required, bool)

    def test_units(self, any_df: "Dataflow") -> None:
        """The :`ExogenousDataFiles.units` property has a :class:`pint.Unit` value."""
        import pint

        assert isinstance(any_df.units, pint.Unit)


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
