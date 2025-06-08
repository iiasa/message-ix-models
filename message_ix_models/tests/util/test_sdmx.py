import logging
import re
import sys

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
from message_ix_models.util.sdmx import (
    DATAFLOW,
    Dataflow,
    ItemSchemeEnumType,
    URNLookupEnum,
    eval_anno,
    read,
)

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
    @testing.MARK[10]
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


_urn_prefix = "urn:sdmx:org.sdmx.infomodel"


class TestItemSchemeEnum:
    @pytest.mark.parametrize(
        "urn, expected",
        (
            ("ICONICS:SSP(2017)", f"{_urn_prefix}.codelist.Code=ICONICS:SSP(2017).1"),
            ("ICONICS:SSP(2024)", f"{_urn_prefix}.codelist.Code=ICONICS:SSP(2024).1"),
            ("SSP(2017)", f"{_urn_prefix}.codelist.Code=ICONICS:SSP(2017).1"),
            ("SSP(2024)", f"{_urn_prefix}.codelist.Code=ICONICS:SSP(2024).1"),
            ("SSP", f"{_urn_prefix}.codelist.Code=ICONICS:SSP(2017).1"),
            ("AGENCIES", f"{_urn_prefix}.base.Agency=IIASA_ECE:AGENCIES(0.1).IEA"),
        ),
    )
    def test_new_class(self, urn: str, expected: str) -> None:
        class Foo(URNLookupEnum, metaclass=ItemSchemeEnumType):
            def _get_item_scheme(self):
                return read(urn)

        # A known URN retrieves an enumeration member
        f = Foo.by_urn(expected)
        assert isinstance(f, Foo)

    def test_bases(self) -> None:
        """:func:`.make_enum` works with :class:`~enum.Flag` and subclasses."""
        from enum import Flag, IntFlag

        class E1(Flag, metaclass=ItemSchemeEnumType):
            def _get_item_scheme(self):
                return read("ICONICS:SSP(2017)")

        # Values are bitwise flags
        assert not isinstance(E1["1"], int)

        def _exp_max_value(cls) -> int:
            """Expected maximum value.

            Currently the NONE value counts towards len(cls) with Python 3.9, but not
            with Python 3.13. It's unclear why.
            """
            L = len(cls) - 1 - (0 if sys.version_info >= (3, 10) else 1)
            return 2**L

        # Expected maximum value
        assert _exp_max_value(E1) == max(member.value for member in E1)

        # Flags can be combined
        flags = E1["1"] | E1["2"]
        assert E1["1"] & flags
        assert E1["2"] & flags
        assert not (E1["3"] & flags)

        # Similar, with IntFlag
        class E2(IntFlag, metaclass=ItemSchemeEnumType):
            def _get_item_scheme(self):
                return read("IIASA_ECE:AGENCIES(0.1)")

        # Values are ints
        assert isinstance(E2["IIASA_ECE"], int)

        # Expected maximum value
        assert _exp_max_value(E2) == max(member.value for member in E2)


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
    "urn, expected",
    (
        ("ICONICS:SSP(2017)", "Codelist=ICONICS:SSP(2017)"),
        ("ICONICS:SSP(2024)", "Codelist=ICONICS:SSP(2024)"),
        ("SSP(2017)", "Codelist=ICONICS:SSP(2017)"),
        ("SSP(2024)", "Codelist=ICONICS:SSP(2024)"),
        ("SSP", "Codelist=ICONICS:SSP(2017)"),
        ("AGENCIES", "AgencyScheme=IIASA_ECE:AGENCIES(0.1)"),
        ("IIASA_ECE:AGENCIES", "AgencyScheme=IIASA_ECE:AGENCIES(0.1)"),
        ("IIASA_ECE:AGENCIES(0.1)", "AgencyScheme=IIASA_ECE:AGENCIES(0.1)"),
    ),
)
def test_read0(urn: str, expected: str) -> None:
    obj = read(urn)
    assert expected in obj.urn


def test_read1() -> None:
    SSPS = read("ssp")

    # Identify an SSP by matching strings in its name
    code0 = next(filter(lambda c: "2" in repr(c), iter(SSPS)))
    code1 = next(filter(lambda c: "SSP2" in repr(c), iter(SSPS)))
    code2 = next(filter(lambda c: "middle of the road" in repr(c).lower(), iter(SSPS)))

    assert code0 is code1 is code2

    with pytest.raises(FileNotFoundError):
        read("foo")
