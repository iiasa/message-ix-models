from copy import copy

import pytest
from genno import Computer

from message_ix_models.project.ssp import (
    SSP,
    SSP_2017,
    SSP_2024,
    generate,
    parse,
    ssp_field,
)
from message_ix_models.project.ssp.data import SSPUpdate  # noqa: F401
from message_ix_models.tools.exo_data import prepare_computer


def test_generate(tmp_path, test_context):
    # Function runs
    generate(test_context, base_dir=tmp_path)

    # Two XML files are created
    assert 2 == len(list(tmp_path.glob("*.xml")))


def test_enum():
    # Enumerations have the expected length
    assert 5 == len(SSP_2017)
    assert 5 == len(SSP_2024)

    # Members can be accessed by ID
    a = SSP_2017["1"]

    # …or by value
    b = SSP_2017(1)

    # …all retrieving the same member
    assert a == b

    # __getattr__ lookup does not invoke _missing_
    with pytest.raises(KeyError):
        SSP_2017[1]

    # Same SSP ID from different enums are not equivalent
    assert SSP_2017["1"] != SSP_2024["1"]
    assert SSP_2017["1"] is not SSP_2024["1"]
    assert SSP["1"] != SSP_2024["1"]


@pytest.mark.parametrize(
    "expected, value",
    (
        (SSP_2017["1"], SSP_2017["1"]),  # Literal value
        (SSP_2017["1"], "1"),  # String ID as appears in the codelist
        (SSP_2017["1"], "SSP1"),  # Prefixed by "SSP"
        (SSP_2017["1"], 1),  # Integer
    ),
)
def test_parse(value, expected):
    assert expected == parse(value)


def test_ssp_field() -> None:
    from dataclasses import dataclass

    @dataclass
    class Foo:
        bar: ssp_field = ssp_field(default=SSP_2017["1"])
        baz: ssp_field = ssp_field(default=SSP_2024["5"])

    # Can be instantiated with no arguments
    f = Foo()
    assert SSP_2017["1"] is f.bar
    assert SSP_2024["5"] is f.baz

    # Can be instantiated with different values
    f = Foo(bar=SSP_2017["3"], baz=SSP_2024["3"])
    assert SSP_2017["3"] is f.bar
    assert SSP_2024["3"] is f.baz

    # Values can be set and are passed through parse()
    f.bar = "SSP2"
    f.baz = "SSP4"

    assert SSP_2017["2"] is f.bar
    assert SSP_2017["4"] is f.baz


def test_cli(mix_models_cli):
    mix_models_cli.assert_exit_0(["ssp", "gen-structures", "--dry-run"])


class TestSSPOriginal:
    @pytest.mark.parametrize(
        "source",
        (
            "ICONICS:SSP(2017).1",
            "ICONICS:SSP(2017).2",
            "ICONICS:SSP(2017).3",
            "ICONICS:SSP(2017).4",
            "ICONICS:SSP(2017).5",
        ),
    )
    @pytest.mark.parametrize(
        "source_kw",
        (
            dict(measure="POP", model="OECD Env-Growth"),
            dict(measure="GDP", model="OECD Env-Growth"),
            # Excess keyword arguments
            pytest.param(
                dict(measure="GDP", model="OECD Env-Growth", foo="bar"),
                marks=pytest.mark.xfail(raises=ValueError),
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

        # Data has the expected dimensions
        assert ("n", "y") == result.dims

        # Data is complete
        assert 14 == len(result.coords["n"])
        assert 14 == len(result.coords["y"])


class TestSSPUpdate:
    @pytest.mark.parametrize(
        "source",
        (
            "ICONICS:SSP(2024).1",
            "ICONICS:SSP(2024).2",
            "ICONICS:SSP(2024).3",
            "ICONICS:SSP(2024).4",
            "ICONICS:SSP(2024).5",
        ),
    )
    @pytest.mark.parametrize(
        "source_kw",
        (
            dict(measure="POP"),
            dict(measure="GDP", model="IIASA GDP 2023"),
            dict(measure="GDP", model="OECD ENV-Growth 2023"),
            # Excess keyword arguments
            pytest.param(
                dict(measure="POP", foo="bar"),
                marks=pytest.mark.xfail(raises=ValueError),
            ),
        ),
    )
    @pytest.mark.parametrize("release", ("preview", "3.0", "3.0.1"))
    def test_prepare_computer(self, test_context, source, source_kw, release):
        # FIXME The following should be redundant, but appears mutable on GHA linux and
        #       Windows runners.
        test_context.model.regions = "R14"

        # Set the release
        source_kw = copy(source_kw)
        source_kw.update(release=release)

        c = Computer()

        keys = prepare_computer(test_context, c, source, source_kw)

        # Preparation of data runs successfully
        result = c.get(keys[0])

        # Data has the expected dimensions
        assert ("n", "y") == result.dims

        # Data is complete
        assert 14 == len(result.coords["n"])
        assert 14 == len(result.coords["y"])
