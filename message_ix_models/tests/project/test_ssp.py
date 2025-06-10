from typing import TYPE_CHECKING

import pytest
from genno import ComputationError, Computer

from message_ix_models.project.ssp import (
    SSP,
    SSP_2017,
    SSP_2024,
    generate,
    parse,
    ssp_field,
)
from message_ix_models.project.ssp.data import SSPOriginal, SSPUpdate

if TYPE_CHECKING:
    from message_ix_models import Context


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
    # NB Ignored because of https://github.com/python/mypy/issues/7568
    assert SSP["1"] != SSP_2024["1"]  # type: ignore [misc]


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
    @pytest.mark.usefixtures("ssp_test_data")
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
                marks=pytest.mark.xfail(raises=TypeError),
            ),
        ),
    )
    def test_add_tasks(self, test_context: "Context", source, source_kw: dict) -> None:
        # FIXME The following should be redundant, but appears mutable on GHA linux and
        #       Windows runners.
        test_context.model.regions = "R14"

        c = Computer()

        keys = SSPOriginal.add_tasks(
            c, context=test_context, source=source, **source_kw
        )

        # Preparation of data runs successfully
        result = c.get(keys[0])

        # Data has the expected dimensions
        assert ("n", "y") == result.dims

        # Data is complete
        assert 14 == len(result.coords["n"])
        assert 14 == len(result.coords["y"])


class TestSSPUpdate:
    @pytest.mark.usefixtures(
        "ssp_test_data",  # For release=preview
        "ssp_user_data",  # For other values of `release`
    )
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
        "release, measure, model",
        (
            ("preview", "GDP", "OECD ENV-Growth 2023"),
            ("preview", "GDP", "IIASA GDP 2023"),
            ("preview", "POP", ""),
            ("3.0", "GDP", "OECD ENV-Growth 2023"),
            ("3.0", "GDP", "IIASA GDP 2023"),
            ("3.0", "POP", ""),
            ("3.0.1", "GDP", "OECD ENV-Growth 2023"),
            ("3.0.1", "GDP", "IIASA GDP 2023"),
            ("3.0.1", "POP", ""),
            ("3.1", "GDP", "OECD ENV-Growth 2023"),
            ("3.1", "GDP", "IIASA GDP 2023"),
            ("3.1", "POP", ""),
            ("3.2.beta", "GDP", "OECD ENV-Growth 2025"),
            pytest.param(
                "3.2.beta",
                "GDP",
                "IIASA GDP 2025",
                marks=pytest.mark.xfail(raises=ComputationError, reason="No data"),
            ),
            ("3.2.beta", "POP", ""),
        ),
    )
    def test_add_tasks(
        self,
        test_context: "Context",
        source: str,
        release: str,
        measure: str,
        model: str,
    ) -> None:
        # FIXME The following should be redundant, but appears mutable on GHA linux and
        #       Windows runners.
        test_context.model.regions = "R14"

        # Prepare source_kw
        source_kw = dict(release=release, measure=measure)
        if model:
            source_kw.update(model=model)
        if release in ("3.2.beta",) and measure == "GDP":
            # Disambiguate units for this release
            source_kw["unit"] = "billion USD_2017/yr"

        c = Computer()

        keys = SSPUpdate.add_tasks(
            c, context=test_context, strict=True, source=source, **source_kw
        )

        # Preparation of data runs successfully
        result = c.get(keys[0])

        # Data has the expected dimensions
        assert ("n", "y") == result.dims

        # Data is complete
        assert 14 == len(result.coords["n"])
        assert 14 == len(result.coords["y"])

        if release == "preview":
            return  # Fuzzed/random data, not meaningful

        # Check for apparent double-counting: if in 2025, values will be at least twice
        # the 2020 values; if in 2020, roughly the opposite

        ratio = (result.sel(y=2025) / result.sel(y=2020)).to_series()
        check = (ratio < 0.55) | (1.9 < ratio)

        assert not check.any(), f"Possible double-counting:\n{ratio[check].to_string()}"
