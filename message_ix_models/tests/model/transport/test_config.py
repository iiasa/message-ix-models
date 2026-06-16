from collections.abc import Iterator

import pytest

from message_ix_models import Context
from message_ix_models.model.transport.config import (
    CL_SCENARIO,
    Config,
    iter_price_emission,
)
from message_ix_models.project.navigate import T35_POLICY
from message_ix_models.project.ssp import SSP_2017, SSP_2024

NAVIGATE = (
    ("", T35_POLICY.REF),
    ("act", T35_POLICY.ACT),
    ("ele", T35_POLICY.ELE),
    ("tec", T35_POLICY.TEC),
    ("act+ele+tec", T35_POLICY.ALL),
    pytest.param("foo+act+tec", None, marks=pytest.mark.xfail(raises=ValueError)),
)

SSP = (
    ("1", SSP_2017["1"]),
    ("2", SSP_2017["2"]),
    ("3", SSP_2017["3"]),
    ("4", SSP_2017["4"]),
    ("5", SSP_2017["5"]),
    (SSP_2024["2"], SSP_2024["2"]),
)


class TestConfig:
    @pytest.fixture
    def c(self) -> Iterator[Config]:
        yield Config()

    def test_fields(self, c: Config) -> None:
        """Settable class property included in :meth:`.ConfigHelper._fields`."""
        assert {"code"} & c._fields()

    @pytest.mark.parametrize("input, expected", SSP)
    def test_ssp0(self, input, expected):
        """Set SSP through the constructor."""
        c = Config(ssp=input)  # Call succeeds
        assert expected == c.ssp  # The expected enum value is stored

    @pytest.mark.parametrize("input, expected", SSP)
    def test_ssp1(self, c, input, expected):
        """Set SSP on an existing instance."""
        c.ssp = input
        assert expected == c.ssp

    @pytest.mark.parametrize("input, expected", NAVIGATE)
    def test_navigate_scenario0(self, input, expected):
        """Set NAVIGATE scenario through the constructor."""
        c = Config(navigate_scenario=input)
        assert expected == c.project["navigate"]

    @pytest.mark.parametrize("input, expected", NAVIGATE)
    def test_navigate_scenario1(self, c, input, expected):
        """Set NAVIGATE scenario on an existing instance."""
        c.set_navigate_scenario(input)
        assert expected == c.project["navigate"]


class TestCL_SCENARIO:
    def test_get(self, test_context: Context) -> None:
        result = CL_SCENARIO.get(force=True)

        # Code list has the expected length
        assert 386 == len(result)

        # Code list contains codes with the expected IDs
        assert {
            "DIGSY-BEST-C",
            "DIGSY-BEST-S",
            "DIGSY-WORST-C",
            "DIGSY-WORST-S",
            "EDITS-CA",
            "EDITS-HA",
            "LED-SSP1",
            "LED-SSP2",
            "SSP1 tax",
            "SSP1",
            "SSP2 tax",
            "SSP2",
            "SSP3 tax",
            "SSP3",
            "SSP4 tax",
            "SSP4",
            "SSP5 tax",
            "SSP5",
        } <= set(result.items.keys())

        # Codes for material-enabled scenarios are present
        c = result["M SSP2"]

        # Config created using these codes has the 'material' module enabled
        cfg = Config.from_context(test_context, dict(code=c))
        assert "material" in cfg.modules

        # Codes with policies discovered in the data dir are present
        c = result["LED-SSP2 exo price 1aa5"]
        c = result["M SSP2 exo price 2e17"]

        assert "SSP_SSP2_v5.3.1/SSP2 - Low Emissions#2" in str(
            c.get_annotation(id="policy").text
        )

        # Codes with project-specific scenario information can be inspected
        cfg = Config()
        cfg.code = result["DIGSY-BEST-C"]

        assert cfg.project_scenario_code is not None
        assert "BEST-C" == cfg.project_scenario_code.id
        assert cfg.project_scenario_code.parent is not None
        assert "CL_SCENARIO_DIGSY" == cfg.project_scenario_code.parent.id


@pytest.mark.parametrize(
    "ssp_or_led, N_exp",
    (
        ("SSP1", 10),
        ("SSP2", 27),
        ("SSP3", 3),
        ("SSP4", 7),
        ("SSP5", 10),
    ),
)
def test_iter_price_emission(ssp_or_led: str, N_exp: int, regions="R12") -> None:
    # Currently only data available for R12
    result = list(iter_price_emission(regions, ssp_or_led))
    assert N_exp == len(result)
