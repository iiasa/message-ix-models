import pytest

from message_ix_models import Context
from message_ix_models.model.transport.config import (
    CL_SCENARIO,
    Config,
    iter_price_emission,
)
from message_ix_models.project.navigate import T35_POLICY
from message_ix_models.project.ssp import SSP_2017, SSP_2024
from message_ix_models.project.transport_futures import SCENARIO as TF_SCENARIO

FUTURES = (
    ("", TF_SCENARIO.BASE),
    ("base", TF_SCENARIO.BASE),
    ("A---", TF_SCENARIO.A___),
    ("debug", TF_SCENARIO.DEBUG),
    pytest.param("foo", None, marks=pytest.mark.xfail(raises=ValueError)),
)

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
    def c(self):
        yield Config()

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

    @pytest.mark.parametrize("input, expected", FUTURES)
    def test_futures_scenario0(self, input, expected):
        """Set Transport Futures scenario through the constructor."""
        c = Config(futures_scenario=input)  # Call succeeds
        assert expected == c.project["futures"]  # The expected enum value is set

    @pytest.mark.parametrize("input, expected", FUTURES)
    def test_futures_scenario1(self, c, input, expected):
        """Set Transport Futures scenario on an existing instance."""
        c.set_futures_scenario(input)
        assert expected == c.project["futures"]

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

    def test_scenario_conflict(self):
        # Giving both raises an exception
        at = "(ACT|TEC)"  # Order differs in Python 3.9
        expr = rf"SCENARIO.A___ and T35_POLICY.{at}\|{at} are not compatible"
        with pytest.raises(ValueError, match=expr):
            c = Config(futures_scenario="A---", navigate_scenario="act+tec")

        # Also a conflict
        c = Config(navigate_scenario="act+tec")
        with pytest.raises(ValueError, match=expr):
            c.set_futures_scenario("A---")


class TestCL_SCENARIO:
    def test_get(self, test_context: Context) -> None:
        result = CL_SCENARIO.get(force=True)

        # Code list has the expected length
        assert 296 == len(result)

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
        c = result["M SSP2 exo price 2e17"]

        assert "SSP_SSP2_v5.3.1/SSP2 - Low Emissions#2" in str(
            c.get_annotation(id="policy").text
        )


@pytest.mark.parametrize(
    "ssp_or_led, N_exp",
    (
        ("SSP1", 7),
        ("SSP2", 22),
        ("SSP3", 1),
        ("SSP4", 5),
        ("SSP5", 7),
    ),
)
def test_iter_price_emission(ssp_or_led: str, N_exp: int, regions="R12") -> None:
    # Currently only data available for R12
    result = list(iter_price_emission(regions, ssp_or_led))
    assert N_exp == len(result)
