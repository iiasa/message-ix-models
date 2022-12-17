import pytest

from message_data.model.transport.config import Config, ScenarioFlags

FUTURES = (
    ("default", ScenarioFlags.DEFAULT),
    ("base", ScenarioFlags.BASE),
    ("A---", ScenarioFlags.A___),
    ("debug", ScenarioFlags.DEBUG),
    pytest.param("foo", None, marks=pytest.mark.xfail(raises=ValueError)),
)

NAVIGATE = (
    ("", ScenarioFlags.DEFAULT),
    ("act", ScenarioFlags.ACT),
    ("ele", ScenarioFlags.ELE),
    ("tec", ScenarioFlags.TEC),
    ("act+ele+tec", ScenarioFlags.ACT | ScenarioFlags.ELE | ScenarioFlags.TEC),
    pytest.param("foo+act+tec", None, marks=pytest.mark.xfail(raises=ValueError)),
)


class TestConfig:
    @pytest.fixture
    def c(self):
        yield Config()

    @pytest.mark.parametrize("input, expected", FUTURES)
    def test_futures_scenario0(self, input, expected):
        """Set Transport Futures scenario through the constructor."""
        c = Config(futures_scenario=input)  # Call succeeds
        assert expected == c.flags  # The expected flags are set

    @pytest.mark.parametrize("input, expected", FUTURES)
    def test_futures_scenario1(self, c, input, expected):
        """Set Transport Futures scenario on an existing instance."""
        c.set_futures_scenario(input)
        assert expected == c.flags

    @pytest.mark.parametrize("input, expected", NAVIGATE)
    def test_navigate_scenario0(self, input, expected):
        """Set NAVIGATE scenario through the constructor."""
        c = Config(navigate_scenario=input)
        assert expected == c.flags

    @pytest.mark.parametrize("input, expected", NAVIGATE)
    def test_navigate_scenario1(self, c, input, expected):
        """Set NAVIGATE scenario on an existing instance."""
        c.set_navigate_scenario(input)
        assert expected == c.flags

    def test_scenario_conflict(self):
        # Giving both raises an exception
        with pytest.raises(
            ValueError, match=r"NAVIGATE scenario 'act\+tec' clashes with existing"
        ):
            c = Config(futures_scenario="A---", navigate_scenario="act+tec")

        # Also a conflict
        c = Config(navigate_scenario="act+tec")
        with pytest.raises(
            ValueError, match="Transport Futures scenario 'A---' clashes with existing"
        ):
            c.set_futures_scenario("A---")
