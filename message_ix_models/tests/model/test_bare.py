import message_ix
import pytest

from message_ix_models import testing

#: Number of items in the respective YAML files.
SET_SIZE = dict(
    commodity=13,
    level=6,
    technology=377,
    node=14 + 1,  # R14 is default, and 'World' exists automatically
    year=28,  # YB is default: 1950, ..., 2020, 2025, ..., 2055, 2060, ..., 2110
)


@pytest.mark.parametrize(
    "settings, expected",
    [
        # Defaults per bare.SETTINGS
        (dict(), dict()),
        #
        # Different regional aggregations
        (dict(regions="R11"), dict(node=11 + 1)),
        (dict(regions="RCP"), dict(node=5 + 1)),
        # MESSAGE-IL
        (dict(regions="ISR"), dict(node=1 + 1)),
        #
        # Different time periods
        (dict(years="A"), dict(year=16)),  # ..., 2010, 2020, ..., 2110
        #
        # Option to add a dummy technology/commodity so the model solves
        (
            dict(res_with_dummies=True),
            dict(
                commodity=SET_SIZE["commodity"] + 1,
                technology=SET_SIZE["technology"] + 2,
            ),
        ),
    ],
)
def test_create_res(request, test_context, settings, expected):
    # Apply settings to the temporary context
    test_context.update(settings)

    # Call bare.create_res() via testing.bare_res(). This ensures the slow step of
    # creating the scenario occurs only once per test session. If it fails, it will
    # either fail within this test, or in some other test function that calls
    # testing.bare_res() with the same arguments.
    scenario = testing.bare_res(request, test_context, solved=False)

    # Returns a Scenario object
    assert isinstance(scenario, message_ix.Scenario)

    # Sets contain the expected number of elements
    sets = SET_SIZE.copy()
    sets.update(expected)
    for name, size in sets.items():
        assert size == len(scenario.set(name))
