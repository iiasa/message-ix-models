import message_ix
import pytest
from message_ix import make_df

from message_ix_models import testing
from message_ix_models.model.bare import Config

#: Number of items in the respective YAML files.
SET_SIZE = dict(
    commodity=35,
    level=7,
    node=14 + 1,  # R14 is default, and 'World' exists automatically
    relation=20,
    technology=423,
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
        # One region, resolved as a single node
        (dict(regions="CA"), dict(node=1 + 1)),
        (dict(regions="EE"), dict(node=1 + 1)),
        (dict(regions="MED"), dict(node=1 + 1)),
        (dict(regions="SEE"), dict(node=1 + 1)),
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
    test_context.model = Config(**settings)

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
        values = scenario.set(name)
        assert size == len(values), (name, values)


def test_create_res_spec_data(test_context):
    """A caller can supply the structure and the parameter data."""
    from message_ix_models.model.bare import create_res, get_spec

    test_context.model = Config(regions="SEE", years="B")
    spec = get_spec(test_context)
    # Reduce the structure: a single technology
    spec.add.set["technology"] = spec.add.set["technology"][:1]

    demand = make_df(
        "demand",
        node="SEE",
        commodity=str(spec.add.set["commodity"][0]),
        level=str(spec.add.set["level"][0]),
        year=spec.add.set["year"][-1],
        time="year",
        value=1.0,
        unit="GWa",
    )

    def data(scenario, dry_run=False):
        return dict(demand=demand)

    scenario = create_res(test_context, spec=spec, data=data)

    assert 1 == len(scenario.set("technology"))
    assert 1 == len(scenario.par("demand"))


class TestConfig:
    def test_init(self):
        c = Config()
        c.regions = "R999"

        # TODO expand
