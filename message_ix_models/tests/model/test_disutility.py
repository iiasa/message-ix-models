import pandas as pd
import pytest
from message_ix import make_df
from sdmx.model import Annotation, Code

from message_ix_models import ScenarioInfo, testing
from message_ix_models.model import disutility
from message_ix_models.util import (
    add_par_data,
    copy_column,
    make_source_tech,
    merge_data,
)

# Common data and fixtures for test_minimal() and other tests

COMMON = dict(
    level="useful",
    node_dest="R14_AFR",
    node_loc="R14_AFR",
    node_origin="R14_AFR",
    node="R14_AFR",
    time_dest="year",
    time_origin="year",
    time="year",
    unit="kg",
)


@pytest.fixture
def groups():
    """List of two consumer groups."""
    yield [Code(id="g0"), Code(id="g1")]


@pytest.fixture
def techs():
    """List of two technologies, for which groups may have different disutilities."""
    yield [Code(id="t0"), Code(id="t1")]


@pytest.fixture
def template():
    """:class:.`Code` object with annotations, for :func:`.disutility.get_spec`."""
    # Template for inputs of conversion technologies, from a technology-specific
    # commodity
    input = dict(commodity="output of {technology}", level="useful", unit="kg")

    # Template for outputs of conversion technologies, to a group–specific demand
    # commodity
    output = dict(commodity="demand of group {mode}", level="useful", unit="kg")

    # Code's ID is itself a template for IDs of conversion technologies
    yield Code(
        id="{technology} usage",
        annotations=[
            Annotation(id="input", text=repr(input)),
            Annotation(id="output", text=repr(output)),
        ],
    )


@pytest.fixture
def spec(groups, techs, template):
    """A prepared spec for the minimal test case."""
    yield disutility.get_spec(groups, techs, template)


@pytest.fixture
def scenario(request, test_context, techs):
    """A :class:`.Scenario` with technologies given by :func:`techs`."""
    s = testing.bare_res(request, test_context, solved=False)
    s.check_out()

    s.add_set("technology", ["t0", "t1"])

    s.commit("Test fixture for .model.disutility")
    yield s


def test_add(scenario, groups, techs, template):
    """:func:`.disutility.add` runs on the bare RES; the result solves."""
    disutility.add(scenario, groups, techs, template)

    # Scenario solves (no demand)
    scenario.solve(quiet=True)
    assert (scenario.var("ACT")["lvl"] == 0).all()


def test_minimal(scenario, groups, techs, template):
    """Minimal test case for disutility formulation."""
    disutility.add(scenario, groups, techs, template)

    # Fill in the data for the test case

    common = COMMON.copy()
    common.pop("node_loc")
    common.update(dict(mode="all"))

    data = dict()

    for t in ("t0", "t1"):
        common.update(dict(technology=t, commodity=f"output of {t}"))
        merge_data(
            data,
            make_source_tech(
                ScenarioInfo(scenario),
                common,
                output=1.0,
                technical_lifetime=5.0,
                var_cost=0.0,
            ),
        )

    # For each combination of (tech) × (group) × (2 years)
    df = pd.DataFrame(
        [
            ["g0", "output of t0", "t0 usage", 2020, 1.0],
            ["g0", "output of t0", "t0 usage", 2025, 1.0],
            ["g0", "output of t1", "t1 usage", 2020, 1.0],
            ["g0", "output of t1", "t1 usage", 2025, 1.0],
            ["g1", "output of t0", "t0 usage", 2020, 1.0],
            ["g1", "output of t0", "t0 usage", 2025, 1.0],
            ["g1", "output of t1", "t1 usage", 2020, 1.0],
            ["g1", "output of t1", "t1 usage", 2025, 1.0],
        ],
        columns=["mode", "commodity", "technology", "year_vtg", "value"],
    )
    data["input"] = make_df("input", **df, **COMMON).assign(
        node_origin=copy_column("node_loc"), year_act=copy_column("year_vtg")
    )

    data["demand"] = make_df(
        "demand",
        **pd.DataFrame(
            [
                ["demand of group g0", 2020, 1.0],
                ["demand of group g0", 2025, 1.0],
                ["demand of group g1", 2020, 1.0],
                ["demand of group g1", 2025, 1.0],
            ],
            columns=["commodity", "year", "value"],
        ),
        **COMMON,
    )

    scenario.check_out()
    add_par_data(scenario, data)
    scenario.commit("Disutility test 1")

    scenario.solve(quiet=True)

    ACT = scenario.var("ACT").query("lvl > 0").drop(columns=["node_loc", "time", "mrg"])

    # For debugging TODO comment before merging
    print(ACT)


def test_data_conversion(scenario, spec):
    """:func:`~.disutility.data_conversion` runs."""
    info = ScenarioInfo(scenario)
    disutility.data_conversion(info, spec)


def test_data_source(scenario, spec):
    """:func:`~.disutility.data_source` runs."""
    info = ScenarioInfo(scenario)
    disutility.data_source(info, spec)


def test_get_data(scenario, spec):
    """:func:`~.disutility.get_data` runs."""
    disutility.get_data(scenario, spec)


def test_get_spec(groups, techs, template):
    """:func:`~.disutility.get_spec` runs and produces expected output."""
    spec = disutility.get_spec(groups, techs, template)

    # Spec requires the existence of the base technologies
    assert {"technology"} == set(spec["require"].set.keys())
    assert techs == spec["require"].set["technology"]

    # Spec removes nothing
    assert set() == set(spec["remove"].set.keys())

    # Spec adds the "disutility" commodity
    assert {"disutility"} == set(map(str, spec["add"].set["commodity"]))

    # Spec adds the "distuility source" technology, and "{tech} usage" for each tech,
    # per the template
    assert {"disutility source", "t0 usage", "t1 usage"} == set(
        map(str, spec["add"].set["technology"])
    )
    # Spec adds two modes
    assert {"g0", "g1"} == set(map(str, spec["add"].set["mode"]))
