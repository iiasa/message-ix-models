from itertools import product

import pandas as pd

# import pandas.testing as pdt
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
    mode="all",
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
    output = dict(commodity="demand of group {group}", level="useful", unit="kg")

    # Code's ID is itself a template for IDs of conversion technologies
    yield Code(
        id="usage of {technology} by {group}",
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
                var_cost=1.0,
            ),
        )

    # For each combination of (tech) × (group) × (2 years)
    input_data = pd.DataFrame(
        [
            ["usage of t0 by g0", 2020, 0.1],
            ["usage of t0 by g0", 2025, 0.1],
            ["usage of t1 by g0", 2020, 0.1],
            ["usage of t1 by g0", 2025, 0.1],
            ["usage of t0 by g1", 2020, 0.1],
            ["usage of t0 by g1", 2025, 0.1],
            ["usage of t1 by g1", 2020, 0.1],
            ["usage of t1 by g1", 2025, 0.1],
        ],
        columns=["technology", "year_vtg", "value"],
    )
    data["input"] = make_df(
        "input", **input_data, commodity="disutility", **COMMON
    ).assign(node_origin=copy_column("node_loc"), year_act=copy_column("year_vtg"))

    # Demand
    c, y = zip(*product(["demand of group g0", "demand of group g1"], [2020, 2025]))
    data["demand"] = make_df("demand", commodity=c, year=y, value=1.0, **COMMON)

    # Activity in the first year
    t = sorted(input_data["technology"].unique())
    for bound in ("lo", "up"):
        par = f"bound_activity_{bound}"
        data[par] = make_df(par, value=0.5, technology=t, year_act=2020, **COMMON)

    # Bounds
    for bound, factor in (("lo", -1.0), ("up", 1.0)):
        par = f"growth_activity_{bound}"
        data[par] = make_df(
            par, value=factor * 0.1, technology=t, year_act=2025, **COMMON
        )

    scenario.check_out()
    add_par_data(scenario, data)
    scenario.commit("Disutility test 1")

    # Pre-solve debugging output
    for par in ("input", "output", "technical_lifetime", "var_cost"):
        scenario.par(par).to_csv(f"debug-{par}.csv")

    scenario.solve(quiet=True)

    # Post-solve debugging output TODO comment before merging
    ACT = scenario.var("ACT").query("lvl > 0").drop(columns=["node_loc", "time", "mrg"])

    print(ACT)

    # commented: pending debugging
    # pdt.assert_series_equal(ACT["year_act"], ACT["year_vtg"])


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

    # Spec adds the "disutility" commodity; and adds (if not existing) the output
    # commodities for t[01] and demand commodities for g[01]
    assert {
        "disutility",
        "output of t0",
        "output of t1",
        "demand of group g0",
        "demand of group g1",
    } == set(map(str, spec["add"].set["commodity"]))

    # Spec adds the "distuility source" technology, and "usage of {tech} by {group}"
    # for each tech × group, per the template
    assert {
        "disutility source",
        "usage of t0 by g0",
        "usage of t0 by g1",
        "usage of t1 by g0",
        "usage of t1 by g1",
    } == set(map(str, spec["add"].set["technology"]))
