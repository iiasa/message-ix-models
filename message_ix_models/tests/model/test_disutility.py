"""Tests of :mod:`.model.disutility`."""
from itertools import product

import pandas as pd
import pandas.testing as pdt
import pytest
from message_ix import make_df
from sdmx.model.v21 import Annotation, Code

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
    """Fixture: list of 2 consumer groups."""
    yield [Code(id="g0"), Code(id="g1")]


@pytest.fixture
def techs():
    """Fixture: list of 2 technologies for which groups can have disutility."""
    yield [Code(id="t0"), Code(id="t1")]


@pytest.fixture
def template():
    """Fixture: :class:.`Code` with annotations, for :func:`.disutility.get_spec`."""
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
    """Fixture: a prepared spec for the minimal test case."""
    yield disutility.get_spec(groups, techs, template)


@pytest.fixture
def scenario(request, test_context, techs):
    """Fixture: a |Scenario| with technologies given by :func:`techs`."""
    test_context.regions = "R14"
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


def minimal_test_data(scenario):
    """Generate data for :func:`test_minimal`."""
    common = COMMON.copy()
    common.pop("node_loc")
    common.update(dict(mode="all"))

    data = dict()

    info = ScenarioInfo(scenario)
    y0 = info.Y[0]
    y1 = info.Y[1]

    # Output from t0 and t1
    for t in ("t0", "t1"):
        common.update(dict(technology=t, commodity=f"output of {t}"))
        merge_data(data, make_source_tech(info, common, output=1.0, var_cost=1.0))

    # Disutility input for each combination of (tech) × (group) × (2 years)
    input_data = pd.DataFrame(
        [
            ["usage of t0 by g0", y0, 0.1],
            ["usage of t0 by g0", y1, 0.1],
            ["usage of t1 by g0", y0, 0.1],
            ["usage of t1 by g0", y1, 0.1],
            ["usage of t0 by g1", y0, 0.1],
            ["usage of t0 by g1", y1, 0.1],
            ["usage of t1 by g1", y0, 0.1],
            ["usage of t1 by g1", y1, 0.1],
        ],
        columns=["technology", "year_vtg", "value"],
    )
    data["input"] = make_df(
        "input", **input_data, commodity="disutility", **COMMON
    ).assign(node_origin=copy_column("node_loc"), year_act=copy_column("year_vtg"))

    # Demand
    c, y = zip(*product(["demand of group g0", "demand of group g1"], [y0, y1]))
    data["demand"] = make_df("demand", commodity=c, year=y, value=1.0, **COMMON)

    # Constraint on activity in the first period
    t = sorted(input_data["technology"].unique())
    for bound in ("lo", "up"):
        par = f"bound_activity_{bound}"
        data[par] = make_df(par, value=0.5, technology=t, year_act=y0, **COMMON)

    # Constraint on activity growth
    annual = (1.1 ** (1.0 / 5.0)) - 1.0
    for bound, factor in (("lo", -1.0), ("up", 1.0)):
        par = f"growth_activity_{bound}"
        data[par] = make_df(
            par, value=factor * annual, technology=t, year_act=y1, **COMMON
        )

    return data, y0, y1


def test_minimal(scenario, groups, techs, template):
    """Expected results are generated from a minimal test case."""
    # Set up structure
    disutility.add(scenario, groups, techs, template)

    # Add test-specific data
    data, y0, y1 = minimal_test_data(scenario)

    scenario.check_out()
    add_par_data(scenario, data)
    scenario.commit("Disutility test 1")

    # commented: pre-solve debugging output
    # for par in ("input", "output", "technical_lifetime", "var_cost"):
    #     scenario.par(par).to_csv(f"debug-{par}.csv")

    scenario.solve(quiet=True)

    # Helper function to retrieve ACT data and condense for inspection
    def get_act(s):
        result = (
            scenario.var("ACT")
            .query("lvl > 0")
            .drop(columns=["node_loc", "mode", "time", "mrg"])
            .sort_values(["year_vtg", "technology"])
            .reset_index(drop=True)
        )
        # No "stray" activity of technologies beyond the vintage periods
        pdt.assert_series_equal(
            result["year_act"], result["year_vtg"], check_names=False
        )
        result = result.drop(columns=["year_vtg"]).set_index(["technology", "year_act"])
        # Return the activity and its inter-period delta
        return result, (
            result.xs(y1, level="year_act") - result.xs(y0, level="year_act")
        )

    # Post-solve debugging output TODO comment before merging
    ACT1, ACT1_delta = get_act(scenario)

    # Increase the disutility of for t0 for g0 in period y1
    data["input"].loc[1, "value"] = 0.2

    # Re-solve
    scenario.remove_solution()
    scenario.check_out()
    scenario.add_par("input", data["input"])
    scenario.commit("Disutility test 2")
    scenario.solve(quiet=True)

    # Compare activity
    ACT2, ACT2_delta = get_act(scenario)

    merged = ACT1.merge(ACT2, left_index=True, right_index=True)
    merged["lvl_diff"] = merged["lvl_y"] - merged["lvl_x"]

    merged_delta = ACT1_delta.merge(ACT2_delta, left_index=True, right_index=True)

    # commented: for debugging
    # print(merged, merged_delta)

    # Group g0 decreases usage of t0, and increases usage of t1, in period y1 vs. y0
    assert merged_delta.loc["usage of t0 by g0", "lvl_y"] < 0
    assert merged_delta.loc["usage of t1 by g0", "lvl_y"] > 0

    # Group g0 usage of t0 is lower when the disutility is higher
    assert merged.loc[("usage of t0 by g0", y1), "lvl_diff"] < 0
    # Group g0 usage of t1 is correspondingly higher
    assert merged.loc[("usage of t1 by g0", y1), "lvl_diff"] > 0


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
    data = disutility.get_data(scenario, spec)

    # Test that the code will not encounter #45 / iiasa/ixmp#425
    for name, df in data.items():
        assert (
            "" not in df["unit"].unique()
        ), f"{repr(name)} data has dimensionless units"


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
