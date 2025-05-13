import logging
from collections.abc import Mapping
from itertools import product
from typing import Optional

import pandas as pd
import pytest
from iam_units import registry
from pytest import param

from message_ix_models.model.transport import build, ldv, testing
from message_ix_models.model.transport.testing import MARK, assert_units
from message_ix_models.project.navigate import T35_POLICY

log = logging.getLogger(__name__)

pytestmark = MARK[10]


@build.get_computer.minimum_version
@pytest.mark.parametrize("dummy_LDV", [False, True])
@pytest.mark.parametrize("years", ["A", "B"])
@pytest.mark.parametrize(
    "regions",
    [
        param("ISR", marks=testing.MARK[3]),
        "R11",
        "R12",
        "R14",
    ],
)
def test_get_ldv_data(tmp_path, test_context, dummy_LDV, regions, years) -> None:
    # Info about the corresponding RES
    ctx = test_context
    # Prepare a Computer for LDV data calculations
    c, info = testing.configure_build(
        ctx,
        tmp_path=tmp_path,
        regions=regions,
        years=years,
        options={"dummy_LDV": dummy_LDV, "navigate_scenario": T35_POLICY.TEC},
    )

    # Key to compute LDV data
    key = ldv.TARGET
    # Earlier keys in the process, for debugging
    # key = "ldv fuel economy:n-t-y:exo"
    # key = "ldv efficiency:n-t-y"
    # key = "transport input factor:t-y"
    # key = "ldv efficiency:n-t-y:adj"

    # print(c.describe(key))  # DEBUG

    # Method runs without error
    data = c.get(key)

    # print(data)  # DEBUG
    # print(data.to_series().to_string())  # DEBUG

    # TODO Merge the following with test_build.test_debug() using Check objects
    # Data are returned for the following parameters
    exp_pars = {
        "bound_new_capacity_lo",
        "bound_new_capacity_up",
        "capacity_factor",
        "historical_new_capacity",
        "input",
        "output",
        "technical_lifetime",
        "var_cost",
    }
    if not dummy_LDV:
        exp_pars |= {
            "emission_factor",
            "fix_cost",
            "inv_cost",
            "relation_activity",
        }

    assert exp_pars == set(data.keys())

    # Input data is returned and has the correct units
    assert {"GW * a / Gv / km", "km", "-", "Gv km"} >= set(
        data["input"]["unit"].unique()
    )

    # Output data is returned and has the correct units
    # for k, df_group in data["output"].groupby("unit"):
    #     if k == "":  # DEBUG Show data with particular units
    #         print(df_group.to_string())
    assert {"Gp km", "Gv km", "Gv * km", "km", "-"} >= set(
        data["output"]["unit"].unique()
    )

    if dummy_LDV:
        return  # Further tests don't apply if dummy data are used

    # Data are generated for multiple year_act for each year_vtg of a particular tech
    for name in "fix_cost", "input", "output":
        tmp = data[name].query("technology == 'ELC_100' and year_vtg == 2050")
        assert 1 < len(tmp["year_act"].unique()), tmp

    # Output data is returned and has the correct units
    for name in ("fix_cost", "inv_cost"):
        assert_units(data[name], registry.Unit("USD_2010 / vehicle"))

    # Expected number of nodes
    N_node = len(info.N[1:])

    # Historical periods from 1995 + all model periods
    y_min = 1995
    y_all = sorted(filter(lambda y: y_min <= y, info.set["year"]))

    # Number of valid (yv, ya) combinations for vintaged technologies
    def include(arg):
        yv, ya = arg
        return yv <= ya and info.y0 <= ya

    N_y_vintaged = len(list(filter(include, product(y_all, y_all))))
    # TODO Retrieve N_tech = 11 from info.set["technology"]

    # Information about returned parameters
    # TODO Include unit checks, above, in this collection
    par_info: Mapping[str, tuple[bool, Optional[list[int]], int]] = {
        "bound_new_capacity_lo": (False, [info.y0], 1),
        "bound_new_capacity_up": (False, [info.y0], 1),
        "emission_factor": (True, None, 0),
        "historical_new_capacity": (
            True,
            list(filter(lambda y: y < info.y0, y_all))[1:],
            11,
        ),
        "output": (False, y_all, 8),  # NB 8 here is arbitrary
        "var_cost": (False, info.Y, 1),
    }

    try:
        for par_name, df in sorted(data.items()):
            # Expected values for this parameter: periods, number of technologies
            skip, exp_y, N_t = par_info.get(par_name, (False, y_all, 11))

            # print(par_name)  # DEBUG

            # No missing entries
            assert not df.isna().any(axis=None), df.tostring()

            if "year_vtg" not in df.columns:
                continue

            # Data covers at least these periods
            assert exp_y is None or set(exp_y) <= set(df["year_vtg"].unique()), par_name

            if skip:
                continue

            # Expected number of (yv, ya) combinations in the data
            try:
                # Check for a vintaged parameter and technology
                assert df.eval("year_act - year_vtg > 0").any(axis=None)
                N_y = N_y_vintaged
            except (pd.errors.UndefinedVariableError, AssertionError):
                N_y = len(exp_y or [])

            # Total length of data is at least the product of:
            # - # of regions
            # - # of technologies
            # - # of periods
            assert N_node * N_t * N_y <= len(df)
    except AssertionError:
        # # Show the data for debugging
        # print(par_name, df.to_string(), sep="\n")
        raise


@build.get_computer.minimum_version
@pytest.mark.parametrize(
    "regions, N_node_loc",
    [
        ("R11", 11),
        ("R12", 12),
        ("R14", 14),
    ],
)
def test_ldv_capacity_factor(test_context, regions, N_node_loc, years="B"):
    c, _ = testing.configure_build(test_context, regions=regions, years=years)

    result = c.get("capacity_factor::LDV+ixmp")
    assert {"capacity_factor"} == set(result)
    df = result.pop("capacity_factor")
    assert not df.isna().any(axis=None)
    assert 1 == len(df["unit"].unique())
    assert N_node_loc == len(df["node_loc"].unique())


@build.get_computer.minimum_version
@pytest.mark.skip(reason="TODO Integrate assertions into test_debug")
@pytest.mark.parametrize(
    "dummy_LDV, regions, years",
    [
        (True, "R11", "A"),
        (False, "R11", "A"),
        (False, "R11", "B"),
        (False, "R12", "B"),
        (False, "R14", "A"),
        # Not implemented
        param(False, "ISR", "A", marks=testing.MARK[3]),
    ],
)
def test_ldv_constraint_data(test_context, dummy_LDV, regions, years):
    # Info about the corresponding RES
    ctx = test_context
    _, info = testing.configure_build(
        ctx, regions=regions, years=years, options={"dummy_LDV": dummy_LDV}
    )

    # Method runs without error
    data = ldv.constraint_data(ctx)  # type: ignore [attr-defined]

    # Data are returned for the following parameters
    assert {
        "bound_new_capacity_up",
        "growth_activity_lo",
        "growth_activity_up",
        "initial_activity_up",
    } == set(data.keys())

    for bound in ("lo", "up"):
        # Constraint data are returned. Use .pop() to exclude from the next assertions
        df = data.pop(f"growth_activity_{bound}")

        # Usage technologies are included
        assert "ELC_100 usage by URLMM" in df["technology"].unique()

        # Data covers all periods except the first
        assert info.Y[1:] == sorted(df["year_act"].unique())
