import logging

import genno
import pandas as pd
import pytest
from iam_units import registry
from pytest import param

from message_ix_models.model.structure import get_codes
from message_ix_models.model.transport import build, testing
from message_ix_models.model.transport.ldv import (
    constraint_data,
    read_USTIMES_MA3T,
    read_USTIMES_MA3T_2,
)
from message_ix_models.model.transport.testing import assert_units
from message_ix_models.project.navigate import T35_POLICY

log = logging.getLogger(__name__)


@build.get_computer.minimum_version
@pytest.mark.parametrize("source", [None, "US-TIMES MA3T"])
@pytest.mark.parametrize("years", ["A", "B"])
@pytest.mark.parametrize(
    "regions",
    [
        param("ISR", marks=testing.MARK[3]),
        "R11",
        "R12",
        param("R14", marks=testing.MARK[2](genno.ComputationError)),
    ],
)
def test_get_ldv_data(tmp_path, test_context, source, regions, years):
    # Info about the corresponding RES
    ctx = test_context
    # Prepare a Computer for LDV data calculations
    c, info = testing.configure_build(
        ctx,
        tmp_path=tmp_path,
        regions=regions,
        years=years,
        options={"data source": {"LDV": source}, "navigate_scenario": T35_POLICY.TEC},
    )

    # Earlier keys in the process, for debugging
    # key = "ldv fuel economy:n-t-y:exo"
    # key = "ldv efficiency:n-t-y"
    # key = "transport input factor:t-y"
    # key = "ldv efficiency:n-t-y:adj"

    key = "transport ldv::ixmp"

    # print(c.describe(key))  # DEBUG

    # Method runs without error
    data = c.get(key)

    # print(data)  # DEBUG
    # print(data.to_series().to_string())  # DEBUG

    # Data are returned for the following parameters
    exp_pars = {
        "bound_new_capacity_up",
        "capacity_factor",
        "growth_activity_lo",
        "growth_activity_up",
        "input",
        "output",
        "var_cost",
    }
    if source == "US-TIMES MA3T":
        exp_pars |= {
            "emission_factor",
            "fix_cost",
            "inv_cost",
            "relation_activity",
            "technical_lifetime",
        }
    if regions == "R12":
        exp_pars |= {"bound_new_capacity_lo", "historical_new_capacity"}

    assert exp_pars == set(data.keys())

    # Input data is returned and has the correct units
    assert {"GW * a / Gv / km", "km", "-"} >= set(data["input"]["unit"].unique())

    # Output data is returned and has the correct units
    assert {"Gv km", "km", "-"} >= set(data["output"]["unit"].unique())

    if source is None:
        return

    # Data are generated for multiple year_act for each year_vtg of a particular tech
    for name in "fix_cost", "input", "output":
        tmp = data[name].query("technology == 'ELC_100' and year_vtg == 2050")
        assert 1 < len(tmp["year_act"].unique()), tmp

    # Output data is returned and has the correct units
    for name in ("fix_cost", "inv_cost"):
        assert_units(data[name], registry.Unit("USD_2010 / vehicle"))

    # Historical periods from 2010 + all model periods
    i = info.set["year"].index(2010)
    y_2010 = info.set["year"][i:]

    # Remaining data have the correct size
    for par_name, df in sorted(data.items()):
        # log.info(par_name)

        # No missing entries
        assert not df.isna().any(axis=None), df.tostring()

        if "year_vtg" not in df.columns:
            continue

        # Data covers at least these periods
        exp_y = {"bound_new_capacity_up": info.Y, "var_cost": info.Y}.get(
            par_name, y_2010
        )

        # FIXME Temporarily disabled for #514
        continue

        assert set(exp_y) <= set(df["year_vtg"].unique())

        # Expected number of (yv, ya) combinations in the data
        N_y = len(exp_y)
        try:
            # Check for a vintaged parameter and technology
            cond = df.eval("year_act - year_vtg > 0").any(axis=None)
        except pd.errors.UndefinedVariableError:
            cond = False
        N_y *= ((len(exp_y) - 1) // 2) if cond else 1

        # Total length of data is at least the product of:
        # - # of regions
        # - # of technologies: 11 if specific to each LDV tech
        # - # of periods
        N_exp = (
            len(info.N[1:])
            * {"bound_new_capacity_up": 1, "var_cost": 1}.get(par_name, 11)
            * N_y
        )
        # log.info(f"{N_exp = } {len(df) = }")

        # # Show the data for debugging
        # print(par_name, df.to_string(), sep="\nq")

        assert N_exp <= len(df)


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

    result = c.get("ldv capacity_factor::ixmp")
    assert {"capacity_factor"} == set(result)
    df = result.pop("capacity_factor")
    assert not df.isna().any(axis=None)
    assert 1 == len(df["unit"].unique())
    assert N_node_loc == len(df["node_loc"].unique())


@build.get_computer.minimum_version
@pytest.mark.parametrize(
    "source, regions, years",
    [
        (None, "R11", "A"),
        ("US-TIMES MA3T", "R11", "A"),
        ("US-TIMES MA3T", "R11", "B"),
        ("US-TIMES MA3T", "R12", "B"),
        ("US-TIMES MA3T", "R14", "A"),
        # Not implemented
        param("US-TIMES MA3T", "ISR", "A", marks=testing.MARK[3]),
    ],
)
def test_ldv_constraint_data(test_context, source, regions, years):
    # Info about the corresponding RES
    ctx = test_context
    _, info = testing.configure_build(
        ctx, regions=regions, years=years, options={"data source": {"LDV": source}}
    )

    # Method runs without error
    data = constraint_data(ctx)

    # Data are returned for the following parameters
    assert {"bound_new_capacity_up", "growth_activity_lo", "growth_activity_up"} == set(
        data.keys()
    )

    for bound in ("lo", "up"):
        # Constraint data are returned. Use .pop() to exclude from the next assertions
        df = data.pop(f"growth_activity_{bound}")

        # Usage technologies are included
        assert "ELC_100 usage by URLMM" in df["technology"].unique()

        # Data covers all periods except the first
        assert info.Y[1:] == sorted(df["year_act"].unique())


@pytest.mark.parametrize(
    "func",
    (
        pytest.param(
            read_USTIMES_MA3T, marks=testing.MARK[5]("R11/ldv-cost-efficiency.xlsx")
        ),
        read_USTIMES_MA3T_2,
    ),
)
def test_read_USTIMES_MA3T(func):
    all_nodes = get_codes("node/R11")
    nodes = all_nodes[all_nodes.index("World")].child
    data = func(nodes, "R11")

    # Expected contents
    names = ["fix_cost", "fuel economy", "inv_cost"]
    assert set(names) == set(data.keys())

    # Correct units
    assert data["inv_cost"].units.dimensionality == {"[currency]": 1, "[vehicle]": -1}
    assert data["fix_cost"].units.dimensionality == {"[currency]": 1, "[vehicle]": -1}
    assert data["fuel economy"].units.dimensionality == {
        "[vehicle]": 1,
        "[length]": -1,
        "[mass]": -1,
        "[time]": 2,
    }

    for name in names:
        # Quantity has the expected name
        assert data[name].name == name
        # Quantity has the expected dimensions
        assert {"n", "t", "y"} == set(data[name].dims)
        # Data is returned for all regions
        assert set(data[name].coords["n"].to_index()) == set(map(str, nodes))
