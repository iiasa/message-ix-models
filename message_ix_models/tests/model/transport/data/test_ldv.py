import pytest
from iam_units import registry
from message_ix_models import testing
from message_ix_models.model.structure import get_codes
from pytest import param

from message_data.model.transport.data.ldv import (
    constraint_data,
    get_ldv_data,
    read_USTIMES_MA3T,
    read_USTIMES_MA3T_2,
)
from message_data.testing import assert_units
from message_data.tests.model.transport import configure_build


@pytest.mark.parametrize(
    "source, regions, years",
    [
        param(
            None,
            "R11",
            "A",
            marks=pytest.mark.xfail(
                raises=AssertionError, reason="Returns extra var_cost data"
            ),
        ),
        ("US-TIMES MA3T", "R11", "A"),
        ("US-TIMES MA3T", "R11", "B"),
        ("US-TIMES MA3T", "R12", "B"),
        ("US-TIMES MA3T", "R14", "A"),
        # Not implemented
        param("US-TIMES MA3T", "ISR", "A", marks=testing.NIE),
    ],
)
def test_get_ldv_data(test_context, source, regions, years):
    # Info about the corresponding RES
    ctx = test_context

    info = configure_build(ctx, regions, years)
    ctx.transport.data_source.LDV = source

    # Method runs without error

    data = get_ldv_data(ctx)

    # Data are returned for the following parameters
    assert {
        "capacity_factor",
        "emission_factor",
        "fix_cost",
        "input",
        "inv_cost",
        "output",
        "relation_activity",
        "technical_lifetime",
    } == set(data.keys())

    # Input data is returned and has the correct units
    assert_units(data["input"], registry("1.0 GWa / (Gv km)"))

    # Output data is returned and has the correct units
    assert_units(data["output"], registry.Unit("Gv km"))

    # Output data is returned and has the correct units
    for name in ("fix_cost", "inv_cost"):
        assert_units(data[name], registry.Unit("USD_2010 / vehicle"))

    # Historical periods from 2010 + all model periods
    i = info.set["year"].index(2010)
    exp = info.set["year"][i:]

    # Remaining data have the correct size
    for par_name, df in data.items():
        # No missing entries
        assert not df.isna().any(axis=None), df.tostring()

        if "year_vtg" not in df.columns:
            continue

        # Data covers these periods
        assert exp == sorted(df["year_vtg"].unique())

        # Total length of data: # of regions × (11 technology × # of periods; plus 1
        # technology (historical ICE) for only 2010)
        try:
            # Use <= because read_USTIMES_MA3T_2 returns additional values
            assert len(info.N[1:]) * ((11 * len(exp)) + 1) <= len(df)
        except AssertionError:
            print(par_name, df.to_string(), sep="\nq")
            raise


@pytest.mark.parametrize(
    "source, regions, years",
    [
        (None, "R11", "A"),
        ("US-TIMES MA3T", "R11", "A"),
        ("US-TIMES MA3T", "R11", "B"),
        ("US-TIMES MA3T", "R12", "B"),
        ("US-TIMES MA3T", "R14", "A"),
        # Not implemented
        param("US-TIMES MA3T", "ISR", "A", marks=testing.NIE),
    ],
)
def test_ldv_constraint_data(test_context, source, regions, years):
    # Info about the corresponding RES
    ctx = test_context

    info = configure_build(ctx, regions, years)
    ctx.transport.data_source.LDV = source

    # Method runs without error

    data = constraint_data(ctx)

    # Data are returned for the following parameters
    assert {"growth_activity_lo", "growth_activity_up"} == set(data.keys())

    for bound in ("lo", "up"):
        # Constraint data are returned. Use .pop() to exclude from the next assertions
        df = data.pop(f"growth_activity_{bound}")

        # Usage technologies are included
        assert "ELC_100 usage by URLMM" in df["technology"].unique()

        # Data covers all periods except the first
        assert info.Y[1:] == sorted(df["year_act"].unique())


@pytest.mark.parametrize("func", (read_USTIMES_MA3T, read_USTIMES_MA3T_2))
def test_read_USTIMES_MA3T(func):
    all_nodes = get_codes("node/R11")
    nodes = all_nodes[all_nodes.index("World")].child
    data = func(nodes, "R11")

    # Expected contents
    names = ["efficiency", "fix_cost", "inv_cost"]
    assert set(names) == set(data.keys())

    # Correct units
    assert data["inv_cost"].units.dimensionality == {"[currency]": 1, "[vehicle]": -1}
    assert data["fix_cost"].units.dimensionality == {"[currency]": 1, "[vehicle]": -1}
    assert data["efficiency"].units.dimensionality == {
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
