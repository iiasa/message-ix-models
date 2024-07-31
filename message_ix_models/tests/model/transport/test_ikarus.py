import pandas as pd
import pytest
from iam_units import registry
from message_ix import make_df
from numpy.testing import assert_allclose
from pandas.testing import assert_series_equal

from message_ix_models.model.transport import build, ikarus, testing
from message_ix_models.model.transport.non_ldv import UNITS
from message_ix_models.model.transport.testing import assert_units
from message_ix_models.project.navigate import T35_POLICY


@pytest.mark.skip(reason="Deprecated, slow")
@pytest.mark.parametrize("years", ["A", "B"])
@pytest.mark.parametrize(
    "regions, N_node", [("R11", 11), ("R12", 12), ("R14", 14), ("ISR", 1)]
)
def test_get_ikarus_data0(test_context, regions, N_node, years):
    ctx = test_context
    _, info = testing.configure_build(ctx, regions=regions, years=years)

    # get_ikarus_data() succeeds on the bare RES
    data = ikarus.get_ikarus_data(ctx)

    # Returns a mapping
    assert {
        "capacity_factor",
        "fix_cost",
        "input",
        "inv_cost",
        "output",
        "technical_lifetime",
    } == set(data.keys())
    assert all(map(lambda df: isinstance(df, pd.DataFrame), data.values()))

    # Retrieve DataFrame for par e.g. 'inv_cost' and tech e.g. 'rail_pub'
    inv = data["inv_cost"]
    inv_rail_pub = inv[inv["technology"] == "rail_pub"]

    # NB: *prep_years* is created to accommodate prepended years before than
    # *firstmodelyear*. See ikarus.py to check how/why those are prepended.
    prep_years = (1 if years == "A" else 2) + len(info.Y)
    # Regions * 13 years (inv_cost has 'year_vtg' but not 'year_act' dim)
    rows_per_tech = N_node * prep_years
    N_techs = 18

    # Data have been loaded with the correct shape and magnitude:
    assert inv_rail_pub.shape == (rows_per_tech, 5), inv_rail_pub
    assert inv.shape == (rows_per_tech * N_techs, 5)

    # Magnitude for year e.g. 2020
    values = inv_rail_pub[inv_rail_pub["year_vtg"] == 2020]["value"]
    value = values.iloc[0]
    assert round(value, 3) == 3.233

    # Units of each parameter have the correct dimensionality
    dims = {
        "capacity_factor": {},  # always dimensionless
        "inv_cost": {"[currency]": 1, "[vehicle]": -1},
        "fix_cost": {"[currency]": 1, "[vehicle]": -1, "[time]": -1},
        "output": {"[passenger]": 1, "[vehicle]": -1},
        "technical_lifetime": {"[time]": 1},
    }
    for par, dim in dims.items():
        assert_units(data[par], dim)

    # Specific magnitudes of other values to check
    checks = [
        # commented (PNK 2022-06-17): corrected abuse of capacity_factor to include
        # unrelated concepts
        # dict(par="capacity_factor", year_vtg=2010, value=0.000905),
        # dict(par="capacity_factor", year_vtg=2050, value=0.000886),
        dict(par="technical_lifetime", year_vtg=2010, value=15.0),
        dict(par="technical_lifetime", year_vtg=2050, value=15.0),
    ]
    defaults = dict(node_loc=info.N[-1], technology="ICG_bus", time="year")

    for check in checks:
        # Create expected data
        par_name = check.pop("par")
        check["year_act"] = check["year_vtg"]
        exp = make_df(par_name, **defaults, **check)
        assert len(exp) == 1, "Single row for expected value"

        # Use merge() to find data with matching column values
        columns = sorted(set(exp.columns) - {"value", "unit"})
        result = exp.merge(data[par_name], on=columns, how="inner")

        # Single row matches
        assert len(result) == 1, result

        # Values match
        assert_series_equal(
            result["value_x"],
            result["value_y"],
            check_exact=False,
            check_names=False,
            atol=1e-4,
        )


@build.get_computer.minimum_version
@pytest.mark.parametrize("years", ["A", "B"])
@pytest.mark.parametrize(
    "regions, N_node",
    [
        ("R11", 11),
        ("R12", 12),
        ("R14", 14),
        pytest.param("ISR", 1, marks=testing.MARK[3]),
    ],
)
@pytest.mark.parametrize("options", [{}, dict(navigate_scenario=T35_POLICY.TEC)])
def test_get_ikarus_data1(test_context, regions, N_node, years, options):
    """Test genno-based IKARUS data prep."""
    ctx = test_context
    c, info = testing.configure_build(
        ctx, regions=regions, years=years, options=options
    )

    # commented: for a manual check that `options` have an effect
    # print(c.get("nonldv efficiency:t-y:adj").to_series().to_string())

    k = "transport nonldv::ixmp+ikarus"

    # commented: for debugging
    # print(k)
    # print(c.describe(k))

    # All calculations complete without error
    data = c.get(k)

    parameters = (
        "fix_cost",
        "input",
        "inv_cost",
        "output",
        "technical_lifetime",
        "var_cost",
    )

    for name in parameters:
        assert name in data

        v = data[name]

        # commented: for debugging
        # print(name)
        # print(v.head().to_string())
        # print(f"{len(v) = }")

        # No null keys or values
        assert not v.isna().any(axis=None)

        # Data have the expected units for the respective parameter
        assert_units(v, registry(UNITS[name]))

        # Data cover the entire model horizon
        assert set(info.Y) <= set(v["year_vtg"].unique())

        # Aviation technologies are present
        assert "con_ar" in v["technology"].unique()

    # Test a particular value in inv_cost
    row = (
        data["inv_cost"]
        .query("technology == 'rail_pub' and year_vtg == 2020")
        .iloc[0, :]
    )
    assert "GUSD_2010 / Gv / km" == row["unit"]
    assert_allclose(23.689086, row["value"])

    # Specific magnitudes of other values to check
    # TODO use testing tools to make the following less verbose
    par_name = "technical_lifetime"
    defaults = dict(node_loc=info.N[-1], technology="ICG_bus", time="year")
    checks = [
        dict(year_vtg=2010, value=15.0),  # values of 14.7 are rounded to 15.0
        dict(year_vtg=2050, value=15.0),  # values of 14.7 are rounded to 15.0
    ]

    for check in checks:
        # Create expected data
        check["year_act"] = check["year_vtg"]
        exp = make_df(par_name, **defaults, **check)
        assert len(exp) == 1, "Single row for expected value"

        # Use merge() to find data with matching column values
        columns = sorted(set(exp.columns) - {"value", "unit"})
        result = exp.merge(data[par_name], on=columns, how="inner")

        # Single row matches
        assert len(result) == 1, result

        # Values match
        assert_series_equal(
            result["value_x"],
            result["value_y"],
            check_exact=False,
            check_names=False,
            atol=1e-4,
        )
