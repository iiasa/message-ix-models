import pandas as pd
import pytest
from message_ix import make_df
from pandas.testing import assert_series_equal

from message_data.model.transport.ikarus import get_ikarus_data
from message_data.testing import assert_units
from message_data.tests.model.transport import configure_build

from .test_demand import demand_computer


@pytest.mark.parametrize("years", ["A", "B"])
@pytest.mark.parametrize(
    "regions, N_node", [("R11", 11), ("R12", 12), ("R14", 14), ("ISR", 1)]
)
def test_get_ikarus_data(test_context, regions, N_node, years):
    ctx = test_context
    info = configure_build(ctx, regions, years)

    # get_ikarus_data() succeeds on the bare RES
    data = get_ikarus_data(ctx)

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
        # commented (PNK 2022-06-17): corrected abuse of caapacity_factor to include
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


@pytest.mark.parametrize("years", ["A", "B"])
@pytest.mark.parametrize(
    "regions, N_node", [("R11", 11), ("R12", 12), ("R14", 14), ("ISR", 1)]
)
def test_get_ikarus_data1(test_context, regions, N_node, years):
    ctx = test_context
    c, info = demand_computer(ctx, None, regions, years)

    for name in (
        "availability",
        "fix_cost",
        "input",
        "inv_cost",
        "technical_lifetime",
        "var_cost",
    ):
        print(c.get(f"ikarus {name}::raw"))
