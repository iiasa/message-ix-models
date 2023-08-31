import xarray as xr
from genno import Quantity

from message_ix_models.report.computations import compound_growth


def test_compound_growth():
    """:func:`.compound_growth` on a 2-D quantity."""
    qty = Quantity(
        xr.DataArray(
            [
                [1.01, 1.0, 1.02, 1e6],  # Varying growth rates for x=x1
                [1.0, 1.0, 1.0, 1.0],  # No rates/constant for x=x2
            ],
            coords=(["x1", "x2"], [2020, 2021, 2030, 2035]),
            dims=("x", "t"),
        )
    )

    # Function runs
    result = compound_growth(qty, "t")

    # Results have expected values
    r1 = result.sel(x="x1")
    assert all(1.0 == r1.sel(t=2020))
    assert all(1.01 == r1.sel(t=2021) / r1.sel(t=2020))
    assert all(1.0 == r1.sel(t=2030) / r1.sel(t=2021))
    assert all(1.02**5 == r1.sel(t=2035) / r1.sel(t=2030))

    assert all(1.0 == result.sel(x="x2"))
