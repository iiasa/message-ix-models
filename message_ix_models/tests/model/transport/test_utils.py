import pandas as pd
import pandas.testing as pdt
import pytest
import xarray as xr
from iam_units import registry
from message_ix_models.util import eval_anno
from pytest import param

from message_data.model.transport.utils import configure, input_commodity_level


def test_add_cl(test_context):
    """:func:`.input_commodity_level` preserves the content of other columns."""
    # Input data missing 'commodity' and 'level'
    df_in = pd.DataFrame(
        [
            ["R11_AFR", None, None, "ICE_conv"],
            ["R11_WEU", None, None, "ELC_100"],
        ],
        columns=["node", "commodity", "level", "technology"],
    )
    configure(test_context)

    df_out = input_commodity_level(df_in, default_level="foo", context=test_context)

    # Output is the same shape
    assert df_out.shape == (2, 4)

    # All NaNs are filled
    assert not df_out.isna().any().any(), df_out

    # Existing columns have the same content
    for col in "node", "technology":
        pdt.assert_series_equal(df_in[col], df_out[col])


@pytest.mark.parametrize(
    "regions",
    [
        None,  # Default, i.e. R11
        "R11",
        "R12",
        "R14",
        param("ISR", marks=pytest.mark.xfail(raises=KeyError)),
    ],
)
def test_configure(test_context, regions):
    """Configuration can be read from files.

    This exercises both :func:`.transport.configure` and :func:`.transport.read_config`.
    """
    # Set the regional aggregation to be used
    ctx = test_context
    if regions is not None:
        ctx.regions = regions

    # configure() returns nothing
    assert configure(ctx) is None

    # Scalar parameters are loaded
    assert "scaling" in ctx["transport config"]
    assert 200 * 8 == registry(ctx["transport config"]["work hours"]).magnitude

    # Codes for the consumer_group set are generated
    codes = ctx["transport set"]["consumer_group"]["add"]
    RUEAA = codes[codes.index("RUEAA")]
    assert "Rural, or “Outside MSA”, Early Adopter, Average" == str(RUEAA.name)

    # xarray objects are generated for advanced indexing
    indexers = ctx["transport set"]["consumer_group"]["indexers"]
    assert all(isinstance(da, xr.DataArray) for da in indexers.values())

    # Codes for commodities are generated
    codes = ctx["transport set"]["commodity"]["add"]
    RUEAA = codes[codes.index("transport pax RUEAA")]
    assert eval_anno(RUEAA, "demand") is True

    # …with expected units
    assert registry.Unit("Gp km") == eval_anno(RUEAA, "units")

    # Codes for technologies are generated, with annotations giving their units
    codes = ctx["transport set"]["technology"]["add"]
    ELC_100 = codes[codes.index("ELC_100")]
    assert registry.Unit("Gv km") == eval_anno(ELC_100, "units")

    # If "ISR" was given as 'regions', then the corresponding config file was loaded
    if regions == "ISR":
        # Check one config value to confirm
        assert {"Israel"} == set(
            ctx["transport config"]["node to census_division"].keys()
        )
