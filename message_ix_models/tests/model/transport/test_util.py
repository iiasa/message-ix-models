import pandas as pd
import pandas.testing as pdt
import pytest
import xarray as xr
from genno.testing import assert_qty_equal
from iam_units import registry
from message_ix_models.report.util import as_quantity
from message_ix_models.util import eval_anno

from message_data.model.transport.config import Config, DataSourceConfig
from message_data.model.transport.util import input_commodity_level


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
    Config.from_context(test_context)

    df_out = input_commodity_level(test_context, df_in, default_level="foo")

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
        None,  # Default per message_ix_models.model.Config
        "R11",
        "R12",
        "R14",
        pytest.param("ISR", marks=pytest.mark.xfail(raises=AssertionError)),
    ],
)
def test_configure(test_context, regions):
    """Configuration can be read from files.

    This exercises :meth:`.Config.from_context`.
    """
    # Set the regional aggregation to be used
    ctx = test_context
    if regions:
        ctx.model.regions = regions

    # Returns the same object stored as Context["transport"]
    cfg = Config.from_context(ctx)

    assert cfg is ctx["transport"]

    # Attributes have the correct types
    assert isinstance(cfg.data_source, DataSourceConfig)

    # Scalar parameters are loaded
    assert cfg.scaling
    assert_qty_equal(as_quantity("200 * 8 hours / passenger / year"), cfg.work_hours)

    # Codes for the consumer_group set are generated
    codes = cfg.set["consumer_group"]["add"]
    RUEAA = codes[codes.index("RUEAA")]
    assert "Rural, or “Outside MSA”, Early Adopter, Average" == str(RUEAA.name)

    # xarray objects are generated for advanced indexing
    indexers = cfg.set["consumer_group"]["indexers"]
    assert all(isinstance(da, xr.DataArray) for da in indexers.values())

    # Codes for commodities are generated
    codes = cfg.set["commodity"]["add"]
    RUEAA = codes[codes.index("transport pax RUEAA")]
    assert eval_anno(RUEAA, "demand") is True

    # …with expected units
    assert registry.Unit("Gp km") == eval_anno(RUEAA, "units")

    # Codes for technologies are generated, with annotations giving their units
    codes = cfg.set["technology"]["add"]
    ELC_100 = codes[codes.index("ELC_100")]
    assert registry.Unit("Gv km") == eval_anno(ELC_100, "units")

    # If "ISR" was given as 'regions', then the corresponding config file was loaded
    if regions == "ISR":
        # Check one config value to confirm
        assert {"Israel"} == set(cfg.node_to_census_division.keys())


@pytest.mark.parametrize(
    "options",
    [
        {},
        pytest.param(
            {"mode-share": "default"}, marks=pytest.mark.xfail(raises=TypeError)
        ),
        {"mode_share": "default"},
        {"mode_share": "INVALID"},
        {"futures_scenario": "default"},
        {"futures_scenario": "base"},
        {"futures_scenario": "A---"},
        {"futures_scenario": "debug"},
    ],
)
def test_configure_options(test_context, options):
    """:func:`.configure` operates with various options."""
    Config.from_context(test_context, options=options)
