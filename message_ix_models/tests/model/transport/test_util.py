import pandas as pd
import pandas.testing as pdt
import pytest

from message_ix_models.model.transport.config import Config


@pytest.mark.xfail(reason="Refactoring")
def test_add_cl(test_context):
    """:func:`.input_commodity_level` preserves the content of other columns."""
    from message_ix_models.model.transport.util import input_commodity_level

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
