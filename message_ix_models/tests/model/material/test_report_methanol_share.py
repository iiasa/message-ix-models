"""The origin share of final methanol is 0 where the final input is solver noise, and within [0, 1]."""

import genno
import pandas as pd
import pytest

from message_ix_models.model.material.report.reporter_utils import (
    _methanol_final_share,
)


def _q(values):
    idx = pd.MultiIndex.from_tuples(
        [("R12_CHN", 2060), ("R12_CHN", 2070), ("R12_AFR", 2070)], names=["nl", "ya"]
    )
    return genno.Quantity(pd.Series(values, index=idx))


def test_noise_final_input_gives_zero_share():
    # R12_CHN 2070: 19.6 GWa supplied, meth_t_d at 1e-6 GWa.
    out = _methanol_final_share(_q([10.0, 19.6, 3.0]), _q([20.0, 1e-6, 9.6]))
    assert out.to_series().tolist() == pytest.approx([0.5, 0.0, 3.0 / 9.6])


def test_share_is_clipped_to_unit_interval():
    out = _methanol_final_share(_q([30.0, -2.0, 9.6]), _q([20.0, 10.0, 9.6]))
    assert out.to_series().tolist() == pytest.approx([1.0, 0.0, 1.0])
