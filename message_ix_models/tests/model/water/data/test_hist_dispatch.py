"""Tests for historical supply-side dispatch."""

import pandas as pd
import pytest

from message_ix_models.model.water.data.demands import groundwater_share_floor
from message_ix_models.model.water.data.hist_dispatch import (
    cap_surfacewater_for_gw_floor,
    merit_order_dispatch,
)


def _s(d):
    return pd.Series(d, dtype=float)


def test_dispatch_uses_node_level_cost_order_and_backstop():
    out = merit_order_dispatch(
        capacity={
            "cheap_a": _s({"A": 30.0, "B": 30.0}),
            "backstop": _s({"A": float("inf"), "B": float("inf")}),
        },
        inputs={
            "cheap_a": {"electr": _s({"A": 0.1, "B": 10.0})},
            "backstop": {"electr": 1.0},
        },
        var_cost={},
        demand=_s({"A": 100.0, "B": 10.0}),
        commodity_prices={"electr": 1.0},
    )

    assert out["cheap_a"]["A"] == pytest.approx(30.0)
    assert out["backstop"]["A"] == pytest.approx(70.0)
    assert out["cheap_a"]["B"] == 0.0
    assert out["backstop"]["B"] == pytest.approx(10.0)


def test_dispatch_rejects_infeasible_demand():
    with pytest.raises(ValueError, match="Infeasible dispatch"):
        merit_order_dispatch(
            capacity={"a": _s({"A": 30.0}), "b": _s({"A": 20.0})},
            inputs={"a": {"electr": 1.0}, "b": {"electr": 2.0}},
            var_cost={},
            demand=_s({"A": 100.0}),
            commodity_prices={"electr": 1.0},
        )


def test_groundwater_share_floor_matches_definition():
    df_sw = pd.DataFrame({"value": [3.0, 0.0, 9.0]})
    df_gw = pd.DataFrame({"value": [1.0, 0.0, 1.0]})
    out = groundwater_share_floor(df_sw, df_gw, buffer=0.95)
    assert out.tolist() == pytest.approx([0.25 * 0.95, 0.0, 0.1 * 0.95])


def test_surfacewater_cap_reserves_groundwater_floor():
    demand = _s({"A": 100.0})
    gw_floor = _s({"A": 0.4})
    sw_cap = cap_surfacewater_for_gw_floor(_s({"A": 90.0}), demand, gw_floor)

    capacity = {
        "extract_surfacewater": sw_cap,
        "extract_groundwater": _s({"A": 10.0}),
        "extract_gw_fossil": _s({"A": float("inf")}),
    }
    inputs = {
        "extract_surfacewater": {"electr": 1.0},
        "extract_groundwater": {"electr": 2.0},
        "extract_gw_fossil": {"electr": 10.0},
    }
    out = merit_order_dispatch(capacity, inputs, {}, demand, {"electr": 1.0})

    total_gw = out["extract_groundwater"]["A"] + out["extract_gw_fossil"]["A"]
    assert total_gw >= 0.4 * 100.0 - 1e-9
    assert out["extract_surfacewater"]["A"] == pytest.approx(60.0)
