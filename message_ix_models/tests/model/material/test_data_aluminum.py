import pytest

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.data_aluminum import (
    gen_alumina_trade_tecs,
    gen_refining_hist_act,
    load_bgs_data,
)


@pytest.mark.parametrize(
    "commodity",
    [
        "aluminum",
        "alumina",
    ],
)
def test_load_bgs_data(commodity):
    out = load_bgs_data(commodity)

    # assert that there is an ISO 3166-1 alpha-3 code and R12 region
    # assigned to every timeseries row
    assert not out[["R12", "ISO"]].isna().any(axis=None)


def test_gen_refining_hist_act():
    out = gen_refining_hist_act()
    for v in out.values():
        assert not v.isna().any(axis=None)
    print()


def test_gen_alumina_trade_tecs():
    info = ScenarioInfo()
    info.set["node"] = ["node0", "node1", "R12_GLB"]
    info.set["year"] = [2020, 2025]
    out = gen_alumina_trade_tecs(info)
    for v in out.values():
        assert not v.isna().any(axis=None)
