from typing import TYPE_CHECKING

import pandas as pd
import pytest
from message_ix import make_df

from message_ix_models.model.material.share_constraints import (
    CommShareConfig,
    gen_com_share_df,
)
from message_ix_models.testing import bare_res

if TYPE_CHECKING:
    from message_ix import Scenario
    from pytest import FixtureRequest

    from message_ix_models import Context


@pytest.fixture
def scenario(request: "FixtureRequest", test_context: "Context") -> "Scenario":
    """Same fixture as in :mod:`.test_tools`."""
    test_context.model.regions = "R12"
    scen = bare_res(request, test_context, solved=False)

    # add R12_GLB region required for steel trade model
    with scen.transact():
        scen.add_set("node", "R12_GLB")
    # add parameter data required for tests
    dims = dict(
        year_act=2020,
        year_vtg=2020,
        technology="coal_i",
        commodity="coal",
        node_loc=["R12_AFR"],
        node_origin=["R12_AFR"],
        mode="M1",
        level="final",
        time="year",
        time_origin="year",
        value=1,
        unit="???",
    )
    inp = make_df("input", **dims)
    with scen.transact():
        scen.add_set("mode", "M1")
        scen.add_par("input", inp)
    return scen


def test_gen_com_share_df() -> None:
    df = gen_com_share_df("test", pd.DataFrame().assign(node=["node1"]))
    assert (make_df("share_commodity_up").columns == df.columns).all()


def test_gen_comm_map(scenario) -> None:
    cfg = CommShareConfig.from_files(scenario, "coal_residual_industry")
    cfg.nodes = ["R12_AFR"]
    cfg.share_name = "test"
    cfg.type_tec_all_name = "test"
    df = cfg.get_map_share_set_total(scenario)
    dims = {
        "node": "R12_AFR",
        "node_share": "R12_AFR",
        "commodity": "coal",
        "level": "final",
        "mode": "M1",
        "shares": "test",
        "type_tec": "test",
    }
    df_expected = pd.DataFrame(dims, index=[0])
    assert sorted(df_expected.columns) == sorted(df.columns)
    assert pd.concat([df, df_expected]).drop_duplicates(keep=False).empty
