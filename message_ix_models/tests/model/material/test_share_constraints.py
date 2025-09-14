from typing import TYPE_CHECKING

import pandas as pd
import pytest
from message_ix import make_df

from message_ix_models.model.material.share_constraints import (
    add_new_share_cat,
    gen_com_share_df,
    gen_comm_map,
    gen_comm_shr_map,
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


def test_add_new_share_cat(scenario) -> None:
    add_new_share_cat(
        scenario, "test", "test_total", "test_share", ["coal_i"], ["coal_i", "elec_i"]
    )


def test_gen_comm_map(scenario) -> None:
    df = gen_comm_map(scenario, "test", "test", ["coal_i"], ["R12_AFR"])
    assert sorted(make_df("map_shares_commodity_share", value=1.0).columns) == sorted(
        df.columns
    )


def test_gen_comm_shr_map(scenario) -> None:
    gen_comm_shr_map(scenario, "test", "test_tot", "test_shr", ["coali"], ["coali"])
