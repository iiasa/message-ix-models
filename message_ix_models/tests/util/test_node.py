"""Tests of :mod:`message_ix_models.util.node`."""
import re

import pandas as pd
import pytest
from genno import Quantity
from message_ix import Scenario, make_df

from message_ix_models.model.bare import create_res
from message_ix_models.model.structure import get_codes
from message_ix_models.util import (
    adapt_R11_R12,
    adapt_R11_R14,
    broadcast,
    identify_nodes,
)
from message_ix_models.util.node import MappingAdapter


def test_mapping_adapter():
    """Generic test of MappingAdapter."""
    a = MappingAdapter({"foo": [("a", "x"), ("a", "y"), ("b", "z")]})

    columns = ["foo", "bar", "value"]

    df = pd.DataFrame([["a", "m", 1], ["b", "n", 2]], columns=columns)

    result = a(df)

    assert all(columns + ["unit"] == result.columns)

    with pytest.raises(TypeError):
        a(1.2)


PAR = "technical_lifetime"
VALUE = [0.1, 0.2]


@pytest.fixture(scope="function")
def input():
    """Fixture: test data for :obj:`.adapt_R11_R14`."""
    R11_all = get_codes("node/R11")
    R11_reg = R11_all[R11_all.index("World")].child
    df = make_df(
        PAR, technology="coal_ppl", year_vtg=[2021, 2022], value=[1.2, 3.4], unit="year"
    ).pipe(broadcast, node_loc=R11_reg)

    # Set a specific value for the regions to be broadcast
    df["value"] = df["value"].where(df["node_loc"] != "R11_CPA", VALUE[0])
    df["value"] = df["value"].where(df["node_loc"] != "R11_FSU", VALUE[1])

    return {PAR: df}


@pytest.mark.parametrize(
    "func,N,expected,target_nodes",
    [
        (adapt_R11_R12, 12, VALUE[0], ("R12_CHN", "R12_RCPA")),
        (adapt_R11_R14, 14, VALUE[1], ("R14_CAS", "R14_RUS", "R14_SCS", "R14_UBM")),
    ],
)
def test_adapt_df(input, func, N, expected, target_nodes):
    """:obj:`.adapt_R11_R14` handles :class:`pandas.DataFrame`."""
    # Function runs
    output = func(input)

    # Output is a dict containing 1 entry
    df_out = output.pop(PAR)
    assert 0 == len(output)

    # Output covers all R14 regions
    all_nodes = get_codes(f"node/R{N}")
    regions = all_nodes[all_nodes.index("World")].child
    assert set(regions) == set(df_out["node_loc"])

    # Output has expected length
    assert N * 2 == len(df_out)

    # Output values for new regions match input value for the base region
    assert (expected == df_out[df_out["node_loc"].isin(target_nodes)]["value"]).all()


@pytest.mark.parametrize(
    "func,expected,node_loc",
    [(adapt_R11_R12, VALUE[0], "R12_CHN"), (adapt_R11_R14, VALUE[1], "R14_CAS")],
)
def test_adapt_qty(input, func, expected, node_loc):
    """:obj:`.adapt_R11_R14` handles :class:`genno.Quantity`."""
    # Convert to genno.Quantity
    df = input[PAR]
    input[PAR] = Quantity.from_series(df.set_index(df.columns[:-2].tolist())["value"])
    input[PAR].attrs["_unit"] = df["unit"].unique()[0]

    # Function runs
    output = func(input)

    # Output is a dict containing 1 entry
    qty_out = output.pop(PAR)
    assert 0 == len(output)

    assert isinstance(qty_out, Quantity)
    assert "year" == qty_out.attrs["_unit"]

    # Output values for new regions match input value for the base region
    assert (
        expected == qty_out.sel(node_loc=node_loc, technology="coal_ppl", year=2022)
    ).all()


@pytest.mark.parametrize("regions", ["R11", "R12", "R14"])
def test_identify_nodes(caplog, test_context, regions):
    ctx = test_context
    ctx.regions = regions
    scenario = create_res(ctx)

    # The ID of the node codelist can be recovered from scenario contents
    assert regions == identify_nodes(scenario)

    # A node like "R11_GLB" is ignored
    with scenario.transact():
        scenario.add_set("node", f"{regions}_GLB")
    assert regions == identify_nodes(scenario)

    # Remove one element from the node set
    with scenario.transact():
        scenario.remove_set("node", scenario.set("node").tolist()[0])

    # No longer any match
    with pytest.raises(
        ValueError, match=f"IDs suggest codelist {repr(regions)}, values do not match"
    ):
        identify_nodes(scenario)


def test_identify_nodes1(test_context):
    mp = test_context.get_platform()
    scenario = Scenario(
        mp, model="identify_nodes", scenario="identify_nodes", version="new"
    )
    scenario.add_set("technology", "t")
    scenario.add_set("year", 0)
    scenario.commit("")

    with scenario.transact():
        scenario.add_set("node", "R99_ZZZ")

    with pytest.raises(
        ValueError,
        match=re.escape("Couldn't identify node codelist from ['R99_ZZZ', 'World']"),
    ):
        identify_nodes(scenario)
