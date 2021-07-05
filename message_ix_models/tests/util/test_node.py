"""Tests of :mod:`message_ix_models.util.node`."""
import pytest
from genno import Quantity
from message_ix import make_df

from message_ix_models.model.structure import get_codes
from message_ix_models.util import adapt_R11_R14, broadcast

PAR = "technical_lifetime"
VALUE = 0.1


@pytest.fixture(scope="module")
def input():
    """Fixture: test data for :func:`.adapt_R11_R14`."""
    R11_all = get_codes("node/R11")
    R11_reg = R11_all[R11_all.index("World")].child
    df = make_df(
        PAR, technology="coal_ppl", year_vtg=[2021, 2022], value=[1.2, 3.4], unit="year"
    ).pipe(broadcast, node_loc=R11_reg)

    # Set a specific value for the regions to be broadcast
    df["value"] = df["value"].where(df["node_loc"] != "R11_FSU", VALUE)

    return {PAR: df}


def test_adapt_R11_R14_0(input):
    """:func:`.adapt_R11_R14` handles :class:`pandas.DataFrame`."""
    # Function runs
    output = adapt_R11_R14(input)

    # Output is a dict containing 1 entry
    df_out = output.pop(PAR)
    assert 0 == len(output)

    # Output covers all R14 regions
    R14_all = get_codes("node/R14")
    R14_reg = R14_all[R14_all.index("World")].child
    assert set(R14_reg) == set(df_out["node_loc"])

    # Output has expected length
    assert 14 * 2 == len(df_out)

    # Output values for new regions match input value for R11_FSU
    target_nodes = ("R14_CAS", "R14_RUS", "R14_SCS", "R14_UBM")
    assert (VALUE == df_out[df_out["node_loc"].isin(target_nodes)]["value"]).all()


def test_adapt_R11_R14_1(input):
    """:func:`.adapt_R11_R14` handles :class:`genno.Quantity`."""
    # Convert to genno.Quantity
    df = input[PAR]
    input[PAR] = Quantity.from_series(df.set_index(df.columns[:-2].tolist())["value"])
    input[PAR].attrs["_unit"] = df["unit"].unique()[0]

    # Function runs
    output = adapt_R11_R14(input)

    # Output is a dict containing 1 entry
    qty_out = output.pop(PAR)
    assert 0 == len(output)

    assert isinstance(qty_out, Quantity)
    assert "year" == qty_out.attrs["_unit"]

    # Output values for new regions match input value for R11_FSU
    assert (
        VALUE == qty_out.sel(node_loc="R14_CAS", technology="coal_ppl", year=2022)
    ).all()
