from message_ix_models.model.structure import get_codes
from message_ix_models.model.transport import testing
from message_ix_models.model.transport.ustimes_ma3t import read_USTIMES_MA3T


@testing.make_mark[5]("R11/ldv-cost-efficiency.xlsx")
def test_read_USTIMES_MA3T():
    """Data from the US-TIMES / MA³T source can be read.

    .. todo:: Adapt to be more like :func:`.test_build.test_debug`, using the output
       of :func:`.ldv.prepare_computer`.
    """
    all_nodes = get_codes("node/R11")
    nodes = all_nodes[all_nodes.index("World")].child
    data = read_USTIMES_MA3T(nodes, "R11")

    # Expected contents
    names = ["fix_cost", "fuel economy", "inv_cost"]
    assert set(names) == set(data.keys())

    # Correct units
    assert data["inv_cost"].units.dimensionality == {"[currency]": 1, "[vehicle]": -1}
    assert data["fix_cost"].units.dimensionality == {"[currency]": 1, "[vehicle]": -1}
    assert data["fuel economy"].units.dimensionality == {
        "[vehicle]": 1,
        "[length]": -1,
        "[mass]": -1,
        "[time]": 2,
    }

    for name in names:
        # Quantity has the expected name
        assert data[name].name == name
        # Quantity has the expected dimensions
        assert {"n", "t", "y"} == set(data[name].dims)
        # Data is returned for all regions
        assert set(data[name].coords["n"].to_index()) == set(map(str, nodes))
