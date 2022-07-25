from message_ix_models.tools.advance import DIMS, advance_data, get_advance_data


def test_get_advance_data():
    # Returns a pd.Series with the expected index levels
    result = get_advance_data()
    assert DIMS == result.index.names

    # Returns a genno.Quantity with the expected units
    result = advance_data("Transport|Service demand|Road|Freight")
    assert {"[length]": 1, "[mass]": 1, "[time]": -1} == result.units.dimensionality
