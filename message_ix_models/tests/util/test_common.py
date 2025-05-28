import genno
import pandas as pd
import pytest
from genno.testing import assert_qty_equal

from message_ix_models.util.common import MappingAdapter, WildcardAdapter


class TestMappingAdapter:
    @pytest.fixture
    def ma0(self) -> MappingAdapter:
        """An adapter that maps on the 'foo' dimension.

        - Coord 'a' is mapped to 'x' and 'y'.
        - Coord 'b' is mapped to 'z'.
        """
        return MappingAdapter({"foo": [("a", "x"), ("a", "y"), ("b", "z")]})

    def test_df(self, ma0: MappingAdapter) -> None:
        """MappingAdapter works on :class:`pandas.DataFrame`."""
        columns = ["foo", "bar", "value"]

        df = pd.DataFrame([["a", "m", 1], ["b", "n", 2]], columns=columns)

        result = ma0(df)

        assert all(columns + ["unit"] == result.columns)

        with pytest.raises(TypeError):
            ma0(1.2)

    def test_qty(self, ma0: MappingAdapter) -> None:
        """MappingAdapter works on :class:`pandas.DataFrame`."""
        idx = pd.MultiIndex.from_tuples([("a", "m"), ("b", "n")], names=["foo", "bar"])
        q = genno.Quantity(pd.Series([1, 2], index=idx), units="kg")

        assert (2, 2) == q.shape

        result = ma0(q)

        # Result has mapped coords along the given dimension
        assert set("xyz") == set(result.coords["foo"].data)

        # Result has a different length along the 'foo' dimension
        assert (3, 2) == result.shape


class TestWildcardAdapter:
    def test_0(self) -> None:
        """Wildcard along one dimension."""
        idx = pd.MultiIndex.from_tuples(
            [("a", "*"), ("a", "n"), ("b", "m"), ("b", "n"), ("c", "*")],
            names=["foo", "bar"],
        )
        q_in = genno.Quantity([0, 1, 2, 3, 4], index=idx)

        wa = WildcardAdapter("bar", ("m", "n"))

        q_out = wa(q_in)

        idx = pd.MultiIndex.from_tuples(
            [("a", "m"), ("a", "n"), ("b", "m"), ("b", "n"), ("c", "m"), ("c", "n")],
            names=["foo", "bar"],
        )
        q_exp = genno.Quantity([0, 1, 2, 3, 4, 4], index=idx, units="")

        assert_qty_equal(q_exp, q_out)
