from typing import TYPE_CHECKING

import pandas as pd
import pytest
from genno import Computer

from message_ix_models.tools import unsd

if TYPE_CHECKING:
    from message_ix_models import Context

#: Reference areas used by :func:`gapped_series`, and the alpha-3 codes they map to.
GEO = ("398", "795")
N = ("KAZ", "TKM")


def gapped_series() -> pd.Series:
    """Return a :func:`.unsd.fetch`-like series with gaps.

    Kazakhstan has observations for 2000 and 2002 but not 2001; Turkmenistan has only
    2000. Both gaps must survive to the output: the defect these fixtures guard against
    is a missing observation becoming a zero or an interpolated value.
    """
    other = {"COMMODITY": "B09_TE", "TRANSACTION": "B07_GA", "UNIT": "HSO"}
    index = pd.MultiIndex.from_tuples(
        [
            (g, *other.values(), period)
            for g, period in ((GEO[0], "2000"), (GEO[0], "2002"), (GEO[1], "2000"))
        ],
        names=["REF_AREA", *other, "TIME_PERIOD"],
    )
    return pd.Series([1.0, 3.0, 5.0], index=index, name="value")


class TestMakeKey:
    """The positional key is the part that fails silently.

    A key whose parts are in the wrong dimension order is answered by the service rather
    than rejected, returning data for a selection nobody asked for. These expectations
    are written by hand from the documented dimension order, not generated from
    :data:`.unsd.KEY_ORDER`, so that reordering that constant fails here.
    """

    def test_order(self) -> None:
        # REF_AREA.COMMODITY.TRANSACTION.UNIT
        assert "398+417.B09_TE.B07_GA.HSO" == unsd.make_key(
            geo=("398", "417"), product=("B09_TE",), flow=("B07_GA",)
        )

    def test_unit_always_constrained(self) -> None:
        """The dataflow carries several units; a mixture cannot be summed."""
        key = unsd.make_key(geo=("398",), product=(), flow=())
        assert "HSO" in key.split(".")

    def test_empty_selection_is_unfiltered(self) -> None:
        """An empty tuple must yield an empty key part, not the string '()'."""
        assert ".." in unsd.make_key(geo=("398",), product=(), flow=())

    def test_label_order_is_normalized(self) -> None:
        """Label order within a dimension carries no meaning to the service.

        Two callers naming the same areas in different orders must produce one key, and
        so one entry in the cache of :func:`.unsd.fetch`, rather than two.
        """
        kw: dict = dict(product=("B09_TE",), flow=("B07_GA",))
        assert unsd.make_key(geo=("398", "417"), **kw) == unsd.make_key(
            geo=("417", "398"), **kw
        )


class TestLoadData:
    """Assembly of the returned frame, with :func:`.unsd.fetch` replaced.

    The defect these guard against is a missing observation becoming a zero. UNSD omits
    years a country did not report; if any step reindexes onto a complete year range,
    “not reported” and “reported as zero” become the same value and no downstream
    consumer can tell them apart.
    """

    @pytest.fixture
    def gapped(self, monkeypatch) -> None:
        """Patch :func:`.unsd.fetch` with a series missing 2001 for one country."""
        monkeypatch.setattr(unsd, "fetch", lambda **kwargs: gapped_series())

    def test_gap_is_not_filled(self, gapped) -> None:
        df = unsd.load_data(geo=["398", "795"])

        assert 3 == len(df)
        assert [2000, 2002] == sorted(df.loc[df["n"] == "KAZ", "y"])
        assert 2001 not in set(df["y"])
        assert 0 not in set(df["value"])

    def test_columns_and_dtypes(self, gapped) -> None:
        df = unsd.load_data(geo=["398"])

        assert ["n", "y", "product", "flow", "value"] == list(df.columns)
        assert df["y"].dtype == int
        # Numeric reference-area codes are mapped to alpha-3
        assert set(N) == set(df["n"])

    def test_unmapped_area_raises(self, monkeypatch) -> None:
        """An aggregate label has no alpha-3 code and must not vanish silently."""
        series = gapped_series().rename(index={"398": "1"}, level="REF_AREA")
        monkeypatch.setattr(unsd, "fetch", lambda **kwargs: series)

        with pytest.raises(ValueError, match=r"alpha-3 code for UNSD reference area"):
            unsd.load_data(geo=["1", "795"])

    def test_extra_dimension_raises(self, monkeypatch) -> None:
        """A dimension the module does not know would be dropped silently."""
        series = gapped_series()
        series.index = series.index.rename(["FOO"] + list(series.index.names[1:]))
        monkeypatch.setattr(unsd, "fetch", lambda **kwargs: series)

        with pytest.raises(RuntimeError, match=r"Unexpected dimension\(s\) \['FOO'\]"):
            unsd.load_data(geo=["398"])


class TestUNSDEnergyBalance:
    """The :mod:`genno` path, with :func:`.unsd.fetch` replaced.

    :meth:`.ExoDataSource.add_tasks` adds the transformations selected by
    :attr:`.BaseOptions.aggregate` and :attr:`.BaseOptions.interpolate`. Both are off by
    default for this class. If interpolation were on, the observed periods would be
    replaced by the model periods and the gaps in the fixture data filled by
    extrapolation — the same defect the plain-data-frame tests above guard against, and
    invisible to them because it happens in the Computer rather than in the loader.
    """

    def test_geo_required(self) -> None:
        with pytest.raises(ValueError, match="at least one reference area"):
            unsd.UNSD_ENERGY_BALANCE()

    @pytest.mark.parametrize("aggregate", (False, True))
    def test_add_tasks(
        self, monkeypatch, test_context: "Context", aggregate: bool
    ) -> None:
        monkeypatch.setattr(unsd, "fetch", lambda **kwargs: gapped_series())
        test_context.model.regions = "R12"

        c = Computer()
        keys = unsd.UNSD_ENERGY_BALANCE.add_tasks(
            c, context=test_context, geo=GEO, aggregate=aggregate
        )
        result = c.get(keys[0])

        # Dimensions and units match .IEA_EWEB
        assert {"n", "y", "product", "flow"} == set(result.dims)
        assert "terajoule" == f"{result.units}"

        # Only the observed periods appear: not the missing 2001, and not the model
        # periods that interpolation would substitute
        assert {2000, 2002} == set(result.coords["y"].data)

        n = set(result.coords["n"].data)
        if aggregate:
            # Countries are replaced by the R12 node(s) containing them
            assert n and not (n & set(N))
            assert all(label.startswith("R12_") for label in n)
        else:
            assert set(N) == n
