from typing import TYPE_CHECKING

import pandas as pd
import pytest
from genno import Computer

from message_ix_models.tools import eurostat

if TYPE_CHECKING:
    from message_ix_models import Context

#: Reference areas used by :func:`gapped_series`, and the alpha-3 codes they map to.
GEO = ("RS", "AL")
N = ("SRB", "ALB")


def gapped_series() -> pd.Series:
    """Return a :func:`.eurostat.fetch`-like series with gaps.

    Serbia has observations for 2000 and 2002 but not 2001; Albania has only 2000. Both
    gaps must survive to the output: the defect these fixtures guard against is a
    missing observation becoming a zero or an interpolated value.
    """
    other = {"freq": "A", "nrg_bal": "GAE", "siec": "TOTAL", "unit": "TJ"}
    index = pd.MultiIndex.from_tuples(
        [
            (g, *other.values(), period)
            for g, period in ((GEO[0], "2000"), (GEO[0], "2002"), (GEO[1], "2000"))
        ],
        names=["geo", *other, "TIME_PERIOD"],
    )
    return pd.Series([1.0, 3.0, 5.0], index=index, name="value")


class TestMakeKey:
    """The positional key is the part that fails silently.

    A key whose parts are in the wrong dimension order is answered by the service rather
    than rejected, returning data for a selection nobody asked for. These expectations
    are written by hand from the documented dimension order, not generated from
    :data:`.eurostat.KEY_ORDER`, so that reordering that constant fails here.
    """

    def test_order(self) -> None:
        # freq.nrg_bal.siec.unit.geo — the flow dimension precedes the product
        # dimension, and geo comes last; both are the reverse of .tools.unsd.
        assert "A.GAE.TOTAL.TJ.AL+RS" == eurostat.make_key(
            geo=("RS", "AL"), product=("TOTAL",), flow=("GAE",)
        )

    def test_unit_always_constrained(self) -> None:
        """The dataflow carries several units; a mixture cannot be summed."""
        key = eurostat.make_key(geo=("RS",), product=(), flow=())
        assert "TJ" in key.split(".")

    def test_empty_selection_is_unfiltered(self) -> None:
        """An empty tuple must yield an empty key part, not the string '()'."""
        assert ".." in eurostat.make_key(geo=("RS",), product=(), flow=())

    def test_label_order_is_normalized(self) -> None:
        """Label order within a dimension carries no meaning to the service.

        Two callers naming the same areas in different orders must produce one key, and
        so one entry in the cache of :func:`.eurostat.fetch`, rather than two.
        """
        kw: dict = dict(product=("TOTAL",), flow=("GAE",))
        assert eurostat.make_key(geo=("RS", "AL"), **kw) == eurostat.make_key(
            geo=("AL", "RS"), **kw
        )


class TestLoadData:
    """Assembly of the returned frame, with :func:`.eurostat.fetch` replaced.

    The defect these guard against is a missing observation becoming a zero. Eurostat
    omits years a country did not report, and coverage begins well after 2000 for
    several non-member countries; if any step reindexes onto a complete year range,
    “not reported” and “reported as zero” become the same value and no downstream
    consumer can tell them apart.
    """

    @pytest.fixture
    def gapped(self, monkeypatch) -> None:
        """Patch :func:`.eurostat.fetch` with a series missing 2001 for one country."""
        monkeypatch.setattr(eurostat, "fetch", lambda **kwargs: gapped_series())

    def test_gap_is_not_filled(self, gapped) -> None:
        df = eurostat.load_data(geo=["RS", "AL"])

        assert 3 == len(df)
        assert [2000, 2002] == sorted(df.loc[df["n"] == "SRB", "y"])
        assert 2001 not in set(df["y"])
        assert 0 not in set(df["value"])

    def test_columns_and_dtypes(self, gapped) -> None:
        df = eurostat.load_data(geo=["RS"])

        assert ["n", "y", "product", "flow", "value"] == list(df.columns)
        assert df["y"].dtype == int
        assert set(N) == set(df["n"])

    @pytest.mark.parametrize("label, expected", (("EL", "GRC"), ("UK", "GBR")))
    def test_non_iso_label(self, monkeypatch, label: str, expected: str) -> None:
        """Eurostat retains labels that are not ISO 3166-1 alpha-2.

        :func:`.iso_3166_alpha_3` returns :any:`None` for each of them, so without
        :data:`.eurostat.GEO` these countries would not raise — they would map to NaN
        and vanish from the result.
        """
        series = gapped_series().rename(index={"RS": label}, level="geo")
        monkeypatch.setattr(eurostat, "fetch", lambda **kwargs: series)

        df = eurostat.load_data(geo=[label])
        assert expected in set(df["n"])

    def test_unmapped_area_raises(self, monkeypatch) -> None:
        """An aggregate label has no alpha-3 code and must not vanish silently."""
        series = gapped_series().rename(index={"RS": "EU27_2020"}, level="geo")
        monkeypatch.setattr(eurostat, "fetch", lambda **kwargs: series)

        with pytest.raises(
            ValueError, match=r"alpha-3 code for Eurostat reference area"
        ):
            eurostat.load_data(geo=["EU27_2020", "AL"])

    def test_extra_dimension_raises(self, monkeypatch) -> None:
        """A dimension the module does not know would be dropped silently."""
        series = gapped_series()
        series.index = series.index.rename(["FOO"] + list(series.index.names[1:]))
        monkeypatch.setattr(eurostat, "fetch", lambda **kwargs: series)

        with pytest.raises(RuntimeError, match=r"Unexpected dimension\(s\) \['FOO'\]"):
            eurostat.load_data(geo=["RS"])


class TestESTATEnergyBalance:
    """The :mod:`genno` path, with :func:`.eurostat.fetch` replaced.

    :meth:`.ExoDataSource.add_tasks` adds the transformations selected by
    :attr:`.BaseOptions.aggregate` and :attr:`.BaseOptions.interpolate`. Both are off by
    default for this class. If interpolation were on, the observed periods would be
    replaced by the model periods and the gaps in the fixture data filled by
    extrapolation — the same defect the plain-data-frame tests above guard against, and
    invisible to them because it happens in the Computer rather than in the loader.
    """

    def test_geo_required(self) -> None:
        with pytest.raises(ValueError, match="at least one reference area"):
            eurostat.ESTAT_ENERGY_BALANCE()

    @pytest.mark.parametrize("aggregate", (False, True))
    def test_add_tasks(
        self, monkeypatch, test_context: "Context", aggregate: bool
    ) -> None:
        monkeypatch.setattr(eurostat, "fetch", lambda **kwargs: gapped_series())
        test_context.model.regions = "R12"

        c = Computer()
        keys = eurostat.ESTAT_ENERGY_BALANCE.add_tasks(
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
