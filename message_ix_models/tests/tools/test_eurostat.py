from typing import TYPE_CHECKING

import pandas as pd
import pytest
from genno import Computer, Quantity

import message_ix_models.util.sdmx
from message_ix_models.tools.eurostat import (
    ESTAT_ENERGY_BALANCE,
    ESTAT_ENERGY_BALANCE_UNSD,
    to_unsd_vocabulary,
)
from message_ix_models.util import package_data_path

if TYPE_CHECKING:
    from message_ix_models import Context

#: Serbia and Greece.
N = ("RS", "EL")


def balance_series() -> pd.Series:
    """Like data from ``NRG_BAL_C``: 2 areas; period 2001 not reported."""
    index = pd.MultiIndex.from_tuples(
        [
            ("A", "GAE", "TOTAL", "TJ", n, y)
            for n, y in ((N[0], "2000"), (N[0], "2002"), (N[1], "2000"))
        ],
        names=["freq", "nrg_bal", "siec", "unit", "geo", "TIME_PERIOD"],
    )
    return pd.Series([1.0, 3.0, 5.0], index=index, name="value")


class TestESTAT_ENERGY_BALANCE:
    def test_add_tasks(self, monkeypatch, test_context: "Context") -> None:
        monkeypatch.setattr(
            message_ix_models.util.sdmx, "fetch_data", lambda *a, **kw: balance_series()
        )

        c = Computer()
        keys = ESTAT_ENERGY_BALANCE.add_tasks(c, context=test_context, n=N)
        result = c.get(keys[0])

        assert {"n", "y", "product", "flow"} == set(result.dims)
        assert "terajoule" == f"{result.units}"
        # Periods not in the source are not filled
        assert {2000, 2002} == set(result.coords["y"].data)
        # Labels—including "EL", not an ISO 3166-1 alpha-2 code—are mapped to alpha-3
        assert {"SRB", "GRC"} == set(result.coords["n"].data)


def unsd_series() -> pd.Series:
    """Like data from ``NRG_BAL_C`` for Serbia, 2000, with codes that aggregate."""
    rows = {
        ("IMP", "C0110"): 1.0,  # Anthracite, part of B00_CL
        ("IMP", "C0220"): 2.0,  # Lignite, part of B00_CL
        ("IMP", "C0311"): 4.0,  # Coke oven coke: a coal product, B01_CP
        ("EXP", "G3000"): 5.0,  # Natural gas exports: positive in Eurostat
        ("TI_EHG_E", "G3000"): 8.0,  # Natural gas into power plants: an input
        ("TO_EHG", "E7000"): 16.0,  # Electricity from all plants, including…
        ("TO_EHG_PH", "E7000"): 2.0,  # …pumped storage, and from…
        ("TI_EHG_E", "RA100"): 6.0,  # …hydro, which is primary production for UNSD
        ("FC_OTH_AF_E", "E7000"): 1.0,  # Agriculture and forestry, part of B48_1232
        ("FC_OTH_FISH_E", "E7000"): 2.0,  # Fishing, part of B48_1232
    }
    index = pd.MultiIndex.from_tuples(
        [("A", flow, product, "TJ", "RS", "2000") for flow, product in rows],
        names=["freq", "nrg_bal", "siec", "unit", "geo", "TIME_PERIOD"],
    )
    return pd.Series(list(rows.values()), index=index, name="value")


class TestESTAT_ENERGY_BALANCE_UNSD:
    def test_query_labels(self) -> None:
        # UNSD codes are translated to the Eurostat labels that make them up
        source = ESTAT_ENERGY_BALANCE_UNSD(n=N, product=("B04_NG",), flow=("B48_1232",))
        assert ("G3000",) == source.query_key["siec"]
        assert ("FC_OTH_AF_E", "FC_OTH_FISH_E") == source.query_key["nrg_bal"]

        # A transformation flow needs both the output and the input labels
        source = ESTAT_ENERGY_BALANCE_UNSD(n=N, product=("B07_EL",), flow=("B11_088",))
        assert {"E7000", "RA100", "RA300", "RA420", "RA500"} == set(
            source.query_key["siec"]
        )
        assert {"TO_EHG", "TI_EHG_E", "TO_EHG_PH"} == set(source.query_key["nrg_bal"])

        # Empty options select every mapped label
        source = ESTAT_ENERGY_BALANCE_UNSD(n=N)
        assert "C0220" in source.query_key["siec"]
        assert "FC_E" in source.query_key["nrg_bal"]

        with pytest.raises(ValueError, match="product code"):
            ESTAT_ENERGY_BALANCE_UNSD(n=N, product=("G3000",))

    def test_add_tasks(self, monkeypatch, test_context: "Context") -> None:
        monkeypatch.setattr(
            message_ix_models.util.sdmx, "fetch_data", lambda *a, **kw: unsd_series()
        )

        c = Computer()
        keys = ESTAT_ENERGY_BALANCE_UNSD.add_tasks(c, context=test_context, n=("RS",))
        result = c.get(keys[0]).to_series()

        assert "terajoule" == f"{c.get(keys[0]).units}"
        # Aggregated to UNSD codes; exports and plant inputs negated; renewable
        # electricity moved to primary production; labels mapped to alpha-3
        assert {
            ("SRB", 2000, "B00_CL", "B02_03"): 3.0,
            ("SRB", 2000, "B01_CP", "B02_03"): 4.0,
            ("SRB", 2000, "B04_NG", "B03_04"): -5.0,
            ("SRB", 2000, "B04_NG", "B11_088"): -8.0,
            ("SRB", 2000, "B07_EL", "B01_01"): 6.0,
            ("SRB", 2000, "B07_EL", "B11_088"): 16.0 - 2.0 - 6.0,
            ("SRB", 2000, "B07_EL", "B48_1232"): 3.0,
        } == result.reorder_levels(["n", "y", "product", "flow"]).to_dict()

    def test_reconcile(self, monkeypatch, test_context: "Context") -> None:
        """Converted Eurostat data reproduce UNSD's own balance for the same country.

        Natural gas, electricity, and heat agree to within 0.1 % in every flow, which
        checks the transformation, renewable, and pumped-storage conventions. For coal
        and oil the two services publish different values from the same national
        return—UNSD's are uniformly lower for lignite consumption, by 18–24 %, and for
        crude oil, by 4–7 %—so those products are held to looser tolerances. Flows of
        less than 1000 TJ, 0.2 % of total supply, are not compared.
        """
        data = pd.read_csv(
            package_data_path("test", "eurostat", "NRG_BAL_C.csv"),
            comment="#",
            dtype={"TIME_PERIOD": str},
        )
        index = ["freq", "nrg_bal", "siec", "unit", "geo", "TIME_PERIOD"]
        monkeypatch.setattr(
            message_ix_models.util.sdmx,
            "fetch_data",
            lambda *a, **kw: data.set_index(index)["value"],
        )
        expected = pd.read_csv(
            package_data_path("test", "unsd", "DF_UNData_EnergyBalance.csv"),
            comment="#",
        ).set_index(["COMMODITY", "TRANSACTION", "TIME_PERIOD"])["value"]

        c = Computer()
        keys = ESTAT_ENERGY_BALANCE_UNSD.add_tasks(c, context=test_context, n=("RS",))
        result = (
            c.get(keys[0])
            .to_series()
            .droplevel("n")
            .reorder_levels(["product", "flow", "y"])
            .rename_axis(expected.index.names)
        )

        # Every UNSD cell has a counterpart
        expected = expected[expected.abs() >= 1000]
        assert expected.index.isin(result.index).all()

        tolerance = {"B04_NG": 1e-3, "B07_EL": 1e-3, "B08_HT": 1e-3, "B00_CL": 0.35}
        for product, exp in expected.groupby(level=0):
            obs = result.reindex(exp.index)
            assert obs.to_dict() == pytest.approx(
                exp.to_dict(), rel=tolerance.get(product, 0.1)
            ), product


def test_to_unsd_vocabulary_unmapped() -> None:
    index = pd.MultiIndex.from_tuples(
        [("RS", 2000, "O4000XBIO", "IMP")], names=["n", "y", "product", "flow"]
    )
    with pytest.raises(ValueError, match="Unmapped Eurostat product"):
        to_unsd_vocabulary(Quantity(pd.Series([1.0], index=index), units="TJ"))
