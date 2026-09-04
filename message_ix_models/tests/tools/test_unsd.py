from typing import TYPE_CHECKING

import pandas as pd
import pytest
from genno import Computer

import message_ix_models.util.sdmx
from message_ix_models.tools.unsd import UNSD_ENERGY, UNSD_ENERGY_BALANCE
from message_ix_models.util import package_data_path

if TYPE_CHECKING:
    from message_ix_models import Context

#: Kazakhstan and Turkmenistan.
N = ("398", "795")


def balance_series() -> pd.Series:
    """Like data from ``DF_UNData_EnergyBalance``: 2 areas; period 2001 not reported."""
    index = pd.MultiIndex.from_tuples(
        [
            (n, "B09_TE", "B07_GA", "HSO", y)
            for n, y in ((N[0], "2000"), (N[0], "2002"), (N[1], "2000"))
        ],
        names=["REF_AREA", "COMMODITY", "TRANSACTION", "UNIT", "TIME_PERIOD"],
    )
    return pd.Series([1.0, 3.0, 5.0], index=index, name="value")


def commodity_frame(rows) -> pd.DataFrame:
    """Like data from ``DF_UNDATA_ENERGY`` with attributes, for Kazakhstan, 2020."""
    columns = [
        "COMMODITY",
        "TRANSACTION",
        "value",
        "UNIT_MEASURE",
        "UNIT_MULT",
        "CONVERSION_FACTOR",
    ]
    return (
        pd.DataFrame(list(rows), columns=columns)
        .assign(FREQ="A", REF_AREA="398", TIME_PERIOD="2020")
        .set_index(["FREQ", "REF_AREA", "COMMODITY", "TRANSACTION", "TIME_PERIOD"])
    )


def patch_fetch_data(monkeypatch, data) -> None:
    """Replace :func:`.fetch_data` with a function returning `data`."""
    monkeypatch.setattr(
        message_ix_models.util.sdmx, "fetch_data", lambda *a, **kw: data
    )


class TestUNSD_ENERGY_BALANCE:
    def test_n_required(self) -> None:
        with pytest.raises(ValueError, match="n="):
            UNSD_ENERGY_BALANCE()

    def test_add_tasks(self, monkeypatch, test_context: "Context") -> None:
        patch_fetch_data(monkeypatch, balance_series())

        c = Computer()
        keys = UNSD_ENERGY_BALANCE.add_tasks(c, context=test_context, n=N)
        result = c.get(keys[0])

        assert {"n", "y", "product", "flow"} == set(result.dims)
        assert "terajoule" == f"{result.units}"
        # Periods not in the source are not filled
        assert {2000, 2002} == set(result.coords["y"].data)
        # Numeric codes are mapped to alpha-3
        assert {"KAZ", "TKM"} == set(result.coords["n"].data)

    def test_unexpected_dimension(self, monkeypatch) -> None:
        patch_fetch_data(
            monkeypatch, pd.concat({"A": balance_series()}, names=["FREQ"])
        )

        with pytest.raises(ValueError, match="FREQ"):
            UNSD_ENERGY_BALANCE(n=N).get()


class TestUNSD_ENERGY:
    def test_add_tasks(self, monkeypatch, test_context: "Context") -> None:
        rows = [
            ("3000", "12", 100.0, "TJ", "0", "1.0"),  # Natural gas: gross → net
            ("4670", "12", 1.0, "TN", "3", "43.0"),  # Thousand tonnes → TJ
            ("7000", "12", 1.0, "GWHR", "0", "3.6"),  # GWh → TJ
            ("EC", "133", 20.0, "MW", "0", ""),  # Capacity: no conversion factor
        ]
        patch_fetch_data(monkeypatch, commodity_frame(rows))

        c = Computer()
        keys = UNSD_ENERGY.add_tasks(c, context=test_context, n=("398",))
        result = c.get(keys[0])

        assert {"n", "y", "product", "flow"} == set(result.dims)
        assert "terajoule" == f"{result.units}"
        # Observations that cannot be converted to TJ are dropped
        assert [90.0, 43.0, 3.6] == pytest.approx(
            result.to_series().sort_index().tolist()
        )

    def test_reconcile(self, monkeypatch, test_context: "Context") -> None:
        """Sums of commodity data reproduce the fuel groups of the energy balance."""
        data = pd.read_csv(
            package_data_path("test", "unsd", "DF_UNDATA_ENERGY.csv"),
            comment="#",
            dtype={"COMMODITY": str, "TRANSACTION": str, "UNIT_MULT": str},
        )
        patch_fetch_data(
            monkeypatch,
            commodity_frame(data.drop(columns="GROUP").itertuples(index=False)),
        )

        # Values from DF_UNData_EnergyBalance for the same area, period, and transaction
        expected = {
            "B00_CL": 332276.142292,
            "B01_CP": 40949.378566,
            "B02_PO": 119.41582805,
            "B03_OP": 508430.6796,
            "B04_NG": 256043.745,
            "B05_BW": 2218.146,
            "B07_EL": 252440.3952,
            "B08_HT": 275947.818,
        }

        c = Computer()
        keys = UNSD_ENERGY.add_tasks(c, context=test_context, n=("398",))
        result = c.get(keys[0]).to_series().rename("value").reset_index()
        group = result["product"].map(data.set_index("COMMODITY")["GROUP"])

        assert expected == pytest.approx(result.groupby(group)["value"].sum().to_dict())
