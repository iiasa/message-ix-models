from typing import TYPE_CHECKING

import pandas as pd
from genno import Computer

import message_ix_models.util.sdmx
from message_ix_models.tools.eurostat import ESTAT_ENERGY_BALANCE

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
