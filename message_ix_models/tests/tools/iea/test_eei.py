from typing import TYPE_CHECKING

import genno
import pandas as pd
import pytest

from message_ix_models.tools.iea.eei import IEA_EEI

if TYPE_CHECKING:
    from message_ix_models import Context

# Infill data for R12 nodes not present in the IEA data
# NB these are hand-picked as of 2022-07-20 so that the ratio of freight activity / GDP
#    is roughly consistent across regions
# FIXME replace with actual data
R12_MAP = [
    ("R12_EEU", "R12_AFR"),
    ("R12_NAM", "R12_CHN"),
    ("R12_EEU", "R12_EEU"),
    ("R12_FSU", "R12_FSU"),
    ("R12_LAM", "R12_LAM"),
    ("R12_PAO", "R12_MEA"),
    ("R12_NAM", "R12_NAM"),
    ("R12_PAO", "R12_PAO"),
    ("R12_LAM", "R12_PAS"),
    ("R12_LAM", "R12_RCPA"),
    ("R12_WEU", "R12_SAS"),
    ("R12_WEU", "R12_WEU"),
]


class TestIEA_EEI:
    @pytest.mark.usefixtures("iea_eei_user_data")
    @pytest.mark.parametrize(
        "source_kw, dimensionality",
        (
            (
                dict(
                    measure="Passenger load factor",
                    # broadcast_map="bc:n-n2",
                ),
                {"Mode/vehicle type", "SECTOR"},
            ),
        ),
    )
    @pytest.mark.parametrize(
        "regions, aggregate, N_n",
        (
            ("R12", False, 28),
            ("R12", True, 5),
        ),
    )
    def test_add_tasks(
        self,
        test_context: "Context",
        source_kw: dict,
        dimensionality: set[str],
        regions: str,
        aggregate: bool,
        N_n: int,
    ) -> None:
        test_context.model.regions = regions

        source_kw.update(aggregate=aggregate)

        c = genno.Computer()
        s = pd.Series(1.0, index=pd.MultiIndex.from_tuples(R12_MAP, names=["n", "n2"]))
        c.add("bc:n-n2", genno.Quantity(s))

        keys = IEA_EEI.add_tasks(c, context=test_context, **source_kw)

        # Keys have informative names
        assert "passenger load factor" == keys[0].name

        # Preparation of data runs successfully
        result = c.get(keys[0])

        # assert 1394 == result.size
        assert 400 <= result.size
        assert {"n", "y"} | dimensionality == set(result.dims)
        assert N_n == len(result.coords["n"])
