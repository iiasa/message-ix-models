from typing import TYPE_CHECKING

import genno
import numpy as np
import pandas as pd
import pytest
from genno import Computer, KeySeq
from genno.operator import random_qty, relabel

from message_ix_models.model.structure import get_codes
from message_ix_models.model.transport.base import format_share_constraints, smooth

if TYPE_CHECKING:
    from genno.types import AnyQuantity


@smooth.minimum_version
def test_smooth(recwarn) -> None:
    c = Computer()

    # Base period
    y0 = 2020
    c.add("y0", y0)

    # Expected base key for input
    k = KeySeq("ue:nl-ya-c-l-h-t")

    # Prepare input data
    periods = list(
        filter(lambda y: y >= y0, map(lambda c: int(c.id), get_codes("year/B")))
    )
    q_in = (
        random_qty(dict(nl=2, ya=len(periods), c=1, l=1, h=1, t=1))
        .pipe(relabel, ya={f"ya{i}": y for i, y in enumerate(periods)})
        .sort_index()
    )

    # pandas.errors.PerformanceWarning
    q_in.loc[:, 2020] = 0.5
    q_in.loc[:, 2110] = 1.0

    # Two consecutive values to fill, starting after the first period
    q_in.loc["nl0", 2025, :] = 0.01
    q_in.loc["nl0", 2030, :] = 0.01
    q_in.loc["nl0", 2035, :] = 1.0

    # One value to fill, starting after the second period
    q_in.loc["nl1", 2025, :] = 0.6
    q_in.loc["nl1", 2030, :] = 0.01
    q_in.loc["nl1", 2035, :] = 1.0

    # Add input
    c.add(k[1], q_in)

    # Function runs, tasks are added to the graph
    key = c.apply(smooth, k[1])

    # Expected key is returned
    assert key == k[2]

    # Result (its partial sum) can be computed without error
    result = c.get(k[2] / tuple("clht"))

    # Two values for nl=nl0 are interpolated
    assert np.isclose(result.loc["nl0", 2025], 2.0 / 3)
    assert np.isclose(result.loc["nl0", 2030], 5.0 / 6)

    # One value for nl=nl1 is interpolated
    assert np.isclose(result.loc["nl1", 2030], 0.8)


@pytest.fixture(scope="module")
def qty() -> "AnyQuantity":
    return genno.Quantity(
        pd.DataFrame(
            columns=["nl", "ya", "t", "value"],
            data=[
                ["R12_AFR", 2020, "elec_trp", 0.0014875088932665413],
                ["R12_AFR", 2020, "eth_fc_trp", 0.0012603672387565318],
                ["R12_AFR", 2025, "elec_trp", 0.0018723207739392755],
                ["R12_AFR", 2025, "eth_fc_trp", 0.0015452722864602048],
                ["R12_CHN", 2020, "elec_trp", 0.10682429727325753],
                ["R12_CHN", 2020, "eth_fc_trp", 0.011632663973514212],
                ["R12_CHN", 2025, "elec_trp", 0.06696025519877587],
                ["R12_CHN", 2025, "eth_fc_trp", 0.005241369125381201],
            ],
        ).set_index(["nl", "ya", "t"])["value"]
    )


@pytest.mark.parametrize("groupby", ([], ["node"], ["year"], ["node", "year"]))
@pytest.mark.parametrize("kind", ("lo", "up"))
def test_format_share_constraints(qty, groupby, kind) -> None:
    from message_ix_models.model.transport import Config
    from message_ix_models.project.ssp import SSP_2024

    config = dict(transport=Config(ssp=SSP_2024["1"]))

    # Function runs
    df = format_share_constraints(qty, config, kind=kind, groupby=groupby)

    assert not df.isna().any(axis=None)
    # TODO Expand with content assertions
