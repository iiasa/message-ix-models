import numpy as np
from genno import Computer, KeySeq
from genno.operator import relabel
from genno.testing import random_qty

from message_ix_models.model.structure import get_codes
from message_ix_models.model.transport.base import smooth


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
