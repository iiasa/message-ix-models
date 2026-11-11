import pytest
from genno import MissingKeyError

from message_ix_models.model.material.report.reporter_utils import (
    add_ammonia_non_energy_computations,
    add_biometh_final_share,
    add_methanol_non_energy_computations,
)
from message_ix_models.tests.test_report import simulated_solution_reporter


def test_biometh_calculation():
    import numpy.testing as npt

    rep = simulated_solution_reporter()

    add_biometh_final_share(rep, "M1")
    comm = "coal"
    key = f"share::{comm}methanol-final"
    result = rep.get(key)
    npt.assert_array_less(result.loc["R11_CPA", 2020], 1)
    npt.assert_allclose(result.loc["R11_CPA", 2020], 0.465132830)


@pytest.mark.xfail(reason="Only partially implemented", raises=MissingKeyError)
def test_add_ammonia_non_energy_computations():
    rep = simulated_solution_reporter()
    add_ammonia_non_energy_computations(rep)
    rep.get("in::nh3-process-energy")


def test_add_methanol_share_calculations():
    import numpy.testing as npt

    rep = simulated_solution_reporter()
    key = add_methanol_non_energy_computations(rep)
    df = rep.get(key)
    npt.assert_allclose(df.loc["R11_CPA", "meth_ng", 2020], 3.663060)


def test_select_absent_label() -> None:
    """:func:`.reporter_utils.select` gives zeros for an absent scalar label."""
    import genno
    import pandas as pd

    from message_ix_models.model.material.report.reporter_utils import select

    qty = genno.Quantity(
        pd.Series(
            [1.0, 2.0],
            index=pd.MultiIndex.from_product([["n"], ["a", "b"]], names=["nl", "t"]),
        )
    )

    # A present label: same as genno.operator.select()
    assert 2.0 == select(qty, {"t": "b"}).item()

    # An absent label gives zeros over the remaining dimensions
    result = select(qty, {"t": "c"})
    assert {"nl"} <= set(result.dims)
    assert 0.0 == result.sum().item()

    # Lists of labels are handled by genno.operator.select()
    assert 1.0 == select(qty, {"t": ["a", "c"]}).sum().item()


def test_group_sum_empty() -> None:
    """:func:`.reporter_utils.group_sum` accepts an empty quantity."""
    import genno
    import pandas as pd

    from message_ix_models.model.material.report.reporter_utils import group_sum

    idx = pd.MultiIndex.from_arrays([[], []], names=["t", "c"])
    result = group_sum(genno.Quantity(pd.Series([], index=idx)), "t", "c")
    assert 0 == len(result)
