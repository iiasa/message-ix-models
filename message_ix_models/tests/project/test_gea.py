from typing import TYPE_CHECKING

import pytest
from genno import Computer, Key

from message_ix_models.project.gea.data import GEA
from message_ix_models.util import HAS_MESSAGE_DATA as FULL

if TYPE_CHECKING:
    from message_ix_models import Context

M = "Final Energy|Transportation|Total"
S = "geama_450_btr_nsink"


@pytest.fixture
def gea_test_data(monkeypatch) -> None:
    """Temporarily allow :func:`path_fallback` to find test data."""
    monkeypatch.setattr(GEA, "use_test_data", True)


class TestGEA:
    @pytest.mark.usefixtures("gea_test_data")
    @pytest.mark.parametrize(
        "source_kw",
        (
            (dict(measure=M, model="GEA", scenario=S)),
            pytest.param(
                dict(measure=M, model="IMAGE", scenario=S),
                marks=pytest.mark.xfail(
                    raises=ValueError,
                    reason="Non-existent (model, scenario) combination",
                ),
            ),
        ),
    )
    @pytest.mark.parametrize(
        "regions, aggregate, N_n, size",
        (
            # Some values are different if testing without MESSAGE_DATA, using the
            # reduced/ fuzzed data
            ("R12", False, 18 if FULL else 4, 198 if FULL else 44),
            # No mapping from GEA to MESSAGE code lists
            ("R12", True, 2 if FULL else 0, 22 if FULL else 0),
        ),
    )
    def test_add_tasks(
        self,
        test_context: "Context",
        source_kw: dict,
        regions: str,
        aggregate: bool,
        N_n: int,
        size: int,
    ) -> None:
        test_context.model.regions = regions

        c = Computer()

        source_kw.update(aggregate=aggregate)

        keys = GEA.add_tasks(c, context=test_context, **source_kw)
        assert all(isinstance(k, Key) for k in keys)

        # Keys have expected names
        assert source_kw["measure"].lower() == keys[0].name

        # Preparation of data runs successfully
        result = c.get(keys[0])

        # Data have expected size, dimensions, and coords
        assert size == result.size
        assert {"n", "y"} == set(result.dims)
        assert N_n == len(result.coords["n"])
