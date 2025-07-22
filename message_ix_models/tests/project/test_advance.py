from typing import TYPE_CHECKING

import pytest
from genno import Computer

from message_ix_models.project.advance.data import ADVANCE

if TYPE_CHECKING:
    from message_ix_models import Context


@pytest.fixture
def advance_test_data(monkeypatch) -> None:
    """Temporarily allow :func:`path_fallback` to find test data."""
    monkeypatch.setattr(ADVANCE, "use_test_data", True)


class TestADVANCE:
    @pytest.mark.usefixtures("advance_test_data")
    @pytest.mark.parametrize(
        "source_kw, dimensionality",
        (
            (
                dict(
                    measure="Transport|Service demand|Road|Freight",
                    model="MESSAGE",
                    scenario="ADV3TRAr2_Base",
                ),
                {"[length]": 1, "[mass]": 1, "[time]": -1},
            ),
            (
                dict(
                    measure="Transport|Service demand|Road|Passenger|LDV",
                    model="MESSAGE",
                    scenario="ADV3TRAr2_Base",
                ),
                {"[length]": 1, "[passenger]": 1, "[time]": -1},
            ),
            # Excess keyword arguments
            pytest.param(
                dict(measure="GDP", model="not a model", foo="bar"),
                None,
                marks=pytest.mark.xfail(raises=TypeError),
            ),
        ),
    )
    @pytest.mark.parametrize("regions, N_n", (("ADVANCE", 7), ("R12", 4)))
    def test_add_tasks(
        self,
        test_context: "Context",
        source_kw: dict,
        dimensionality: dict[str, int],
        regions: str,
        N_n: int,
    ) -> None:
        test_context.model.regions = regions

        c = Computer()

        keys = c.apply(ADVANCE.add_tasks, context=test_context, **source_kw)

        # Preparation of data runs successfully
        result = c.get(keys[0])

        # Data have the expected dimensions
        assert ("n", "y") == result.dims

        # Data units have the expected dimensionality
        assert dimensionality == result.units.dimensionality

        # Data are complete
        assert N_n == len(result.coords["n"])
        assert 14 == len(result.coords["y"])
