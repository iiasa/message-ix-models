from typing import TYPE_CHECKING

import pytest
from genno import Computer

from message_ix_models.tools.gfei import GFEI

if TYPE_CHECKING:
    from message_ix_models import Context


@pytest.fixture
def gfei_test_data(monkeypatch) -> None:
    """Temporarily allow :func:`path_fallback` to find test data."""
    monkeypatch.setattr(GFEI, "use_test_data", True)


class TestGFEI:
    @pytest.mark.usefixtures("gfei_test_data")
    @pytest.mark.parametrize(
        "regions, aggregate, N_n, size",
        (
            ("R12", False, 50, 317),
            ("R12", True, 11, 77),
        ),
    )
    def test_add_tasks(
        self,
        test_context: "Context",
        regions: str,
        aggregate: bool,
        N_n: int,
        size: int,
    ) -> None:
        test_context.model.regions = regions

        source_kw = dict(aggregate=aggregate, plot=True)

        c = Computer()

        keys = GFEI.add_tasks(c, context=test_context, **source_kw)

        # Preparation of data and plotting runs successfully
        c.add("tmp", [keys[0], "plot GFEI debug"])
        result, *_ = c.get("tmp")

        # Data have the expected dimensions, coords, and size
        assert {"n", "t", "y"} == set(result.dims)
        assert N_n == len(result.coords["n"])
        assert {2017} == set(result.coords["y"].data)
        assert size == result.size
