import pytest
from genno import Computer

from message_ix_models.tools.exo_data import prepare_computer
from message_ix_models.tools.gfei import GFEI  # noqa: F401


class TestGFEI:
    @pytest.mark.parametrize("regions, N_n", (("R12", 4),))
    def test_prepare_computer(self, test_context, regions, N_n):
        source = "GFEI"
        source_kw = dict(plot=True)
        test_context.model.regions = regions

        c = Computer()

        keys = prepare_computer(test_context, c, source, source_kw)

        # Preparation of data and plotting runs successfully
        c.add("tmp", [keys[0], "plot GFEI debug"])
        result, *_ = c.get("tmp")

        # Data have the expected dimensions, coords, and size
        assert {"n", "t", "y"} == set(result.dims)
        assert {2017} == set(result.coords["y"].data)
        assert 317 == result.size
