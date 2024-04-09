import pytest
from genno import Computer

from message_ix_models.project.gea.data import GEA  # noqa: F401
from message_ix_models.tools.exo_data import prepare_computer
from message_ix_models.util import HAS_MESSAGE_DATA as FULL

M = "Final Energy|Transportation|Total"
S = "geama_450_btr_nsink"


class TestGEA:
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
    def test_prepare_computer(
        self, test_context, source_kw, regions, aggregate, N_n, size
    ):
        test_context.model.regions = regions

        c = Computer()

        source = "GEA"
        source_kw.update(aggregate=aggregate)

        keys = prepare_computer(test_context, c, source, source_kw)

        # Keys have expected names
        assert source_kw["measure"].lower() == keys[0].name

        # Preparation of data runs successfully
        result = c.get(keys[0])

        # Data have expected size, dimensions, and coords
        assert size == result.size
        assert {"n", "y"} == set(result.dims)
        assert N_n == len(result.coords["n"])
