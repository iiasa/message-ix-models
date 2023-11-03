import pytest
from genno import Computer

from message_ix_models.project.advance.data import ADVANCE  # noqa: F401
from message_ix_models.tools.exo_data import prepare_computer


class TestADVANCE:
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
                marks=pytest.mark.xfail(raises=ValueError),
            ),
        ),
    )
    @pytest.mark.parametrize("regions, N_n", (("ADVANCE", 7), ("R12", 4)))
    def test_prepare_computer(
        self, test_context, source_kw, dimensionality, regions, N_n
    ):
        source = "ADVANCE"
        test_context.model.regions = regions

        c = Computer()

        keys = prepare_computer(test_context, c, source, source_kw)

        # Preparation of data runs successfully
        result = c.get(keys[0])

        # Data have the expected dimensions
        assert ("n", "y") == result.dims

        # Data units have the expected dimensionality
        assert dimensionality == result.units.dimensionality

        # Data are complete
        assert N_n == len(result.coords["n"])
        assert 14 == len(result.coords["y"])
