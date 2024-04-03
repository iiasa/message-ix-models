import pytest
from genno import Computer

from message_ix_models.project.gea.data import GEA  # noqa: F401
from message_ix_models.tools.exo_data import prepare_computer


class TestGEA:
    @pytest.mark.parametrize(
        "source_kw, dimensionality",
        (
            (
                dict(
                    measure="Final Energy|Transportation|Total",
                    model="GEA",
                    scenario="geama_450_atr_full",
                ),
                {},
            ),
        ),
    )
    @pytest.mark.parametrize("regions, N_n", (("R12", 4),))
    def test_prepare_computer(
        self, test_context, source_kw, dimensionality, regions, N_n
    ):
        source = "GEA"
        test_context.model.regions = regions

        c = Computer()

        keys = prepare_computer(test_context, c, source, source_kw)

        # Keys have expected names
        assert source_kw["measure"].lower() == keys[0].name

        # Preparation of data runs successfully
        result = c.get(keys[0])

        # Data have expected size, dimensions, and coords
        assert 198 == result.size
        assert {"n", "y"} == set(result.dims)
        assert {
            "AFR",
            "ASIA",
            "CPA",
            "EEU",
            "FSU",
            "LAM",
            "MAF",
            "MEA",
            "NAM",
            "North",
            "OECD90",
            "PAO",
            "PAS",
            "REF",
            "SAS",
            "South",
            "WEU",
            "World",
        } == set(result.coords["n"].data)
