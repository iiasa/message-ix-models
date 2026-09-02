import pytest
from genno import Computer

from message_ix_models import Context
from message_ix_models.tools.gains import EmissionFactor


class TestEmissionFactor:
    def test_init(self) -> None:
        # Object can be instantiated
        EmissionFactor(scenario="SSP2", variant="L")

        # Exception is raised if invalid scenario and variant are given
        with pytest.raises(ValueError, match="invalid combination"):
            EmissionFactor(scenario="SSP1", variant="H")

    def test_full(self, test_context: Context) -> None:
        """:meth:`.EmissionFactor.add_tasks` adds tasks that produce valid data."""
        test_context.model.regions = "R12"

        c = Computer()
        key, *_ = EmissionFactor.add_tasks(
            c,
            context=test_context,
            nodes=test_context.model.regions,
            scenario="SSP2",
            variant="L",
        )

        # Data can be computed
        result = c.get(key)

        # Data have the expected size
        assert 40124 == len(result)

        # Data have expected coordinates
        assert {"CO", "VOC"} < set(result.coords["e"].data)  # Some emission species
        assert 12 == len(result.coords["n"])  # All nodes
        # Some technologies are present
        assert {"End_Use_Transport_Coal", "End_Use_Transport_HDT_HLF"} < set(
            result.coords["t"].data
        )
        assert {2020, 2110} < set(result.coords["y"].data)  # First and last periods
