from typing import TYPE_CHECKING

import pytest
from genno import Computer

from message_ix_models.tools.jgcri import CEDS

if TYPE_CHECKING:
    from message_ix_models import Context

SKIP_BIG = pytest.mark.skip(reason="High memory use/large cache files.")


class TestCEDS:
    @pytest.mark.parametrize(
        "dataflow, options, exp_size",
        (
            # Default CEDS.Options: aggregate=False, year_min=2000
            ("aggregate", {}, 3616060),
            ("aggregate", dict(aggregate=True), 291640),
            pytest.param("aggregate", dict(year_min=1750), 43078280, marks=SKIP_BIG),
            ("detailed", {}, 11932630),
            pytest.param("detailed", dict(year_min=1750), 142153940, marks=SKIP_BIG),
            pytest.param(
                "supplementary_bunkers",
                {},
                0,
                marks=pytest.mark.xfail(
                    raises=KeyError,
                    reason="These files have a different format. They are missing the "
                    "'em' column,",
                ),
            ),
            ("supplementary_extension", {}, 14812),  # with default year_min=2000
            pytest.param(
                "supplementary_extension", dict(year_min=1750), 175812, marks=SKIP_BIG
            ),
        ),
    )
    def test_add_tasks(
        self, test_context: "Context", dataflow: str, options: dict, exp_size: int
    ) -> None:
        """:meth:`.CEDS.add_tasks` adds functional tasks to a Computer."""
        test_context.model.regions = "R12"

        c = Computer()

        # Tasks are added without error
        keys = CEDS.add_tasks(c, context=test_context, dataflow=dataflow, **options)

        # Preparation of data runs successfully
        result = c.get(keys[0])

        # Data have the expected dimensions
        assert set("centy") == set(result.dims)

        # Data have the expected size
        assert exp_size == result.size

        # Data have the expected units
        assert "kt" == result.units
