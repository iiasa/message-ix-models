import re
from typing import TYPE_CHECKING

import pytest
from genno import Computer

from message_ix_models.tools.cepii import BACI

if TYPE_CHECKING:
    from message_ix_models import Context


class TestBACI:
    class TestOptions:
        def test_post_init(self) -> None:
            with pytest.raises(
                ValueError,
                match=re.escape("non-existent dimension(s): ['x', 'y', 'z']"),
            ):
                BACI.Options(filter_pattern={"k": "", "x": "", "y": "", "z": ""})

    @pytest.mark.parametrize(
        "measure",
        [
            "quantity",
            "value",
            pytest.param("foo", marks=pytest.mark.xfail(raises=ValueError)),
        ],
    )
    @pytest.mark.parametrize(
        "test_data, filter_pattern, size",
        # Subset of the product codes for MESSAGE commodity="coal"
        [
            pytest.param(
                False,
                dict(k="270(11[129]|[246]..)"),
                112319,
                marks=pytest.mark.skip(reason="High resource usage"),
            ),
            (True, dict(k="270(11[129]|[246]..)"), 110),
        ],
    )
    def test_add_tasks(
        self,
        test_context: "Context",
        measure: str,
        filter_pattern: dict,
        test_data: bool,
        size: int,
    ) -> None:
        test_context.model.regions = "R12"

        c = Computer()

        keys = BACI.add_tasks(
            c,
            context=test_context,
            measure=measure,
            filter_pattern=filter_pattern,
            test=test_data,
        )

        # Preparation of data runs successfully
        result = c.get(keys[0])

        # Data have the expected dimensions and size
        assert {"t", "i", "j", "k"} == set(result.dims)
        assert size == result.size
