from typing import TYPE_CHECKING

import pytest
from genno import Computer

from message_ix_models.tools.exo_data import (
    DemoSource,
    ExoDataSource,
    prepare_computer,
    register_source,
)

if TYPE_CHECKING:
    from message_ix_models import Context


class TestDemoSource:
    @pytest.mark.parametrize("method", ("apply", "call"))
    @pytest.mark.parametrize("regions, N_n", [("R12", 12), ("R14", 14)])
    def test_add_tasks(
        self, test_context: "Context", method: str, regions: str, N_n: int
    ) -> None:
        test_context.model.regions = regions

        c = Computer()

        if method == "apply":
            keys = c.apply(DemoSource.add_tasks, measure="POP", scenario="s1")
        elif method == "call":
            keys = DemoSource.add_tasks(
                c, context=test_context, measure="POP", scenario="s1"
            )

        # Computation of data runs successfully
        result = c.get(keys[-1])

        # Data has the expected dimensions
        assert ("n", "y") == result.dims

        # Data is complete
        assert N_n == len(result.coords["n"])
        assert 14 == len(result.coords["y"])


class TestExoDataSource:
    def test_abstract(self) -> None:
        with pytest.raises(TypeError, match="Can't instantiate"):
            ExoDataSource()  # type: ignore [abstract]

    def test_register_source(self) -> None:
        with pytest.raises(ValueError, match="already registered for"):
            register_source(DemoSource)


@pytest.mark.parametrize("regions, N_n", [("R12", 12), ("R14", 14)])
def test_deprecated_prepare_computer(test_context, regions, N_n) -> None:
    """:func:`.exo_data.prepare_computer` works as intended."""
    test_context.model.regions = regions

    c = Computer()

    # Function runs successfully `c`
    keys = prepare_computer(test_context, c, "test s1", dict(measure="POP"))

    # print(c.describe(keys[-1]))

    # Computation of data runs successfully
    result = c.get(keys[-1])

    # Data has the expected dimensions
    assert ("n", "y") == result.dims

    # Data is complete
    assert N_n == len(result.coords["n"])
    assert 14 == len(result.coords["y"])


def test_deprecated_prepare_computer_exc(test_context: "Context") -> None:
    """Exceptions raised from :func:`prepare_computer`."""
    c = Computer()

    with pytest.raises(ValueError, match="No source found that can handle"):
        prepare_computer(test_context, c, "test s1", dict(measure="FOO"))

    with pytest.raises(ValueError, match="No source found that can handle"):
        prepare_computer(test_context, c, "not a source")
