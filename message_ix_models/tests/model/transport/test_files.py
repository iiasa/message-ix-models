from typing import TYPE_CHECKING

import genno
import pytest
from genno import Key

from message_ix_models.model.transport import build, testing
from message_ix_models.model.transport.files import FILES

if TYPE_CHECKING:
    from genno import Computer

    from message_ix_models.model.transport.files import ExogenousDataFile


class TestExogenousDataFile:
    """Test :class:`.ExogenousDataFile."""

    # TODO Use a broader-scoped context to allow (scope="class")
    @pytest.fixture
    def build_computer(self, test_context):
        """A :class:`.Computer` from :func:`.configure_build`.

        This in turn invokes :func:`.transport.build.add_exogenous_data`, which adds
        each of :data:`.FILES` to a Computer.
        """
        c, _ = testing.configure_build(test_context, regions="R12", years="B")
        yield c

    @build.get_computer.minimum_version
    @pytest.mark.parametrize("file", FILES, ids=lambda f: "-".join(f.parts))
    def test_configure_build(
        self, build_computer: "Computer", file: "ExogenousDataFile"
    ) -> None:
        """Input data can be read and has the expected dimensions."""
        c = build_computer

        # Task runs
        result = c.get(file.key)

        # Quantity is loaded
        assert isinstance(result, genno.Quantity)

        # Dimensions are as expected
        assert set(Key(result).dims) == set(file.key.dims)
