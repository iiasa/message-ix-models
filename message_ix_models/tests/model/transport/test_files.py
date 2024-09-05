from typing import TYPE_CHECKING

import genno
import pytest
from genno import Key

from message_ix_models.model.transport import build, testing
from message_ix_models.model.transport.files import (
    FILES,
    ExogenousDataFile,
    collect_structures,
    read_structures,
)

if TYPE_CHECKING:
    from genno import Computer


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
    @pytest.mark.parametrize("file", FILES, ids=lambda f: "-".join(f.path.parts))
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

    def test_generate_csv_template(self):
        with pytest.raises(NotImplementedError):
            FILES[0].generate_csv_template()

    def test_repr(self):
        assert (
            "<ExogenousDataFile freight-activity.csv → freight activity:n:exo>"
            == repr(FILES[0])
        )

    def test_required(self):
        """The :`ExogenousDataFiles.required` property has a :class:`bool` value."""
        assert isinstance(FILES[0].required, bool)

    def test_units(self):
        """The :`ExogenousDataFiles.units` property has a :class:`pint.Unit` value."""
        import pint

        assert isinstance(FILES[0].units, pint.Unit)


def test_collect_structures():
    sm1 = collect_structures()

    sm2 = read_structures()

    # Structures are retrieved from file successfully
    # The value is either 30 or 31 depending on whether .build.add_exogenous_data() has
    # run
    assert 30 <= len(sm1.dataflow) == len(sm2.dataflow)
