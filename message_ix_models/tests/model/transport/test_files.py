import genno
import pytest
from genno import Key

from message_ix_models.model.transport import build, testing
from message_ix_models.model.transport.files import FILES


@build.get_computer.minimum_version
@pytest.mark.parametrize("file", FILES, ids=lambda f: "-".join(f.parts))
def test_data_files(test_context, file):
    """Input data can be read."""
    c, _ = testing.configure_build(test_context, regions="R12", years="B")

    # Task runs
    result = c.get(file.key)

    # Quantity is loaded
    assert isinstance(result, genno.Quantity)

    # Dimensions are as expected
    assert set(Key(result).dims) == set(file.key.dims)
