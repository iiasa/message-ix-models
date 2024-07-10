from shutil import copyfile

import pytest

from message_ix_models.project.edits import gen_demand, pasta_native_to_sdmx
from message_ix_models.util import package_data_path


@pytest.fixture
def test_pasta_data(test_context):
    """Copy the test input data file to the temporary directory for testing."""
    parts = ("edits", "pasta.csv")
    target = test_context.get_local_path(*parts)
    target.parent.mkdir(parents=True, exist_ok=True)
    copyfile(package_data_path("test", *parts), target)


def test_pasta_native_to_sdmx(test_context, test_pasta_data):
    pasta_native_to_sdmx()

    dir = test_context.get_local_path("edits")

    # Files were created
    assert dir.joinpath("pasta-data.xml").exists()
    assert dir.joinpath("pasta-structure.xml").exists()


def test_gen_demand(test_context, test_pasta_data):
    pasta_native_to_sdmx()

    result = gen_demand(test_context)
    assert 3 == len(result)
