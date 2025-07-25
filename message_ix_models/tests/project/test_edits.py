from shutil import copyfile

import pytest

from message_ix_models.project.edits import gen_demand, pasta_native_to_sdmx
from message_ix_models.project.edits.structure import SCENARIO, get_cl_scenario
from message_ix_models.util import package_data_path

URN = "urn:sdmx:org.sdmx.infomodel.codelist.Code=IIASA_ECE:EDITS_MCE_SCENARIO(0.1).CA"


@pytest.fixture
def test_pasta_data(test_context):
    """Copy the test input data file to the temporary directory for testing."""
    parts = ("edits", "pasta.csv")
    target = test_context.get_local_path(*parts)
    target.parent.mkdir(parents=True, exist_ok=True)
    copyfile(package_data_path("test", *parts), target)


class TestSCENARIO:
    def test_members(self) -> None:
        # Members can be accessed as attributes
        SCENARIO.CA  # type: ignore [attr-defined]
        SCENARIO.HA  # type: ignore [attr-defined]
        SCENARIO._Z  # type: ignore [attr-defined]

        # Members can be accessed by name
        SCENARIO["CA"]
        SCENARIO["HA"]
        SCENARIO["_Z"]

        # Members can be accessed by URN
        assert SCENARIO["CA"] is SCENARIO.by_urn(URN)

    def test_urn(self) -> None:
        # Member URN can be retrieved
        assert URN == SCENARIO["CA"].urn


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


def test_get_cl_scenario() -> None:
    # Code list can be generated
    result = get_cl_scenario()

    # Code list has expected number of members
    assert 3 == len(result)

    # Items have URNs that can be referenced
    assert URN == result["CA"].urn
