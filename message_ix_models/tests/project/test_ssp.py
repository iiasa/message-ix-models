import pytest

from message_ix_models.project.ssp import SSP, SSP_2017, SSP_2024, generate


def test_generate(tmp_path, test_context):
    generate(test_context, base_dir=tmp_path)

    assert 3 == len(list(tmp_path.glob("*.xml")))


def test_enum():
    # Enumerations have the expected length
    assert 5 == len(SSP_2017)
    assert 5 == len(SSP_2024)

    # Members can be accessed by ID
    a = SSP_2017["1"]

    # …or by value
    b = SSP_2017(1)

    # …all retrieving the same member
    assert a == b

    # __getattr__ lookup does not invoke _missing_
    with pytest.raises(KeyError):
        SSP_2017[1]

    # Same SSP ID from different enums are not equivalent
    assert SSP_2017["1"] != SSP_2024["1"]
    assert SSP_2017["1"] is not SSP_2024["1"]
    assert SSP["1"] != SSP_2024["1"]


def test_cli(mix_models_cli):
    mix_models_cli.assert_exit_0(["ssp", "gen-structures", "--dry-run"])
