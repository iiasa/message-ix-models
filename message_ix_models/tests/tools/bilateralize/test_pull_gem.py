import pytest

from message_ix_models.tools.bilateralize.pull_gem import gem_region, import_gem


def test_gem_region() -> None:
    gem_region()


@pytest.mark.xfail(
    raises=FileNotFoundError,
    reason="Input files not available for testing: "
    "Global Energy Monitor/GEM-GOIT-*.csv",
)
@pytest.mark.parametrize(
    "input_file",
    # Values appearing where the function is called in prepare_edit_files()
    [
        ("GEM-GGIT-Gas-Pipelines-2024-12.csv"),
        ("GEM-GOIT-Oil-NGL-Pipelines-2025-03.csv"),
    ],
)
def test_import_gem(input_file: str) -> None:
    import_gem(
        input_file=input_file,
        trade_technology="foo",
        flow_technology="bar",
        flow_commodity="baz",
    )
