import pytest

from message_ix_models.tools.bilateralize.pull_gem import gem_region, import_gem


def test_gem_region() -> None:
    gem_region()


@pytest.mark.xfail(
    raises=FileNotFoundError,
    reason="Input files not available for testing: "
    "Global Energy Monitor/GEM-GOIT-*.xlsx",
)
@pytest.mark.parametrize(
    "input_file, input_sheet",
    # Values appearing where the function is called in prepare_edit_files()
    [
        ("GEM-GGIT-Gas-Pipelines-2024-12.xlsx", "Gas Pipelines 2024-12-17"),
        ("GEM-GOIT-Oil-NGL-Pipelines-2025-03.xlsx", "Pipelines"),
    ],
)
def test_import_gem(input_file: str, input_sheet: str) -> None:
    import_gem(
        input_file,
        input_sheet,
        trade_technology="foo",
        flow_technology="bar",
        flow_commodity="baz",
    )
