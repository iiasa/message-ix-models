from typing import TYPE_CHECKING

import pandas as pd
import pytest

from message_ix_models.project.ssp.transport import (
    prepare_computer,
    process_df,
    process_file,
)
from message_ix_models.tests.tools.iea.test_web import user_local_data  # noqa: F401
from message_ix_models.util import package_data_path

if TYPE_CHECKING:
    import pathlib


# Test data file paths
V1 = "SSP_dev_SSP2_v0.1_Blv0.18_baseline_prep_lu_bkp_solved_materials_2025_macro.csv"
V2 = "SSP_LED_v2.3.1_baseline.csv"


@pytest.fixture(scope="module")
def input_csv_path() -> "pathlib.Path":
    return package_data_path("test", "report", V2)


@pytest.fixture(scope="module")
def input_xlsx_path(tmp_path_factory, input_csv_path) -> "pathlib.Path":
    import pandas as pd

    result = (
        tmp_path_factory.mktemp("ssp-transport")
        .joinpath(input_csv_path.name)
        .with_suffix(".xlsx")
    )

    pd.read_csv(input_csv_path).to_excel(result, index=False)

    return result


# Enumeration/flags for codes appearing in VARIABLE
IN_ = 1  #         Data appears in the input file only
OUT = 2  #         Data appears in the output file *and* with a modified magnitude
I_O = IN_ | OUT  # Both


#: Mapping from emission species (appearing in IAMC-structured / reported data, not
#: model internal) to flags indicating whether data for associated ‘Variable’ codes
#: appear in the input file and/or are modified in the output file. See
#: :func:`expected_variables`.
SPECIES = {
    "CH4": I_O,
    "BC": IN_,  # No emissions factor data for e=BCA
    "CO": I_O,
    "CO2": IN_,
    "N2O": I_O,
    "NH3": IN_,  # No emissions factor data for e=NH3
    "NOx": I_O,
    "OC": IN_,  # No emissions factor data for e=OCA
    "Sulfur": IN_,  # No emissions factor data for e=SO2; only SOx
    "VOC": I_O,
}


def expected_variables(flag: int) -> set[str]:
    """Set of expected ‘Variable’ codes according to `flag`."""
    # Shorthand
    edb = "Energy|Demand|Bunkers"
    edt = "Energy|Demand|Transportation"

    result = set()
    for e, exp in SPECIES.items():
        result |= (
            {
                f"Emissions|{e}|{edb}",
                f"Emissions|{e}|{edb}|International Aviation",
                f"Emissions|{e}|{edt}",
                f"Emissions|{e}|{edt}|Road Rail and Domestic Shipping",
            }
            if flag & exp
            else set()
        )

    return result


def check(df_in: pd.DataFrame, df_out: pd.DataFrame, method: str) -> None:
    """Common checks for :func:`test_process_df` and :func:`test_process_file`."""
    # Identify dimension columns
    dims_wide = list(df_in.columns)[:5]  # …in 'wide' layout
    dims = dims_wide + ["Year"]  # …in 'long' layout

    # Convert wide to long; sort
    def _to_long(df):
        return (
            df.melt(dims_wide, var_name=dims[-1])
            .astype({dims[-1]: int})
            .sort_values(dims)
        )

    # df_out.to_csv("debug-out.csv")  # DEBUG Dump to file

    df_in = _to_long(df_in)
    df_out = _to_long(df_out)

    # Input data already contains the variable names to be modified
    assert expected_variables(IN_) <= set(df_in["Variable"].unique())
    region = set(df_in["Region"].unique())

    # Data have the same length
    assert len(df_in) == len(df_out)

    # Output has the same set of region codes as input
    assert region == set(df_out["Region"].unique())

    # Diff data:
    # - Outer merge.
    # - Compute diff and select rows where diff is larger than a certain value
    df = df_in.merge(df_out, how="outer", on=dims, suffixes=("_in", "_out")).query(
        "abs(value_out - value_in) > 1e-16"
    )

    # Possible number of modified values. In each tuple, the first is the count with
    # the full IEA EWEB dataset, the second with the fuzzed data
    N = {
        "A": (5434, None),
        "B": (4660, 2660),
    }[method]

    try:
        full_iea_eweb_data = N.index(len(df)) == 0  # True if the full dataset
    except ValueError:
        assert False, f"Unexpected number of modified values: {len(df)}"

    # df.to_csv("debug-diff.csv")  # DEBUG Dump to file
    # print(df.to_string(max_rows=50))  # DEBUG Show in test output

    # All of the expected 'variable' codes have been modified
    assert expected_variables(OUT) == set(df["Variable"].unique())

    cond = df.query("value_out < 0")
    if len(cond):
        msg = "Negative emissions totals after processing"
        print(f"\n{msg}:", cond.to_string(), sep="\n")
        assert not full_iea_eweb_data, msg


@prepare_computer.minimum_version
@pytest.mark.parametrize(
    "method",
    (
        pytest.param("A", marks=pytest.mark.xfail(reason="Obsolete/not maintained")),
        "B",
    ),
)
def test_process_df(input_csv_path, method) -> None:
    df_in = pd.read_csv(input_csv_path)

    # Code runs
    df_out = process_df(df_in, method=method)

    # Output satisfies expectations
    check(df_in, df_out, method)


@prepare_computer.minimum_version
# @pytest.mark.usefixtures("user_local_data")
@pytest.mark.parametrize(
    "method",
    (
        pytest.param("A", marks=pytest.mark.xfail(reason="Obsolete/not maintained")),
        "B",
    ),
)
def test_process_file(tmp_path, test_context, input_csv_path, method) -> None:
    """Code can be called from Python."""

    # Locate a temporary data file
    path_in = input_csv_path
    path_out = tmp_path.joinpath("output.csv")

    # Code runs
    process_file(path_in=path_in, path_out=path_out, method=method)

    # Output path exists
    assert path_out.exists()

    # Read input and output files
    df_in = pd.read_csv(path_in)
    df_out = pd.read_csv(path_out)

    # Output satisfies expectations
    check(df_in, df_out, method)


@prepare_computer.minimum_version
def test_cli(tmp_path, mix_models_cli, input_xlsx_path) -> None:
    """Code can be invoked from the command-line."""
    from shutil import copyfile

    # Locate a temporary data file
    input_file = input_xlsx_path
    path_in = tmp_path.joinpath(input_file.name)

    # Copy the input file to the test data directory
    copyfile(input_file, path_in)

    # Code runs
    result = mix_models_cli.invoke(["ssp", "transport", "--method=A", f"{path_in}"])
    assert 0 == result.exit_code, result.output

    # Output path was determined automatically and exists
    path_out = tmp_path.joinpath(path_in.stem + "_out.xlsx")
    assert path_out.exists()

    # Messages were printed about file handling
    for message in (
        "Convert Excel input to ",
        "No PATH_OUT given; write to ",
        "Convert CSV output to ",
    ):
        assert message in result.output
