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


@pytest.fixture(scope="module")
def input_csv_path() -> "pathlib.Path":
    return package_data_path(
        "test",
        "report",
        "SSP_dev_SSP2_v0.1_Blv0.18_baseline_prep_lu_bkp_solved_materials_2025_macro.csv",
    )


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
IN_ = 1  # Data appears in the input file only
OUT = 2  # Data appears in the output file *and* with a modified magnitude
I_O = IN_ | OUT  # Both

edt = "Energy|Demand|Transportation"

#: IAMC 'Variable' codes appearing in the input file and/or output file.
VARIABLE = {
    (0, f"Emissions|CH4|{edt}"),
    (OUT, f"Emissions|CH4|{edt}|Aviation"),
    (0, f"Emissions|CH4|{edt}|Aviation|International"),
    (I_O, f"Emissions|CH4|{edt}|Road Rail and Domestic Shipping"),
    # No emissions factors available for BC
    (IN_, f"Emissions|BC|{edt}"),
    (IN_, f"Emissions|BC|{edt}|Aviation"),
    (IN_, f"Emissions|BC|{edt}|Aviation|International"),
    #
    (I_O, f"Emissions|CO|{edt}|Aviation"),
    (I_O, f"Emissions|CO|{edt}|Road Rail and Domestic Shipping"),
    #
    (OUT, f"Emissions|N2O|{edt}|Aviation"),
    (I_O, f"Emissions|N2O|{edt}|Road Rail and Domestic Shipping"),
    #
    (IN_, f"Emissions|NH3|{edt}"),
    (IN_, f"Emissions|NH3|{edt}|Aviation"),
    (IN_, f"Emissions|NH3|{edt}|Aviation|International"),
    #
    (I_O, f"Emissions|NOx|{edt}|Aviation"),
    (I_O, f"Emissions|NOx|{edt}|Road Rail and Domestic Shipping"),
    #
    (IN_, f"Emissions|OC|{edt}"),
    (IN_, f"Emissions|OC|{edt}|Aviation"),
    (IN_, f"Emissions|OC|{edt}|Aviation|International"),
    # FIXME Should be OUT
    (0, f"Emissions|Sulfur|{edt}|Aviation"),
    #
    (IN_, f"Emissions|VOC|{edt}"),
    (I_O, f"Emissions|VOC|{edt}|Aviation"),
    (IN_, f"Emissions|VOC|{edt}|Aviation|International"),
    (I_O, f"Emissions|VOC|{edt}|Road Rail and Domestic Shipping"),
}


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

    df_in = _to_long(df_in)
    df_out = _to_long(df_out)

    # Input data already contains the variable names to be modified
    exp = {v[1] for v in VARIABLE if IN_ & v[0]}
    assert exp <= set(df_in["Variable"].unique())
    region = set(df_in["Region"].unique())

    # Data have the same length
    assert len(df_in) == len(df_out)

    # Output has the same set of region codes as input
    assert region == set(df_out["Region"].unique())

    # Diff data:
    # - Outer merge.
    # - Compute diff and select rows where diff is larger than a certain value
    df = df_in.merge(df_out, how="outer", on=dims).query(
        "abs(value_y - value_x) > 1e-16"
    )

    # Possible number of modified values. In each tuple, the first is the count with
    # the full IEA EWEB dataset, the second with the fuzzed data
    N = {
        "A": (2840, None),
        "B": (2400, 1400),
    }[method]

    try:
        full_iea_eweb_data = N.index(len(df)) == 0  # True if the full dataset
    except IndexError:
        assert False, f"Unexpected number of modified values: {len(df)}"

    # df.to_csv("debug0.csv")  # DEBUG Dump to file
    # print(df.to_string(max_rows=50))  # DEBUG Show in test output

    # All of the expected 'variable' codes have been modified
    exp = {v[1] for v in VARIABLE if OUT & v[0]}
    assert exp == set(df["Variable"].unique())

    cond = df.query("value_y < 0")
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
