from collections.abc import Callable, Hashable
from functools import cache
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import pytest

from message_ix_models.project.ssp.transport import (
    METHOD,
    get_computer,
    get_scenario_code,
    process_df,
    process_file,
)
from message_ix_models.testing import MARK
from message_ix_models.tools.iea import web
from message_ix_models.util import package_data_path

if TYPE_CHECKING:
    import pathlib

METHOD_PARAM = (
    METHOD.A,
    METHOD.B,
    pytest.param(METHOD.C, marks=MARK[0]),
)

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

#: Emissions species codes appearing in the IAMC-structured / reported data; these are
#: *different* from those internal to the model.
SPECIES = {"CH4", "BC", "CO", "CO2", "N2O", "NH3", "NOx", "OC", "Sulfur", "VOC"}

#: Species for which no aviation-specific emission factor values are available.
SPECIES_WITHOUT_EF: set[str] = set()


def check(df_in: pd.DataFrame, df_out: pd.DataFrame, method: METHOD) -> None:
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
    assert expected_variables(IN_, method) <= set(df_in["Variable"].unique())
    region = set(df_in["Region"].unique())

    # Data have the same length
    assert len(df_in) == len(df_out)

    # Output has the same set of region codes as input
    assert region == set(df_out["Region"].unique())

    # Diff data:
    # - Outer merge.
    # - Fill NaNs resulting from insert_nans()
    # - Compute diff and select rows where diff is larger than a certain value
    df = (
        df_in.merge(df_out, how="outer", on=dims, suffixes=("_in", "_out"))
        .fillna(0)
        .query("abs(value_out - value_in) > 1e-16")
    )

    # Identify the directory from which IEA EWEB data is read
    iea_eweb_dir = web.dir_fallback(
        web.FILES[("IEA", "2024")][0], where=web.IEA_EWEB._where()
    )
    # True if the fuzzed test data are being used
    iea_eweb_test_data = iea_eweb_dir.match("message_ix_models/data/test/iea/web")

    # All regions and "World" have modified values
    N_reg = {METHOD.A: 13, METHOD.B: 9, METHOD.C: 13}[method]
    assert N_reg <= len(df["Region"].unique())

    # Number of modified values
    N_exp = {
        (METHOD.A, False): 10280,
        (METHOD.A, True): 10280,
        (METHOD.B, False): 10120,
        (METHOD.B, True): 7720,
        (METHOD.C, False): 7000,
        (METHOD.C, True): 7000,
    }[(method, iea_eweb_test_data)]

    if N_exp != len(df):
        # df.to_csv("debug-diff.csv")  # DEBUG Dump to file
        # print(df.to_string(max_rows=50))  # DEBUG Show in test output
        msg = f"Unexpected number of modified values: {N_exp} != {len(df)}"
        assert N_exp == len(df)

    # All of the expected 'variable' codes have been modified
    assert expected_variables(OUT, method) == set(df["Variable"].unique())

    cond = df.query("value_out < 0")
    if len(cond):
        msg = "Negative emissions totals after processing"
        print(f"\n{msg}:", cond.to_string(), sep="\n")
        assert iea_eweb_test_data, msg  # Negative values → fail if NOT using test data


@cache
def expected_variables(flag: int, method: METHOD) -> set[str]:
    """Set of expected ‘Variable’ codes according to `flag` and `method`."""
    # Shorthand
    edb = "Energy|Demand|Bunkers"
    edt = "Energy|Demand|Transportation"

    result = set()
    for e in SPECIES:
        # Expected data flows in which these variable codes appear
        exp = IN_ if (e in SPECIES_WITHOUT_EF and method != METHOD.A) else I_O
        if flag & exp:
            result |= {
                f"Emissions|{e}|{edb}",
                f"Emissions|{e}|{edb}|International Aviation",
                f"Emissions|{e}|{edt}",
                f"Emissions|{e}|{edt}|Road Rail and Domestic Shipping",
            }

    return result


def insert_nans(
    df: pd.DataFrame, variable_expr: str, year_cond: Callable[[Hashable], bool]
) -> pd.DataFrame:
    """Replace zeros with :py:`np.nan` in `df`.

    This occurs only where:

    1. The 'Variable' column contains a string that matches `variable_expr`.
    2. The `year_cond` returns :any:`True` for the column name.
    """
    return df.where(
        ~df.Variable.str.fullmatch(variable_expr),
        df.replace({c: {0: np.nan} for c in filter(year_cond, df.columns)}),
    )


@get_computer.minimum_version
def test_cli(tmp_path, mix_models_cli, test_context, input_xlsx_path) -> None:
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


@pytest.mark.parametrize(
    "expected_id, model_name, scenario_name",
    [
        ("LED-SSP1", "SSP_LED_v2.3.1", "baseline_1000f"),
        ("LED-SSP1", "SSP_LED_v2.3.1", "baseline"),
        ("LED-SSP2", "SSP_LED_v2.3.1", "SSP2 - Very Low Emissions"),
        ("SSP1", "SSP_SSP1_v2.3.1", "baseline_1000f"),
        ("SSP1", "SSP_SSP1_v2.3.1", "baseline"),
        ("SSP1", "SSP_SSP1_v2.3.1", "SSP1 - Low Emissions"),
        ("SSP1", "SSP_SSP1_v2.3.1", "SSP1 - Very Low Emissions"),
        ("SSP2", "SSP_SSP2_v2.4.1", "baseline_1000f"),
        ("SSP2", "SSP_SSP2_v2.4.1", "baseline"),
        ("SSP2", "SSP_SSP2_v2.4.1", "SSP2 - Low Emissions"),
        ("SSP2", "SSP_SSP2_v2.4.1", "SSP2 - Low Overshoot"),
        ("SSP2", "SSP_SSP2_v2.4.1", "SSP2 - Medium Emissions"),
        ("SSP2", "SSP_SSP2_v2.4.1", "SSP2 - Medium-Low Emissions"),
        ("SSP3", "SSP_SSP3_v2.4.1", "baseline_1000f"),
        ("SSP3", "SSP_SSP3_v2.4.1", "SSP3 - High Emissions"),
        ("SSP4", "SSP_SSP4_v2.3.1", "baseline_1000f"),
        ("SSP4", "SSP_SSP4_v2.3.1", "baseline"),
        ("SSP4", "SSP_SSP4_v2.3.1", "SSP4 - Low Overshoot"),
        ("SSP5", "SSP_SSP5_v2.4.1", "baseline_1000f"),
        ("SSP5", "SSP_SSP5_v2.4.1", "SSP5 - High Emissions"),
        ("SSP5", "SSP_SSP5_v2.4.1", "SSP5 - Low Overshoot"),
    ],
)
def test_get_scenario_code(expected_id, model_name, scenario_name) -> None:
    result = get_scenario_code(model_name, scenario_name)
    assert expected_id == result.id


@get_computer.minimum_version
@pytest.mark.usefixtures("iea_eweb_test_data", "ssp_user_data")
@pytest.mark.parametrize("method", METHOD_PARAM)
def test_process_df(test_context, input_csv_path, method) -> None:
    # - Read input data
    # - Replace some 0 values with np.nan to replicate conditions in calling code.
    df_in = pd.read_csv(input_csv_path).pipe(
        insert_nans,
        r"Emissions\|.*\|International Aviation",
        lambda c: str(c).isnumeric() and int(c) >= 2020,
    )

    # Code runs
    df_out = process_df(df_in, method=method)

    # Output satisfies expectations
    check(df_in, df_out, method)


@get_computer.minimum_version
@pytest.mark.usefixtures("iea_eweb_test_data", "ssp_user_data")
@pytest.mark.parametrize("method", METHOD_PARAM)
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
