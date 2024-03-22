from pathlib import Path
from typing import Optional

import pandas as pd

from .iamc_tree import sum_iamc_sectors
from .util import EmissionsAggregator


class pp_utils:
    all_years: Optional[list[int]] = None
    years: Optional[list[int]] = None
    firstmodelyear: Optional[int] = None
    all_tecs: Optional[pd.Series] = None
    regions: Optional[dict[str, str]] = None
    region_id: Optional[str] = None
    verbose: bool = False
    globalname: Optional[str] = None
    model_nm: Optional[str] = None
    scen_nm: Optional[str] = None
    unit_conversion = None


def write_xlsx(df: pd.DataFrame, path: Path, model_name: str, scen_name: str):
    """Writes final results dataframe to xlsx file.

    Parameters
    ----------
    df : pd.DataFrame
    path : Path
        path to where the xlsx file should be written

    """

    out_path = path / f"{model_name}_{scen_name}.xlsx"
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="data", index=False)


def numcols(df: pd.DataFrame) -> list[str]:
    """Retrieves numeric columns of a dataframe.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    list : list[str]
        column names of numeric columns
    """
    return [x for x in df.columns if pd.api.types.is_numeric_dtype(df[x])]


def iamc_it(
    df: pd.DataFrame, what: str, mapping: dict, rm_totals: bool = False
) -> pd.DataFrame:
    """Creates any missing parent variables if these are missing and
    adds a pre-fix to the variable name.

    Variable parent/child relationships are determined by separating
    the variable name by "|".

    Parameters
    ----------
    df : dataframe
    what : string
        variable pre-fix
    mapping : dictionary
        contains the variable tree aggregate settings
    rm_totals : True/False (optional, default = False)
        if True then all totals a removed and recalculated.


    Returns
    -------
    df : pd.DataFrame
    """

    root = what
    df = sum_iamc_sectors(df, root=root, mode="Add", cleanname=False).reset_index()
    if rm_totals:
        df = df[df.Variable != what]
    df = EmissionsAggregator(df, mapping=mapping).add_variables().df
    return df.sort_index()
