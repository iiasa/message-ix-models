"""Tools for working with IAMC-structured data."""

from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Literal, Optional

import genno
import pandas as pd
from sdmx.message import StructureMessage
from sdmx.model import common, v21

from message_ix_models.util import cached
from message_ix_models.util.pycountry import iso_3166_alpha_3

if TYPE_CHECKING:
    import pathlib

    from genno.types import AnyQuantity

__all__ = [
    "describe",
    "iamc_like_data_for_query",
]


def describe(data: pd.DataFrame, extra: Optional[str] = None) -> StructureMessage:
    """Generate SDMX structure information from `data` in IAMC format.

    Parameters
    ----------
    data :
        Data in "wide" or "long" IAMC format.
    extra : str, optional
        Extra text added to the description of each Codelist.

    Returns
    -------
    sdmx.message.StructureMessage
        The message contains one :class:`.Codelist` for each of the MODEL, SCENARIO,
        REGION, VARIABLE, and UNIT dimensions. Codes for the VARIABLE code list have
        annotations with :py:`id="preferred-unit-measure"` that give the corresponding
        UNIT Code(s) that appear with each VARIABLE.
    """

    sm = StructureMessage()

    def _cl(dim: str) -> common.Codelist:
        result: common.Codelist = common.Codelist(
            id=dim,
            description=f"Codes appearing in the {dim!r} dimension of "
            + (extra or "data")
            + ".",
            is_final=True,
            is_external_reference=False,
        )
        sm.add(result)
        return result

    for dim in ("MODEL", "SCENARIO", "REGION"):
        cl = _cl(dim)
        for value in sorted(data[dim].unique()):
            cl.append(common.Code(id=value))

    # Handle "VARIABLE" and "UNIT" jointly
    dims = ["VARIABLE", "UNIT"]
    cl_variable = _cl("VARIABLE")
    cl_unit = _cl("UNIT")
    for variable, group_data in (
        data[dims].sort_values(dims).drop_duplicates().groupby("VARIABLE")
    ):
        group_units = group_data["UNIT"].unique()
        cl_variable.append(
            common.Code(
                id=variable,
                annotations=[
                    v21.Annotation(
                        id="preferred-unit-measure", text=", ".join(group_units)
                    )
                ],
            )
        )
        for unit in group_units:
            try:
                cl_unit.append(common.Code(id=unit))
            except ValueError:
                pass

    return sm


def _assign_n(df: pd.DataFrame, *, missing: Literal["keep", "discard"]) -> pd.DataFrame:
    if missing == "discard":
        return df.assign(n=df["REGION"].apply(iso_3166_alpha_3))
    else:
        return df.assign(n=df["REGION"].apply(lambda v: iso_3166_alpha_3(v) or v))


def _drop_unique(
    df: pd.DataFrame, *, columns: str, record: MutableMapping[str, Any]
) -> pd.DataFrame:
    """Drop `columns` so long as they each contain a single, unique value."""
    _columns = columns.split()
    for column in _columns:
        values = df[column].unique()
        if len(values) > 1:
            raise RuntimeError(f"Not unique {column!r}: {values}")
        record[column] = values[0]
    return df.drop(_columns, axis=1)


def _raise_empty(df: pd.DataFrame, *, query: str) -> pd.DataFrame:
    if len(df) == 0:
        raise RuntimeError(f"0 rows matching {query!r}")
    return df


@cached
def iamc_like_data_for_query(
    path: "pathlib.Path",
    query: str,
    *,
    archive_member: Optional[str] = None,
    drop: Optional[list[str]] = None,
    non_iso_3166: Literal["keep", "discard"] = "discard",
    replace: Optional[dict] = None,
    unique: str = "MODEL SCENARIO VARIABLE UNIT",
    **kwargs,
) -> "AnyQuantity":
    """Load data from `path` in an IAMC-like format and transform to :class:`.Quantity`.

    The steps involved are:

    1. Read the data file. Additional `kwargs` are passed to :func:`pandas.read_csv`.
       By default (unless `kwargs` explicitly give a different value), pyarrow is used
       for better performance.
    2. Pass the result through :func:`to_quantity`, with the parameters `query`,
       `drop`, `non_iso_3166`, `replace`, and `unique`.
    3. Cache the result using :obj:`.cached`. Subsequent calls with the same arguments
       will yield the cached result rather than repeating steps (1) and (2).

    Parameters
    ----------
    archive_member : bool, optional
        If given, `path` may be a tar or ZIP archive with 1 or more members. The member
        named by `archive_member` is extracted and read using :class:`tarfile.TarFile`
        or :class:`zipfile.ZipFile`.

    Returns
    -------
    genno.Quantity
        of the same structure returned by :func:`to_quantity`.
    """
    import pandas as pd

    # Identify the source object/buffer to read from
    if archive_member:
        if path.suffix.rpartition(".")[2] in ("gz", "xz"):
            # A single member in an LZMA-compressed tar archive that has ≥1 members
            import tarfile

            tf = tarfile.open(path, mode="r:*")
            source: Any = tf.extractfile(archive_member)
        else:
            # A single member in a ZIP archive that has ≥1 members
            import zipfile

            zf = zipfile.ZipFile(path)
            source = zf.open(archive_member)
    else:
        # A direct path, possibly compressed
        source = path

    kwargs.setdefault("engine", "pyarrow")

    return to_quantity(
        pd.read_csv(source, **kwargs),
        query=query,
        drop=drop,
        non_iso_3166=non_iso_3166,
        replace=replace,
        unique=unique,
    )


def to_quantity(
    data: "pd.DataFrame",
    *,
    query: str,
    drop: Optional[list[str]] = None,
    non_iso_3166: Literal["keep", "discard"] = "discard",
    replace: Optional[dict] = None,
    unique: str = "MODEL SCENARIO VARIABLE UNIT",
) -> "AnyQuantity":
    """Convert `data` in IAMC ‘wide’ structure to :class:`genno.Quantity`.

    `data` is processed via the following steps:

     1. Drop columns given in `drop`, if any.
     2. Apply `query`. This is done early to reduce the data handled in subsequent
        steps. The query string must use the original column names (with matching case)
        as appearing in `data` (or, for :func:`iamc_like_data_for_query`, in the file at
        `path`).
     3. Apply replacements from `replace`, if any.
     4. Drop columns that are entirely empty.
     5. Rename all columns/dimensions to upper case.
     6. Assert that the `unique` columns each contain exactly 1 unique value, then
        drop these columns. This means that `query` **must** result in data with unique
        values for these dimensions.
     7. Transform "REGION" codes via :func:`.iso_3166_alpha_3` to an "n" dimension
        containing ISO 3166-1 alpha-3 codes. If `non_iso_3166`, preserve codes that do
        not appear in the standard.
     8. Drop entire time series where (7) does not yield an "n" code.
     9. Transform to :class:`pandas.Series` with "n" and "y" index levels; ensure the
        latter are :class:`int`.
    10. Transform to :class:`.Quantity` and attach units.

    Parameters
    ----------
    data :
        Data frame in IAMC ‘wide’ format. The column names "Model", "Scenario",
        "Region", "Variable", and "Unit" may be in any case.
    query :
        Query to select a subset of data, passed to :meth:`pandas.DataFrame.query`.
    drop :
        Identifiers of columns in `data`, passed to :meth:`pandas.DataFrame.drop`.
    non_iso_3166 :
        If "discard" (default), "region" labels that are not ISO 3166-1 country names
        are discarded, along with associated data. If "keep", such labels are kept.
    replace :
        Replacements for values in columns, passed to :meth:`pandas.DataFrame.replace`.
    unique :
        Columns which must contain unique values. These columns are dropped from the
        result.

    Returns
    -------
    genno.Quantity
        with at least dimensions :py:`("n", "y")`, and then a subset of :py:`("MODEL",
        "SCENARIO", "VARIABLE", "UNIT")`—only those dimensions *not* indicated by
        `unique`. If "UNIT" is in `unique`, the quantity has the given, unique units;
        otherwise, it is dimensionless.
    """
    set_index = ["n"] + sorted(
        set(["MODEL", "SCENARIO", "VARIABLE", "UNIT"]) - set(unique.split())
    )

    unique_values: dict[str, Any] = dict()
    tmp = (
        data.drop(columns=drop or [])
        .query(query)
        .replace(replace or {})
        .dropna(how="all", axis=1)
        .rename(columns=lambda c: c.upper())
        .pipe(_raise_empty, query=query)
        .pipe(_drop_unique, columns=unique, record=unique_values)
        .pipe(_assign_n, missing=non_iso_3166)
        .dropna(subset=["n"])
        .drop("REGION", axis=1)
        .set_index(set_index)
        .rename(columns=lambda y: int(y))
        .rename_axis(columns="y")
        .stack()
        .dropna()
    )
    return genno.Quantity(
        tmp, units=unique_values["UNIT"] if "UNIT" in unique else "dimensionless"
    )
