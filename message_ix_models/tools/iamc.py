"""Tools for working with IAMC-structured data."""

from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Literal, Optional

import genno
import pandas as pd
import sdmx.model.v21 as m
from sdmx.message import StructureMessage

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

    def _cl(dim: str) -> m.Codelist:
        result = m.Codelist(
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
            cl.append(m.Code(id=value))

    # Handle "VARIABLE" and "UNIT" jointly
    dims = ["VARIABLE", "UNIT"]
    cl_variable = _cl("VARIABLE")
    cl_unit = _cl("UNIT")
    for variable, group_data in (
        data[dims].sort_values(dims).drop_duplicates().groupby("VARIABLE")
    ):
        group_units = group_data["UNIT"].unique()
        cl_variable.append(
            m.Code(
                id=variable,
                annotations=[
                    m.Annotation(
                        id="preferred-unit-measure", text=", ".join(group_units)
                    )
                ],
            )
        )
        for unit in group_units:
            try:
                cl_unit.append(m.Code(id=unit))
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
    """Load data from `path` in IAMC-like format and transform to :class:`.Quantity`.

    The steps involved are:

    1. Read the data file; use pyarrow for better performance.
    2. Immediately apply `query` to reduce the data to be handled in subsequent steps.
    3. Assert that Model, Scenario, Variable, and Unit are unique; store the unique
       values. This means that `query` **must** result in data with unique values for
       these dimensions.
    4. Transform "Region" labels to ISO 3166-1 alpha-3 codes using
       :func:`.iso_3166_alpha_3`.
    5. Drop entire time series without such codes; for instance "World".
    6. Transform to a pd.Series with "n" and "y" index levels; ensure the latter are
       int.
    7. Transform to :class:`.Quantity` with units.

    The result is :obj:`.cached`.

    Parameters
    ----------
    archive_member : bool, optional
        If given, `path` may be an archive with 2 or more members. The member named by
        `archive_member` is extracted and read.
    non_iso_3166 : bool, optional
        If "discard" (default), "region" labels that are not ISO 3166-1 country names
        are discarded, along with associated data. If "keep", such labels are kept.
    """
    import pandas as pd

    # Identify the source object/buffer to read from
    if archive_member:
        # A single member in a ZIP archive that has >1 members
        import zipfile

        zf = zipfile.ZipFile(path)
        source: Any = zf.open(archive_member)
    else:
        # A direct path, possibly compressed
        source = path

    kwargs.setdefault("engine", "pyarrow")
    set_index = ["n"] + sorted(
        set(["MODEL", "SCENARIO", "VARIABLE", "UNIT"]) - set(unique.split())
    )

    unique_values: dict[str, Any] = dict()
    tmp = (
        pd.read_csv(source, **kwargs)
        .drop(columns=drop or [])
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
