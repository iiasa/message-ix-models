"""Tools for working with IAMC-structured data."""

from typing import Optional

import pandas as pd
import sdmx.model.v21 as m
from sdmx.message import StructureMessage


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
