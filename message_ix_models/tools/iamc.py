"""Tools for working with IAMC-structured data."""
from typing import Optional

import pandas as pd
import sdmx.model.v21 as m
from sdmx.message import StructureMessage


def describe(data: pd.DataFrame, extra: Optional[str] = None):
    """Generate code lists from `data` in IAMC format."""

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
